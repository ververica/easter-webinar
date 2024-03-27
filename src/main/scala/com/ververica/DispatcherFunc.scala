/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ververica

import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.OutputTag

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.state.ReadOnlyBroadcastState
import org.apache.flink.runtime.state.KeyedStateFunction

import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*
import scala.util.Random
import org.slf4j.LoggerFactory

import DeliveryEvent.*
import DispatcherFunc.*

object DispatcherFunc:
  val routeStateDesc = MapStateDescriptor("route", stringInfo, listInfo[String])
  val bunniesStateDesc = MapStateDescriptor(
    "bunnies",
    stringInfo,
    BunnyLocation.bunnyLocationTypeInfo
  )
  val pendingOrdersDesc =
    ListStateDescriptor("pending-orders", Order.orderTypeInfo)

  case class StreamEnv(
      routes: ReadOnlyBroadcastState[String, List[String]],
      bunnies: ReadOnlyBroadcastState[String, BunnyLocation],
      ctx: Option[ROContext] = None
  )

  type ROContext = KeyedBroadcastProcessFunction[
    String,
    Order,
    DeliveryEvent,
    Order
  ]#ReadOnlyContext

class DispatcherFunc(
    bunnyLocationOutput: OutputTag[BunnyLocation],
    maxLoad: Long = 2
) extends KeyedBroadcastProcessFunction[String, Order, DeliveryEvent, Order]:

  @transient private val orderRetryInterval = 5000L
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass())

  var locationLoad: MapState[String, Long] = _
  var allocatedBunnies: ListState[BunnyLocation] = _
  var pendingOrders: ListState[Order] = _

  override def open(parameters: Configuration): Unit =
    locationLoad = getRuntimeContext.getMapState(
      MapStateDescriptor(
        "location-load",
        stringInfo,
        longInfo
      )
    )
    allocatedBunnies = getRuntimeContext.getListState(
      ListStateDescriptor(
        "allocated-bunnies",
        BunnyLocation.bunnyLocationTypeInfo
      )
    )
    pendingOrders = getRuntimeContext
      .getListState(pendingOrdersDesc)

  override def processElement(
      order: Order,
      ctx: ROContext,
      out: Collector[Order]
  ): Unit =
    val streamEnv = StreamEnv(
      ctx.getBroadcastState(routeStateDesc),
      ctx.getBroadcastState(bunniesStateDesc),
      Some(ctx)
    )

    val wait = processOrder(
      order,
      b =>
        ctx.output(
          bunnyLocationOutput,
          b
        ),
      out,
      streamEnv
    )
    if wait then
      logger.info(s"Buffering new order: $order")
      pendingOrders
        .add(
          order
        )

  private def processOrder(
      order: Order,
      bunnyOutput: (BunnyLocation) => Unit,
      out: Collector[Order],
      streamEnv: StreamEnv
  ): Boolean =
    order.status match
      case OrderStatus.EggIsReady =>
        processReadyOrder(order, bunnyOutput, out, streamEnv)
      case OrderStatus.Delivered =>
        processDeliveredOrder(order)
        false
      case _ =>
        logger.warn(s"Unexpected order staus: ${order.status}")
        false

  private def processDeliveredOrder(order: Order) =
    val load = Option(locationLoad.get(order.deliveryLocation)).getOrElse(0L)
    val newLoad = Math.max(0, load - 1)
    logger.info(
      s"Decreasing traffic load for ${order.deliveryLocation} to $newLoad"
    )
    locationLoad.put(order.deliveryLocation, newLoad)
    false

  private def processReadyOrder(
      order: Order,
      bunnyOutput: (BunnyLocation) => Unit,
      out: Collector[Order],
      streamEnv: StreamEnv
  ) =
    val route = streamEnv.routes.contains(order.deliveryLocation)

    if route then
      val load =
        Option(locationLoad.get(order.deliveryLocation)).getOrElse(0L)
      val bunnies = streamEnv.bunnies.immutableEntries().asScala

      lazy val freeBunny =
        Random.shuffle(bunnies.toList).collectFirst {
          case b
              if b
                .getValue()
                .notAssigned =>
            b.getValue
        }
      if load < maxLoad then
        freeBunny match
          case Some(bunny) =>
            // 1 - Increase Traffic per location
            locationLoad.put(order.deliveryLocation, load + 1)

            // 2 - Allocate Bunny
            val allocatedBunny = bunny.copy(location = order.deliveryLocation)
            // bunnies.put(b.bunnyName, allocatedBunny) // TODO: How to not allocate the same bunny from a different Flink task ?
            bunnyOutput(allocatedBunny)
            allocatedBunnies.add(allocatedBunny)

            // 3 - Ship Order
            val shipingOrder = order.copy(
              eventTime = order.eventTime + 1, // simulating delay
              status = OrderStatus.Shipped,
              bunny = Some(allocatedBunny.bunnyName),
              distance = streamEnv.routes.get(order.deliveryLocation).size
            )
            out.collect(shipingOrder)
            logger.info(
              s"Order shipped, id = ${order.id}, bunny = ${bunny.bunnyName}, delivery location = ${order.deliveryLocation}"
            )

            // 4 - Deallocate Bunny by scheduling a timer
            // TODO: if bunny is not taken by any non-Delivered order
            streamEnv.ctx.foreach(c =>
              c.timerService()
                .registerProcessingTimeTimer(
                  c.timerService().currentProcessingTime() +
                    Delays.deliveryDelay(shipingOrder.distance)
                )
            )
            false
          case _ =>
            logger.info("No free bunny found")
            true
      else
        logger.info(
          s"Location ${order.deliveryLocation} is loaded to max possible value $load"
        )
        true
    else
      logger.info(
        s"No route for order location: ${order.deliveryLocation}"
      )
      true

  private def applyToKeyedState(
      ctx: KeyedBroadcastProcessFunction[
        String,
        Order,
        DeliveryEvent,
        Order
      ]#Context,
      out: Collector[Order]
  ) = new KeyedStateFunction[String, ListState[Order]]:

    override def process(key: String, state: ListState[Order]): Unit =
      retryPendingOrders(
        Some(key),
        state,
        out,
        b =>
          ctx.output(
            bunnyLocationOutput,
            b
          ),
        StreamEnv(
          ctx.getBroadcastState(routeStateDesc),
          ctx.getBroadcastState(bunniesStateDesc)
        )
      )

  private def retryPendingOrders(
      location: Option[String],
      state: ListState[Order],
      out: Collector[Order],
      bunnyOutput: (BunnyLocation) => Unit,
      streamEnv: StreamEnv
  ) =
    val orders = state.get().asScala.toList
    if orders.size > 0 then
      logger.info(
        s"Retrying pending ${orders.size} orders ${location.map(l => s"at location $l").getOrElse("")}"
      )

    val remaining = orders
      .filter(o =>
        processOrder(
          o,
          bunnyOutput,
          out,
          streamEnv
        )
      )
      .asJava
    state.update(remaining)

  override def processBroadcastElement(
      event: DeliveryEvent,
      ctx: KeyedBroadcastProcessFunction[
        String,
        Order,
        DeliveryEvent,
        Order
      ]#Context,
      out: Collector[Order]
  ): Unit =
    val orders = event match
      case CityRoute(locationName, route) =>
        logger.info(s"updating route at location = $locationName")
        ctx.getBroadcastState(routeStateDesc).put(locationName, route)

        ctx.applyToKeyedState(
          pendingOrdersDesc,
          applyToKeyedState(ctx, out)
        )

      case b @ BunnyLocation(bunnyName, location, eventTime) =>
        logger.info(
          s"updating bunny = $bunnyName, location = $location"
        )
        ctx.getBroadcastState(bunniesStateDesc).put(bunnyName, b)
        ctx.applyToKeyedState(
          pendingOrdersDesc,
          applyToKeyedState(ctx, out)
        )

  override def onTimer(
      timestamp: Long,
      ctx: KeyedBroadcastProcessFunction[
        String,
        Order,
        DeliveryEvent,
        Order
      ]#OnTimerContext,
      out: Collector[Order]
  ): Unit =
    // TODO: maybe this needs to be done by DeliveryService
    allocatedBunnies
      .get()
      .forEach { b =>
        logger.info(s"Deallocating bunny ${b.bunnyName}")
        ctx.output(bunnyLocationOutput, b.toDeallocated)
      }
    allocatedBunnies.update(List.empty[BunnyLocation].asJava)
