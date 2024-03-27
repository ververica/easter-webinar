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

import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.configuration.{Configuration, ConfigConstants}
import org.apache.flink.configuration.RestOptions.BIND_PORT
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import upickle.default.ReadWriter
import org.slf4j.LoggerFactory

import DeliveryEvent.*
import KafkaSettings.*
import FlinkSettings.*

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters.*

object OrderDispatcher:
  val logger = LoggerFactory.getLogger(this.getClass())

  def main(args: Array[String]): Unit =
    val brokers = args.headOption.getOrElse(defaultBroker)
    val topic = if args.length > 1 then args(1) else orderTopic
    val locationsTopic = if args.length > 2 then Some(args(2)) else None
    val routesTopic = if args.length > 3 then Some(args(3)) else None
    val maxLoad =
      if args.length > 4 then args(4).toIntOption.getOrElse(2) else 2
    val kafkaProps = Properties()

    if brokers.startsWith("my-kafka.svc:9092") then
      kafkaProps.setProperty("security.protocol", "SASL_PLAINTEXT")
      kafkaProps.setProperty("sasl.mechanism", "SCRAM-SHA-256")

    kafkaProps.setProperty("enable.auto.commit", "true")
    kafkaProps.setProperty("auto.commit.interval.ms", "1000")
    kafkaProps.setProperty("commit.offsets.on.checkpoint", "true")
    kafkaProps.setProperty("partition.discovery.interval.ms", "10000")
    kafkaProps.setProperty("client.id", "order-dispatcher-client")
    kafkaProps.setProperty("isolation.level", "read_uncommitted")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.enableObjectReuse()
    // .createLocalEnvironmentWithWebUI(
    //   config("taskmanager.numberOfTaskSlots" -> "50", BIND_PORT.key -> "8082")
    // )

    val orderSource = KafkaSource
      .builder[Order]()
      .setBootstrapServers(brokers)
      .setTopics(topic)
      .setGroupId("order-dispatcher")
      .setStartingOffsets(
        OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
      )
      .setProperties(kafkaProps)
      .setDeserializer(OrderDeserializationScheme())
      .build

    val sink =
      KafkaSink
        .builder[Order]()
        .setBootstrapServers(brokers)
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setRecordSerializer(
          OrderSerializationSchema(topic)
        )
        .setKafkaProducerConfig(kafkaProps)
        .build()

    val bunniesSink =
      KafkaSink
        .builder[BunnyLocation]()
        .setBootstrapServers(brokers)
        .setRecordSerializer(
          BunnySerializationSchema(locationsTopic.getOrElse(bunnyLocationTopic))
        )
        .setKafkaProducerConfig(kafkaProps)
        .build()

    // val watermarkStrategy = WatermarkStrategy
    //   .forBoundedOutOfOrderness[Order](
    //     Duration.ofSeconds(5)
    //   )
    //   .withTimestampAssigner((order, timestamp) => order.eventTime)

    val orderStream = env
      .fromSource(orderSource, WatermarkStrategy.noWatermarks(), "Order Source")
      .filter(o =>
        o.status == OrderStatus.EggIsReady || o.status == OrderStatus.Delivered
      )
      .keyBy(_.deliveryLocation)

    val locationSource = KafkaSource
      .builder[String]()
      .setBootstrapServers(brokers)
      .setTopics(locationsTopic.getOrElse(bunnyLocationTopic))
      .setGroupId("order-dispatcher-location")
      .setStartingOffsets(
        OffsetsInitializer.earliest()
      )
      .setValueOnlyDeserializer(SimpleStringSchema())
      .setProperties(kafkaProps)
      .build

    val locationStream = env
      .fromSource(
        locationSource,
        WatermarkStrategy.noWatermarks(),
        "Location Source"
      )
      .map[DeliveryEvent] { l =>
        logger.info(s"parsing bunny location: $l")
        BunnyLocation.fromJson(l)
      }

    val routeSource = KafkaSource
      .builder[String]()
      .setBootstrapServers(brokers)
      .setTopics(routesTopic.getOrElse(routeTopic))
      .setGroupId("order-dispatcher-route")
      .setStartingOffsets(
        OffsetsInitializer.earliest()
      )
      .setProperties(kafkaProps)
      .setValueOnlyDeserializer(SimpleStringSchema())
      .build

    val routesStream = env
      .fromSource(
        routeSource,
        WatermarkStrategy.noWatermarks(),
        "Route Source"
      )
      .map[DeliveryEvent] { r =>
        logger.info(s"parsing city route location: $r")
        CityRoute.fromJson(r)
      }

    val deliveryStream = locationStream
      .union(routesStream)
      .broadcast(DispatcherFunc.routeStateDesc, DispatcherFunc.bunniesStateDesc)

    val outputTag = OutputTag[BunnyLocation]("bunny-location-output")

    val dispatcherStream = orderStream
      .connect(deliveryStream)
      .process(DispatcherFunc(outputTag, maxLoad))

    dispatcherStream
      .getSideOutput(outputTag)
      .sinkTo(bunniesSink)

    dispatcherStream.sinkTo(sink)

    env.execute("Easter Egg Dispatcher")
