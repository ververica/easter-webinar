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

import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.configuration.RestOptions.BIND_PORT
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema

import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.slf4j.LoggerFactory

import FlinkSettings.*
import KafkaSettings.*
import DeliveryEvent.BunnyLocation

import java.time.Duration
import java.util.Properties
import scala.concurrent.duration.*
import scala.util.Random

object DeliveryService:
  val logger = LoggerFactory.getLogger(this.getClass())

  def main(args: Array[String]): Unit =
    val brokers = args.headOption.getOrElse(defaultBroker)
    val topic = if args.length > 1 then Some(args(1)) else None
    val kafkaProps = Properties()

    if brokers.startsWith("my-kafka.svc:9092") then
      kafkaProps.setProperty("security.protocol", "SASL_PLAINTEXT")
      kafkaProps.setProperty("sasl.mechanism", "SCRAM-SHA-256")

    kafkaProps.setProperty("partition.discovery.interval.ms", "10000")

    val env =
      StreamExecutionEnvironment.getExecutionEnvironment // createLocalEnvironmentWithWebUI(
        //   config(BIND_PORT.key -> "8083")
        // )
    val deliveryDelaySeconds = 5

    val source = KafkaSource
      .builder[Order]()
      .setBootstrapServers(brokers)
      .setTopics(topic.getOrElse(orderTopic))
      .setGroupId("delivery-service")
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
        .setRecordSerializer(
          OrderSerializationSchema(topic.getOrElse(orderTopic))
        )
        .setKafkaProducerConfig(kafkaProps)
        .build()

    env
      .fromSource(source, WatermarkStrategy.noWatermarks(), "Order Source")
      .filter(_.status == OrderStatus.Shipped)
      .keyBy(_.deliveryLocation)
      .window(
        TumblingProcessingTimeWindows.of(Time.seconds(deliveryDelaySeconds))
      )
      .apply[Order] { (location, window, orders, collector) =>
        orders
          .map { o =>            
            o.copy(
              eventTime = o.eventTime + Delays.deliveryDelay(o.distance),
              status = OrderStatus.Delivered
            )
          }
          .foreach(collector.collect)
      }
      .sinkTo(sink)

    env.execute("Easter Delivery Service")
