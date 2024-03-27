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
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*
import scala.util.Random
import scala.jdk.CollectionConverters.*
import java.time.Duration
import java.util.Properties

import KafkaSettings.*
import FlinkSettings.*

object EggFactory:
  val logger = LoggerFactory.getLogger(this.getClass())

  def main(args: Array[String]): Unit =
    val brokers = args.headOption.getOrElse(defaultBroker)
    val topic = if args.length > 1 then args(1) else orderTopic
    val kafkaProps = Properties()

    if brokers.startsWith("my-kafka.svc:9092") then
      kafkaProps.setProperty("security.protocol", "SASL_PLAINTEXT")
      kafkaProps.setProperty("sasl.mechanism", "SCRAM-SHA-256")

    kafkaProps.setProperty("enable.auto.commit", "true")
    kafkaProps.setProperty("auto.commit.interval.ms", "1000")
    kafkaProps.setProperty("commit.offsets.on.checkpoint", "true")
    kafkaProps.setProperty("partition.discovery.interval.ms", "10000")
    kafkaProps.setProperty("client.id", "egg-factory-consumer-producer")
    kafkaProps.setProperty("isolation.level", "read_uncommitted")

    val env =
      StreamExecutionEnvironment.getExecutionEnvironment // createLocalEnvironmentWithWebUI(config())        

    logger.info(s"Kafka topic for source and sink: $topic")
    val source = KafkaSource
      .builder[String]()
      .setBootstrapServers(brokers)
      .setTopics(topic)
      .setGroupId("egg-factory-service")
      .setStartingOffsets(
        OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
      )
      .setProperties(kafkaProps)
      .setValueOnlyDeserializer(SimpleStringSchema())
      .build

    val productionDelayseconds = 5

    val rand = Random()

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

    env
      .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
      .map(Order.fromJson)
      .filter(_.status == OrderStatus.New)
      .windowAll(
        TumblingProcessingTimeWindows.of(Time.seconds(productionDelayseconds))
      )
      .apply[Order] { (timeWindow, orders, collector) =>
        logger.info(s"Processing window for ${timeWindow.toString}")
        orders
          .map(o =>
            o.copy(
              eventTime = o.eventTime + rand
                .between(
                  productionDelayseconds,
                  2 * productionDelayseconds
                )
                .seconds
                .toMillis,
              status = OrderStatus.EggIsReady
            )
          )
          .foreach(collector.collect)
      }
      .sinkTo(sink)

    env.execute("Easter Egg Factory")
