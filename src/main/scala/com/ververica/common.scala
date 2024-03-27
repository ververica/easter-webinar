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

import org.apache.flink.configuration.{Configuration, ConfigConstants}
import org.apache.flink.configuration.RestOptions.BIND_PORT
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

import DeliveryEvent.BunnyLocation

import scala.jdk.CollectionConverters.*
import scala.util.Random
import scala.concurrent.duration.*
import java.util.Properties

object KafkaSettings:
  val orderTopic = "orders"
  val bunnyLocationTopic = "bunny_location"
  val routeTopic = "location_route"

  val defaultBroker = "kafka:9092"

  def locationSource(brokers: String, topic: String, props: Properties) =
    KafkaSource
      .builder[String]()
      .setBootstrapServers(brokers)
      .setTopics(topic)
      .setProperties(props)
      .setValueOnlyDeserializer(SimpleStringSchema())
      .build

object FlinkSettings:
  def config(props: (String, String)*) = Configuration.fromMap(
    (Map(
      BIND_PORT.key -> "8081",
      "execution.checkpointing.interval" -> "5 s"
    ) ++ props).asJava
  )

object Delays:
  val rand = Random()

  def deliveryDelay(distance: Int): Long =
    rand
      .between(
        distance,
        2 * distance
      )
      .second
      .toMillis

class OrderSerializationSchema(topicName: String)
    extends KafkaRecordSerializationSchema[Order]:

  val logger = LoggerFactory.getLogger(this.getClass())

  override def serialize(
      order: Order,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: java.lang.Long
  ): ProducerRecord[Array[Byte], Array[Byte]] =
    ProducerRecord(
      topicName,
      order.idAsJson.getBytes,
      order.toJson.getBytes
    )

class OrderDeserializationScheme
    extends KafkaRecordDeserializationSchema[Order]:

  override def getProducedType(): TypeInformation[Order] =
    Order.orderTypeInfo

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[Order]
  ): Unit =
    out.collect(Order.fromJson(String(record.value())))

class BunnySerializationSchema(topicName: String)
    extends KafkaRecordSerializationSchema[BunnyLocation]:

  override def serialize(
      bunny: BunnyLocation,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: java.lang.Long
  ): ProducerRecord[Array[Byte], Array[Byte]] =
    ProducerRecord(
      topicName,
      bunny.idAsJson.getBytes,
      bunny.toJson.getBytes
    )
