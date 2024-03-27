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
import org.apache.flinkx.api.serializer.MappedSerializer.TypeMapper
import org.apache.flink.api.common.typeinfo.TypeInformation

import upickle.default.{ReadWriter, macroRW, *}

import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import java.time.ZoneOffset
import java.time.LocalDateTime
import java.sql.Timestamp

enum OrderStatus derives ReadWriter:
  case New, EggIsReady, Shipped, Delivered

class OrderStatusMapper extends TypeMapper[OrderStatus, String]:
  override def map(a: OrderStatus): String = a.toString

  override def contramap(b: String): OrderStatus =
    OrderStatus.valueOf(b)

object OrderStatus:
  given mapper: TypeMapper[OrderStatus, String] = OrderStatusMapper()
  given orderStatusTypeInfo: TypeInformation[OrderStatus] =
    mappedTypeInfo[OrderStatus, String]

sealed trait DeliveryEvent extends Product with Serializable:
  def key: String

object DeliveryEvent:
  case class CityRoute(location: String, route: List[String])
      extends DeliveryEvent derives ReadWriter:
    override def key: String = location

  object CityRoute:
    def fromJson(s: String): CityRoute =
      val route = s.replace(
        "{",
        s"""{"$$type": "${classOf[CityRoute].getCanonicalName}","""
      )
      upickle.default.read[CityRoute](route)

  case class BunnyLocation(
      bunnyName: String,
      location: String,
      eventTime: EventTime
  ) extends DeliveryEvent
      derives ReadWriter:
    def key: String = location

    def notAssigned: Boolean =
      location == null || location.isBlank()

    def toDeallocated: BunnyLocation = 
      copy(location = "")  

  object BunnyLocation:
    import EventTime.given
    given bunnyLocationTypeInfo: TypeInformation[BunnyLocation] =
      deriveTypeInformation

    def fromJson(s: String): BunnyLocation =
      val location = s.replace(
        "{",
        s"""{"$$type": "${classOf[BunnyLocation].getCanonicalName}","""
      )

      upickle.default.read[BunnyLocation](location)

    extension (b: BunnyLocation)
      def toJson: String =
        upickle.default.write[BunnyLocation](b)

      def idAsJson: String =
        upickle.default.write(Map("bunnyName" -> b.bunnyName))

  given eventTypeInfo: TypeInformation[DeliveryEvent] =
    deriveTypeInformation

type EventTime = Long

object EventTime:
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  given eventTimeRW: ReadWriter[EventTime] =
    readwriter[String].bimap[EventTime](
      eventTime =>
        formatter
          .format(Timestamp(eventTime).toLocalDateTime),
      str => {
        val datetime =
          LocalDateTime.parse(str.padTo(20, '.').padTo(23, '0'), formatter)
        Timestamp.valueOf(datetime).getTime
      }
    )

case class Order(
    id: String,
    deliveryLocation: String,
    eventTime: EventTime,
    status: OrderStatus,
    bunny: Option[String] = None,
    distance: Int = 0
) derives ReadWriter

object Order:
  import EventTime.given
  given orderTypeInfo: TypeInformation[Order] = deriveTypeInformation

  def fromJson(s: String): Order =
    upickle.default.read[Order](String(s))

  extension (o: Order)
    def idAsJson: String =
      upickle.default.write(Map("id" -> o.id))

    def toJson: String =
      upickle.default.write[Order](o)
