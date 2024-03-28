# Hop to it! Innovating Easter Egg Delivery with Ververica Cloud

![Easter Egg Delivery](screens/delivery-monitoring.png "Easter Egg Delivery")

This repository contains an example of Egg Delivery solution:

1. Flink SQL code for data preparation and monitoring
2. Flink DataStream Scala code for core solution
3. SQL DDL code for MySQL database

To build a JAR with all Flink jobs run:

```bash
sbt assembly
```

Once executed, find a JAR file in the target/scala folder.

## Main Arguments

_com.ververica.EggFactory_ arguments:
1. String argument with a comma-separated list of Kafka brokers. For example:
"my-kafka-1.kafka:9092,my-kafka-2.kafka:9092"
2. String argument with Kafka topic name for Egg orders, For example:
"orders"

_com.ververica.OrderDispatcher_ arguments:
1. String argument with a comma-separated list of Kafka brokers
2. String argument with Kafka topic name for Egg orders, For example:
"orders"
3. String argument with Kafka topic name for bunny locations, For example:
"bunny_location"
4. String argument with Kafka topic name for city routes, For example:
"location_route"
5. Numeric argument with maximum load allowed per bunny location. For example:
4

_com.ververica.DeliveryService_ arguments:
1. String argument with a comma-separated list of Kafka brokers
2. String argument with Kafka topic name for Egg orders, For example:
"orders"

## Flink SQL Code

In file `ddl-vvc.sql`, please change values for Kafka broker host names as well for the MySQL JDBC url and its credentials.

```sql
-- change values of these properties with respect to your environment
  ,'url' = '<put your jdbc host here>'
  ,'username' = '${secret_values.rds-username}'
  ,'password' = '${secret_values.rds-password}'

--
  ,'properties.bootstrap.servers' = '<put your broker hosts here>'  
```


![VVC Deployments](screens/vvc-deployments.png "VVC Deployments")