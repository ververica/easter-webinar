CREATE TABLE `easter`.`bunny_location` (
  `bunnyName`  STRING NOT NULL
  ,`location`  STRING
  ,`eventTime` TIMESTAMP(3)
  ,PRIMARY KEY (bunnyName) NOT ENFORCED
  ,WATERMARK FOR `eventTime` AS eventTime - INTERVAL '1' MINUTE
)
COMMENT ''
WITH (
  'connector' = 'upsert-kafka'
  ,'properties.bootstrap.servers' = '<put your broker hosts here>'
  ,'properties.group.id' = 'easter-egg'
  ,'topic' = 'alexey.bunny_location'
  ,'key.format' = 'json'
  ,'value.format' = 'json'
);

drop table `easter`.`bunny_location_gen`
;
-- select * from `easter`.bunny_location_data;
select count(*) from easter.bunny_location;

----- LOCATION ROUTE -----------------------------------------------------
DROP VIEW location_route_data;

CREATE TABLE `easter`.`location_route` (
  location  STRING NOT NULL
  ,route    ARRAY<STRING> NOT NULL
  ,PRIMARY KEY (location) NOT ENFORCED
)
COMMENT ''
WITH (
  'connector' = 'upsert-kafka'
  ,'properties.bootstrap.servers' = '<put your broker hosts here>'
  ,'properties.group.id' = 'easter-egg'
  ,'topic' = 'alexey.location_route'
  ,'key.format' = 'json'
  ,'value.format' = 'json'
);

select * from easter.location_route;
drop table location_route;
------------------------------------------

----------------- ORDERS --------------------
drop table  `easter`.`orders_gen`;
CREATE TABLE `easter`.`orders_gen` (
  `id`                STRING NOT NULL
  ,`deliveryLocation` STRING NOT NULL
  ,`eventTime`        TIMESTAMP(3)
  ,`status`           STRING NOT NULL
  ,`bunny`            ARRAY<STRING>
)
COMMENT 'Egg Orders'
WITH (
  'connector' = 'faker'
  ,'rows-per-second' = '1'
  ,'fields.id.expression' = '#{Internet.UUID}'
  ,'fields.deliveryLocation.expression' = 
  '#{regexify ''(Bank|White House|Yellow House|Pets|Bowling|Fire Department|School|Diner|Food Store|Fountain|Lake|River House){1}''}'
  ,'fields.eventTime.expression' = '#{date.past ''5'',''SECONDS''}'
  ,'fields.status.expression' = 'New'
  ,'fields.bunny.expression' = ''
  ,'fields.bunny.null-rate' = '1'
);

CREATE TEMPORARY TABLE bunnynames (
  name STRING
) with (
  'connector'='faker',
  'fields.name.expression' = '#{superhero.name}'
);
select * from bunnynames;

select * from `easter`.`orders_gen`;

CREATE TABLE `easter`.`orders` (
  `id`                STRING NOT NULL
  ,`deliveryLocation` STRING NOT NULL
  ,`eventTime`        TIMESTAMP(3)
  ,`status`           STRING NOT NULL
  ,`bunny`            ARRAY<STRING>
  ,PRIMARY KEY (id) NOT ENFORCED
  ,WATERMARK FOR `eventTime` AS eventTime - INTERVAL '1' SECOND
)
COMMENT ''
WITH (
  'connector' = 'upsert-kafka'
  ,'properties.bootstrap.servers' = '<put your broker hosts here>'
  ,'topic' = 'alexey.orders'
  ,'key.format' = 'json'
  ,'value.format' = 'json'
  ,'value.fields-include' = 'ALL'
);

drop table easter.orders_append;

CREATE TABLE `easter`.`orders_append`
(
  `id`                STRING NOT NULL
  ,`deliveryLocation` STRING NOT NULL
  ,`eventTime`        TIMESTAMP(3)
  ,`status`           STRING NOT NULL
  ,`bunny`            ARRAY<STRING>
  ,distance           INT
  ,WATERMARK FOR `eventTime` AS eventTime - INTERVAL '1' SECOND
)
COMMENT ''
WITH (
  'connector' = 'kafka'
  ,'properties.bootstrap.servers' = '<put your broker hosts here>'
  ,'properties.group.id' = 'easter-egg'
  ,'topic' = 'alexey.orders'
  ,'key.format' = 'json'
  ,'key.fields' = 'id'
  ,'value.format' = 'json'
  ,'value.fields-include' = 'ALL'
  ,'scan.startup.mode' = 'earliest-offset'
);

select id, count(*) as order_statuses from easter.orders_append group by id;
select * from easter.orders;

select
  status
  ,count(*) as _count
from easter.`orders_append`
group by
  status; 


--- 1. Order Fulfillment ---------------------------------------------------------

drop table easter.order_fulfilment_report;
select * from easter.order_fulfilment_report;
CREATE TABLE easter.order_fulfilment_report (
  event_timestamp TIMESTAMP(3)
  ,order_status  STRING
  ,order_count  BIGINT NOT NULL
  ,PRIMARY KEY (order_status) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc'
  ,'url' = '<put your jdbc host here>'
  ,'username' = '${secret_values.rds-username}'
  ,'password' = '${secret_values.rds-password}'
  ,'table-name' = 'order_fulfilment_report'
  ,'sink.buffer-flush.max-rows' = '1'
);

select
  status
  ,count(*) as cnt
from (
  select
    CASE
      WHEN status = 'New' THEN 'Open'
      WHEN status in ('EggIsReady', 'Shipped') THEN 'In Progress'
      ELSE 'Delivered'
    END as status
  from `easter`.`orders`
)
group by
  status
;
---------------------------------------------------------------------------------

select
  *
from easter.location_route
;

select * from easter.orders;

select
  *
from easter.orders_append
order by eventTime
;

select id ,collect(status) ,count(*)
from easter.orders_append
group by
  id;

--- 2 Delivery Duration

drop table easter.delivery_duration_report
;

drop table easter.delivery_duration_report;
CREATE TABLE easter.delivery_duration_report
(
   order_id          STRING NOT NULL
  ,location         STRING NOT NULL
  ,duration_seconds BIGINT NOT NUll
  ,deliveryTimestamp TIMESTAMP(3)
  ,PRIMARY KEY (order_id) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc'
  ,'url' = '<put your jdbc host here>'
  ,'username' = '${secret_values.rds-username}'
  ,'password' = '${secret_values.rds-password}'
  ,'table-name' = 'delivery_duration_report'
  ,'sink.buffer-flush.max-rows' = '5'
);

SELECT
  T.id
  ,T.astatus
  ,T.location
  ,T.aeventTime
  ,T.bstatus
  ,T.beventTime
  ,TIMESTAMPDIFF(
    SECOND
    ,T.aeventTime
    ,T.beventTime
  ) as deliveryDurationSec
FROM easter.orders_append
    MATCH_RECOGNIZE (
  PARTITION BY id
  ORDER BY eventTime
  MEASURES
        A.status AS astatus,
        B.status AS bstatus,
        A.eventTime AS aeventTime,
        B.eventTime AS beventTime,
        B.deliveryLocation AS location
  ONE ROW PER MATCH
  AFTER MATCH SKIP TO NEXT ROW
  PATTERN (A C* B)
  WITHIN INTERVAL '15' MINUTE
  DEFINE
        A AS status = 'New',
        C AS status = 'EggIsReady'
  or status = 'Shipped',
        B AS status = 'Delivered'
) AS T
;

select
  min(eventTime)
  ,max(eventTime)
FROM easter.orders_append
where status = 'Delivered'
;
--------------------------------------------------------------------------- 
--- 3. Egg Demand vs. bunnies assignment per Location Name

select
  location
  ,collect(bunnyName)
from easter.bunny_location
group by
  location
;
-- select * from easter.bunny_location;

-- bunny assignments

SELECT
  CASE
    WHEN location = ''
    or location is null THEN 'Egg Factory'
    ELSE location
  END AS current_location
  ,count(*) as bunnies_count
FROM easter.bunny_location
GROUP BY
  location;

DROP TABLE easter.bunny_assignment;
CREATE TABLE easter.bunny_assignment
(
  current_location  STRING NOT NULL
  ,bunnies_count    BIGINT NOT NUll
  ,PRIMARY KEY (current_location) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc'
  ,'url' = '<put your jdbc host here>'
  ,'username' = '${secret_values.rds-username}'
  ,'password' = '${secret_values.rds-password}'
  ,'table-name' = 'bunny_assignment'
  ,'sink.buffer-flush.max-rows' = '10'
);

-- undelivered yet

select
  deliveryLocation,
  count(*) as non_delivered_orders
from easter.orders
where status <> 'Delivered'
group by deliveryLocation;

drop table easter.undelivered_orders;
CREATE TABLE easter.undelivered_orders(
  deliveryLocation  STRING NOT NULL
  ,order_count      BIGINT NOT NUll
  ,measurement_date TIMESTAMP(3) NOT NULL
  ,PRIMARY KEY (deliveryLocation) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc'
  ,'url' = '<put your jdbc host here>'
  ,'username' = '${secret_values.rds-username}'
  ,'password' = '${secret_values.rds-password}'
  ,'table-name' = 'undelivered_orders'
  ,'sink.buffer-flush.max-rows' = '1'
);

----- 4. orders per Bunny delivered
drop TABLE easter.delivered_per_bunny;
CREATE TABLE easter.delivered_per_bunny
(
  bunnyName        STRING NOT NULL
  ,deliveredOrders BIGINT NOT NUll
  ,PRIMARY KEY (bunnyName) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc'
  ,'url' = '<put your jdbc host here>'
  ,'username' = '${secret_values.rds-username}'
  ,'password' = '${secret_values.rds-password}'
  ,'table-name' = 'delivered_per_bunny'
  ,'sink.buffer-flush.max-rows' = '5'
);

select
  bunny[1] as bunnyName,
  count(*) as deliveredOrders
from easter.orders_append
where status = 'Delivered'
group by
  bunny;

