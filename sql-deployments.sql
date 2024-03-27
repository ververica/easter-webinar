--- SQL Deployments:

--  easter-egg-gen-locations
CREATE TEMPORARY VIEW `easter`.bunny_location_data AS (
    select bunnyName, eventTime from (
    VALUES 
        ('Inaba',  now()),
        ('Ares',  now()),
        ('Artemis', now()),    
        ('Hermes',  now()),
        ('Jupiter', now()),
        ('Mars',  now()),
        ('Neptune', now()),    
        ('Pushinka', now()),
        ('Bugs',  now()),
        ('Sherlock',  now())
        -- ('The Aqualad Spirit',  now()),
        -- ('Magog',  now()),
        -- ('Mr Spider',  now()),
        -- ('General Omega Red',  now()),
        -- ('Captain Colossus',  now()),
        -- ('Mr Sif Brain',  now()),
        -- ('Doctor Guardian Fist',  now()),
        -- ('Giant Speedball',  now()),
        -- ('Raven Ivy',  now()),
        -- ('Spectre Strange',  now()),
        -- ('Cerebra',  now()),
        -- ('Cypher II',  now()),
        -- ('Morph X',  now())  
    ) as t (bunnyName, eventTime)
);
insert into `easter`.bunny_location(bunnyName, eventTime) 
select bunnyName, eventTime from `easter`.bunny_location_data;

-- easter-egg-order-fulfillment-report

CREATE TEMPORARY VIEW easter.order_fulfilment_view
AS 
  select 
    CASE WHEN status = 'New' THEN 'Open' 
        WHEN status in ('EggIsReady', 'Shipped')  THEN 'In Progress' 
        ELSE 'Delivered' 
    END as order_status,
    count(*) as order_count
  from `easter`.`orders` group by status;
insert into easter.order_fulfilment_report select now() as event_timestamp, order_status, order_count from easter.order_fulfilment_view;


--  easter-egg-undelivered-orders
insert into easter.undelivered_orders
select
    deliveryLocation
    ,count(*) as order_count
    ,now() as measurement_date
from easter.orders
where status <> 'Delivered'
group by
    deliveryLocation
;

-- easter-egg-gen-orders
insert into easter.orders
select
    *
from (
    select
        id
        ,deliveryLocation
        ,eventTime
        ,status
        ,ARRAY[cast(null as string)]
    from easter.orders_gen -- limit 10   
);

-- easter-egg-bunny-assignment
insert into easter.bunny_assignment
SELECT 
  CASE WHEN location = '' or location is null THEN 'Egg Factory' ELSE location END AS current_location, 
  count(*) as bunnies_count 
FROM easter.bunny_location 
  GROUP BY location;

-- easter-egg-delivery-duration
CREATE TEMPORARY VIEW easter.delivery_duration_view as
SELECT T.id, T.astatus, T.location, T.aeventTime, T.bstatus, T.beventTime as deliveryTimestamp, TIMESTAMPDIFF(SECOND, T.aeventTime, T.beventTime) as deliveryDurationSec
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
      PATTERN (A C* B) WITHIN INTERVAL '5' MINUTES
      DEFINE
        A AS status = 'New',
        C AS status = 'EggIsReady' or status =  'Shipped',
        B AS status = 'Delivered'        
    ) AS T;
insert into easter.delivery_duration_report select id, location, deliveryDurationSec, deliveryTimestamp from easter.delivery_duration_view;  

-- easter-egg-delivered-per-bunny
insert into easter.delivered_per_bunny
select bunny[1] as bunnyName, count(*) as deliveredOrders from easter.orders_append where status = 'Delivered' group by bunny;

-- easter-egg-gen-routes
CREATE TEMPORARY VIEW location_route_data AS (
  SELECT
    location,
    route
  FROM
    (
      VALUES
        ('Bank', ARRAY ['Mill', 'Vernon', 'Main']),
        ('Pets', ARRAY ['Mill', 'Vernon', 'Main']),
        ('White House', ARRAY ['Mill', 'Vernon']),
        ('Yellow House', ARRAY ['Mill']),
        ('River House', ARRAY ['Mill', 'Vernon', 'Dale']),
        ('Fountain', ARRAY ['Mill', 'Vernon', 'Main', 'Oak']),
        ('Lake', ARRAY ['Mill', 'Vernon', 'Main', 'Pine']),
        ('Food Store', ARRAY ['River', 'Grove']),
        ('Bowling', ARRAY ['River']),
        ('Diner', ARRAY ['River', 'Mapple']),
        ('Fire Department', ARRAY ['River', 'Mapple']),
        ('School', ARRAY ['River', 'Grove', 'School'])
    ) AS t (location, route)
);

insert into easter.location_route select * from location_route_data;