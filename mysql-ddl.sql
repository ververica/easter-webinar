create table order_fulfilment_report (
    event_timestamp DATETIME(3),
    order_status varchar(20) primary key,
    order_count BIGINT
);

create table delivery_duration_report (
    order_id varchar(40) primary key,
    location varchar(20),
    duration_seconds BIGINT,
    deliveryTimestamp DATETIME(3)
);

create table bunny_assignment (
    current_location varchar(20) primary key,
    bunnies_count BIGINT
);

CREATE TABLE undelivered_orders (
    deliveryLocation varchar(20) primary key,
    order_count BIGINT,
    measurement_date DATETIME(3)   
);

CREATE TABLE delivered_per_bunny (
    bunnyName varchar(20) primary key,
    deliveredOrders BIGINT
);

delete from order_fulfilment_report;
delete from delivery_duration_report;
delete from bunny_assignment;
delete from undelivered_orders;
delete from delivered_per_bunny;