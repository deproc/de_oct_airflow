create or replace table "ETL_AF"."DEV_DB"."PRESTAGE_IPHONEX_GROUP5"(
    order_id int
    ,product varchar(10)
    ,quantity int
    ,price int
    ,amount int
    ,customer_id int
    ,customer_name varchar
    ,customer_street varchar
    ,customer_city varchar
    ,customer_state char(2)
    ,customer_zip int
    ,order_status varchar
);