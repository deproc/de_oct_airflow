--1) Update the dim table
--New data is not added regularly, but old data may be changed regularly
--Since the table size is relatively small, 

--Option1: we simply copy all data from src file to target everyday
--First truncate the target table
--truncate table "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5";
--Then, insert all data from src table to target table
--insert into "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5"
--select * from  US_STOCKS_DAILY.PUBLIC.COMPANY_PROFILE;

--Option2: merge the data in both src and target tables
--If we only use merge, we will need to update all the existing rows.
--To minimize the number of rows to update, we first find all the differences using except
--Assume that nothing has been deleted in the source file
create or replace temporary table "ETL_AF"."DEV_DB".table_changes_group5 as
select * from "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE"
except 
select * from "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5";

--then, we merge the temp table into our target using merge
merge into "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5" a
using "ETL_AF"."DEV_DB".table_changes_group5 b
on  a.$1=b.$1
when matched then update 
set a.$2=b.$2, a.$3= b.$3, a.$4=b.$4, a.$5= b.$5,
    a.$6=b.$6, a.$7= b.$7, a.$8=b.$8, a.$9= b.$9,
    a.$10=b.$10, a.$11= b.$11, a.$12=b.$12, a.$13= b.$13,
    a.$14=b.$14, a.$15= b.$15, a.$16=b.$16, a.$17= b.$17, a.$18= b.$18
when not matched then insert
    values (b.$1,b.$2,b.$3,b.$4,b.$5,b.$6,b.$7,b.$8,b.$9,b.$10,
            b.$11,b.$12,b.$13,b.$14,b.$15,b.$16,b.$17,b.$18);

--drop the temp table, to save space
drop table "ETL_AF"."DEV_DB".table_changes_group5;

--2) Update the fact table
--New data is added regularly; old data rarely be changed
--Table size is huge --> not easy to scan all rows to look for updates

--Option1: insert data starting from a specific date
--We assume there is no update on past data, only adding new data
--Approach: find all rows in src that has date greater than the most recent date in target table

insert into "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP5"
with cte as(
    select * from "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY"
    --only insert rows with date > the most recent date in target file
    where date > (select ifnull(max(date),'1970-01-01') --if null, then append all data from 1970
                from "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP5")
)
select a.id, b.date, b.open, b.high, b.low, b.close, b.volume, b.adjclose
from cte b
left join "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE" a
on a.symbol = b.symbol;

--Option2: use merge just as we did for updating the dim table
--If we only use merge, we will need to update all the existing rows.
--To minimize the number of rows to update, we first find all the differences using except
--Assume that nothing has been deleted in the source file
-- create or replace temporary table "ETL_AF"."DEV_DB".table_changes_group5
-- select * from "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE"
-- except 
-- select $2,$3,$2,$3,$2,$3,$2,$3,$2,$3,$2,$3
-- from "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP5";

-- --then, we merge the temp table into our target using merge
-- merge into "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5" a
-- using "ETL_AF"."DEV_DB".dim_table_updated_group5 b
-- on  a.$1=b.$1
-- when matched then update 
-- set a.$2=b.$2, a.$3= b.$3, a.$4=b.$4, a.$5= b.$5,
--     a.$6=b.$6, a.$7= b.$7, a.$8=b.$8, a.$9= b.$9,
--     a.$10=b.$10, a.$11= b.$11, a.$12=b.$12, a.$13= b.$13,
--     a.$14=b.$14, a.$15= b.$15, a.$16=b.$16, a.$17= b.$17, a.$18= b.$18
-- when not matched then insert
--     values (b.$1,b.$2,b.$3,b.$4,b.$5,b.$6,b.$7,b.$8,b.$9,b.$10,
--             b.$11,b.$12,b.$13,b.$14,b.$15,b.$16,b.$17,b.$18);


