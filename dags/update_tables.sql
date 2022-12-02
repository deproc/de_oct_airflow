--1) Update the dim table
--New data is not added regularly, but old data may be changed regularly
--Since the table size is relatively small, 
--we simply copy all data from src file to target everyday

--First truncate the target table
truncate table "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5";
--Then, insert all data from src table to target table
insert into "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5"
select * from  US_STOCKS_DAILY.PUBLIC.COMPANY_PROFILE;


--2) Update the fact table
--New data is added regularly; old data rarely be changed
--Table size is huge --> not easy to scan all rows to look for updates
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
