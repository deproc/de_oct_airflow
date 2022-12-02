create or replace temporary table "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP4_temp1"
(
ID	NUMBER(38,0),
SYMBOL	VARCHAR(16),
DATE	DATE,
OPEN	NUMBER(18,8),
HIGH	NUMBER(18,8),
LOW	NUMBER(18,8),
CLOSE	NUMBER(18,8),
VOLUME	NUMBER(38,8),
ADJCLOSE	NUMBER(18,8),
his_id int identity(1,1)
);
insert into "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP4_temp1"(
ID,
SYMBOL,
DATE,
OPEN,
HIGH,
LOW,
CLOSE,
VOLUME,
ADJCLOSE
)
select *  from "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY";
  
merge into "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP4" as a using "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP4_temp1" as b
    on (a.his_id=b.his_id)
    when matched then
      update set a.symbol = b.symbol, a.date = b.date, a.open = b.open, a.high = b.high, a.low = b.low, a.close = b.close,
      a.volume = b.volume, a.adjclose = b.adjclose
    when not matched then
        insert (a.symbol, date, open, high, low, close, volume, adjclose,his_id)
        values (b.symbol, b.date, b.open, b.high, b.low, b.close, b.volume, b.adjclose, b.his_id);

-- insert into ETL_AF.DEV_DB.fact_Stock_History_Group4

-- select * from (
-- select * from US_STOCKS_DAILY.PUBLIC.Stock_History
-- EXCEPT
-- select * from ETL_AF.DEV_DB.fact_Stock_History_Group4
-- )

merge into ETL_AF.DEV_DB.fact_Stock_History_Group4 as a using US_STOCKS_DAILY.PUBLIC.Stock_History as b
    on (a.symbol = b.symbol and a.date = b.date and a.open = b.open and a.high = b.high and a.low = b.low and a.close = b.close 
    and a.volume = b.volume and a.adjclose = b.adjclose)
    -- when matched then 
    --    update set a.symbol = b.symbol, a.date = b.date, a.open = b.open, a.high = b.high, a.low = b.low, a.close = b.close,
    -- a.volume = b.volume, a.adjclose = b.adjclose
    when not matched then 
        insert (symbol, date, open, high, low, close, volume, adjclose) values (b.symbol, b.date, b.open, b.high, b.low, b.close, b.volume, b.adjclose);


