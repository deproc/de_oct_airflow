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
<<<<<<< HEAD
 --stock history
merge into ETL_AF.DEV_DB.fact_Stock_History_Group4 as a using US_STOCKS_DAILY.PUBLIC.Stock_History as b
    on (a.symbol = b.symbol and a.date = b.date and a.open = b.open and a.high = b.high and a.low = b.low and a.close = b.close 
    and a.volume = b.volume and a.adjclose = b.adjclose)
    -- when matched then 
    --    update set a.symbol = b.symbol, a.date = b.date, a.open = b.open, a.high = b.high, a.low = b.low, a.close = b.close,
    -- a.volume = b.volume, a.adjclose = b.adjclose
    when not matched then 
        insert (symbol, date, open, high, low, close, volume, adjclose) values (b.symbol, b.date, b.open, b.high, b.low, b.close, b.volume, b.adjclose);


=======

-- merge into ETL_AF.DEV_DB.fact_Stock_History_Group4 as a using US_STOCKS_DAILY.PUBLIC.Stock_History as b
--     on (a.symbol = b.symbol and a.date = b.date and a.open = b.open and a.high = b.high and a.low = b.low and a.close = b.close
--     and a.volume = b.volume and a.adjclose = b.adjclose)
--     -- when matched then
--     --    update set a.symbol = b.symbol, a.date = b.date, a.open = b.open, a.high = b.high, a.low = b.low, a.close = b.close,
--     -- a.volume = b.volume, a.adjclose = b.adjclose
--     when not matched then
--         insert (symbol, date, open, high, low, close, volume, adjclose)
--         values (b.symbol, b.date, b.open, b.high, b.low, b.close, b.volume, b.adjclose);


-- merge into ETL_AF.DEV_DB.fact_Stock_History_Group4 as a using (select a1.id,a1.symbol,a1.date,a1.open,a1.high,a1.low,a1.close,a1.volume,a1.adjclose, SYMBOL_ID
--   from US_STOCKS_DAILY.PUBLIC.Stock_History a1
--   join "ETL_AF"."DEV_DB"."DIM_SYMBOLS_GROUP4" a2
--     on a1.symbol=a2.symbol) as b
--     on (a.symbol_id = b.symbol_id and a.date = b.date and a.open = b.open and a.high = b.high and a.low = b.low and a.close = b.close and a.volume = b.volume and a.adjclose = b.adjclose)
--     -- when matched then
--     --    update set a.symbol = b.symbol, a.date = b.date, a.open = b.open, a.high = b.high, a.low = b.low, a.close = b.close,
--     -- a.volume = b.volume, a.adjclose = b.adjclose
--     when not matched then
--         insert (a.symbol, date, open, high, low, close, volume, adjclose, symbol_id)
--         values (b.symbol, b.date, b.open, b.high, b.low, b.close, b.volume, b.adjclose, b.symbol_id);


-- merge into ETL_AF.DEV_DB.fact_Stock_History_Group4 as a using (select a1.id,a1.symbol,a1.date,a1.open,a1.high,a1.low,a1.close,a1.volume,a1.adjclose, SYMBOL_ID
--   from US_STOCKS_DAILY.PUBLIC.Stock_History a1
--   join "ETL_AF"."DEV_DB"."DIM_SYMBOLS_GROUP4" a2
--     on a1.symbol=a2.symbol) as b
--     on (a.symbol_id = b.symbol_id and a.date = b.date and a.open = b.open and a.high = b.high and a.low = b.low and a.close = b.close and a.volume = b.volume and a.adjclose = b.adjclose)
--     -- when matched then
--     --    update set a.symbol = b.symbol, a.date = b.date, a.open = b.open, a.high = b.high, a.low = b.low, a.close = b.close,
--     -- a.volume = b.volume, a.adjclose = b.adjclose
--     when not matched then
--         insert (a.symbol, date, open, high, low, close, volume, adjclose, symbol_id)
--         values (b.symbol, b.date, b.open, b.high, b.low, b.close, b.volume, b.adjclose, b.symbol_id);

-- merge into ETL_AF.DEV_DB.DIM_COMPANY_PROFILE_GROUP4 as a
-- using (
-- select
--   a1.ID , a1.SYMBOL , a1.PRICE ,a1.BETA ,a1.VOLAVG ,a1.MKTCAP ,a1.LASTDIV ,a1.RANGE, a1.CHANGES,a1.COMPANYNAME
--  ,a1.EXCHANGE ,a1.INDUSTRY,a1.WEBSITE,a1.DESCRIPTION,a1.CEO,a1.SECTOR ,a1.DCFDIFF,a1.DCF
-- from "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE" a1
-- join "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP4" a2
-- on a1.symbol = a2.symbol) as b
-- on (a.ID = b.ID  and a.SYMBOL= b.SYMBOL and  a.PRICE = b.PRICE and a.BETA=b.BETA and a.VOLAVG = b.VOLAVG and  a.MKTCAP = b.MKTCAP and  a.LASTDIV = b.LASTDIV and
-- a.RANGE = b.RANGE and a.CHANGES = b.CHANGES and  a.COMPANYNAME = b.COMPANYNAME and  a.EXCHANGE = b.EXCHANGE and a.INDUSTRY = b.INDUSTRY and a.WEBSITE = b.WEBSITE and
-- a.DESCRIPTION = b.DESCRIPTION and  a.CEO = b.CEO and  a.SECTOR = b.SECTOR and  a.DCFDIFF = b.DCFDIFF and  a.DCF = b.DCF)
-- when not matched then
-- insert( a.ID , a.SYMBOL , a.PRICE , a.BETA , a.VOLAVG , a.MKTCAP , a.LASTDIV ,  a.RANGE,  a.CHANGES, a.COMPANYNAME
--  , a.EXCHANGE , a.INDUSTRY, a.WEBSITE, a.DESCRIPTION, a.CEO, a.SECTOR , a.DCFDIFF, a.DCF)
--  values (b.ID , b.SYMBOL , b.PRICE , b.BETA , b.VOLAVG , b.MKTCAP , b.LASTDIV ,  b.RANGE,  b.CHANGES, b.COMPANYNAME
--  , b.EXCHANGE , b.INDUSTRY, b.WEBSITE, b.DESCRIPTION, b.CEO, b.SECTOR , b.DCFDIFF, b.DCF);

>>>>>>> 5c37bcf800d7fb9fa7c5838fbcb93674f1dfbdfe
merge into ETL_AF.DEV_DB.DIM_COMPANY_PROFILE_GROUP4 as a
using (
select
  a1.ID , a1.SYMBOL , a1.PRICE ,a1.BETA ,a1.VOLAVG ,a1.MKTCAP ,a1.LASTDIV ,a1.RANGE, a1.CHANGES,a1.COMPANYNAME
 ,a1.EXCHANGE ,a1.INDUSTRY,a1.WEBSITE,a1.DESCRIPTION,a1.CEO,a1.SECTOR ,a1.DCFDIFF,a1.DCF
<<<<<<< HEAD
from "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP4_test" a1
join "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP4" a2
on a1.symbol = a2.symbol) as b
on (a.ID = b.ID  and a.SYMBOL= b.SYMBOL)

-- This is for the case, when we want to update the existing record
when matched then
update set a.ID=b.ID , a.SYMBOL=b.SYMBOL , a.PRICE=b.PRICE , a.BETA=b.BETA , a.VOLAVG=b.VOLAVG , a.MKTCAP=b.MKTCAP
           , a.LASTDIV=b.LASTDIV ,  a.RANGE=b.RANGE,  a.CHANGES=b.CHANGES, a.COMPANYNAME=b.COMPANYNAME
           , a.EXCHANGE=b.exchange , a.INDUSTRY=b.industry, a.WEBSITE=b.website, a.DESCRIPTION=b.description
           , a.CEO=b.ceo, a.SECTOR=b.sector, a.DCFDIFF=b.dcfdiff, a.DCF=b.dcf
        
-- This is for the case, when we want to add a new record 
-- when we have a new symbol added into the public_company_profile
when not matched then 
insert( a.ID , a.SYMBOL , a.PRICE , a.BETA , a.VOLAVG , a.MKTCAP , a.LASTDIV ,  a.RANGE,  a.CHANGES, a.COMPANYNAME
        , a.EXCHANGE , a.INDUSTRY, a.WEBSITE, a.DESCRIPTION, a.CEO, a.SECTOR , a.DCFDIFF, a.DCF)
values (b.ID , b.SYMBOL , b.PRICE , b.BETA , b.VOLAVG , b.MKTCAP , b.LASTDIV ,  b.RANGE,  b.CHANGES, b.COMPANYNAME
       , b.EXCHANGE , b.INDUSTRY, b.WEBSITE, b.DESCRIPTION, b.CEO, b.SECTOR , b.DCFDIFF, b.DCF);

create or replace table  "ETL_AF"."DEV_DB"."DIM_SYMBOLS_GROUP4"  as
select  id, symbol, name, exchange, row_number() over(partition by symbol order by id) as symbol_id 
from "US_STOCKS_DAILY"."PUBLIC"."SYMBOLS"
=======
from "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE" a1
join "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP4" a2
on a1.symbol = a2.symbol) as b
on (a.ID = b.ID  and a.SYMBOL= b.SYMBOL)
when matched then
update set a.ID=b.ID , a.SYMBOL=b.SYMBOL , a.PRICE=b.PRICE , a.BETA=b.BETA , a.VOLAVG=b.VOLAVG , a.MKTCAP=b.MKTCAP , a.LASTDIV=b.LASTDIV ,  a.RANGE=b.RANGE,  a.CHANGES=b.CHANGES, a.COMPANYNAME=b.COMPANYNAME
 , a.EXCHANGE=b.exchange , a.INDUSTRY=b.industry, a.WEBSITE=b.website, a.DESCRIPTION=b.description, a.CEO=b.ceo, a.SECTOR=b.sector, a.DCFDIFF=b.dcfdiff, a.DCF=b.dcf ;
>>>>>>> 5c37bcf800d7fb9fa7c5838fbcb93674f1dfbdfe
