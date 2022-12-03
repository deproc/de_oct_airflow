--update DIM table
CREATE OR REPLACE TABLE "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5" AS
SELECT * FROM "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE";
--update fact table:
--1. store the difference between sorce table and the fact table
CREATE OR REPLACE TEMPORARY TABLE "ETL_AF"."DEV_DB"."UPDATED_RECORDS_GROUP5" AS
SELECT
    c.id
    ,s.date
    ,s.open
    ,s.high
    ,s.low
    ,s.close
    ,s.volume
    ,s.adjclose
FROM "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY" s
    LEFT JOIN "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE" c
        ON c.symbol=s.symbol
EXCEPT
SELECT
    *
FROM "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP5";

--2. merge the differences to the fact table from the updated-records tables
MERGE "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP5" T
USING "ETL_AF"."DEV_DB"."UPDATED_RECORDS_GROUP5" S
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.date = s.date, t.open = s.open, t.high = s.high, t.low = s.low, t.close = s.close, t.volume = s.volume, t.adjclose = s.adjclose
WHEN NOT MATCHED THEN INSERT (id, date, open, high, low, close, volume, adjclose)
    VALUES (s.id, s.date, s.open, s.high, s.low, s.close, s.volume, s.adjclose);
