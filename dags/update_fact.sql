INSERT INTO "ETL_AF"."DEV_DB"."FACT_STOCKHISTORY_GROUP3_TEST" (symbol_id, date, open, high, low, close, volume, adjclose)
select MD5_NUMBER_LOWER64(symbol), date, open, high, low, close, volume, adjclose
FROM "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY"
where date = '{{ds}}'