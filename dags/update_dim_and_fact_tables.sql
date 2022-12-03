CREATE OR REPLACE TEMPORARY TABLE TEMP AS
SELECT
    sym.id AS symbol_id
    ,c.id AS company_id
    ,s.date
    ,s.open
    ,s.high
    ,s.low
    ,s.close
    ,s.volume
    ,s.adjclose
FROM "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY" s
LEFT JOIN "US_STOCKS_DAILY"."PUBLIC"."SYMBOLS" sym
ON s.symbol = sym.symbol
LEFT JOIN "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE" c
ON c.symbol=s.symbol
EXCEPT
SELECT
    *
FROM FACT_STOCK_GROUP1;

-- Use merge to insert new data or update the old data
MERGE FACT_STOCK_GROUP1 t
USING TEMP s
ON t.symbol_id = s.symbol_id AND t.company_id = s.company_id AND t.date = s.date
WHEN MATCHED THEN UPDATE SET t.open = s.open, t.high = s.high, t.low = s.low, t.close = s.close, t.volume = s.volume, t.adjclose = s.adjclose
WHEN NOT MATCHED THEN INSERT (symbol_id, company_id, date, open, high, low, close, volume, adjclose)
    VALUES (s.symbol_id, s.company_id, s.date, s.open, s.high, s.low, s.close, s.volume, s.adjclose);

-- Insert or update data into the dimension table
CREATE OR REPLACE TABLE DIM_STOCK_GROUP1 as
SELECT * FROM "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE";

CREATE OR REPLACE TABLE DIM2_STOCK_GROUP1 as
SELECT * FROM "US_STOCKS_DAILY"."PUBLIC"."SYMBOLS";





