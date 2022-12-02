-- create dim table using the company profile table
CREATE OR REPLACE TABLE ETL_AF.DEV_DB.DIM_COMPANYPROFILE_GROUP3
AS
SELECT MD5_NUMBER_LOWER64(symbol) as id
    , symbol
    , price
    , beta
    , volavg
    , mktcap
    , lastdiv
    , range
    , changes
    , companyname
    , exchange
    , industry
    , website
    , description
    , ceo
    , sector
    , dcfdiff
    , dcf
FROM "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE";

-- create fact table
CREATE OR REPLACE TABLE ETL_AF.DEV_DB.FACT_STOCKHISTORY_GROUP3(
    ID NUMBER(38,0) IDENTITY(1,1)
    , SYMBOL_ID NUMBER(38,0)
    , DATE date
    , OPEN NUMBER(18,8)
    , HIGH NUMBER(18,8)
    , LOW NUMBER(18,8)
    , CLOSE NUMBER(18,8)
    , VOLUME NUMBER(38,8)
    , ADJCLOSE NUMBER(18,8)
);

-- populate fact table using stock history table
INSERT INTO "ETL_AF"."DEV_DB"."FACT_STOCKHISTORY_GROUP3" (symbol_id, date, open, high, low, close, volume, adjclose)
SELECT MD5_NUMBER_LOWER64(symbol) as symbol
    , date
    , open
    , high
    , low
    , close
    , volume
    , adjclose
FROM "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY";