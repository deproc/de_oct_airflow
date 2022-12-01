-- Due to the source table are shares, we need to copy the DDL first and then insert
CREATE OR REPLACE TABLE "ETL_AF"."DEV_DB"."FACT_STOCK_GROUP1"
LIKE "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY";

insert into "ETL_AF"."DEV_DB"."FACT_STOCK_GROUP1"
select * from "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY";

--Update the ID column based on the dimension table (SYMBOLS)
merge into "ETL_AF"."DEV_DB"."FACT_STOCK_GROUP1"  t
using "US_STOCKS_DAILY"."PUBLIC"."SYMBOLS" s
on t.SYMBOL = s.SYMBOL
when matched then
    update set t.ID = s.ID;

-- Drop the redandent column
ALTER TABLE "ETL_AF"."DEV_DB"."FACT_STOCK_GROUP1"
DROP COLUMN SYMBOL;

-- Check the results
-- SELECT * FROM "ETL_AF"."DEV_DB"."FACT_STOCK_GROUP1";

-- Check difference ratio between the adjclose price and close price since they appears to have the same value
-- The result is 15.6%, which means it can not be dropped
SELECT
    COUNT(*)/(SELECT COUNT(*) FROM "ETL_AF"."DEV_DB"."FACT_STOCK_GROUP1") AS close_and_adjclose_diff_ratio
FROM "ETL_AF"."DEV_DB"."FACT_STOCK_GROUP1"
WHERE CLOSE != ADJCLOSE;

-- Create the dimension table in the same way
CREATE OR REPLACE TABLE "ETL_AF"."DEV_DB"."DIM_STOCK_GROUP1"
LIKE "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE";

insert into "ETL_AF"."DEV_DB"."DIM_STOCK_GROUP1"
select * from "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE";

-- Add primary key and foreign key
ALTER TABLE "ETL_AF"."DEV_DB"."FACT_STOCK_GROUP1" ADD PRIMARY KEY (ID,DATE);
ALTER TABLE "ETL_AF"."DEV_DB"."DIM_STOCK_GROUP1" ADD PRIMARY KEY (ID);
ALTER TABLE "ETL_AF"."DEV_DB"."FACT_STOCK_GROUP1" ADD FOREIGN KEY (ID) REFERENCES "ETL_AF"."DEV_DB"."DIM_STOCK_GROUP1"(ID);
