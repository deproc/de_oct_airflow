--create fact table using profile table
create or replace table "ETL_AF"."DEV_DB"."Fact_Profile_QiaoXu"
(SYMBOL VARCHAR(16),
 PRICE NUMBER(18,8),
 COMPANYNAME VARCHAR(512),
 EXCHANGE VARCHAR(64),
 INDUSTRY VARCHAR(64),
 WEBSITE VARCHAR(64),
 DESCRIPTION VARCHAR(2048),
 CEO VARCHAR(64),
 SECTOR VARCHAR(64)
);

insert into "ETL_AF"."DEV_DB"."Fact_Profile_QiaoXu"
select SYMBOL
       ,PRICE
       ,COMPANYNAME
       ,EXCHANGE
       ,INDUSTRY
       ,WEBSITE
       ,DESCRIPTION
       ,CEO
       ,SECTOR
from "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE";

--create dim table using history table
create or replace table "ETL_AF"."DEV_DB"."Dim_History_QiaoXu"
(ID NUMBER(38,0),
 SYMBOL VARCHAR(16),
 DATE DATE,
 OPEN NUMBER(18,8),
 CLOSE NUMBER(18,8)
);

insert into "ETL_AF"."DEV_DB"."Dim_History_QiaoXu"
select row_number() over(order by SYMBOL, DATE) as ID
       ,SYMBOL
       ,DATE
       ,OPEN
       ,CLOSE
from "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY";

--add primary key of two table
alter table "ETL_AF"."DEV_DB"."Fact_Profile_QiaoXu" add primary key (SYMBOL);
alter table "ETL_AF"."DEV_DB"."Dim_History_QiaoXu" add primary key (ID);

--add foreign key of dim table to dact table
alter table "ETL_AF"."DEV_DB"."Dim_History_QiaoXu" add foreign key (SYMBOL)
 references "ETL_AF"."DEV_DB"."Fact_Profile_QiaoXu"(SYMBOL);