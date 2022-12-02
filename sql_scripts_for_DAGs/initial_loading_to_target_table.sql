#project_2:
--create dim table
create or replace table dim_company_profile_group5(
    id int primary key
    ,symbol varchar
    ,price float
    ,beta float
    ,volavg int
    ,mktcap int
    ,lastdiv float
    ,range varchar
    ,changes float
    ,companyname varchar
    ,exchange varchar
    ,industry varchar
    ,website varchar
    ,description varchar
    ,ceo varchar
    ,sector varchar
    ,dcfdiff float
    ,dcf float
);

--create fact table
create or replace table fact_stock_history_group5(
    company_id int
    ,date date
    ,open float
    ,high float
    ,low float
    ,close float
    ,volume float
    ,adjclose float
    ,primary key (company_id, date)
    ,foreign key (company_id) references dim_company_profile_group5(id)
);

#initial data load
-- load data from source table into dim table
insert into "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5"(
    id
    ,symbol
    ,price
    ,beta
    ,volavg
    ,mktcap
    ,lastdiv
    ,range
    ,changes
    ,companyname
    ,exchange
    ,industry
    ,website
    ,description
    ,ceo
    ,sector
    ,dcfdiff
    ,dcf
)
select
    *
from "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE";

--load data from source table into fact table
insert into "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP5"
select
    c.id
    ,s.date
    ,s.open
    ,s.high
    ,s.low
    ,s.close
    ,s.volume
    ,s.adjclose
from "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY" s
    left join "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE" c
        on c.symbol=s.symbol;
