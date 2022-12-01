#project_2:
#initial data load
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