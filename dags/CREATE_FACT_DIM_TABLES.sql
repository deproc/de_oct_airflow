create or replace table "ETL_AF"."DEV_DB".fact_stock_history_group2(
    ID int,
    SYMBOL varchar,
    DATE DATE,
    OPEN float8,
    HIGH float8,
    LOW float8,
    CLOSE float8,
    VOLUME float8,
    ADJCLOSE float8,
    constraint fkey_1 foreign key (SYMBOL) references "ETL_AF"."DEV_DB".dim_company_profile_group2 (SYMBOL)
);

INSERT INTO "ETL_AF"."DEV_DB".fact_stock_history_group2 (
    SELECT
        ID,
        SYMBOL,
        DATE,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        ADJCLOSE
    FROM"US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY"
);

create or replace table "ETL_AF"."DEV_DB".dim_company_profile_group2(
    ID int,
    SYMBOL varchar,
    EXCHANGE varchar,
    INDUSTRY varchar,
    WEBSITE varchar,
    DESCRIPTION varchar,
    CEO varchar,
    SECTOR varchar,
    constraint pkey_1 primary key (SYMBOL) not enforced
);

INSERT INTO "ETL_AF"."DEV_DB".dim_company_profile_group2 (
    SELECT
        ID,
        SYMBOL,
        EXCHANGE,
        INDUSTRY,
        WEBSITE,
        DESCRIPTION,
        CEO,
        SECTOR
    FROM "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE"
);
