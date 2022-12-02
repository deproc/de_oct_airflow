import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'ETL_AF'
SNOWFLAKE_SCHEMA = 'DEV_DB'
SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'

SNOWFLAKE_SOURCE_DATABASE = 'US_STOCKS_DAILY'
SNOWFLAKE_SOURCE_SCHEMA = 'PUBLIC'
SNOWFLAKE_SOURCE_STOCK_HISTORY = 'STOCK_HISTORY'
SNOWFLAKE_SOURCE_COMPANY_PROFILE = 'COMPANY_PROFILE'

SNOWFLAKE_TARGET_DATABASE = 'ELT_AF'
SNOWFLAKE_TARGET_SCHEMA = 'DEV_DB'
SNOWFLAKE_TARGET_FACT_TABLE = 'fact_stock_history_group2'
SNOWFLAKE_TARGET_DIM_TABLE = 'dim_company_profile_group2'

# SNOWFLAKE_STAGE = 's3_airflow_project'


SQL_update_fact = f'''INSERT INTO "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP2" (ID, SYMBOL, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR)
SELECT 
  ID, SYMBOL, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR
FROM "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP2"
EXCEPT 
SELECT 
  ID, SYMBOL, INDUSTRY, WEBSITE, DESCRIPTION, CEO, SECTOR
FROM "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY"'''

SQL_update_DIM = f'''

'''

with DAG(
        "project1_group2",
        start_date=datetime(2022, 11, 30),
        schedule_interval='*/10 * * * *',
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['beaconfire'],
        catchup=True,
) as dag:
    snowflake_update_fact = SnowflakeOperator(
        task_id='snfk_update_fact',
        sql = , # query to update fact table
        table = SNOWFLAKE_TARGET_FACT_TABLE,
        schema = SNOWFLAKE_TARGET_SCHEMA,
        database = SNOWFLAKE_TARGET_DATABASE,
        warehouse = SNOWFLAKE_WAREHOUSE,
        role = SNOWFLAKE_ROLE,
    )

    snowflake_update_dim = SnowflakeOperator(
        task_id = 'snfk_update_dim',
        sql = , # query to update dim table
        table = SNOWFLAKE_TARGET_DIM_TABLE,
        schema = SNOWFLAKE_TARGET_SCHEMA,
        database = SNOWFLAKE_TARGET_DATABASE,
        warehouse = SNOWFLAKE_WAREHOUSE,
        role = SNOWFLAKE_ROLE,
    )

    snowflake_update_dim >> snowflake_update_fact