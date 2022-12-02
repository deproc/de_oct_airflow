import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# declaring varibles:

SNOWFLAKE_CONN_ID = 'snowflake_conn'

SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'
# SNOWFLAKE_STAGE = 'beaconfire_stage'
DATE_PARAMETER = "ds[0:4]+'-'+ds[5:7]+'-'+ds[8:10]"

# SNOWFLAKE_DATABASE_TARGET = 'ETL_AF'
# SNOWFLAKE_SCHEMA_TARGET = 'DEV_DB'
#
# SNOWFLAKE_DATABASE_SOURCE = 'US_STOCKS_DAILY'
# SNOWFLAKE_SCHEMA_SOURCE = 'PUBLIC'
#
# SNOWFLAKE_FACT_TABLE = "FACT_STOCK_HISTORY_GROUP4"
# SNOWFLAKE_DIM_TABLE = "DIM_COMPANY_PROFILE_GROUP5"
# SQL updating fact table commands
SQL_INSERT_TO_FACT = f'''
INSERT INTO "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP5" 
SELECT C.ID, S.DATE,S.OPEN, S.HIGH, S.LOW, S.CLOSE, S.VOLUME, S.ADJCLOSE
FROM "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY" s
    LEFT JOIN "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE" c 
        ON C.SYMBOL = S.SYMBOL 
WHERE DATE = {DATE_PARAMETER}
'''
# SQL updating DIM table commands
SQL_UPDATE_COMPANY_PROFILE = f'CREATE OR REPLACE "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5" CLONE "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE"'

DAG_ID = "Airflow_project_2_Group5"
# [START howto_operator_snowflake]
with DAG(
        DAG_ID,
        start_date=datetime(2022, 11 , 4),
        schedule_interval='12 14 * * *',
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['beaconfire'],
        catchup=False,
) as dag:
    # [START snowflake_example_dag]
    snowflake_update_FACT_table = SnowflakeOperator(
        task_id='snowflake_update_FACT_table',
        sql=SQL_INSERT_TO_FACT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        # database=SNOWFLAKE_DATABASE,
        # schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_update_DIM_table = SnowflakeOperator(
        task_id='snowflake_update_DIM_table',
        sql=SQL_UPDATE_COMPANY_PROFILE,
        # parameters={"id": 5},
        warehouse=SNOWFLAKE_WAREHOUSE,
        # database=SNOWFLAKE_DATABASE,
        # schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_update_DIM_table >> snowflake_update_FACT_table
