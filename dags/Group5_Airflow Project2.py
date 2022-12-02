import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# declaring varibles:

SNOWFLAKE_CONN_ID = 'snowflake_conn'

SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'
# SNOWFLAKE_STAGE = 'beaconfire_stage'

# SNOWFLAKE_FACT_TABLE = 'FACT_STOCK_HISTORY_GROUP5'
# SNOWFLAKE_DIM_TABLE = 'DIM_COMPANY_PROFILE_GROUP5'

#SQL updating fact table commands
SQL_INSERT_TO_FACT = '''
INSERT INTO "ETL_AF"."DEV_DB"."FACT_STOCK_HISTORY_GROUP5" 
SELECT C.ID, S.DATE,S.OPEN, S.HIGH, S.LOW, S.CLOSE, S.VOLUME, S.ADJCLOSE
FROM "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY" s
    LEFT JOIN "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE" c 
        ON C.SYMBOL = S.SYMBOL 
WHERE DATE = DATEADD(DAY, 3, DATEADD(MONTH, -1, CURRENT_DATE()));
'''
# SQL updating DIM table commands
SQL_UPDATE_COMPANY_PROFILE = '''
CREATE OR REPLACE TABLE "ETL_AF"."DEV_DB"."DIM_COMPANY_PROFILE_GROUP5" AS 
SELECT * FROM "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE";
'''

DAG_ID = "Airflow_project_2_Group5"
# [START howto_operator_snowflake]
with DAG(
        DAG_ID,
        start_date=datetime(2022, 11 , 4),
        schedule_interval='12 19 * * *',
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['beaconfire'],
        catchup=True,
) as dag:
    # [START snowflake_example_dag]
    snowflake_update_FACT_table = SnowflakeOperator(
        task_id='snowflake_update_FACT_table',
        sql=SQL_INSERT_TO_FACT,
        warehouse=SNOWFLAKE_WAREHOUSE,
#         database=SNOWFLAKE_DATABASE_TARGET,
#         schema=SNOWFLAKE_SCHEMA_TARGET,
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
