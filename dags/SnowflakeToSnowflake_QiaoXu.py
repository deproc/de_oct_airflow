import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'ETL_AF'
SNOWFLAKE_SCHEMA = 'DEV_DB'
SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'

with DAG(
    "StockData_SnowFlakeToSnowFlake_QiaoXu",
    start_date=datetime(2022, 12, 1),
    schedule_interval='0 7 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['qiaoxu'],
    catchup=False,
) as dag:
    StockData_SnowFlakeToSnowFlake = SnowflakeOperator(
       task_id='StockData_SnowFlakeToSnowFlake',
       sql='./StockData_SnowFlakeToSnowFlake.sql',
       split_statements=True,
    )

    StockData_SnowFlakeToSnowFlake
