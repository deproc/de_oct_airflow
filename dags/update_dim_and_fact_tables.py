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
    "update_dim_and_fact_tables",
    start_date=datetime(2022, 12, 1),
    schedule_interval='0 2 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:

    update_dim_and_fact_tables = SnowflakeOperator(
       task_id='update_dim_and_fact_tables',
       sql='./update_dim_and_fact_tables.sql',
       split_statements=True,
    )

    update_dim_and_fact_tables       
    
