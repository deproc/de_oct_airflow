import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'ETL_AF'
SNOWFLAKE_SCHEMA = 'DEV_DB'
SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'

with DAG(
    "project2_snowflake_to_snowflake_group5",
    start_date=datetime(2022, 12, 1),
    schedule_interval='8 3 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:

    update_tables = SnowflakeOperator(
        task_id='update_tables',
        sql='project2_update_tables_group5.sql',
        split_statements=True,
    )

    update_tables
