"""
Example use of Snowflake related operators.
"""

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

SNOWFLAKE_STAGE = 'S3_AIRFLOW_PROJECT'

with DAG(
        "project1_group2",
        start_date=datetime(2022, 11, 30),
        max_active_runs=3,
        schedule_interval='*/5 * * * *',
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['beaconfire'],
        catchup=True,
) as dag:
    copy_into_prestg = S3ToSnowflakeOperator(
        task_id='prestage_sales_group2',
        s3_keys=['Airflow_Group2_{{ds[5:7]+ds[8:10]+ds[0:4]}}.csv'],
        table='prestage_sales_group2',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

copy_into_prestg