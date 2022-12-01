
'''
Most part of this python script is based on the 's3_data_copy_test' file which Aaron and Carina provided for us.
You can revise as you want.

I didn't write creating table in the dag because it makes no sense to create or replace a same table every day at
7:00 am.
'''


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

SNOWFLAKE_STAGE = 's3_airflow_project' # I use show stage in snowflake to check there is a stage linked to our s3 bucket

with DAG(
    "s3_data_copy_test_group1",
    start_date=datetime(2022, 11, 30), # One day before the execution date
    schedule_interval='0 7 * * *',  # every 7:00 am
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True, # Can be set to default, but in case some error happens, we need to catchup all the record
) as dag:

    copy_into_prestg = S3ToSnowflakeOperator(
        task_id='prestg_QiaoXuTest_Group1',
        s3_keys=['QiaoXuTest_Group1_{{ ds[0:4]+ds[5:7]+ds[8:10] }}.csv'], # Changed to our group file name
        table='PRESTAGE_QIAOXUTEST_GROUP1', # Pre-created in the snowflake for loading the data
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )
