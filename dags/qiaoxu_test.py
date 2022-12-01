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

SNOWFLAKE_STAGE = 's3_airflow_project'
#S3_FILE_PATH = 'QiaoXuTest_Group1_20221130.csv'
#S3_FILE_PATH = 'QiaoXuTest_Group1_20221201.csv'
#S3_FILE_PATH = 'QiaoXuTest_Group1_20221202.csv'

with DAG(
    "s3_data_copy_test_qiaoxu",
    start_date=datetime(2022, 11, 30),
    schedule_interval='00 07 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:

    copy_into_prestg = S3ToSnowflakeOperator(
        task_id='prestg_QiaoXuTest_Group1',
        s3_keys=['QiaoXuRest_Group1_{{ds[0:4]+ds[5:7]+ds[8:10]}}.csv'],
        table='prestg_QiaoXuTest_Group1',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )