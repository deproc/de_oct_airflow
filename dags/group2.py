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
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {prestage_sales_group2} (ORDERNUMBER INT, QUANTITYORDERED INT, PRICEEACH FLOAT, SALES FLOAT, ORDERDATE DATETIME, YEAR_ID INT, CUSTOMERNAME VARCHAR, CITY VARCHAR, STATE VARCHAR, COUNTRY VARCHAR);"
)
SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'

SNOWFLAKE_STAGE = 'S3_AIRFLOW_PROJECT'
#S3_FILE_PATH = 'product_order_trans_07152022.csv'

with DAG(
    "s3_data_copy_test",
    start_date=datetime(2022, 11, 30),
    max_active_runs=3,
    schedule_interval='*/5 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:
    # Create the table if does not exist
    snowflake_table_create = SnowflakeOperator(
        task_id='table_creation',
        sql=CREATE_TABLE_SQL_STRING,
        params={"table_name": 'prestage_sales_group2'},
    )
    copy_into_prestg = S3ToSnowflakeOperator(
        task_id='prestage_sales_group2',
        s3_keys=['Airflow_Group2_{{ ds[5:7]+ds[8:10]+ds[0:4] }}.csv'],
        table='prestage_sales_group2',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )

    copy_into_prestg