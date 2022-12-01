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

SNOWFLAKE_STAGE = 's3_airflow_project'
#S3_FILE_PATH = 'product_order_trans_07152022.csv'

# SNOWFLAKE_SAMPLE_TABLE = 'PRESTG_PRODUCT_TEMP_GROUP4'
# CREATE_TABLE_SQL_STRING = (
#     f"CREATE OR REPLACE TEMPORARY TABLE {SNOWFLAKE_SAMPLE_TABLE} (INVOICENO NUMBER(38,0), \
#         STOCKCODE VARCHAR(36), DESCRIPTION VARCHAR(100), QUANTITY NUMBER(38,0), INVOICEDATE TIMESTAMP NTZ(9), MONTH NUMBER(2,0), DAY NUMBER(2,0), UNITPRICE NUMBER(10,0), CUSTOMERID NUMBER(38,0),
#     COUNTRY VARCHAR(100), COUNTRYCODE VARCHAR(2));"
# )

#SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} SELECT * FROM "

with DAG(
    "PRESTG_PRODUCT_GROUP4",
    start_date=datetime(2022, 11, 29),
    schedule_interval='59 23 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:

    copy_into_prestg = S3ToSnowflakeOperator(
        task_id='prestg_product_order_trans_group4',
        s3_keys=['Airflow_Group4_{{ ds[5:7]+ds[8:10]+ds[0:4] }}.csv'],
        table= 'prestg_product_temp_group4',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = NONE \
            ESCAPE_UNENCLOSED_FIELD = NONE, RECORD_DELIMITER = '\n')''',
    )

    # snowflake_op_sql_str = SnowflakeOperator(
    #     task_id='snowflake_op_sql_str',
    #     sql=CREATE_TABLE_SQL_STRING,
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     role=SNOWFLAKE_ROLE,
    # )

    user_query_insert = SnowflakeOperator(
       task_id='product_insert',
       sql='./product_insert.sql',
       split_statements=True,
    )

    user_query_temptable = SnowflakeOperator(
       task_id='temp_table',
       sql='./temp_table.sql',
       split_statements=True,
    )

    (
        user_query_temptable 
        >> [
        copy_into_prestg,
        user_query_insert
        ]

    )

