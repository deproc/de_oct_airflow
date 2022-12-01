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

with DAG(
    "update_stock_history",
    start_date=datetime(2022, 12, 1),
    schedule_interval='0 7 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:

    # copy_into_prestg = S3ToSnowflakeOperator(
    #     task_id='prestg_product_order_trans',
    #     s3_keys=['Airflow_Group4_{{ ds[0:4]+ds[5:7]+ds[8:10] }}.csv'],
    #     table= 'prestg_product_group4',
    #     schema=SNOWFLAKE_SCHEMA,
    #     stage=SNOWFLAKE_STAGE,
    #     file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
    #         NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
    #         ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    # )

    # snowflake_op_sql_str = SnowflakeOperator(
    #     task_id='snowflake_op_sql_str',
    #     sql=CREATE_TABLE_SQL_STRING,
    #     warehouse=SNOWFLAKE_WAREHOUSE,
    #     database=SNOWFLAKE_DATABASE,
    #     schema=SNOWFLAKE_SCHEMA,
    #     role=SNOWFLAKE_ROLE,
    # )

    user_query_insert = SnowflakeOperator(
       task_id='insert_into_fact',
       sql='./update_fact_table.sql',
       split_statements=True,
    )

    # user_query_temptable = SnowflakeOperator(
    #    task_id='temp_table',
    #    sql='./temp_table.sql',
    #    split_statements=True,
    # )

    
        # user_query_temptable 
        # >>
    user_query_insert
        # >>
        # user_query_insert
        

    

