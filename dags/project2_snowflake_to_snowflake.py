import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'ETL_AF'
SNOWFLAKE_SCHEMA= 'DEV_DB'

SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'

SNOWFLAKE_STAGE = 's3_airflow_project' 
# S3_FILE_PATH = 'iphoneX_Group5_20221130.csv'

UPDATE_DIM_TABLE_SQL = '''create or replace dim_company_profile_group5 
                        clone US_STOCKS_DAILY.PUBLIC.COMPANY_PROFILE;'''

with DAG(
    "project2_snowflake_to_snowflake",
    start_date=datetime(2022, 11, 29),
    schedule_interval='30 6 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:

    # copy_into_prestg = S3ToSnowflakeOperator(
    #     task_id='prestg_product_order_trans',
    #     s3_keys=['iphoneX_Group5_{{ds[0:4]+ds[5:7]+ds[8:10]}}.csv'],
    #     table='prestage_iphoneX_Group5', 
    #     schema=SNOWFLAKE_SCHEMA,
    #     stage=SNOWFLAKE_STAGE,
    #     file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
    #         NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
    #         ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    # )

    update_dim_tables = SnowflakeOperator(
        task_id='update_dim_tables',
        sql=UPDATE_DIM_TABLE_SQL,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

    # update_fact_table = SnowflakeOperator(
    #    task_id='update_fact_table',
    #    sql='update_fact_table.sql',
    #    split_statements=True,
    # )

    update_dim_tables