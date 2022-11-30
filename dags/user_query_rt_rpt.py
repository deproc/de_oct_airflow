"""
Example use of Snowflake related operators.
"""
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
    "user_query_rt_rpt",
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 7 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:

    prestg_last_load_chk = SnowflakeCheckOperator(
    conn_id=SNOWFLAKE_CONN_ID,
    task_id="prestg_last_load_chk",
    sql="select count(*) from prestg_account_query_hist where date(load_utc_ts)>='{{ ds }}'",
    )

    user_query_rt_rpt = SnowflakeOperator(
       task_id='user_query_rt_rpt',
       sql='./user_query_rt_rpt.sql',
       split_statements=True,
    )

    prestg_last_load_chk >> user_query_rt_rpt
