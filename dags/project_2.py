# All imports go here
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pendulum

# define snowflake variables
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'ETL_AF'
SNOWFLAKE_SCHEMA = 'DEV_DB'
SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'

with DAG(
    'Project2_Group3',
    start_date=pendulum.datetime(2022, 12, 1, tz='US/Eastern'),
    schedule_interval='0 0 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['Project'],
    catchup=True,
) as dag:
    snowflake_update_dim = SnowflakeOperator(
        task_id='update_dim_table',
        sql='update_dim.sql',
        split_statements=False,
    )
    snowflake_update_fact = SnowflakeOperator(
        task_id='update_fact_table',
        sql='update_fact.sql',
        split_statements=False,
    )
    snowflake_update_dim >> snowflake_update_fact