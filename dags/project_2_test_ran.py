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

# sql command
stock_table_name = "FACT_STOCKHISTORY_GROUP3_TEST"
stock_history_increment = f"""
INSERT INTO {stock_table_name} (symbol_id, date, open, high, low, close, volume, adjclose)
select MD5_NUMBER_LOWER64(symbol), date, open, high, low, close, volume, adjclose
FROM "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY"
where date = {{ds}}
"""

with DAG(
    'Project2_Group3_test_ran',
    start_date=pendulum.datetime(2022, 11, 1, tz='US/Eastern'),
    end_date = pendulum.datetime(2022, 11, 5, tz='US/Eastern'),
    schedule_interval='0 0 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['GROUP3'],
    catchup=True,
) as dag:
    snowflake_update_dim = SnowflakeOperator(
        task_id='update_dim_table',
        sql='update_dim.sql',
        split_statements=False,
    )
    snowflake_update_fact = SnowflakeOperator(
        task_id='update_fact_table',
        sql=stock_history_increment,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    snowflake_update_dim >> snowflake_update_fact