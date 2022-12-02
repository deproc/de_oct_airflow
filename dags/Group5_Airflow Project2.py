import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# declaring varibles:

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'

DAG_ID = "Airflow_project_2_Group5"

# [START howto_operator_snowflake]
with DAG(
        DAG_ID,
        start_date=datetime(2022, 12 , 1),
        schedule_interval='00 14 * * *',
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['beaconfire'],
        catchup=True,
) as dag:
    # [START snowflake_dag]
    snowflake_update_DIM_and_FACT_tables = SnowflakeOperator(
        task_id='snowflake_update_FACT_table',
        sql='Airflow_Project_2_ETL.sql',
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
        split = Ture,
    )


    snowflake_update_DIM_and_FACT_tables
