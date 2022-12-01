"""
Example use of Snowflake related operators.
"""
import os
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator



SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'ETL_AF'
SNOWFLAKE_SCHEMA = 'DEV_DB'

SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'
#SNOWFLAKE_STAGE = 'beaconfire_stage'


SNOWFLAKE_SAMPLE_TABLE = 'airflow_testing'
# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)


DAG_ID = "beaconfire_dev_db_test"
# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval='30 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=False,
) as dag:
    # [START snowflake_example_dag]
    snowflake_op_sql_str = SnowflakeOperator(
        task_id='snowflake_op_sql_str',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_with_params = SnowflakeOperator(
        task_id='snowflake_op_with_params',
        sql=SQL_INSERT_STATEMENT,
        parameters={"id": 5},
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_sql_list = SnowflakeOperator(task_id='snowflake_op_sql_list', sql=SQL_LIST)

    snowflake_op_sql_multiple_stmts = SnowflakeOperator(
        task_id='snowflake_op_sql_multiple_stmts',
        sql=SQL_MULTIPLE_STMTS,
        split_statements=True,
    )

    snowflake_op_template_file = SnowflakeOperator(
       task_id='snowflake_op_template_file',
       sql='beaconfire_dev_db_test.sql',
       split_statements=True,
    )

    # [END howto_operator_snowflake]


    (
        snowflake_op_sql_str
        >> [
            snowflake_op_with_params,
            snowflake_op_sql_list,
            snowflake_op_template_file,
            # copy_into_table,
            snowflake_op_sql_multiple_stmts,
        ]

    )
    # [END snowflake_example_dag]
