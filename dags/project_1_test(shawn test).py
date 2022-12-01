# All imports go here
from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
import pendulum

# define snowflake variables
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'ETL_AF'
SNOWFLAKE_SCHEMA = 'DEV_DB'
SNOWFLAKE_ROLE = 'BF_DEVELOPER'
SNOWFLAKE_WAREHOUSE = 'BF_ETL'
SNOWFLAKE_STAGE = 's3_airflow_project'
# AIRFLOW_CONN_SNOWFLAKE_DEFAULT = {
#     "conn_type": "snowflake",
#     "conn_id" : SNOWFLAKE_CONN_ID,
#     "login": "dezhangwu",
#     "password": "1029384756Ww",
# }

# SQL command
table_name = 'PRESTAGE_USERS_GROUP3'
create_table = (
    f"""
    CREATE TABLE IF NOT EXISTS {table_name}(
   id NUMBER(38,0)
  ,avatar VARCHAR()
  ,email VARCHAR()
  ,first_name VARCHAR()
  ,last_name VARCHAR()
  ,age NUMBER(3, 0)
  ,Birthday DATE
  ,country VARCHAR()
  ,zipcode VARCHAR()
  ,gender VARCHAR(11)
  ,ip_address VARCHAR(15)
  ,create_at TIMESTAMP_NTZ(9)
    );
    """
)

# define dag
with DAG(
        "Project1_Group3_Shawn_test",
        start_date = pendulum.datetime(2022, 11, 29, tz='US/Eastern'),
        schedule_interval = '0 7 * * *',
        default_args = {'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags = ['beaconfire'],
        catchup = True,
) as dag:
    # Create the table if does not exist
    snowflake_table_create = SnowflakeOperator(
        task_id='table_creation',
        sql=create_table,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )
    snowflake_load_from_S3 = S3ToSnowflakeOperator(
        task_id='table_insert',
        s3_keys=['Users_Group3_{{ ds[0:4]+ds[5:7]+ds[8:10] }}.csv'],
        table=table_name,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
                NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
                ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',
    )
    snowflake_table_create >> snowflake_load_from_S3
