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

# table name
table_name = 'PRESTAGE_USERS_GROUP3'

# define dag
with DAG(
        "Project1_Group3",
        start_date=pendulum.datetime(2022, 11, 30, tz='US/Eastern'),
        schedule_interval='0 7 * * *',
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['beaconfire'],
        catchup=True,
) as dag:
    # Create the table if does not exist
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
    snowflake_load_from_S3
