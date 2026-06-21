import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"

def yt_etl_data_quality_check(schema):
    try:
        task = BashOperator(
            task_id=f"soda_check_{schema}",
            bash_command=f"soda scan -d {DATASOURCE} -c {SODA_PATH}/configuration.yaml -v SCHEMA={schema} {SODA_PATH}/checks.yaml",
        )
        return task
    except Exception as e:
        logger.error(f"Error creating Soda check task for schema {schema}")
        raise e