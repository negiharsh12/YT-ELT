import os
import sys
import pytest
import psycopg2
from unittest import mock
from airflow.models import Variable, Connection, DagBag

# Target the actual dags folder explicitly so nested imports work during testing
dags_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags"))
if dags_folder not in sys.path:
    sys.path.insert(0, dags_folder)

@pytest.fixture
def api_key():
    with mock.patch.dict(os.environ, AIRFLOW_VAR_YOUTUBE_API_KEY="MOCK_KEY1234"):
        yield Variable.get("YOUTUBE_API_KEY")

@pytest.fixture
def channel_handle():
    with mock.patch.dict(os.environ, AIRFLOW_VAR_YOUTUBE_CHANNEL_HANDLE="MOCK_HANDLE1234"):
        yield Variable.get("YOUTUBE_CHANNEL_HANDLE")
    
@pytest.fixture
def mock_postgres_connection():
    conn = Connection(
        login="mock_user",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db"     # schema is database name for Postgres
    )
    conn_uri = conn.get_uri()
    with mock.patch.dict(os.environ, AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets("POSTGRES_DB_YT_ELT")

@pytest.fixture
def dagbag():
    yield DagBag()

@pytest.fixture
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)

    return get_airflow_variable

@pytest.fixture
def postgres_connection():
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        yield conn
    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to Postgres: {e}")
    finally:
        if conn:
            conn.close()