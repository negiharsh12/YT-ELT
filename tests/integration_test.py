import requests
import pytest
import psycopg2

def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("YOUTUBE_API_KEY")
    channel_handle = airflow_variable("YOUTUBE_CHANNEL_HANDLE")

    url = (
        "https://youtube.googleapis.com/youtube/v3/channels"
        f"?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    )

    try:
        response = requests.get(url)
        assert response.status_code == 200
    except requests.RequestException as e:
        pytest.fail(f"API request failed: {e}")

def test_postgres_connection(postgres_connection):
    cursor = None
    try:
        cursor = postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        assert result[0] == 1
    except psycopg2.Error as e:
        pytest.fail(f"Postgres connection test failed: {e}")
    finally:
        if cursor:
            cursor.close()