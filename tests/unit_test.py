def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"

def test_channel_handle(channel_handle):
    assert channel_handle == "MOCK_HANDLE1234"

def test_postgres_connection(mock_postgres_connection):
    conn = mock_postgres_connection
    assert conn.login == "mock_user"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db"

def test_dags_integrity(dagbag):
    # No import errors in DAG files
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("=========")
    print(dagbag.import_errors)

    # Check if the expected DAGs are loaded
    expected_dags = ["fetch_yt_data_extract", "yt_load_to_postgres", "yt_data_quality_check"]
    loaded_dag_ids = list(dagbag.dags.keys())
    print("=========")
    print(dagbag.dags.keys())

    for dag_id in expected_dags:
        assert dag_id in loaded_dag_ids, f"DAG '{dag_id}' not found in the DAG bag."

    # Count of DAGs loaded
    assert dagbag.size() == 3, f"Expected {len(expected_dags)} DAGs, but found {len(loaded_dag_ids)}."
    print("=========")
    print(dagbag.size())

    # Check expected number of tasks in each DAG
    expected_task_counts = {
        "fetch_yt_data_extract": 5,
        "yt_load_to_postgres": 3,
        "yt_data_quality_check": 2
    }
    print("=========")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts.get(dag_id)
        if expected_count is not None:
            actual_count = len(dag.tasks)
            assert actual_count == expected_count, f"DAG '{dag_id}' has {actual_count} tasks, expected {expected_count}."
            print(dag_id, len(dag.tasks))