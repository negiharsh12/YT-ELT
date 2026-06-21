from datetime import timedelta

from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dataquality.soda import yt_etl_data_quality_check
import pendulum
from api.video_stats import get_channel_playlist_id, get_video_ids, get_video_stats, save_to_json

from datawarehouse.datawarehouse_main import snapshot_table, transform_table

# define local timezone
local_tz = pendulum.timezone("Asia/Kolkata")
snapshot_layer = "snapshot"
transform_layer = "transform"

# default args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "on_failure_callback": [
        SmtpNotifier(
            from_email="airflow@yourdomain.com", 
            to="negiharsh12@gmail.com"
        )
    ],
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2026, 5, 1, tz=local_tz),
    # "end_date": pendulum.datetime(2030, 6, 30, tzinfo=local_tz),
}

with DAG(
    dag_id = "fetch_yt_data_extract",
    default_args = default_args,
    description = "DAG to fetch YouTube channel data and save it as JSON",
    schedule=CronTriggerTimetable("0 0 * * *", timezone=local_tz),
    catchup = False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    
    # DAG tasks
    playlist_id = get_channel_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_stats = get_video_stats(video_ids)
    save_data_to_json = save_to_json(video_stats)
    trigger_yt_load_to_postgres_DAG = TriggerDagRunOperator(
        task_id="trigger_yt_load_to_postgres",
        trigger_dag_id="yt_load_to_postgres",
    )

    # Dependencies
    playlist_id >> video_ids >> video_stats >> save_data_to_json >> trigger_yt_load_to_postgres_DAG

with DAG(
    dag_id = "yt_load_to_postgres",
    default_args = default_args,
    description = "DAG to process data in snapshot and transform tables in Postgres",
    # schedule=CronTriggerTimetable("0 1 * * *", timezone=local_tz),
    schedule = None,
    catchup = False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    
    # DAG tasks
    update_snapshot_table = snapshot_table()
    update_transform_table = transform_table()
    trigger_yt_data_quality_check_DAG = TriggerDagRunOperator(
        task_id="trigger_yt_data_quality_check",
        trigger_dag_id="yt_data_quality_check",
    )

    # Dependencies
    update_snapshot_table >> update_transform_table >> trigger_yt_data_quality_check_DAG

with DAG(
    dag_id = "yt_data_quality_check",
    default_args = default_args,
    description = "DAG to perform data quality checks on Snapshot and Transform layers using Soda",
    # schedule=CronTriggerTimetable("30 1 * * *", timezone=local_tz),
    schedule = None,
    catchup = False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
) as dag:
    
    # DAG tasks
    Data_quality_check_snapshot = yt_etl_data_quality_check(snapshot_layer)
    Data_quality_check_transform = yt_etl_data_quality_check(transform_layer)

    # Dependencies
    Data_quality_check_snapshot >> Data_quality_check_transform