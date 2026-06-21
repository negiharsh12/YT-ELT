from datawarehouse.data_loading import load_data
from datawarehouse.data_transformation import transform_date
from datawarehouse.data_modifications import insert_row, update_row, delete_rows
from datawarehouse.data_utils import create_schema, create_table, get_video_ids, get_conn_cursor, close_conn_cursor, get_all_rows

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def snapshot_table():
    schema = "snapshot"
    conn, cursor = None, None

    try:
        conn, cursor = get_conn_cursor()
        Yt_data_extract = load_data()

        # Create schema and table if not exists
        create_schema(schema)
        create_table(schema)

        # Get video id list from snapshot table to set for last lookup
        snapshot_video_id_list = set(get_video_ids(cursor, schema))

        total_video_ids = set()

        # upsert logic for snapshot table
        for row in Yt_data_extract:
            current_row_video_id = row['video_id']
            total_video_ids.add(current_row_video_id)

            # if video id is already present in snapshot then update
            if current_row_video_id in snapshot_video_id_list:
                update_row(conn, cursor, schema, row)
            # if video id is not present in snapshot then insert
            else:
                insert_row(conn, cursor, schema, row)
        
        # get difference of set between source file and data present in snapshot table
        video_ids_to_delete = set(snapshot_video_id_list) - total_video_ids

        # delete rows which were no present in source file
        if video_ids_to_delete:
            delete_rows(conn, cursor, schema, video_ids_to_delete)
        
        logger.info(f"Data processing completed for {schema}.{table} -> {len(Yt_data_extract)} records processed.")
    
    except Exception as e:
        logger.error(f"Error processing data for {schema}.{table}: {e}")
        raise e

    finally:
        if conn and cursor:
            close_conn_cursor(conn, cursor)

@task
def transform_table():
    transform_schema = "transform"
    snapshot_schema = "snapshot"
    conn, cursor = None, None

    try:
        conn, cursor = get_conn_cursor()

        # create schema and table if not exists
        create_schema(transform_schema)
        create_table(transform_schema)

        # get latest data from snapshot table
        snapshot_data = get_all_rows(cursor, snapshot_schema)

        # get video ids from transform table
        transform_video_id_list = set(get_video_ids(cursor, transform_schema))
        snapshot_ids = set()

        # upsert logic for transform table
        for row in snapshot_data:
            current_row_video_id = row['Video_ID']
            snapshot_ids.add(current_row_video_id)

            # convert ISO 8601 duration to normal time for video duration column
            modified_row = transform_date(row)

            # if video id is already present in transform then update
            if current_row_video_id in transform_video_id_list:
                update_row(conn, cursor, transform_schema, modified_row)
            # if video id is not present in transform then insert
            else:
                insert_row(conn, cursor, transform_schema, modified_row)

        # get difference of set between transform and snapshot table
        video_ids_to_delete = transform_video_id_list - snapshot_ids

        # delete rows which were no present in source file
        if video_ids_to_delete:
            delete_rows(conn, cursor, transform_schema, video_ids_to_delete)
        
        logger.info(f"Data processing completed for {transform_schema}.{table} -> {len(snapshot_data)} records processed.")
    
    except Exception as e:
        logger.error(f"Error processing data for {transform_schema}.{table}: {e}")
        raise e

    finally:
        if conn and cursor:
            close_conn_cursor(conn, cursor)