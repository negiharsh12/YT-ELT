import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_row(conn, cursor, schema, row):
    try:

        if schema == "snapshot":
            video_id = 'video_id';

            insert_sql = f"""
                    INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                    VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                """      
            cursor.execute(insert_sql, row)
        else:
            video_id = 'Video_ID';

            insert_sql = f"""
                    INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                    VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s);
                """
            cursor.execute(insert_sql, row)
        
        conn.commit()
        logger.info(f"Row inserted successfully with Video_ID: {row[video_id]}")
    
    except Exception as e:
        logger.error(f"Error inserting row with Video_ID: {row[video_id]}")
        raise e

def update_row(conn, cursor, schema, row):
    try:
        if schema == "snapshot":
            video_id = "video_id"
            upload_date = "publishedAt"
            video_title = "title"
            video_views = "viewCount"
            likes_count = "likeCount"
            comments_count = "commentCount"
        else:
            video_id = "Video_ID"
            upload_date = "Upload_Date"
            video_title = "Video_Title"
            video_views = "Video_Views"
            likes_count = "Likes_Count"
            comments_count = "Comments_Count"

        cursor.execute(
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s, 
                "Likes_Count" = %({likes_count})s, 
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """,
            row,
        )
        
        conn.commit()
        logger.info(f"Row updated successfully with Video_ID: {row[video_id]}")
    
    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]}")
        raise e
    
def delete_rows(conn, cursor, schema, video_id_list):
    try:
        cursor.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" = ANY(%s);
            """,
            (video_id_list,),
        )
        
        conn.commit()
        logger.info(f"Rows deleted successfully with Video_IDs: {video_id_list}")
    
    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs: {video_id_list}")
        raise e