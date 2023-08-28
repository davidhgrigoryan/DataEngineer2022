from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from datetime import datetime, timedelta

# Airflow DAG settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 1,
    'retry_delay': timedelta(hours=3),
}

dag = DAG(
    'data_injection',
    default_args=default_args,
    description='DAG to import data from source files into raw tables',
    schedule_interval=timedelta(hours=4),
    catchup=False,
)

###################### LOADING RAW DATA ##################################################################
# Loading users
sql_file = open("/home/davo/airflow/dags/read_raw_users.sql", "r")
sql_query = sql_file.read()
sql_file.close()

import_users = BigQueryExecuteQueryOperator(
    task_id='import_users',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

# Loading videos
sql_file = open("/home/davo/airflow/dags/read_raw_videos.sql", "r")
sql_query = sql_file.read()
sql_file.close()
 
import_videos = BigQueryExecuteQueryOperator(
    task_id='import_videos',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

# Loading events
sql_file = open("/home/davo/airflow/dags/read_raw_events.sql", "r")
sql_query = sql_file.read()
sql_file.close()

import_events = BigQueryExecuteQueryOperator(
    task_id='import_events',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)
###################### LOADING RAW DATA.DONE #############################################################

###################### CLEAR BAD DATA ####################################################################

sql_file = open("/home/davo/airflow/dags/clear_bad_users.sql", "r")
sql_query = sql_file.read()
sql_file.close()

# Task to run SQL query in BigQuery
clear_bad_users = BigQueryExecuteQueryOperator(
    task_id='clear_bad_users',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

sql_file = open("/home/davo/airflow/dags/clear_bad_videos.sql", "r")
sql_query = sql_file.read()
sql_file.close()

# Task to run SQL query in BigQuery
clear_bad_videos = BigQueryExecuteQueryOperator(
    task_id='clear_bad_videos',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)
###################### CLEAR BAD DATA. DONE ################################################################

###################### CALC HASH ################################################################

sql_file = open("/home/davo/airflow/dags/add_calc_hash.sql", "r")
sql_query = sql_file.read()
sql_file.close()

# Task to run SQL query in BigQuery
calc_hash = BigQueryExecuteQueryOperator(
    task_id='calc_hash',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)
###################### CALC HASH. DONE ################################################################

###################### MERGING DATA       ################################################################
sql_file = open("/home/davo/airflow/dags/delete_duplicates_users.sql", "r")
sql_query = sql_file.read()
sql_file.close()

del_dupl_users = BigQueryExecuteQueryOperator(
    task_id='del_dupl_users',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

sql_file = open("/home/davo/airflow/dags/merge_users.sql", "r")
sql_query = sql_file.read()
sql_file.close()

merge_users = BigQueryExecuteQueryOperator(
    task_id='merge_users',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

sql_file = open("/home/davo/airflow/dags/delete_duplicates_videos.sql", "r")
sql_query = sql_file.read()
sql_file.close()

del_dupl_videos = BigQueryExecuteQueryOperator(
    task_id='del_dupl_videos',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

sql_file = open("/home/davo/airflow/dags/merge_videos.sql", "r")
sql_query = sql_file.read()
sql_file.close()

merge_videos = BigQueryExecuteQueryOperator(
    task_id='merge_videos',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)
###################### MERGING DATA. DONE ################################################################

move_users_files_to_archive = GCSToGCSOperator(
    task_id='move_users_files_to_archive',
    source_bucket='dgedu',
    source_object='rawdata/Users/*.csv',
    destination_bucket='dgedu',
    destination_object='rawdata/Archive/Users/',
    dag=dag,
    gcp_conn_id='google_cloud_default',
    move_object=True   
)

move_videos_files_to_archive = GCSToGCSOperator(
    task_id='move_videos_files_to_archive',
    source_bucket='dgedu',
    source_object='rawdata/Videos/*.csv',
    destination_bucket='dgedu',
    destination_object='rawdata/Archive/Videos/',
    dag=dag,
    gcp_conn_id='google_cloud_default',
    move_object=True   
)

move_events_files_to_archive = GCSToGCSOperator(
    task_id='move_events_files_to_archive',
    source_bucket='dgedu',
    source_object='rawdata/Events/*.jsonl',
    destination_bucket='dgedu',
    destination_object='rawdata/Archive/Events/',
    dag=dag,
    gcp_conn_id='google_cloud_default',
    move_object=True   
)

# Define task dependency
import_users >> import_videos >> import_events >> clear_bad_users >> clear_bad_videos >> calc_hash >> del_dupl_users >> del_dupl_videos >> merge_users >> merge_videos >> move_users_files_to_archive >> move_videos_files_to_archive >> move_events_files_to_archive
