from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

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

# Define task

# SQL query to execute in BigQuery
sql_query = """TRUNCATE TABLE
  `cosmic-rarity-392606.DG_Task4.events_raw`;
TRUNCATE TABLE
  `cosmic-rarity-392606.DG_Task4.users_raw`;
TRUNCATE TABLE
  `cosmic-rarity-392606.DG_Task4.videos_raw`;
LOAD DATA OVERWRITE
  `cosmic-rarity-392606.DG_Task4.users_raw`
FROM FILES ( format = 'CSV',
    uris = ['gs://dgedu/rawdata/Users/*.csv']);
LOAD DATA OVERWRITE
  `cosmic-rarity-392606.DG_Task4.videos_raw`
FROM FILES ( format = 'CSV',
    uris = ['gs://dgedu/rawdata/Videos/*.csv']);
LOAD DATA OVERWRITE
  `cosmic-rarity-392606.DG_Task4.events_raw`
FROM FILES ( format = 'JSON',
    uris = ['gs://dgedu/rawdata/Events/*.jsonl'])"""


# Task to run SQL query in BigQuery
import_data = BigQueryExecuteQueryOperator(
    task_id='import_data',
    sql=sql_query,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

sql_query2 = """EXPORT DATA
  OPTIONS ( uri = 'gs://dgedu/rawdata/Invalid/users_*.csv',
    format = 'CSV',
    OVERWRITE = TRUE,
    header = TRUE,
    field_delimiter = ',') AS (
  SELECT
    *
  FROM
    `cosmic-rarity-392606.DG_Task4.users_raw`
  WHERE
    NOT REGEXP_CONTAINS(email, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    OR fname IS NULL
    OR lname IS NULL
    OR email IS NULL);
DELETE
FROM
  `cosmic-rarity-392606.DG_Task4.users_raw`
WHERE
  NOT REGEXP_CONTAINS(email, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
  OR fname IS NULL
  OR lname IS NULL
  OR email IS NULL; -- EXTRACT invalid videos
EXPORT DATA
  OPTIONS ( uri = 'gs://dgedu/rawdata/Invalid/videos_*.csv',
    format = 'CSV',
    OVERWRITE = TRUE,
    header = TRUE,
    field_delimiter = ',') AS (
  SELECT
    *
  FROM
    `cosmic-rarity-392606.DG_Task4.videos_raw`
  WHERE
    NOT REGEXP_CONTAINS(url, r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\'.,<>?«»“”‘’]))") );
DELETE
FROM
  `cosmic-rarity-392606.DG_Task4.videos_raw`
WHERE
  NOT REGEXP_CONTAINS(url, r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\'.,<>?«»“”‘’]))")
;
alter table `cosmic-rarity-392606.DG_Task4.users_raw`
add column datahash bytes;

update `cosmic-rarity-392606.DG_Task4.users_raw` set datahash=SHA256(upper(concat(FNAME, LNAME, EMAIL, COUNTRY, SUBSCRIPTION))) WHERE 42=42;

alter table `cosmic-rarity-392606.DG_Task4.videos_raw`
add column datahash bytes;

update `cosmic-rarity-392606.DG_Task4.videos_raw` set datahash=SHA256(upper(concat(NAME, URL, CREATOR_ID, PRIVATE))) WHERE 42=42;"""


# Task to run SQL query in BigQuery
process_data = BigQueryExecuteQueryOperator(
    task_id='process_data',
    sql=sql_query2,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

sql_merge_users = """DELETE
FROM
  `cosmic-rarity-392606.DG_Task4.users_raw`
WHERE
  EXISTS (
  SELECT
    1
  FROM
    `cosmic-rarity-392606.DG_Task4.fact_users` FU
  WHERE
    `cosmic-rarity-392606.DG_Task4.users_raw`.datahash=FU.datahash);


MERGE
  `cosmic-rarity-392606.DG_Task4.fact_users` AS output
USING
  (
  SELECT
    src.ID AS PSEUDO_ID,
    src.*
  FROM
    cosmic-rarity-392606.DG_Task4.users_raw AS src

  UNION ALL
  
  SELECT
    NULL AS PSEUDO_ID,
    dup.*
  FROM
    `cosmic-rarity-392606.DG_Task4.users_raw` AS dup
  INNER JOIN
    `cosmic-rarity-392606.DG_Task4.fact_users` AS trg
  ON
    dup.ID = trg.USER_ID
  WHERE
    trg.effective_end = '9999-01-01'
    AND trg.EFFECTIVE_START < TIMESTAMP_SECONDS(dup.updated_timestamp) ) AS input
ON
  input.PSEUDO_ID = output.USER_ID 
  WHEN NOT MATCHED THEN INSERT (USER_KEY, USER_ID, FNAME, LNAME, EMAIL, COUNTRY, SUBSRIPTION, EFFECTIVE_START, EFFECTIVE_END, ACTIVE) 
  VALUES ( GENERATE_UUID(),input.ID,input.fname,input.lname,input.email,input.country,CAST(input.subscription AS integer),TIMESTAMP_SECONDS(input.updated_timestamp),'9999-01-01',TRUE ) -------------------------------
  WHEN MATCHED
  AND output.effective_end = '9999-01-01'
  AND output.effective_start < TIMESTAMP_SECONDS(input.updated_timestamp) THEN
UPDATE
SET
  ACTIVE = FALSE,
  effective_end = TIMESTAMP_SECONDS(input.updated_timestamp) 

  WHEN MATCHED
  AND output.effective_end <> '9999-01-01'
  AND output.effective_start < TIMESTAMP_SECONDS(input.updated_timestamp)
  AND output.effective_end > TIMESTAMP_SECONDS(input.updated_timestamp)
  AND output.ACTIVE=FALSE THEN
UPDATE
SET
  effective_end = TIMESTAMP_SECONDS(input.updated_timestamp);
  
  update `cosmic-rarity-392606.DG_Task4.fact_users` set datahash=SHA256(upper(concat(FNAME, LNAME, EMAIL, COUNTRY, SUBSRIPTION))) where datahash is NULL;"""

merge_users_step = BigQueryExecuteQueryOperator(
    task_id='merge_users',
    sql=sql_merge_users,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

sql_merge_videos = """DELETE
FROM
  `cosmic-rarity-392606.DG_Task4.videos_raw`
WHERE
  EXISTS (
  SELECT
    1
  FROM
    `cosmic-rarity-392606.DG_Task4.fact_videos` FV
  WHERE
    `cosmic-rarity-392606.DG_Task4.videos_raw`.datahash=FV.datahash);


MERGE
  `cosmic-rarity-392606.DG_Task4.fact_videos` AS output
USING
  (
  SELECT
    src.ID AS PSEUDO_ID,
    src.*
  FROM
    `cosmic-rarity-392606.DG_Task4.videos_raw` AS src

  UNION ALL
  
  SELECT
    NULL AS PSEUDO_ID,
    dup.*
  FROM
    `cosmic-rarity-392606.DG_Task4.videos_raw` AS dup
  INNER JOIN
    `cosmic-rarity-392606.DG_Task4.fact_videos` AS trg
  ON
    dup.ID = trg.VIDEO_ID
  WHERE
    trg.effective_end = '9999-01-01'
    AND trg.EFFECTIVE_START < TIMESTAMP_SECONDS(dup.updated_timestamp) ) AS input
ON
  input.PSEUDO_ID = output.VIDEO_ID 
  WHEN NOT MATCHED THEN INSERT (VIDEO_KEY, VIDEO_ID, NAME, URL, CREATOR_ID, PRIVATE, EFFECTIVE_START, EFFECTIVE_END, ACTIVE) 
  VALUES ( GENERATE_UUID(),input.ID,input.name,input.url,input.creator_id,input.private,TIMESTAMP_SECONDS(input.updated_timestamp),'9999-01-01',TRUE ) 
  WHEN MATCHED
  AND output.effective_end = '9999-01-01'
  AND output.effective_start < TIMESTAMP_SECONDS(input.updated_timestamp) THEN
UPDATE
SET
  ACTIVE = FALSE,
  effective_end = TIMESTAMP_SECONDS(input.updated_timestamp) 
  WHEN MATCHED
  AND output.effective_end <> '9999-01-01'
  AND output.effective_start < TIMESTAMP_SECONDS(input.updated_timestamp)
  AND output.effective_end > TIMESTAMP_SECONDS(input.updated_timestamp)
  AND output.ACTIVE=FALSE THEN
UPDATE
SET
  effective_end = TIMESTAMP_SECONDS(input.updated_timestamp);
  
  update `cosmic-rarity-392606.DG_Task4.fact_videos` set datahash=SHA256(upper(concat(NAME, URL, CREATOR_ID, PRIVATE))) WHERE datahash is NULL;"""

merge_videos_step = BigQueryExecuteQueryOperator(
    task_id='merge_videos',
    sql=sql_merge_videos,
    gcp_conn_id='google_cloud_default',  # Airflow connection ID for BigQuery
    dag=dag, 
    use_legacy_sql=False
)

# Define task dependency
import_data >> process_data >> merge_users_step >> merge_videos_step
