import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession \
    .builder \
    .config("temporaryGcsBucket", "test_project_bucket_pyspark") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.gs.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile",
            r"C:\Work\Data Engineer\data-n-analytics-edu-345714-360a930480bd.json") \
    .master("local[1]") \
    .appName("pyspark_job3") \
    .getOrCreate()

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + "Reading users data from parquet from GS")
df_users = spark.read.parquet("gs://da_edu2022q4_dg/dataframe/users/*")
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + "Reading videos data from parquet from GS")
df_videos = spark.read.parquet("gs://da_edu2022q4_dg/dataframe/videos/*")
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + "Reading events data from parquet from GS")
df_events = spark.read.option("mergeSchema", "true").parquet("gs://da_edu2022q4_dg/dataframe/events/")

df_likes = df_events.where(col("event") == "like")
df_videos = df_videos.withColumn("creation_timestamp", from_unixtime(col("creation_timestamp")))

# df_videos.join(df_likes, df_videos.id == df_likes.video_id, "inner").show(truncate=False)
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + "Videos by likes count")
df_videos.join(df_likes, df_videos.id == df_likes.video_id, "inner").groupBy("name").count().show(truncate=False)
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + "Videos by likes by date")
df_videos.join(df_likes, df_videos.id == df_likes.video_id, "inner").withColumn("event_date", to_date(col("timestamp"))).groupBy("name", "event_date").count().show(
    truncate=False)
