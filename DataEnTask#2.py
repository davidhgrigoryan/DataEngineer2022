import json
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

from mylogger import log_entry

regex_url = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\'.,<>?«»“”‘’]))"
regex_eml = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
data_path = "C:\\Work\\Data Engineer\\Sample Data"
cloud_path_df = "gs://da_edu2022q4_dg/dataframe"
cloud_path_rdd = "gs://da_edu2022q4_dg/rdd"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def regex_checker(string, regex_value):
    url = re.findall(regex_value, string)
    url = [x[0] for x in url]
    if url:
        return True
    else:
        return False


spark = SparkSession \
    .builder \
    .config("temporaryGcsBucket", "test_project_bucket_pyspark") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.gs.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile",
            r"C:\Work\Data Engineer\data-n-analytics-edu-345714-360a930480bd.json") \
    .master("local[1]") \
    .appName("pyspark_job2") \
    .getOrCreate()

########################################################################################################################
########################################################################################################################
########################################################################################################################

# Dataframe model
# Read events JSON file into dataframe
log_entry("Reading events JSON file into the dataframe")
df_events_raw = spark.read.json(data_path + "\\events.jsonl")
# There are 2 "types" or rows in the JSON file, so we need to parse them differently
# {"user_id":4,"video_id":100,"event":"like","timestamp":1642943347}
# {"events":[{"user_id":2,"video_id":100,"event":"like","timestamp":1642953347},{"user_id":2,"video_id":100,"event":"commented","timestamp":1642954347,"comment":"oh, it"s so cute!!!!"}]}

log_entry("Splitting for different types of records")
# Split into 2 dataframes
df_events_type_1 = df_events_raw.where(col("events").isNull())
df_events_type_2 = df_events_raw.where(col("events").isNotNull()).select("events")

df_events_1 = df_events_type_1.drop(col("events"))
# Expanding
log_entry("Expanding 2nd type...")
df_events_type_2 = df_events_type_2.withColumn("events", explode("events")).select("*", "events.*").select("comment",
                                                                                                           "event",
                                                                                                           "timestamp",
                                                                                                           "user_id",
                                                                                                           "video_id")
# Now, union 2 dataframes into the single one
log_entry("Performing union")
df_events = df_events_1.unionByName(df_events_type_2, allowMissingColumns=True)
# Convert timestamp to user-friendly format
log_entry("Convert timestamp to user-friendly format")
df_events = df_events.withColumn("timestamp", col("timestamp").cast(TimestampType()))
log_entry("Saving events to the cloud storage")
df_events.write.format("parquet").partitionBy("event").mode("overwrite").option("path",
                                                                                cloud_path_df + "/events").save()
log_entry("Done. Events with DF")
######################################################
# Read videos CSV file into dataframe
log_entry("Reading videos CSV file into the dataframe")
df_videos = spark.read.options(header="True").csv(data_path + "\\videos.csv")
# Push videos with correct URL
log_entry("Saving valid videos to the cloud storage")
df_valid_videos = df_videos.filter(df_videos["url"].rlike(regex_url))
df_non_valid_videos = df_videos.subtract(df_valid_videos)

df_valid_videos.write.format("parquet").mode("overwrite").option("path",
                                                                 cloud_path_df + "/videos").save()

# Push videos with incorrect URL
log_entry("Saving invalid videos to the cloud storage")
df_non_valid_videos.write.format("parquet").mode("overwrite").option("path",
                                                                     cloud_path_df + "/videos_non_valid").save()
log_entry("Done. Videos with DF")
######################################################
# Read users CSV file into dataframe
log_entry("Reading users CSV file into the dataframe")
df_users = spark.read.options(header="True").csv(data_path + "\\users.csv")

df_valid_users = df_users.filter(
    (~df_users["fname"].isNull()) & (~df_users["lname"].isNull()) & (~df_users["email"].isNull()) & df_users[
        "email"].rlike(regex_eml))
df_non_valid_users = df_videos.subtract(df_valid_videos)
log_entry("Saving valid users to the cloud storage")
df_valid_users.write.format(
    "parquet").mode("overwrite").option("path",
                                        "gs://da_edu2022q4_dg/dataframe/users").save()
log_entry("Saving invalid users to the cloud storage")
df_non_valid_users.write.format(
    "parquet").mode("overwrite").option("path",
                                        cloud_path_df + "/users_non_valid").save()

log_entry("Done. Users with DF")
log_entry("Done with datasets")
########################################################################################################################
########################################################################################################################
########################################################################################################################
log_entry("Let's do the same with RDD")
# RDD model

# Read videos CSV file into RDD
log_entry("Reading videos CSV file into the RDD")
rdd_videos_raw = spark.sparkContext.textFile(data_path + "\\videos.csv")
# Clean up
log_entry("Splitting...")
rdd_videos = rdd_videos_raw.map(lambda x: x.split(","))
videos_header = rdd_videos.first()
rdd_data_videos = rdd_videos.filter(lambda row: row != videos_header)

rdd_videos_correct = rdd_data_videos.filter(lambda x: regex_checker(x[2], regex_url))
rdd_videos_wrong = rdd_data_videos.filter(lambda x: not regex_checker(x[2], regex_url))

# Push RDDs to the cloud
log_entry("Saving valid videos to the cloud storage")
rdd_videos_correct.toDF(videos_header).write.format("parquet").mode("overwrite").option("path",
                                                                                        cloud_path_rdd + "/videos_rdd").save()

# A little beet hardcode cheating with the array size here...
log_entry("Saving invalid videos to the cloud storage")
rdd_videos_wrong.toDF(videos_header[:5]).write.format("parquet").mode("overwrite").option("path",
                                                                                          cloud_path_rdd + "/videos_rdd_non_valid").save()
log_entry("Done. Videos with RDD")
# Read users CSV file into RDD
log_entry("Reading users CSV file into the RDD")
rdd_users_raw = spark.sparkContext.textFile(data_path + "\\users.csv")
# Clean up
rdd_users = rdd_users_raw.map(lambda x: x.split(","))
users_header = rdd_users.first()
rdd_data_users = rdd_users.filter(lambda row: row != users_header)

rdd_users_correct = rdd_data_users.filter(lambda r: regex_checker(r[3], regex_eml))
rdd_users_wrong = rdd_data_users.filter(lambda r: not regex_checker(r[3], regex_eml))
# Push RDDs to the cloud
log_entry("Saving valid users to the cloud storage")
rdd_users_correct.toDF(users_header).write.format("parquet").mode("overwrite").option("path",
                                                                                      cloud_path_rdd + "/users_rdd").save()
log_entry("Saving invalid users to the cloud storage")
rdd_users_wrong.toDF(users_header).write.format("parquet").mode("overwrite").option("path",
                                                                                    cloud_path_rdd + "/users_rdd_non_valid").save()
log_entry("Done. Users with RDD")

# Read events JSON file into RDD
# There are 2 "types" or rows in the JSON file, so we need to parse them differently
# {"user_id":4,"video_id":100,"event":"like","timestamp":1642943347}
# {"events":[{"user_id":2,"video_id":100,"event":"like","timestamp":1642953347},{"user_id":2,"video_id":100,"event":"commented","timestamp":1642954347,"comment":"oh, it"s so cute!!!!"}]}
log_entry("Reading events JSON file into the RDD")
rdd_events_raw = spark.sparkContext.textFile(data_path + "\\events.jsonl")
# Split and expand
log_entry("Splitting and expand...")
events_rdd = rdd_events_raw.filter(lambda x: ("events" not in x)).map(lambda x: json.loads(x))
events_rdd2 = rdd_events_raw.filter(lambda x: ("events" in x)).map(lambda x: json.loads(x)).flatMap(
    lambda x: x["events"])

# Union into the single dataframe
log_entry("Union")
events_rdd = events_rdd.union(events_rdd2)
# Convert timestamp to user-friendly format
log_entry("Convert timestamp to user-friendly format")
df_events_rdd = events_rdd.toDF().withColumn("timestamp", col("timestamp").cast(TimestampType()))
log_entry("Saving events to the cloud storage")
df_events.write.format("parquet").partitionBy("event").mode("overwrite").option("path",
                                                                                cloud_path_rdd + "/events").save()
log_entry("Done. Events with RDD")
log_entry("Done with RDD")
