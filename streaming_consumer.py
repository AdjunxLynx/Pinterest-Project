from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json, col, regexp_replace, when, explode, split, sum, desc, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import pyspark, ast, findspark, os
import multiprocessing
import json
findspark.init()

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 --driver-class-path /Users/kamil/Downloads/postgresql-42.2.27.jre7.jar streaming_consumer.py pyspark-shell"
kafka_topic_name = 'MyFirstKafkaTopic'
kafka_bootstrap_server = 'localhost:9092'

# We should always start with session in order to obtain
# context and session if needed

#ssc = StreamingContext(session.sparkContext, batchDuration=30)

schema = StructType([
    StructField("index", IntegerType()),
    StructField("unique_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("poster_name", StringType()),
    StructField("follower_count", StringType()),
    StructField("tag_list", StringType()),
    StructField("is_image_or_video", StringType()),
    StructField("image_src", StringType()),
    StructField("save_location", StringType()),
    StructField("category", StringType())
])


url = "jdbc:postgresql://192.168.8.107:5432/pinterest_streaming"
login = {"user": "admin", "password": "Myacount1"}


spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

category_list = ['event-planning', 'art', 'home-decor', 'diy-and-crafts', 'education', 'christmas', 'mens-fashion', 'tattoos', 'vehicles', 'travel', 'beauty', 'quotes', 'finance']

json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data"))
json_df = json_df.select(col("data.*"))
json_df = json_df.withColumn("follower_count", \
    when(col("follower_count").contains("k"), \
        regexp_replace(col("follower_count"), "k", "").cast("integer") * 1000) \
    .when(col("follower_count").contains("m"), \
        regexp_replace(col("follower_count"), "m", "").cast("integer") * 1000000) \
    .otherwise(col("follower_count").cast("integer")))

json_df = json_df.withColumn("category", \
    when(col("category").isin(category_list), col("category")) \
    .otherwise(None))


json_df = json_df.withColumn("category_outlier", \
    when(col("category").isNull(), col("category")) \
    .otherwise(None))
json_df = json_df.drop("downloaded")

json_df = json_df.withColumn("category", split("category", ","))


existing_data_df = spark.read.jdbc(url=url, table="experimental_data", properties=login)

joined_json_df = json_df.join(existing_data_df, ["unique_id"], "leftanti")


def write_to_postgresql(batch_df, batch_id):
    batch_id_df = spark.read.jdbc(url=url, table="batch_id", properties=login)
    latest_batch_id = batch_id_df.first()["value"]
    new_batch_id = latest_batch_id + 1
    batch_id_df = spark.createDataFrame([(new_batch_id,)], ["value"])
    batch_df = batch_df.withColumn("batch_id", lit(new_batch_id))
    batch_id_df.write.jdbc(url=url, table="batch_id", mode="overwrite", properties=login)
    existing_data_df = spark.read.jdbc(url=url, table="experimental_data", properties=login)
    
    batch_df = batch_df.dropDuplicates(["unique_id"])
    batch_df = batch_df.join(existing_data_df, ["unique_id"], "leftanti")



    batch_df.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", url) \
        .option("dbtable", "experimental_data") \
        .option("user", login['user']) \
        .option("password", login['password']) \
        .mode("append") \
        .option("isolationLevel", "NONE") \
        .option("truncate", "false") \
        .option("fetchSize", "1000") \
        .option("escapeQuotes", "true") \
        .option("rewriteBatchedStatements", "true") \
        .option("continueOnError", "true") \
        .save()
    
    batch_id_df = spark.createDataFrame([(new_batch_id,)], ["value"])
    batch_id_df.write.jdbc(url=url, table="batch_id", mode="overwrite", properties=login)
    
joined_json_df.repartition(1).writeStream \
    .foreachBatch(write_to_postgresql) \
    .start() \
    .awaitTermination()

