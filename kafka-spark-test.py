from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import pyspark, ast, findspark, os

import multiprocessing
findspark.init()

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 kafka-spark-test.py pyspark-shell"
kafka_topic_name = 'MyFirstKafkaTopic'
kafka_bootstrap_server = 'localhost:9092'

spark = SparkSession \
    .builder \
    .appName("Kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data_df = spark \
    .readStream \
    .format("Kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

#data_df.writeStream.outputMode("append").format("console").start().awaitTermination()
data_df = data_df.selectExpr("CAST(value as STRING)")
data_df.writeStream.format("console").outputMode("append").start().awaitTermination()
