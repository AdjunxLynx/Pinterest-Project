"""Script to consume and upload all data sent to Kafka Consumer to an AWS datalake"""

#all import statements
from kafka import KafkaConsumer
import boto3
import findspark
import multiprocessing
import pyspark
import os

#connection data
topic = "PinterestPosts"
consumer = KafkaConsumer(topic, bootstrap_servers = "localhost:9092")

s3_resource = boto3.resource("s3")
bucket = s3_resource.Bucket('pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e')

#spark config
findspark.init()
cfg = (
    pyspark.SparkConf()
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    .setAppName("TestApp")
    .set("spark.eventlog.enable", False)
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    .setIfMissing("spark.executor.memory", "1g")
)
session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

#loop to read and upload consumer data
print("working")
i = 0
for message in consumer:
    """Loops through each message sent to the Kafka Consumer and uploads it to an AWS bucket"""
    new_message = str(message.value.decode("ascii"))
    filename = ("message_" + str(i) + ".json")

    payload = new_message
    response = bucket.put_object(Body= payload, Key = filename)
    i += 1
    print("End of Upload batch")