from kafka import KafkaConsumer
import boto3
import findspark
import json
import multiprocessing
import pyspark
import os


topic = "MyFirstKafkaTopic"
consumer = KafkaConsumer(topic, bootstrap_servers = "localhost:9092")

s3_resource = boto3.resource("s3")
bucket = s3_resource.Bucket('pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e')

findspark.init()
cfg = (
    pyspark.SparkConf()
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    .setAppName("TestApp")
    .set("spark.eventlog.enable", False)
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    .setIfMissing("spark.executor.memory", "1g")
)
#print(cfg.get("spark.executor.memory"))
#print(cfg.toDebugString())
session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()



print("working")
i = 0
for message in consumer:
    new_message = str(message.value.decode("ascii"))
    filename = ("message_" + str(i) + ".json")

    payload = new_message
    response = bucket.put_object(Body= payload, Key = filename)
    i += 1
    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")