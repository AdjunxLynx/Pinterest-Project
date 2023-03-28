from kafka import KafkaConsumer
import boto3
import findspark
import json
import multiprocessing
import pyspark
import os


topic = "MyFirstKafkaTopic"
consumer = KafkaConsumer(topic, bootstrap_servers = "localhost:9092")

s3_client = boto3.client("s3")


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
    created = False
    new_message = str(message.value.decode("ascii"))


    with open("s3_data.json", "w+") as s3_data:
        try:
            s3_client.download_fileobj("pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e", "pinterest_data.json", s3_data)
            file_data = s3_data.read()
        except:
            file_data = "[]"
            created = True
            print("No file to read")
    try:
        if file_data[0] != "[":
            file_data = "[" + file_data

        if file_data[-1] == "]":
            file_data = file_data[0::-2]

        if not created:
            file_data = file_data + ", "
        file_data = file_data + new_message
        file_data = file_data + "]"
    except Exception as E:
        print(E)


    with open("s3_data.json", "w+") as s3_data:
        print(file_data)
        dumped = json.loads(file_data)
        json.dump(dumped, s3_data)

    with open("s3_data.json", "r+") as s3_data:
        response = s3_client.upload_file("s3_data.json", "pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e", "pinterest_data.json")
    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")