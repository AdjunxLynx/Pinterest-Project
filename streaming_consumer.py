from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark
import ast


cfg = pyspark.SparkConf().setMaster("local[*]").setAppName("spark_example")
sc = SparkContext(conf=cfg)
spark = SparkSession(sc).builder.appName("spark_example").getOrCreate()


topic = "MyFirstKafkaTopic"
consumer = KafkaConsumer(topic, bootstrap_servers = "localhost:9092")


print("working")
for message in consumer:
    value = message.value.decode("ascii")
    value = ast.literal_eval(value)
    print(value)

    stream_file = open("stream_file.json", "w+")


    dataframe = spark.read.json("stream_file.json")
    dataframe.show()




    stream_file.close()
    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")