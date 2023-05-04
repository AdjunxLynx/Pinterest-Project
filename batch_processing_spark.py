""""Script to download all .json files from an AWS datalake, clean it and then to upload to a PostGreSQL server for long term storage"""

#import statements
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import when, regexp_replace
import time
import os
import yaml
from yaml.loader import SafeLoader

#pyspark configurations
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 --driver-class-path /Users/kamil/Downloads/postgresql-42.2.27.jre7.jar batch_processing_spark.py pyspark-shell"
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')
sc=SparkContext(conf=conf)

#connection data
with open("Priv.yaml") as priv:
    priv_data = yaml.load(priv, Loader=SafeLoader)
url = priv_data["url"]
login = {"user": priv_data["PSQL_user"], "password": priv_data["PSQL_password"]}
accessKeyId = priv_data["accessKeyId"]
secretAccessKey = priv_data["secretAccessKey"]


hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider', 'fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain') # Allows the package to authenticate with AWS


#DataFrame format
schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", IntegerType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

spark=SparkSession(sc)

before = time.time()

def clean_df(df):
    """this function returns a cleaned dataframe using pyspark"""
    df = df.drop('downloaded')
    df = df.distinct()
    image_or_video = ['video', 'image', 'multi-video(story page format)']
    category_list = ['event-planning', 'art', 'home-decor', 'diy-and-crafts', 'education', 'christmas', 'mens-fashion', 'tattoos', 'vehicles', 'travel', 'beauty', 'quotes', 'finance']
    df = df.filter(df.is_image_or_video.isin(image_or_video))
    df = df.filter(df.category.isin(category_list))

    #column cleaning operations
    df = df.withColumn('follower_count', when(df.follower_count.endswith('k'), regexp_replace(df.follower_count,'k','000'))
                   .when(df.follower_count.endswith('M'), regexp_replace(df.follower_count,'M','000000'))
                   .otherwise(df.follower_count))
    df = df.withColumn('description', when(df.description == 'No description available Story format',  'null')
                   .otherwise(df.description))
    df = df.withColumn('tag_list', when(df.tag_list.contains(','), regexp_replace(df.tag_list, ',', ''))
                   .otherwise(df.tag_list))
    df.withColumn("follower_count", df.follower_count.cast(IntegerType()))
    
    return df


# Read from the S3 bucket
df = spark.read.option("mode", "PERMISSIVE") \
    .schema(schema) \
    .option("header", True) \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("s3a://pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e/*.json") \
    .cache()

# s3a://pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e/pinterest_data.json

###DF cleaning operations
cleaned_data = clean_df(df)
#print and upload to PostGreSQL
df.show()
df.write.jdbc(url=url, table="long_term_user_data", mode="overwrite", properties=login)

after = time.time()
print('took ' + str(round(after-before)) +  ' seconds')
quit()