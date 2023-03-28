from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when


import os
import boto3

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"

conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

sc=SparkContext(conf=conf)


schema = StructType([
    StructField("category", StringType(), True),
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
    StructField("_corrupt_record", StringType(), True)
])



# Configure the setting to read from the S3 bucket
accessKeyId="AKIAWU5CQUVY2FKMX5BG"
secretAccessKey="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"
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider', 'fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain') # Allows the package to authenticate with AWS

# Create our Spark session
spark=SparkSession(sc)

session = boto3.Session(aws_access_key_id = accessKeyId, aws_secret_access_key = secretAccessKey)



s3 = session.resource("s3")
bucket_name = ("pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e")
my_bucket= s3.Bucket(bucket_name)
for obj in my_bucket.objects.all():
    print(obj.key)
    print("@@@@@@@@@@@@@@@@@@@@@@@@")



# Read from the S3 bucket
df = spark.read.option("mode", "PERMISSIVE").schema(schema).option("header", True).option("columnNameOfCorruptRecord", "_corrupt_record").json("s3a://pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e/pinterest_data.json").cache()
# You may want to change this to read csv depending on the files your reading from the bucket
# s3a://pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e/pinterest_data.json

df = df.withColumn('follower_count_new', when(df.follower_count.endswith('k'), regexp_replace(df.follower_count,'k','000'))
                   .when(df.follower_count.endswith('m'), regexp_replace(df.follower_count,'m','000000'))
                   .otherwise(df.follower_count))

df.withColumn("follower_count_new", df.follower_count.cast(IntegerType()))


df.printSchema()
df.show()