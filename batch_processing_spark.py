from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when, regexp_replace


from collections import OrderedDict 
import time
from datetime import datetime

import os
import boto3

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"

conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

sc=SparkContext(conf=conf)


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
    pass
    #print(obj.key)
    #print("@@@@@@@@@@@@@@@@@@@@@@@@")

before = time.time()

# Read from the S3 bucket
df = spark.read.option("mode", "PERMISSIVE").schema(schema).option("header", True).option("columnNameOfCorruptRecord", "_corrupt_record").json("s3a://pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e/*.json").cache()
# You may want to change this to read csv depending on the files your reading from the bucket
# s3a://pinterest-data-d4a3a0c7-0a92-4efb-bdcc-4b3e20242e1e/pinterest_data.json
df.show()
df = df.drop('downloaded')
#df = df.drop('follower_count', 'title', 'unique_id', 'index', 'tag_list', 'image_src', ' save_location','category')
df = df.distinct()
image_or_video = ['video', 'image', 'multi-video(story page format)']
category_list = ['event-planning', 'art', 'home-decor', 'diy-and-crafts', 'education', 'christmas', 'mens-fashion', 'tattoos', 'vehicles', 'travel', 'beauty', 'quotes', 'finance']
df = df.filter(df.is_image_or_video.isin(image_or_video))
df = df.filter(df.category.isin(category_list))


df = df.withColumn('follower_count', when(df.follower_count.endswith('k'), regexp_replace(df.follower_count,'k','000'))
                   .when(df.follower_count.endswith('M'), regexp_replace(df.follower_count,'M','000000'))
                   .otherwise(df.follower_count))

df = df.withColumn('description', when(df.description == 'No description available Story format',  'null')
                   .otherwise(df.description))

df = df.withColumn('tag_list', when(df.tag_list.contains(','), regexp_replace(df.tag_list, ',', ''))
                   .otherwise(df.tag_list))


df.withColumn("follower_count", df.follower_count.cast(IntegerType()))


if False:
    tags = df.rdd.map(lambda x: x.tag_list).collect() #collects all the tags from df
    tag_list = list(OrderedDict.fromkeys(tags)) #removes duplicate lists
    tag_list = (' '.join(tag_list)).lower() #converts list to string in lower case
    tag_list = tag_list.replace(',', ' ')
    tag_list = tag_list.split(' ')
    tag_list = sorted(list(dict.fromkeys(tag_list))) #removes duplicates




#print(tag_list)
#df.printSchema()
df.show()
df.write.mode("overwrite").csv("csv")



after = time.time()
print('took ' + str(round(after-before)) +  ' seconds')

