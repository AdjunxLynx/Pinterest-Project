# Pinterest Project

Please use requirements.txt to install all dependencies

## batch_consumer.py

batch consumer uses the KafkaConsumer class to read all data sent to the Kafka brokers. It then decodes the message from `ascii`. a custom filename is created and used to upload the data to an AWS bucket using a configuration data stored in a variable.

<img width="718" alt="image" src="https://github.com/AdjunxLynx/Pinterest-Project/assets/117390288/0ed2d3da-01b4-49c2-a438-daa46ccf2f98">

here is an image of the AWS datalake filled with 

## batch_processing_spark.py

this script first submits all the necessary packages (AWS apis) and installs them. configurations such as pyspark schema, pyspark appname and the pyspark context are created.
The script then utilises pyspark to read all files ending in .json, combines that into a DataFrame. The Dataframe is then cleaned to ensure:
1. `follower_count` is of an integer type.
2. `category` is a valid category type.
3. `description` is null when empty to save space and readability.
4. `tag_list` is formatted correctly for easier code scraping when/if used later.
5. the `downloaded` column is removed as it only ever had `1` as a value.


The DataFrame is then sent to a PostGreSQL Server for long term storage

## streaming_consumer.py

this script is very similar to batch_consumer, but uses pyspark streaming instead to do all the cleaning in real time. all the pyspark arguments are installed(spark to sql api and spark to PostGreSQL). the script reads from the KafkaConsumer and creates a DataFrame Stream that sends every 1000 rows, or once a second. this stream is cleaned to ensure:
1. `follower_count` is of an integer type.
2. `category` is a valid category type.
3. the `downloaded` column is removed as it only ever had `1` as a value.
4. no duplicate data is sent to the PostGreSQL server in a single batch

