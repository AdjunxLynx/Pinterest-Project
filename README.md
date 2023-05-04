#Pinterest Project

Please use requirements.txt to install all dependencies

##batch_consumer.py

batch consumer uses the KafkaConsumer class to read all data sent to the Kafka brokers. It then decodes the message from `ascii`. a custom filename is created and used to upload the data to an AWS bucket using a configuration data stored in a variable.

##batch_processing_spark.py

this script first submits all the necessary packages (AWS apis) and installs them. configurations such as pyspark schema, pyspark appname and the pyspark context are created.
The script then utilises pyspark to read all files ending in .json, combines that into a DataFrame. The Dataframe is then cleaned to ensure:
1. `follower_count` is of an integer type.
2. `category` is a valid category type.
3. `description` is null when empty to save space and readability.
4. `tag_list` is formatted correctly for easier code scraping when/if used later.
5. removes the `downloaded` column as it only ever had `1` as a value.

The DataFrame is then sent to a PostGreSQL Server for long term storage

##streaming_consumer.py

