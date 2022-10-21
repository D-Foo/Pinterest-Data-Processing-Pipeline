from ctypes import cast
import logging
from kafka import KafkaConsumer
from json import loads

import os
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from pyspark.sql.functions import schema_of_json, regexp_replace
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import DataFrame

from sqlalchemy import create_engine

import findspark
findspark.init()

import pyspark
from collections import deque
import json
from dotenv import load_dotenv
import psycopg2

# create our consumer to retrieve the message from the topics
#data_stream_consumer = KafkaConsumer(
#    bootstrap_servers="localhost:9092",    
#    value_deserializer=lambda message: loads(message),
#    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
#)

#data_stream_consumer.subscribe(topics=["PinTopic"])

# Loops through all messages in the consumer and prints them out individually
#for message in data_stream_consumer:
#    print(message)

errorCount = 0
processingCount = 0
my_list = []
image_video_dict = {
        "image" : 0,
        "video" : 0,
        "multi-video(story page format)" : 0        
}
table_name = 'stream_data'
db_name = 'pinterest_streaming'
load_dotenv()
HOST = 'localhost'
USER = os.getenv('PGUSERNAME')
PASSWORD = os.getenv('PASSWORD')
DATABASE = db_name
PORT = 5432

conn =  psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT)
cur = conn.cursor()
            


def proccess(dataframe, epoch_id):

        json_schema = StructType([ \
                StructField("index",IntegerType(),True), \
                StructField("unique_id",StringType(),True), \
                StructField("title",StringType(),True), \
                StructField("description", StringType(), True), \
                StructField("poster_name", StringType(), True), \
                StructField("follower_count", StringType(), True), \
                StructField("tag_list", StringType(), True), \
                StructField("is_image_or_video", StringType(), True), \
                StructField("image_src", StringType(), True), \
                StructField("downloaded", IntegerType(), True), \
                StructField("save_location", StringType(), True), \
                StructField("category", StringType(), True) 
                        ])

        dataframe = dataframe.selectExpr("CAST(value as STRING)")

        # Select the value part of the kafka message and cast it to a string.
        dataframe = dataframe.withColumn("value", from_json(dataframe["value"], json_schema)).select(col("value.*"))

        #Convert follwer count to int type
        dataframe = dataframe.withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))
        dataframe = dataframe.withColumn('follower_count', regexp_replace('follower_count', 'm', '000000'))
        dataframe = dataframe.withColumn('follower_count', regexp_replace('follower_count', 'null', '0'))
        dataframe = dataframe.withColumn('follower_count', dataframe.follower_count.cast(IntegerType()))

        #Format quotes for insertion into postgresql db
        dataframe = dataframe.withColumn('title', regexp_replace('title', '\'', '\'\''))
        dataframe = dataframe.withColumn('description', regexp_replace('description', '\'', '\'\''))
        dataframe = dataframe.withColumn('tag_list', regexp_replace('description', '\'', '\'\''))

        # Get substring for topic type

        # Retain last 100 in queue and sum most popular unique categories
        rows = dataframe.collect()
        for row in rows:
                my_list.append(row.asDict()['category'])
                image_video_dict[row.asDict()['is_image_or_video']] += 1
        # Pop front if size > 100
        if len(my_list) > 100:
                for _ in range(len(my_list) - 100):
                        my_list.pop(0)

        # Print most popular 
        most_common_element = max(set(my_list), key=my_list.count)
        print(f'Most common tag: {most_common_element}({my_list.count(most_common_element)})')
        dataframe.write.mode('append').format('console').save()

        # Print image video percentages
        sum = 0
        for s, i in image_video_dict.items():
                sum += i
        print( f'Image/Video Percentages: Image {(image_video_dict["image"]/sum) * 100:.2f}%, Video {(image_video_dict["video"]/sum) * 100:.2f}%, Multi-video {(image_video_dict["multi-video(story page format)"]/sum) * 100:.2f}%' )

        #Insert into local postgres DB
        #table_name = 'stream_data'
        #db_name = 'pinterest_streaming'
        #dataframe.write.format('jdbc')\
        #        .option('url', f'jdbc:postgresql://localhost:5432/{db_name}')\
        #        .option('driver', 'org.postgresql.Driver').option('dbtable', table_name)\
        #        .option('user', os.getenv('PGUSERNAME')).option('password', os.getenv('PASSWORD')).save()
        for row in rows:
                cur.execute(f"""
            INSERT INTO {table_name} (index, unique_id, title, description, poster_name, follower_count, tag_list, is_image_or_video, image_src, downloaded, save_location, category)
            VALUES({row.asDict()['index']}, \'{row.asDict()['unique_id']}\', \'{row.asDict()['title']}\', \'{row.asDict()['description']}\', \'{row.asDict()['poster_name']}\', \'{row.asDict()['follower_count']}\',
                 \'{row.asDict()['tag_list']}\', \'{row.asDict()['is_image_or_video']}\', \'{row.asDict()['image_src']}\', {row.asDict()['downloaded']}, \'{row.asDict()['save_location']}\', \'{row.asDict()['category']}\') 
            ON CONFLICT (unique_id) DO NOTHING;""")
                conn.commit()
        
# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'
# specify the topic we want to stream data from.
kafka_topic_name = "PinTopic"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("PinKafkatoSpark") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()



# Check for malformed data and print % data errors
#print(f'Error Percentage: {(float(errorCount)/float(processingCount)) * 100}%')
#Clean follower count

# outputting the messages to the console 
stream_df.writeStream.foreachBatch(proccess).start().awaitTermination()
cur.close()
conn.close()
