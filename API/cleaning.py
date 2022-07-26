from hashlib import sha3_512
from lib2to3.pytree import Base
#from msilib import Table
from struct import Struct
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
import pandas as pd
import numpy as np
import multiprocessing
import logging
import json
import re
import string

import pyspark
import pyspark.sql
from pyspark.sql.functions import when, regexp_replace, split, column
from pyspark.sql.types import StructType, StructField, BooleanType, IntegerType, ArrayType, StringType

from cassandra.cluster import Cluster

def PullData(filename) -> json:
    s3_client = boto3.client('s3')
    try:
        bucket = 'pindatabucket'
        key = f'data/{filename}.json'
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        s = obj['Body'].read().decode('utf-8')
        trimmed_s = '[{' + re.search('{(.*)}', s).group(1) + '}]'
        return json.loads(trimmed_s)
    except ClientError as e:
        logging.error(e)
    except BaseException as e:
        logging.error(e)

def CleanData(df) -> pyspark.sql.DataFrame:
    #Replace no tags available
    df = df.withColumn('tag_list', when(df.tag_list == 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', 'No Tags')
            .otherwise(df.tag_list))

    #Trim 'Local save in ' from save_location
    df = df.withColumn('save_location', regexp_replace('save_location', 'Local save in ', ''))

    #Drop follower_count entries with User Info Error and image_src Image src error.
    df = df.filter((df.follower_count != 'User Info Error') & (df.image_src != 'Image src error.'))

    #Replace follower_count 43k to 43000
    df = df.withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))
    df = df.withColumn('follower_count', regexp_replace('follower_count', 'm', '000000'))
    
    
    
    #Create schema and apply modified df to schema

    #downloaded -> BooleanType
    #follower_count -> IntegerType
    #index -> IntegerType
    #Check is_image_or_video is only image/video
    #tag_list -> ArrayType(StringType)
    #schema = StructType([
    #    StructField("downloaded", BooleanType()),
    #    StructField("follower_count", IntegerType()),
    #    StructField("index", IntegerType()),
    #    StructField("is_image_or_video", StringType()),
    #    StructField("tag_list", ArrayType(StringType())),
    #    ])
   
    df = df.withColumn("downloaded", df.downloaded.cast(BooleanType()))
    df = df.withColumn("follower_count", df.follower_count.cast(IntegerType()))
    df = df.withColumn("index", df.index.cast(IntegerType()))
    df = df.withColumn("tag_list", split(column("tag_list"), ","))
    return df

def PassDataToCassandra(df):
    keyspace_name = 'test_keyspace'
    table_name = 'cleaned_pin_table'
    cluster = Cluster()
    cass_session = cluster.connect(keyspace_name)

    data_collect = df.collect()
    for row in data_collect:

        stripped_unique_id = row.unique_id.replace("'", "")

    #print( f"""
    #        INSERT INTO {table_name} (id, category, description, downloaded, follower_count, image_src, is_image_or_video, pin_index, save_location, tag_list, title)
    #        VALUES({stripped_unique_id}, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    #        """,
    #        (row.category, row.description, row.downloaded, row.follower_count, row.image_src, row.is_image_or_video, row['index'], row.save_location, row.tag_list, row.title)
   
        #Insert into cassandra table
        cass_session.execute(
            f"""
            INSERT INTO {table_name} (id, category, description, downloaded, follower_count, image_src, is_image_or_video, pin_index, save_location, tag_list, title)
            VALUES({stripped_unique_id}, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            """,
            (row.category, row.description, row.downloaded, row.follower_count, row.image_src, row.is_image_or_video, row['index'], row.save_location, row.tag_list, row.title)
        )



if __name__ == '__main__':

    logging.basicConfig(level=logging.ERROR)

    cfg = (
    pyspark.SparkConf()
    # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("TestApp")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
    )

    py_session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
    sc = py_session.sparkContext


    #TODO!!!!
    #PULL DATA 0001-0050 (Create string with leading zeroes)
    json_num = ""
    combined_rdd = sc.parallelize(PullData('0001'))
    
    for i in range(2, 51):  #Data from 0001.json -> 0050.json
        json_num = str(i)
        json_num = json_num.zfill(4)
        raw_json = PullData(json_num)
         
        #Create combined RDD
        json_rdd = sc.parallelize(raw_json)
        combined_rdd = combined_rdd.union(json_rdd)
   
    #Create large DF THEN process
    df = py_session.read.json(combined_rdd, allowSingleQuotes=True, multiLine=True, mode='DROPMALFORMED')
    df = CleanData(df)
    df.show()
    df.printSchema()

    #Send Data to Cassandra
    PassDataToCassandra(df)