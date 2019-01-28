import os
import json
import logging
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import boto
from boto.s3.key import Key
from gzipstream import GzipStreamFile
from pyspark.sql.types import *
from datetime import datetime
import warc
import tldextract as te




#log_level = 'INFO'
#logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sparkContext=sc)

# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()


schema = StructType([
    StructField("url", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("tld", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("length", IntegerType(), True),
    StructField("date_time", TimestampType(), True)

])


#s3 setting
awsAccessKeyId=os.environ.get('AWS_ACCESS_KEY_ID')
awsSecretAccessKey=os.environ.get('AWS_SECRET_ACCESS_KEY')
sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId',awsAccessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey',awsSecretAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3.endpoint','s3.us-east-1.amazonaws.com')
sc._jsc.hadoopConfiguration().set('fs.s3.impl','org.apache.hadoop.fs.s3native.NativeS3FileSystem')

df_input = sqlContext\
            .read.parquet("s3a://insightdemozhi/cc/*")

#df_output.filter(df_input['tld'] = 'com')
df_input.createOrReplaceTempView("url_temp")
df_output = sqlContext.sql("SELECT * FROM url_temp where tld='com'")





#PostgreSQL connection setting
db_uer=os.environ.get('DB_USER')
db_password=os.environ.get('DB_PASS')
mode = "overwrite"
url_connect = "jdbc:postgresql://ec2-3-92-151-139.compute-1.amazonaws.com:5432/cc_dev"
properties = {"user": db_uer,"password": db_password,"driver": "org.postgresql.Driver"}

#Write the output data frame to PostgreSQL database
df_output.write \
         .jdbc(url=url_connect, table="cc_url", mode=mode, properties=properties)
