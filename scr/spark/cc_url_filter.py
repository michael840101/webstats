import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto
from boto.s3.key import Key
from datetime import datetime



conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sparkContext=sc)

#s3 setting
awsAccessKeyId=os.environ.get('AWS_ACCESS_KEY_ID')
awsSecretAccessKey=os.environ.get('AWS_SECRET_ACCESS_KEY')
sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId',awsAccessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey',awsSecretAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3.endpoint','s3.us-east-1.amazonaws.com')
sc._jsc.hadoopConfiguration().set('fs.s3.impl','org.apache.hadoop.fs.s3native.NativeS3FileSystem')

#PostgreSQL connection setting
db_uer=os.environ.get('DB_USER')
db_password=os.environ.get('DB_PASS')
mode = "append"
url_connect = "jdbc:postgresql://ec2-3-92-151-139.compute-1.amazonaws.com:5432/cc_dev"
properties = {"user": db_uer,"password": db_password,"driver": "org.postgresql.Driver"}

#output df table structure
schema = StructType([
    StructField("url", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("tld", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("length", IntegerType(), True),
    StructField("date_time", TimestampType(), True)

])


#read from parquet to df
df_current_month = sqlContext\
            .read.parquet("s3a://insightdemozhi/cc/*")
df_previouse_month = sqlContext\
            .read.parquet("s3a://insightdemozhi/cc_test/*")

#create temp table from df
df_current_month.createOrReplaceTempView("urls_curr_mon")
df_previouse_month.createOrReplaceTempView("urls_pre_mon")

#query for creating output df
df_domain_url_total = sqlContext.sql("SELECT domain, count(url) as ct\
                        FROM urls_curr_mon \
                        GROUP BY domain")

df_domain_url_increase = sqlContext.sql("SELECT domain, count(url)\
                        FROM urls_curr_mon cm \
                        WHERE cm.url NO IN \
                        (SELECT url FROM urls_pre_mon)\
                        GROUP BY domain )

df_domain_url_decrease = sqlContext.sql("SELECT domain, count(url)\
                        FROM urls_pre_mon pm \
                        WHERE pm.url NO IN \
                        (SELECT url FROM urls_curr_mon)\
                         GROUP BY domain ))

#calculate rank for page counts using window functions
ranked =  df_domain_url_total.withColumn(\
  "ct_rank", dense_rank().over(Window.partitionBy("domain").orderBy(desc("ct"))))

#Join df for the output table
#Date



#Write the output data frame to PostgreSQL database
df_output.write \
         .jdbc(url=url_connect, table="cc_url", mode=mode, properties=properties)
