import os
import boto
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
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




#read from parquet to df
current_month0 = sqlContext\
           .read.parquet("s3a://insightdemozhi/cc-2019-01/*")

#create temp table from df
current_month0.createOrReplaceTempView("urls_curr_mon")

#create df for repartition
df_current_month0=sqlContext.sql("select * from urls_curr_mon")
split_col = split(df_current_month0['domain'], '')

df_current_month0_repa=df_current_month0.withColumn("domainFirst",split_col.getItem(0))
df_current_month0_repa.write.partitionBy("domainFirst").format('parquet')\
                    .mode('append').option('path', "hdfs://ec2-3-93-221-130.compute-1.amazonaws.com:9000/user/cc_repa_201901/").saveAsTable('urls_repa_201901')
