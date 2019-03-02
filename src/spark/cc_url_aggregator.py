# -*- coding: utf-8 -*-
import os
import boto
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from boto.s3.key import Key
from datetime import datetime

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sparkContext=sc)

# s3 setting

awsAccessKeyId = os.environ.get('AWS_ACCESS_KEY_ID')
awsSecretAccessKey = os.environ.get('AWS_SECRET_ACCESS_KEY')
sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId',
                                  awsAccessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey',
                                  awsSecretAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3.endpoint',
                                  's3.us-east-1.amazonaws.com')
sc._jsc.hadoopConfiguration().set('fs.s3.impl',
                                  'org.apache.hadoop.fs.s3native.NativeS3FileSystem'
                                  )

# PostgreSQL connection setting

db_uer = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASS')
url_connect = os.environ.get('URL_CONNECT')
host = os.environ.get('DB_HOST')
db_schema= os.environ.get('DB_SCHEMA')

mode = 'append'

properties = {'user': db_uer, 'password': db_password,
              'driver': 'org.postgresql.Driver'}

# output df table structure

schema = StructType([
    StructField('url', StringType(), True),
    StructField('domain', StringType(), True),
    StructField('tld', StringType(), True),
    StructField('ip', StringType(), True),
    StructField('length', IntegerType(), True),
    StructField('date_time', TimestampType(), True),
    ])

# read from parquet to df

df_current_month = \
    sqlContext.read.parquet('s3a://insightdemozhi/cc-2019-01/*')
df_previouse_month = \
    sqlContext.read.parquet('s3a://insightdemozhi/cc-2018-12/*')

# create temp table from df

df_current_month.createOrReplaceTempView('urls_curr_mon')
df_previouse_month.createOrReplaceTempView('urls_pre_mon')

# query for creating output df

df_domain_total_t = \
    sqlContext.sql("SELECT domain, count(url) as url_total, SUM(cont_length) as total_content\
                        FROM urls_curr_mon \
                        GROUP BY domain"
                   )

df_domain_url_increase = \
    sqlContext.sql("SELECT dis_url.domain, COUNT(dis_url.url) as url_increased \
                        FROM (SELECT c.domain, c.url FROM urls_curr_mon c LEFT JOIN urls_pre_mon ct on c.url=ct.url \
                        WHERE ct.url is null) AS dis_url GROUP BY dis_url.domain"
                   )

df_domain_url_decrease = \
    sqlContext.sql("SELECT dis_url.domain, COUNT(dis_url.url) as url_decreased \
                        FROM (SELECT c.domain, c.url FROM urls_pre_mon c LEFT JOIN urls_curr_mon ct on c.url=ct.url\
                        WHERE ct.url is null) AS dis_url GROUP BY dis_url.domain"
                   )

df_increse = df_domain_total_t.join(df_domain_url_increase, ['domain'])
df_in_decrese = df_increse.join(df_domain_url_decrease, ['domain'])

# calculate rank for page counts using window functions

ranked = df_in_decrese.withColumn('pg_rank',
                                  dense_rank().over(Window.orderBy(desc('url_total'
                                  ))))

cl_ranked = ranked.withColumn('content_rank',
                              dense_rank().over(Window.orderBy(desc('total_content'
                              ))))

# Join df for the output table
# Date

output_df = cl_ranked.withColumn('stats_date', current_date())

# Write the output data frame to PostgreSQL database

output_df.write.jdbc(url=url_connect, table='domains', mode=mode,
                     properties=properties)
