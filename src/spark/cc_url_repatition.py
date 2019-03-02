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

#taking input arguments from command line
hdfs_path = str(sys.argv[1])
s3_path = str(sys.argv[2])
repa_table = str(sys.argv[3])


#read from parquet to df
current_month0 = sqlContext\
           .read.parquet(s3_path)

#create temp table from df
current_month0.createOrReplaceTempView("urls_curr_mon")

#create df for repartition
df_current_month0=sqlContext.sql("select * from urls_curr_mon")
split_col = split(df_current_month0['domain'], '')

df_current_month0_repa=df_current_month0.withColumn("domainFirst",split_col.getItem(0))
df_current_month0_repa.write.partitionBy("domainFirst").format('parquet')\
                    .mode('append').option('path', hdfs_path).saveAsTable(repa_table)
