"""
One of main run script of the pipeline. It takes monthly urls dataset and
repartition it by using the initial letter of domain name
It includes a main function that is used in spark submit.
"""
import os
import boto
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from boto.s3.key import Key
from datetime import datetime

############################################################################
# The function that  read parquet from s3 as dataframe and repartitioned it
############################################################################
def data_repartition(sqlContext,  s3_path):
    #read from parquet file in s3 into spark df
    current_month0 = sqlContext\
               .read.parquet(s3_path)

    #create temp table from df
    current_month0.createOrReplaceTempView("urls_curr_mon")

    #create df for repartition
    df_current_month0=sqlContext.sql("select * from urls_curr_mon")
    split_col = split(df_current_month0['domain'], '')

    #add the inital from domian name as additional column to df
    df_current_month0_repa=df_current_month0.withColumn("domainFirst",split_col.getItem(0))

    return df_current_month0_repa

############################################################################
# The function that save repartitioned dataframe as parquet in spark cluster
############################################################################
def save_output_df(output_df, hdfs_path, repa_table):
    #Write the df back to hdfs as parquet file by using the partitionBy first letter of domain name
    output_df.write.partitionBy("domainFirst").format('parquet')\
                        .mode('append').option('path', hdfs_path).saveAsTable(repa_table)




######################################################################################
# main function that run the urls repartition spark jobs once it's excuted by spark submit
######################################################################################
if __name__ == '__main__':

    ## create Spark Context from session
    conf = SparkConf()
    sc = SparkContext(conf = conf)

    #s3 setting for spark to s3 connection
    awsAccessKeyId=os.environ.get('AWS_ACCESS_KEY_ID')
    awsSecretAccessKey=os.environ.get('AWS_SECRET_ACCESS_KEY')
    sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId',awsAccessKeyId)
    sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey',awsSecretAccessKey)
    sc._jsc.hadoopConfiguration().set('fs.s3.endpoint','s3.us-east-1.amazonaws.com')
    sc._jsc.hadoopConfiguration().set('fs.s3.impl','org.apache.hadoop.fs.s3native.NativeS3FileSystem')

    #create sql Context from spark context
    sqlContext = SQLContext(sparkContext=sc)

    #taking input arguments from command line
    hdfs_path = str(sys.argv[1])
    s3_path = str(sys.argv[2])
    repa_table = str(sys.argv[3])

    #run the url dataset repartition job by using spark sqlContext
    output_df=data_repartition(sqlContext,  s3_path)

    #save the repartitioned df
    save_output_df(output_df, hdfs_path, repa_table)
