"""
One of main run script of the pipeline. It aggregates two month urls datasets and
calculate the url increaing numbers, decreaing numbers from previous month,
as well as ranking per domain
It includes a main function that is used in spark submit.
"""
import os
import boto
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from boto.s3.key import Key
from datetime import datetime


############################################################################
# this function takes the input directory path of repartitioned url dataset(parquet)
# and create temp view of the current and previous month url for aggregation
############################################################################
def create_urls_temp_view(sqlContext, s3_curr_month, s3_past_month):
    # read from parquet to df
    df_current_month = \
        sqlContext.read.parquet(s3_curr_month)
    df_previouse_month = \
        sqlContext.read.parquet(s3_past_month)

    # create temp table from df
    df_current_month.createOrReplaceTempView('urls_curr_mon')
    df_previouse_month.createOrReplaceTempView('urls_pre_mon')



############################################################################
# this function takes the input directory path of repartitioned url dataset(parquet)
# and create temp view of the current and previous month url for aggregation
############################################################################
def aggregate_url_to_domain(sqlContext):
    # query for creating output df
    # query for calculating total web pages, content length for each domian
    df_domain_total_t = \
        sqlContext.sql("SELECT domain, count(url) as url_total, SUM(cont_length) as total_content\
                            FROM urls_curr_mon \
                            GROUP BY domain"
                       )
    # aggregate the previous month and current month url dataset to calculate the url increaing per domian
    df_domain_url_increase = \
        sqlContext.sql("SELECT dis_url.domain, COUNT(dis_url.url) as url_increased \
                            FROM (SELECT c.domain, c.url FROM urls_curr_mon c LEFT JOIN urls_pre_mon ct on c.url=ct.url \
                            WHERE ct.url is null) AS dis_url GROUP BY dis_url.domain"
                       )
    # aggregate the previous month and current month url dataset to calculate the url decreaing per domian
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

    # Join current timestamp df for the output table
    output_df = cl_ranked.withColumn('stats_date', current_date())

    return output_df


############################################################################
# The function that save pre-calculated dataframe into postgresql
############################################################################
def save_output_df(output_df, url_connect, mode, properties):
    output_df.write.jdbc(url=url_connect, table='domains', mode=mode,
                         properties=properties)




######################################################################################
# main function that run the url aggregator spark jobs once it's excuted by spark submit
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

    # PostgreSQL connection setting
    # taking input arguments from system enviornment
    db_uer = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASS')
    url_connect = os.environ.get('URL_CONNECT')
    host = os.environ.get('DB_HOST')
    db_schema= os.environ.get('DB_SCHEMA')
    mode = 'append'
    properties = {'user': db_uer, 'password': db_password,
                  'driver': 'org.postgresql.Driver'}

    #taking input arguments from command line
    s3_curr_month = str(sys.argv[1])
    s3_past_month = str(sys.argv[2])



    #Create temp view table from current and previous month urls by using spark sqlContext
    create_urls_temp_view(sqlContext, s3_curr_month, s3_past_month)

    #run the url aggregation job by using spark sqlContext and return the output df
    output_df=aggregate_url_to_domain(sqlContext, s3_curr_month, s3_past_month)

    #save the output df from the aggregation job into postgresql database
    save_output_df(output_df, url_connect, mode, properties)
