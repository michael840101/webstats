"""
One of main run script of the pipeline. It parses commoncrawl's monthly crawl data
to structure dataset includes urls, domain, content related metrics
It includes a main function that is used in spark submit.
"""

import os
import sys
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from datetime import datetime

def get_json(fields_1):
    """
    The function that reconstract json objects tructure
    :param fields_1: The string that contains semi-json structure

    :return: structured json objects
    """
    return ast.literal_eval('{' + fields_1)

def parse_json(fields_1):
    """
    The function that parsing json data to tuple
    :param fields_1: The string that contains json structure

    :return: tuple of page statistic elements lead by url
    """
    try:
        json_data = json.loads('{' + fields_1)
        mime = json_data['mime']
        language = None
        url = None
        try:
            url = json_data['url']
        except:
            pass
        try:
            language = json_data['languages']
        except:
            pass
        content_len = json_data['length']

        return (url, mime, language, content_len)
    except:
        continue

def parse_dom(f_0):
    """
    The function that parse header of Json Obj to domain and tld
    :param f_0: The string that contains json header

    :return: tuple of domains elements lead by domain name
    """
    try:
        parts = f_0.split()
        date = parts[1]
        dom_part = parts[0].split(')')[0].split(',')
        if len(dom_part)>2:
            domain=dom_part[2]+'.'+dom_part[1] + '.' + dom_part[0]
            tld=dom_part[1] + '.' + dom_part[0]
        else:
            domain = dom_part[1] + '.' + dom_part[0]
            tld = dom_part[0]
        return (domain, tld, date)
    except:
        continue

def parse_fields(line):
    """
    The function that separate header and json content
    :param line: The string that contains commoncrawl raw file

    :return: tuple of header string and semi-josn string
    """
    fields = line.split('{')
    (f0, f1) = (fields[0], fields[1])
    return (f0, f1)


def main(sc, file_uri, output_path):
    """
    The main function that take crawl file from input  path(file_uri)
    do the parsing jobs and save the structured output dataframe in
    the directory of output_path
    :param sc: The sparkContext obj from Spark session
    :param file_uri: The string that dictates the uri of commoncrawl raw data in s3
    :param output_path: The string that dictates the uri of parsed dataset to be saved in s3

    :return:
    """
    # Creating SQL Context Objects from Spark Context
    sqlContext = SQLContext(sparkContext=sc)

    #read file through spark Context
    lines = sc.textFile(file_uri)

    # parsing text data to rdd for output
    urls = lines.map(parse_fields).map(lambda f: (parse_json(f[1]),
                                       parse_dom(f[0]))).map(lambda pair: (
            pair[0][0],
            pair[0][1],
            pair[0][3],
            pair[1][0],
            pair[1][1],
            pair[0][2],
            pair[1][2],
            ))

    # Construct schema for output  dataframe
    schema = StructType([
        StructField('url', StringType(), True),
        StructField('mime', StringType(), True),
        StructField('cont_length', StringType(), True),
        StructField('domain', StringType(), True),
        StructField('tld', StringType(), True),
        StructField('language', StringType(), True),
        StructField('date_time', StringType(), True),
        ])

    # Save the df into s3 as table in parquet
    sqlContext.createDataFrame(urls, schema=schema).write.format('parquet'
            ).mode('append').option('path', output_path).saveAsTable('urls')



######################################################################################
# main function that run the urls repartition spark jobs once it's excuted by spark submit
######################################################################################
if __name__ == '__main__':

    # Creating Spark Context and SQL Context Objects
    conf = SparkConf()
    sc = SparkContext(conf=conf)

    # Set the Credential Keys for AWS S3 Connection
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
    #taking input arguments from command line
    hdfs_path = str(sys.argv[1])
    s3_path = str(sys.argv[2])
    file_uri = hdfs_path
    output_path = s3_path

    # execute the main function jobs
    main(sc, file_uri, output_path)
