# utf-8
import os
import sys
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from datetime import datetime

# Creating Spark Context and SQL Context Objects

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sparkContext=sc)

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

# reconstract json structure
def get_json(fields_1):
    return ast.literal_eval('{' + fields_1)

# Parsing json data to tuple
def parse_json(fields_1):
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

# Parse header of Json file to tuple
def parse_dom(f_0):
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

#separate header and json content
def parse_fields(line):
    fields = line.split('{')
    (f0, f1) = (fields[0], fields[1])
    return (f0, f1)

#taking input arguments from command line
hdfs_path = str(sys.argv[1])
s3_path = str(sys.argv[2])
file_uri = hdfs_path
output_path = s3_path

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
