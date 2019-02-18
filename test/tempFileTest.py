import os
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from gzipstream import GzipStreamFile
from pyspark.sql.types import *
from datetime import datetime
import warc
import tldextract as te
import ast

from io import BytesIO
from tempfile import TemporaryFile

# Creating Spark Context and SQL Context Objects
conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sparkContext=sc)

#Set the Credential Keys for AWS S3 Connection
awsAccessKeyId=os.environ.get('AWS_ACCESS_KEY_ID')
awsSecretAccessKey=os.environ.get('AWS_SECRET_ACCESS_KEY')
sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId',awsAccessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey',awsSecretAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3.endpoint','s3.us-east-1.amazonaws.com')
sc._jsc.hadoopConfiguration().set('fs.s3.impl','org.apache.hadoop.fs.s3native.NativeS3FileSystem')


#Parsing the url related data elements for each line in the file
def get_urls(line):
        try:
            fields=line.split('{')
            json_data=None
            try:
                json_data=ast.literal_eval('{'+fields[1])
            except:
                pass

            language=None
            url=None
            mime=json_data['mime']

            try:
                url=json_data['url']
            except:
                pass

            try:
                language=json_data['languages']
            except:
                pass

            content_len=json_data['length']

            field_0=fields[0]

            parts=field_0.split()
            date=parts[1]

            dom_part=parts[0].split(')')[0].split(',')
            domain=dom_part[1]+'.'+dom_part[0]
            tld=dom_part[0]

            return (url,mime,content_len,domain,tld,language,date)
        except:
            return (None,None,None,None,None,None,None)


lines = sc.textFile('hdfs://ec2-18-207-73-113.compute-1.amazonaws.com:9000/user/cc_data/cdx-00033')
print(lines.count())
