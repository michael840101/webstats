import os
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import boto
from boto.s3.key import Key
from gzipstream import GzipStreamFile
from pyspark.sql.types import *
from datetime import datetime
import warc
import tldextract as te



def get_urls(id_, iterator):
    conn = boto.connect_s3(anon=True, host='s3.amazonaws.com')
    bucket = conn.get_bucket('commoncrawl')

    for uri in iterator:
        key_ = Key(bucket, uri)
        file_ = warc.WARCFile(fileobj=GzipStreamFile(key_))

        for record in file_:
            if record['Content-Type'] == 'application/json':
                record = json.loads(record.payload.read())

                try:
                    url=record['Envelope']\
                                ['WARC-Header-Metadata']\
                                ['WARC-Target-URI'].strip()
                    ext=te.extract(url)
                    reg_dom=ext.registered_domain
                    tld=ext.suffix


                    time_format="%Y-%m-%dT%H:%M:%SZ"
                    date_time=record['Envelope']\
                                ['WARC-Header-Metadata']\
                                ['WARC-Date'].strip()
                    ip=record['Envelope']\
                                ['WARC-Header-Metadata']\
                                ['WARC-IP-Address'].strip()
                    cont_length=record['Envelope']\
                                ['WARC-Header-Metadata']\
                                ['Content-Length'].strip()

                    yield (url,reg_dom,tld,ip,cont_length,date_time)
                except KeyError:
                    continue


conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sparkContext=sc)
awsAccessKeyId=os.environ.get('AWS_ACCESS_KEY_ID')
awsSecretAccessKey=os.environ.get('AWS_SECRET_ACCESS_KEY')
sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId',awsAccessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey',awsSecretAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3.endpoint','s3.us-east-1.amazonaws.com')
sc._jsc.hadoopConfiguration().set('fs.s3.impl','org.apache.hadoop.fs.s3native.NativeS3FileSystem')


files = sc.textFile('hdfs://ec2-18-207-73-113.compute-1.amazonaws.com:9000/user/hadoop/wat.paths.1.gz')
urls = files.mapPartitionsWithSplit(get_urls).map(lambda l:(l[0],l[1],l[2],l[3],l[4],l[5])).collect()

#urls = files.mapPartitionsWithSplit(get_urls).collect()
#               .map(lambda x: (x, 1)) \
#               .reduceByKey(lambda x, y: x + y)

schema = StructType([
    StructField("url", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("tld", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("length", StringType(), True),
    StructField("date_time", StringType(), True)

])

sqlContext.createDataFrame(urls, schema=schema) \
          .write \
          .format("parquet") \
          .option('path','s3://insightdemozhi/cc')\
          .saveAsTable('urls')
