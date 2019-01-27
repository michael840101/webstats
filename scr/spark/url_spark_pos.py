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
                    date_time=datetime.strptime(record['Envelope']\
                                ['WARC-Header-Metadata']\
                                ['WARC-Date'].strip(),time_format)
                    ip=record['Envelope']\
                                ['WARC-Header-Metadata']\
                                ['WARC-IP-Address'].strip()
                    cont_length=int(record['Envelope']\
                                ['WARC-Header-Metadata']\
                                ['Content-Length'].strip())

                    yield (url,reg_dom,tld,ip,cont_length,date_time)
                except KeyError:
                    continue


conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sparkContext=sc)



# data.write.jdbc(url=url, table="cc_url", mode=mode, properties=properties)

# url_connect = "jdbc:postgresql://198.123.43.24:1234"
# table = "test_result"
# mode = "overwrite"
# properties = {"user":"postgres", "password":"password"}

#my_writer.jdbc(url_connect, table, mode, properties)


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
    StructField("length", IntegerType(), True),
    StructField("date_time", TimestampType(), True)

])


#s3 setting
awsAccessKeyId=os.environ.get('AWS_ACCESS_KEY_ID')
awsSecretAccessKey=os.environ.get('AWS_SECRET_ACCESS_KEY')
sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId',awsAccessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey',awsSecretAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3.endpoint','s3.us-east-1.amazonaws.com')
sc._jsc.hadoopConfiguration().set('fs.s3.impl','org.apache.hadoop.fs.s3native.NativeS3FileSystem')


# sqlContext.createDataFrame(urls, schema=schema) \
#           .write \
#           .format("parquet") \
#           .option('path','s3://insightdemozhi/cc')\
#           .saveAsTable('urls')


#PostgreSQL setting
mode = "overwrite"
url_connect = "jdbc:postgresql://ec2-3-92-151-139.compute-1.amazonaws.com:5432/cc_dev"
properties = {"user": "wccuser","password": "c8608857","driver": "org.postgresql.Driver"}

sqlContext.createDataFrame(urls, schema=schema) \
           .write \
           .jdbc(url=url_connect, table="cc_url", mode=mode, properties=properties)
