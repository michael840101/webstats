# WebStats
A project to allow internet online users visualizing the web page generation activities per domain and geographic location

### Project Idea:
To facilitate online users with detailed statistic information regarding web domain content activities.
Motivation and Business Case:
Websites and web pages created and faded in tremendous amounts everyday.
Page generation statistic metric help users and business understand the insight of global web structure and content distribution trend.Currently there is no public resource providing such information in details.This is what I achieved during my project which provides a web interface that takes a domain name and visualize monthly web page creation and deletion statistic in graph.

### Data Sources:
1.	Common Crawl(http://commoncrawl.org/the-data/): The Common Crawl dataset lives on Amazon S3 as part of the Amazon Public Datasets program, it contains petabytes of data collected over the last 7 years. Every month Common Crawl will release the previous month crawl corpus which contains raw web page data,meta-data and text extractions. For December 2018, the corpus has 20 TB GZIP crawl data of 3.2 billion web pages.

### Pipeline:
![](scr/img/Pipeline.png)
### AWS Clusters:
There are 4 main clusters as part of the pipeline:
•	4 m4.large for Spark() cluster
•	4 m4.large for second Spark() cluster
•	2 m4.large for PostgreSQL() database
•	1 m4.large for Apache Web Application server


## Layers of the Pipeline:
## 1.	Data Ingestion Layer:
The data from Common Crawl is available on Amazon S3, but it's in GZIP file, needed to move to Spark Clusters for decompression to reach the best Spark process performance.
## 2.	Data Processing and Modeling Layer:
Apache Spark is a distributed processing engine which can crunch a huge amount of data and through Spark SQL, data can be modeled in the way you require. The data that I'm working on needs to be cleaned, pre-processed, processed(joins, aggregations) and modeled in the form that I require.
### Cleaning:
The raw html data is messy, and the url link is Primary Keys in my database. I have utilized the python library to format the data so that would become the Primary Keys in my database.
### Pre-Processing:
Common Crawl Archive data is Warc file of html format and I'm  interested in meta-data: url,domain name, content length...  Utilize Warc Parser to filter the meta data out from html content block.
### Processing:
#### Joins:


The invention of relation is possible by joining the data from different sources. I have outer joined the current month urls data-set with previous month data-set to find the number of new generated web pages and deleted web pages under each domain. This resulted in partitioning of data based on joining key domain. However, there are more than 20 million domains which make the partitioning no sense. Hence, I repartitioned the data based on the first letter of domain name and have performed the aggregations. The join that the Spark Catalyst chose is Sort-Merge join. However, I can experiment with other joins to check for any performance improvement.
#### Caching:
The interim dataframe that are operated on are cached to improve the performance by 2 times.
#### Aggregations:
The aggregation can be a costly operation in a distributed environment as it involves a shuffle operation. I have chosen to use Spark Window Aggregation Functions which combines the values at a partition level and then shuffles them. This reduced the amount of network traffic and improved the performance.
#### Modeling:
The data is formed as normalized tables after aggregation to be fed to my PostgreSQL database.
## 3.	Serving Layer:
S3:The parsed html data from Spark is kept in AWS S3 buckets as public resources with separated schema attached.
PostgreSQL: The aggregated data from Spark is kept in three normalized tables(Domains,domain_activities,languages).
## 4.	UI Layer:
The dashboard is created using Dash to search for the domain name for which you can look at the visualization of page creation metric and geographic details for the domain.

# Links to my works:
#### Google Slides: (https://bit.ly/2RI4ytR)
#### Demo: (http://webstates.com)
