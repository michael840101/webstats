#!/bin/bash
#for i in $(seq 299 $END); do
for i in $(seq 0 1 9); do
 if [ $i -lt 99 ]
 then
   echo $i;
   now=$(date +"%T")
   echo "Current time : $now"
   #Download from s3
   aws s3 cp s3://commoncrawl/cc-index/collections/CC-MAIN-2018-51/indexes/cdx-0000$i.gz ./
   now=$(date +"%T")
   echo "Current time : $now"
   #Decopress teh gz file
   for f in *.gz; do
     STEM=$(basename "${f}" .gz)
     gunzip "$f"
     dtdir="hdfs://ec2-18-207-73-113.compute-1.amazonaws.com:9000/user/cc_data/${STEM}"
     s3uri="s3://insightdemozhi/cc-2018-12/${STEM}"
     now=$(date +"%T")
     echo "Current time : $now"
     echo $dtdir
     #copy to hdfs
     hdfs dfs -copyFromLocal ./"${STEM}" $dtdir
     now=$(date +"%T")
     echo "Current time : $now"
     #run the spark job
     spark-submit --name url-parser\
      --master spark://ec2-18-207-73-113.compute-1.amazonaws.com:7077\
      --executor-memory 6G \
      --driver-memory 6G\
      --executor-cores 6\
        test_url_collecter.py $dtdir $s3uri
      now=$(date +"%T")
      echo "Current time : $now"
     hdfs dfs -rm $dtdir
     rm ./"${STEM}"
   done

 else
   echo $i;
 fi

done
