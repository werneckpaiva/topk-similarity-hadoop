#!/bin/bash

hadoopPath=${hadoopPath}

$hadoopPath/bin/hadoop fs -rmr ${hdfs.outputFolder}
$hadoopPath/bin/hadoop jar target/topk-similarity-0.0.1-SNAPSHOT-job.jar -k $1 -m $2 -i ${hdfs.dataFolder}/input/ -o ${hdfs.outputFolder}

