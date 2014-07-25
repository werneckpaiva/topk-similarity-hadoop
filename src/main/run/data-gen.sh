#!/bin/bash

if [ $# -lt 3 ] 
then
    echo "Usage: $0 <dimension> <numberOfPoints> <numberOfFiles> [<destFolder>]"
    exit 1
fi

dimension=$1
numPoints=$2
numFiles=$3
output=$4

folder="${hdfs.dataFolder}/input/"
hadoopPath=${hadoopPath}
execdir=`dirname $0`
jardir=${execdir}/..

if [ -z "$output" ]; then

    echo "Removing dir /topk/data"
    $hadoopPath/bin/hadoop fs -rmr hdfs://sn05.ic.uff.br/topk/data

    echo "Creating base dir"
    $hadoopPath/bin/hadoop fs -mkdir hdfs://sn05.ic.uff.br/topk/data

    # echo "Creating input dir"
    # $hadoopPath/bin/hadoop fs -mkdir "hdfs://sn05.ic.uff.br/topk/data/input/"
    extraParam=" -h "
else
    if [ ! -d "$output" ]; then
        mkdir -p "$output"
    fi
    folder=$output
    extraParam=""
fi

$hadoopPath/bin/hadoop jar "${jardir}/topk-similarity-0.0.1-SNAPSHOT.jar" \
    br.uff.mestrado.hadoop.topksimilarity.RandomDataGenerator \
    -create $extraParam -o "$folder" -d $dimension -n $numPoints -f $numFiles





# ----------------------
# How to list array file
# $hadoopPath/bin/hadoop jar target/topk-similarity-0.0.1-SNAPSHOT.jar br.uff.mestrado.hadoop.topksimilarity.RandomDataGenerator -listarray "$file"
# How to list sequence file
# $hadoopPath/bin/hadoop jar target/topk-similarity-0.0.1-SNAPSHOT.jar br.uff.mestrado.hadoop.topksimilarity.RandomDataGenerator -listsequence "$file"
