#!/bin/bash

if [ $# -lt 4 ] 
then
    echo "Usage: <split|copy> <hostfile> <localFolder> <destFolder>"
    echo "  copy : copy all files to all nodes"
    echo "  split: split files into all nodes" 
    exit 1
fi

action=$1
hostfile="$2"
folder=$3
destFolder="$4"

hosts=`cat $hostfile | grep -v "^ *#" | grep -v "^ *$"`
numHosts=`echo $hosts | wc -w`

for host in $hosts
do 
   echo "Creating destination folder \"$destFolder\" in \"$host\""
   ssh "$host" "mkdir -p $destFolder 2> /dev/null && rm -f "$destFolder/*" && [ -d $destFolder ]"
   if [ $? -ne 0 ]
   then
   		exit 1
   fi
done


if [ "$action" == "copy" ]
then
	for host in $hosts
	do
		echo "${host}:${destFolder}"
		scp ${folder}/* "${host}:${destFolder}/"
	done
else
	i=1
	for file in ${folder}/*
	do
	    let "line=(i % numHosts) + 1"
		host=`printf "$hosts" | sed -n "${line}p"`
		echo "$file -> ${host}:${destFolder}"
		scp "$file" "${host}:${destFolder}/"
		let "i++"
	done
fi
