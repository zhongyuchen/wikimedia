#!/bin/bash
PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:~/bin:/usr/hadoop-2.6.4/bin
export PATH
echo "This node is cleaning it's hdfs..."
rm -rf /state/partition1/data/
rm -rf /state/partition1/tmp/
rm -rf /state/partition1/nodename/
echo "This node is recreate it's hdfs file..."
mkdir /state/partition1/data/
mkdir /state/partition1/tmp/
mkdir /state/partition1/nodename/
echo "This node is formating it's new file... "
hdfs namenode -format
echo "All operation has done."
