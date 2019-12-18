#!/bin/bash
##用于同步环境

##配置文件路径
HADOOP_ETC=/etc

##需要同步的文件
ETC_FILES="profile"

##需要同步的主机
HOSTS="compute-0-36 compute-0-37 compute-0-38 compute-0-39 compute-0-40 compute-0-41 compute-0-42 compute-0-43 compute-0-44 compute-0-45 compute-0-46 compute-0-47 compute-0-48 compute-0-49 compute-0-50 compute-0-51 compute-0-52 compute-0-53 compute-0-54"
OP_DATE=$(date +%Y%m%d_%H%M%S)

echo "配置文件开始同步"
for H in $HOSTS;do
echo "##Sycn for $H##"
for F in $ETC_FILES;do
echo "backup and scp..."
ssh $H cp $HADOOP_ETC/$F $HADOOP_ETC/${F}_${OP_DATE}
scp $HADOOP_ETC/$F $H:$HADOOP_ETC
done
done

