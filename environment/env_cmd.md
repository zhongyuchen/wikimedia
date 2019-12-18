# Commands for Building Environment

## 基于Docker的Hadoop完全分布式安装？？

（最后并没有用基于docker的部署方法）

## Login

管理员：
root@10.141.200.205
raritan@413

公用账户：
ds19
123456


## nodes安装情况

从compute-0-36开始的，
21~35都是废的
36～55可用，用17个nodes，55,50,49是坏的

new:
0-15,0-14,0-19down
36,37,38,39,40,41,42,43,44,45,46,47,49,51,52,53,54,
36-47,49,51-54

## 让master知道有什么slaves可以连接

把/usr/hadoop-2.6.4/etc/hadoop/slaves文件
slaves要改成目前可用的datanodes的名字，包括master自己的名字，因为master（namenode）本身也可以是datanode
（但是目前只有17个nodes，主机本身设置不成datanode）

## 防火墙

service iptables status
service iptables stop

## 查看host

cat /etc/hosts查看hosts

## rock cluster命令

rocks list host
rocks sync config
jps查看目前启动了的服务


## 重启httpd：使得可以访问ganglia网站

service httpd restart
service httpd start
service httpd stop
查看占用80端口的应用netstat -lnp|grep 80
或者kill掉占用80端口的应用netstat -lnp|grep 80，再start


## 启动、停止

/usr/hadoop-2.6.4/sbin中
bash start-all.sh启动所有节点
bash stop-all.sh停止所有

## 测试：（必须要先启动所有节点！）

cp /usr/hadoop-2.6.4/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar ./
hadoop jar hadoop-mapreduce-examples-2.6.4.jar pi 5 5


## version

hadoop version
cat /etc/centos-release (centos 6.3)
java -version

## usershells

root的usershells中可以把hadoop等安装好的东西，从主机，复制到其他节点。还有其他关于用户的创建、删除、移动的sh文件.

## 验证节点是可以连接得上的

ssh compute-0-36进入其他节点

## 查看空间

df -h
du -h --max-depth=1

## 存储

root用户在root文件夹中
hadoop装在usr文件夹里
在export中的student_home里放用户
在export中的rawdata里放公用数据


## 查看节点信息

hdfs dfsadmin -report

## 监控网站/确认节点情况

http://10.141.200.205/ganglia/
http://10.141.200.205:50070/
http://10.141.200.205:8088/

## rocks安装计算节点compute nodes（设置MAC，连接slave）

http://central-7-0-x86-64.rocksclusters.org/roll-documentation/base/7.0/install-compute-nodes.html

## Apache

https://hadoop.apache.org/docs/r2.6.4/index.html

## 挂起

nohup后面接上正常的命令，就可以在后台运行命令行
nohup cmd >redirect_file.txt &

## Data

wiki介绍网址
https://dumps.wikimedia.org/enwiki/20191101/
数据下载链接
https://dumps.wikimedia.org/enwiki/20191101/enwiki-20191101-pages-articles-multistream.xml.bz2
数据的索引下载链接
https://dumps.wikimedia.org/enwiki/20191101/enwiki-20191101-pages-articles-multistream-index.txt.bz2

后台下载wiki
nohup wget https://dumps.wikimedia.org/enwiki/20191101/enwiki-20191101-pages-articles-multistream.xml.bz2 >wiki.txt &

sogou news 2012
https://www.sogou.com/labs/resource/cs.php
数据下载链接
https://www.sogou.com/labs/resource/ftp.php?dir=/Data/SogouCS/news_sohusite_xml.full.zip


## 用户创建

创建用户要有hadoop权限：
rocks sync users
授权/tmp文件夹：
hadoop fs -chmod -R 777 /tmp

添加用户组
groupadd studentgroup
把用户加入用户组
usermod -a -G studentgroup ds19

## HDFS

hdfs上建文件夹
hadoop fs -mkdir /data
hdfs dfs -mkdir -p /wifi/classify
上传文件
hadoop fs -put data.txt /data
下载文件
hadoop fs -get /data/data.txt /user
查看hdfs上的文件
hdfs dfs  -ls /data
hadoop fs  -ls /data
删除
hadoop fs -rm -r /data

scp rm_hadoop.sh root@compute-0-36:/usr/
