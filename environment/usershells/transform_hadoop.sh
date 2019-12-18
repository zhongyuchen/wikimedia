#!/bin/bash
# This script is used to transform jdk files to compute nodes
ssh root@compute-0-36 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-37 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-38 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-39 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-40 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-41 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-42 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-43 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-44 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-45 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-46 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-47 -C "/bin/bash" < rm_hadoop.sh
#ssh root@compute-0-48 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-49 -C "/bin/bash" < rm_hadoop.sh
#ssh root@compute-0-50 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-51 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-52 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-53 -C "/bin/bash" < rm_hadoop.sh
ssh root@compute-0-54 -C "/bin/bash" < rm_hadoop.sh
#ssh root@compute-0-55 -C "/bin/bash" < rm_hadoop.sh

scp -r /usr/hadoop-2.6.4 root@compute-0-36:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-37:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-38:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-39:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-40:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-41:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-42:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-43:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-44:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-45:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-46:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-47:/usr
#scp -r /usr/hadoop-2.6.4 root@compute-0-48:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-49:/usr
#scp -r /usr/hadoop-2.6.4 root@compute-0-50:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-51:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-52:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-53:/usr
scp -r /usr/hadoop-2.6.4 root@compute-0-54:/usr
#scp -r /usr/hadoop-2.6.4 root@compute-0-55:/usr
