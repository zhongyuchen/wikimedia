#!/bin/bash

export PATH=/bin:/sbin:/usr/bin:/usr/sbin:
for username in $(cat ~/usershells/newusers.txt)
do
	usermod -d /state/partition1/students_home/$username -m $username 
	echo "new home of $username is created."
done

