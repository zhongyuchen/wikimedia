#! /bin/bash

export PATH=/bin:/sbin:/usr/bin:/usr/sbin: 
for username in $(cat newusers.txt)
do
if [ -n $username ]
then
  userdel -r $username
  echo
  #echo "123456" | passwd --stdin $username
  echo
  echo "Username $username's passoword in deleted!" 
else
  echo "The username is null"
fi
done
