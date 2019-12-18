#! /bin/bash

export PATH=/bin:/sbin:/usr/bin:/usr/sbin: 
for username in $(cat newusers.txt)
do
if [ -n $username ]
then
  useradd -m -g hdusers $username
  echo
  echo "123456" | passwd --stdin $username
  echo
  echo "Username $username's passoword in changed!" 
else
  echo "The username is null"
fi
done
