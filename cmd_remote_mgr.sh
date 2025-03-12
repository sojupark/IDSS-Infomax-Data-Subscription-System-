#!/bin/bash

echo $*

MYHOST=$1
MYDIR=$2

shift 2
if [ "${MYHOST}" == "ftp2" ];then
	ssh ${MYHOST}  "source ~/.bash_profile;cd ${MYDIR};python3 myfeedmgr3.py $*"
else
	ssh ${MYHOST}  "source ~/.bash_profile;cd ${MYDIR};python3.6 myfeedmgr3.py $*"
fi

