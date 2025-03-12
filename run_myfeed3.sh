#!/bin/bash
JOBSTATUS=${1}
MYCMDDIR=${2}
MYLOGDIR=${MYCMDDIR}/log
MYERRDIR=${MYCMDDIR}/err
MYLOGMGR=${MYCMDDIR}/myfeedmgr.log

PYTHON_VER="python3.6"
PYTHON_3_6=$(which ${PYTHON_VER} | wc -l)

if [ ${PYTHON_3_6} -eq 0 ];then
	PYTHON_VER="python3"
fi

cd ${MYCMDDIR}

if [ ! -d ${MYLOGDIR} ];then
	mkdir ${MYLOGDIR}
fi


if [ ! -d ${MYERRDIR} ];then
	mkdir ${MYERRDIR}
fi


if [ "${MYCMDDIR}" == "" ];then
	echo "run_myfeed3.sh [jobstatus<RUN,TEST ...>] [HOMEDIR]"
else
	find ${MYCMDDIR}/data -type f -mtime +7 | xargs rm -f
	
	if [ "${JOBSTATUS}" == "RUN" ];then
		${PYTHON_VER} ${MYCMDDIR}/myfeedmgr3.py -run > ${MYLOGMGR} 2>&1
	else
		${PYTHON_VER} ${MYCMDDIR}/myfeedmgr3.py -j ${JOBSTATUS} > ${MYLOGMGR} 2>&1
	fi
	
	myerr=$(cat ${MYLOGMGR} | grep -i error)
	if [ "${myerr}" != "" ];then
		ssh sp1 sms 상우 "myfeedmgr : ${myerr}"
	fi


	if [ ${PYTHON_3_6} -ne 0 ];then
		# check myfeedmgr framework
		for checkPROC in "myfeedmgr_tcp" "myfeedmgr_ws" "myfeedmgr_restapi" "myfeedmgr_stream_broker";do
			checkPROC_LOG=${MYERRDIR}/${checkPROC}.log
			if [ $(ps -ef | grep "${checkPROC}.py" | grep -v "grep" | grep -v "vi" | wc -l) -eq 0 ];then
				${PYTHON_VER} ${MYCMDDIR}/${checkPROC}.py > ${checkPROC_LOG} 2>&1 &
			fi
			myerr=$(cat ${checkPROC_LOG} | grep -i error | wc -l)
			if [ ${myerr} -gt 0 ];then
				ssh sp1 sms 상우 "${checkPROC} $(hostname) : ${myerr}"
				cat ${checkPROC_LOG} > ${checkPROC_LOG}_$(date +"%Y%m%d-%H%M%S")
				cat /dev/null > ${checkPROC_LOG}
			fi
		done
	fi
fi
