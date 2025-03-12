#-*-coding:utf8-*-
import sys,os,glob
import socket
import websocket
import requests
import math
#import signal
import re
import json
from threading import Thread
import subprocess
from multiprocessing import Process
import logging
import logging.handlers
from wcwidth import wcswidth

import mydbapi3 as mydbapi
import myutil3 as myutil

from myutil3 import getDeltaDate

sp_utf8_euckr = {
"¥":"￥"
,"ô":"o"
,"é":"e"
,"á":"a"
,"ã":"a"
,"í":"i"
,"ñ":"n"
,"É":"E"
,"Ö":"O"
,"ó":"o"
,"–":"-"
,"ü":"u"
,"ö":"o"
,"ê":"e"
,"ç":"c"
,"ú":"u"
,"å":"a"
,"è":"e"
,"ï":"i"
,"ä":"a"
,"Ã":"A"
,"£":"￡"
,"Å":"A"
,"õ":"o"
,"©":"ⓒ"
,"Á":"A"
,"ë":"e"
,"∙":"ㆍ"

}

MYHOME=os.getcwd()

# set the log
logger = logging.getLogger('myfeedmgr')

FMTER = logging.Formatter('%(asctime)s|%(process)d|%(message)s')
fileMaxByte = 1024 * 1024 * 110 #110MB
FH = logging.handlers.RotatingFileHandler(MYHOME+'/log/myfeedmgr.log', maxBytes=fileMaxByte, backupCount=10)
FH.setFormatter(FMTER)
logger.addHandler(FH)
logger.setLevel(logging.INFO)

# system default
#myEncoding='utf8'
myEncoding=os.environ.get("LANG").split(".")[1].lower()

myFileEncoding = 'utf8'

#def signal_term_handler(signum, frame):
#	logger.info("stop server---signum[{}]".foramt(signum))
#
#signal.signal(signal.SIGTERM, signal_term_handler)
#signal.signal(signal.SIGINT, signal_term_handler)

# feedmanager main database
myfeedmgr_db_host='ftp2'
if socket.gethostname().lower() == myfeedmgr_db_host:
	myfeedmgr_db_host = "localhost"


def getJoinData(rootRowDic, thisROW, joinPackName, join_info):
	pass


def getListItem(filestr, myval):
	if type(myval) == dict:
		for k,v in myval.items():
			if type(v) == dict or type(v) == list:	
				getListItem(filestr, v)
			else:
				filestr.append(v)
	elif type(myval) == list:
		for v in myval:
			if type(v) == dict or type(v) == list:	
				getListItem(filestr, v)
			else:
				filestr.append(v)
	return filestr

def trunc(x, dec):
	return math.trunc(x*math.pow(10,dec))/math.pow(10,dec)	


# limit length and align in korean
def fmt_k2(x, w, align='L'):
	align=align.lower()
	x = str(x)
	l = wcswidth(x)
	s = w -l
	if s <= 0:
		return x
	if align == 'l':
		return x + ' '*s
	if align == 'c':
		sl = s//2
		sr = s - sl
		return ' '*sl + x + ' '*sr
	return ' '*s + x


def hash2str(mystr, mykind='md5'):
	global myFileEncoding
	import hashlib
	return eval("hashlib."+mykind+"(mystr.encode('"+myFileEncoding+"')).hexdigest()")

def send2Client(mycon, myfile, file_extension, myjobDic, adjust_files):
	global myFileEncoding
	# get the client info
	for c_url in myjobDic['c_url'].strip().replace("\r","").replace("\n","").replace("\t","").split(","):
		sql = """select cd.*, wss.*, case when (select url from myfeed.ws_status where name = wss.pipeline) is null then '' else (select url from myfeed.ws_status where name = wss.pipeline) end as pipeline_url
							from myfeed.client_dns cd
							left outer join myfeed.ws_status wss on cd.c_host = wss.name
							where c_url = '{}'""".format(c_url)
		logger.info(sql)
		titems = mycon.exeQry("G",  sql, useDict=True)
		if titems is not None and len(titems) > 0:
			for titem in titems:
				myjobDic_temp = {}
				myjobDic_temp = myjobDic.copy()
				myjobDic_temp.update(titem)
				myToken = myjobDic_temp['c_id']+"@"+myjobDic_temp['c_passwd']+"@"+myjobDic_temp['c_name']+"@"+str(myjobDic_temp['c_job_num'])+"@"+myjobDic_temp['c_host']

				if myjobDic_temp['c_proto'] == 'FTP': #ftp
					goFTP(myfile, myjobDic_temp['c_host'], myjobDic_temp['c_port'], myjobDic_temp['c_dir'], myjobDic_temp['c_id'], myjobDic_temp['c_passwd'])
				elif myjobDic_temp['c_proto'] == 'SFTP': #sftp
					goSFTP(myfile, myjobDic_temp['c_host'], myjobDic_temp['c_port'], myjobDic_temp['c_dir'], myjobDic_temp['c_id'], myjobDic_temp['c_passwd'])
				elif myjobDic_temp['c_proto'][:8] == 'REST_API': #websocket intersystem
					if myjobDic_temp['c_proto'].split("_")[-1] == "SERVER":
						#post
						try:
							myheaders = "Content-Type: application/json"
							if file_extension != 'json':
								myheaders = "Content-Type: application/x-www-form-urlencoded"

							myCmd = "curl -v -X POST {} -H \"{}\" -d @{} {}".format(" ".join(myjobDic_temp['etc'].split(",")), myheaders, myfile, myjobDic_temp['c_host']) 
							logger.info("{}".format(myCmd))
							myout = subprocess.check_output(myCmd,shell=True, stderr=subprocess.STDOUT).decode(myEncoding,'ignore')
							logger.info("{}".format(myout))
						except:
							logger.info("error - {} -- {}".format(myjobDic_temp['c_host'], myutil.getErrLine(sys)))
							raise
					else: # get
						if myjobDic_temp['fromreq'].strip() == c_url:
							logger.info("--->connect ws to {}".format(myjobDic_temp['pipeline_url']))

							if myjobDic_temp['pipeline_url'] == '':
								logger.info("error - pipeline url is empty")
								raise
							else:
								ws = websocket.create_connection(myjobDic_temp['pipeline_url'])

								for myline in open(myfile, 'r', encoding=myFileEncoding).readlines():
									#logger.info("send to {} data {}".format(myjobDic_temp['url'], str({"id":"infomax_sys","passwd":"infomax_sysinfomax!@","to":myjobDic_temp['c_id'],"data":str(myline)})))
									#logger.info("send to {} data ".format(myjobDic_temp['pipeline_url']))
									logger.info("send to {} data {}".format(myjobDic_temp['url'], str({"token":"infomax_sys@systemmgr","to":myToken,"data":str(myline)})))
									try:
										#ws.send(str({"id":"infomax_sys@systemmgr","to":myjobDic_temp['c_id'],"data":str(myline)}))
										ws.send(str({"token":"infomax_sys@systemmgr","to":myToken,"data":str(myline)}))
									except:
										pass

								ws.close()	

				elif myjobDic_temp['c_proto'][:3] == 'WS_': #websocket
					logger.info("connect ws to {}".format(myjobDic_temp['url']))
					ws = websocket.create_connection(myjobDic_temp['url'])

					# >= 3.8
					#import websockets 
					#with websockets.connect(myjobDic_temp['url']) as ws:
					logger.info("success to connect ws to {}".format(myjobDic_temp['url']))
					if myjobDic_temp['c_proto'][3:] == "SERVER":
						# send authentication
						pass
						#sendPacket = '{"type":"auth","data":{"id":"'+myjobDic_temp['c_id']+'","passwd":"'+myjobDic_temp['c_passwd']+'"}}'
						#ws.send(sendPacket)

					for myline in open(myfile, 'r', encoding=myFileEncoding).readlines():
						#logger.info("send to {} data {}".format(myjobDic_temp['url'], str({"id":"infomax_sys","passwd":"infomax_sysinfomax!@","to":myjobDic_temp['c_id'],"data":str(myline)})))
						logger.info("send to {} data ".format(myjobDic_temp['url']))
						try:
							#ws.send(str({"id":"infomax_sys","passwd":"infomax_sysinfomax!@","to":myjobDic_temp['c_id'],"data":str(myline)}))
							ws.send(str({"token":"infomax_sys@systemmgr","to":myToken,"data":str(myline)}))
						except:
							pass

					ws.close()	

				# email
				elif myjobDic_temp['email_target'] is not None and myjobDic_temp['email_target'].strip() != '' and myjobDic_temp['email_target'].strip() != "None": 
					goEMAIL2(myfile, myjobDic_temp['email_target'].encode(myEncoding,'replace').decode(myEncoding,'replace'), myjobDic_temp['email_target_cc'].encode(myEncoding,'replace').decode(myEncoding,'replace'), myjobDic_temp['email_title'].encode(myEncoding,'replace').decode(myEncoding), myjobDic_temp['email_body'].encode(myEncoding,'replace').decode(myEncoding), myFileEncoding, adjust_files)






def copy2Target(myfile, myhost, mylocalhost, mytarget_dir_list, inc_target="", inc_skip=-1):
	inc_skip = int(inc_skip)

	for mytarget_dir in mytarget_dir_list.split(","):
		# local
		if myhost.lower().strip() == mylocalhost.lower().strip():
			# check dir
			myCmd = "find "+mytarget_dir
			try:
				logger.info("check...dir...{}".format(myCmd))
				myout = subprocess.check_output(myCmd,shell=True, stderr=subprocess.STDOUT).decode(myEncoding,'ignore')
				if myout.find("directory") != -1 or myout.find("디렉터리".encode(myEncoding).decode(myEncoding,"ignore")) != -1:
					myCmd = "mkdir -m 775 "+mytarget_dir
					logger.info("{}".format(myCmd))
					subprocess.check_output(myCmd, shell=True)

			except subprocess.CalledProcessError as e:
				# mkdir
				myCmd = "mkdir -m 775 "+mytarget_dir
				logger.info("{}".format(myCmd))
				try:
					subprocess.check_output(myCmd, shell=True)
				except:
					logger.info("fail ----> {}".format(myCmd))

			# copy
			myCmd = "cp "+myfile+" "+mytarget_dir
			if inc_target != "":
				myCmd = "cat "+myfile
				if inc_skip > 0:
					inc_skip+=1
					myCmd+= " | tail -n +"+str(inc_skip)
				myCmd+=" >> "+mytarget_dir+"/"+inc_target
			logger.info("{}".format(myCmd))
			try:
				subprocess.check_output(myCmd, shell=True)
			except:
				logger.info("fail ----> {}".format(myCmd))
		#remote	
		else:
			# check dir
			myCmd = "ssh "+mylocalhost+" \"find "+mytarget_dir+"\""
			try:
				logger.info("check...dir...{}".format(myCmd))
				myout = subprocess.check_output(myCmd,shell=True, stderr=subprocess.STDOUT).decode(myEncoding,'ignore')
				if myout.find("directory") != -1 or myout.find("디렉터리".encode(myEncoding).decode(myEncoding, 'replace')) != -1:
					myCmd = "ssh "+mylocalhost+" mkdir -m 775 "+mytarget_dir
					logger.info("{}".format(myCmd))
					myout = subprocess.check_output(myCmd, shell=True, stderr=subprocess.STDOUT)

			except subprocess.CalledProcessError as e:
				myout = e.output.decode(myEncoding)
				logger.info("{}----{}".format(myout, myCmd))
				# mkdir
				myCmd = "ssh "+mylocalhost+" mkdir -m 775 "+mytarget_dir
				logger.info("{}".format(myCmd))
				try:
					myout = subprocess.check_output(myCmd, shell=True, stderr=subprocess.STDOUT)
				except subprocess.CalledProcessError as e:
					myout = e.output.decode(myEncoding)
					logger.info("fail ----> {}--->{}".format(myCmd, myout))

			# copy
			myCmd = "scp "+myfile+" "+mylocalhost+":"+mytarget_dir
			if inc_target != "":
				myCmd = "cat "+myfile
				if inc_skip > 0:
					inc_skip+=1
					myCmd+= " | tail -n +"+str(inc_skip)

				myCmd += "| ssh "+mylocalhost+' \"cat >> '+mytarget_dir+"/"+inc_target+'\"'
			logger.info("{}".format(myCmd))
			myTry = 0
			while myTry < 5:
				try:
					myout = subprocess.check_output(myCmd, shell=True, stderr=subprocess.STDOUT)
					break
				except subprocess.CalledProcessError as e:
					myout = e.output.decode(myEncoding)
					logger.info("retry[%d]----> [%s]--->[%s]" % (myTry, myCmd, myout))
					myutil.rsleep(2,3)
					myTry +=1
		

def goEMAIL2(myfile, toYouStr, toYouStrCC, toSubject, toBody, myFileEncoding, inline_info=[]):

	# treat special char
	toSubject = myutil.getSpecialStr(toSubject)
	toBody = myutil.getSpecialStr(toBody)

	toYouStr = toYouStr.replace("\n","").replace("\r","").replace("\t","")
	toYouStrCC = toYouStrCC.replace("\n","").replace("\r","").replace("\t","")	

	if toYouStr is not None and toYouStr.strip() != "" and toYouStr.strip() != "None":
		# if the size of file is greater than the limit(30M), zip the file as spliting
		LIMIT_FILE_SIZE = 1024 * 1024 * 20
		myfilesize = os.stat(myfile).st_size

		targetlist=[]
		try:

			# split for attached files
			if myfilesize > LIMIT_FILE_SIZE:
				try:
					# rm the existed
					targetlist=glob.glob(myfile+".z*")
					for rmfile in targetlist:
						os.remove(rmfile)
				except:
					logger.info("{}".format(myutil.getErrLine(sys)))

				myDIR = os.path.dirname(myfile)+"/"
				myFILENM = os.path.basename(myfile)

				mycmd = "cd {2};zip -r -s {1} {0}.zip {0}".format(myFILENM, LIMIT_FILE_SIZE, myDIR)
				logger.info("{}".format(mycmd))
				#wait
				subprocess.call(mycmd, shell=True)

				targetlist=glob.glob(myfile+".z*")
			else:
				targetlist=[myfile]
			targetlist.sort()

	
			# set the email 
			import smtplib
			from email.mime.text import MIMEText
			from email.mime.multipart import MIMEMultipart
			from email.mime.base import MIMEBase
			from email.mime.image import MIMEImage
			from email import encoders
			from email.utils import formataddr

		
			i = 1
			addStr=""

			sender="infomaxemailservice@gmail.com"
			msg = MIMEMultipart()
			msg['Subject'] = toSubject+addStr
			msg['From'] = formataddr(('infomax',sender))
			msg['To'] = toYouStr
			if toYouStrCC.strip() != "":
				msg['Cc'] = toYouStrCC

			# general text or inline image
			if len(inline_info) > 0:
				i = 0 
				for mytext in toBody.split("{"):
					if mytext.find("}") != -1:
						mynum, mytext = mytext.split("}")
						# contract number
						mynum = mynum.split(":")[0]
						image_path = inline_info[int(mynum)]['fileNm']+".jpg"
						with open(image_path, 'rb') as image_fd:
							image_data = image_fd.read()
							image = MIMEImage(image_data, name=image_path.split("/")[-1])
							image.add_header('Content-ID', '<image'+str(i)+'>')
							msg.attach(image)
							#msg.attach(MIMEText("<img src='cid:image"+str(i)+"'>", 'html','utf8'))
						if mytext != "":
							msg.attach(MIMEText(mytext, 'plain','utf8'))
					else:
						msg.attach(MIMEText(mytext, 'plain','utf8'))
					i += 1
			else:
				# general text 
				msg.attach(MIMEText(addStr+toBody, 'plain','utf8'))
				

			# check attach files
			for mysendfile in targetlist:
				onlyfilenm = os.path.basename(mysendfile)
				part = MIMEBase('application','octet-stream')
				part.set_payload(open(mysendfile,"rb").read())
				encoders.encode_base64(part)
				onlyfilenm = os.path.basename(mysendfile)
				part.add_header('Content-Disposition','attachment; filename = "%s"' %(onlyfilenm))
				msg.attach(part)
	
			# send mail	
			trySMTP=0
			while trySMTP < 5:
				try:
					with smtplib.SMTP_SSL('smtp.gmail.com',465) as smtp:
						smtp.login(sender,'bjum gobq buib ptia')
						logger.info("{}-CC{}--{}".format(toYouStr,toYouStrCC, 'send mail!!!'))
						smtp.sendmail(sender, toYouStr.split(",")+toYouStrCC.split(","), msg.as_string())
						logger.info("{}".format('acommplish to send mail!!!'))
						logger.info("{}-{}".format(toYouStr,'send mail!!!'))

					break	
				except:
					trySMTP +=1
					logger.info("{} try {} time....30 Sec later".format(myutil.getErrLine(sys), trySMTP))
					myutil.rsleep(29,30)
			
			i +=1
		except:
			logger.info("goEMAIL{}".format(myutil.getErrLine(sys)))
			raise
	

def goFTP(myfile, toHost, toPort, toDir, toId, toPasswd):
	try:
		tarHost = ""
		tarPort = 0
		if toHost.strip() != "":
			from ftplib import FTP
			tarHost = toHost
			if toPort is None or toPort.strip() == '':
				tarPort = 21
			else:
				tarPort = int(toPort)

			onlyfilenm = os.path.basename(myfile)

			myftp = FTP()
			
			logger.info("tarHost[{0}],tarPort[{1}]".format(tarHost,tarPort))
			myftp.connect(host=tarHost,port=tarPort)
			myftp.login(user=toId, passwd=toPasswd)
			myftp.cwd(toDir)
			logger.info("login success for user-{}, dir = {}".format(toId, toDir))
			myftp.storbinary("STOR "+onlyfilenm, open(myfile, "rb"))
			logger.info("send file {}, STOR {}".format(myfile, onlyfilenm))
			myftp.quit()
	except:
		logger.info("{}".format(myutil.getErrLine(sys)))


def goSFTP(myfile, toHost, toPort, toDir, toId, toPasswd):
	try:
		import paramiko
		with paramiko.SSHClient() as myssh:
			logger.info("sftp-{}-{}-{}-{}".format(toHost, toId, toPasswd, toDir))
			myssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
			myssh.connect(toHost, username=toId, password=toPasswd)
			ss_sftp = myssh.open_sftp()
	
			if toDir.strip() == "":
				toDir = "./"
			ss_sftp.put(myfile, toDir+os.path.basename(myfile))	
		logger.info("sftp success!!{}")

	except:
		logger.info("fail to process sftp in paramiko ---> {}".format(myutil.getErrLine(sys)))
		try:
			tarHost = ""
			tarPort = 0
			if toHost.strip() != "":
				myCmd="sftp "+toId+"@"+toHost+":"+toDir+" <<< $'put "+myfile+"'"
				logger.info("{}".format(myCmd))
				subprocess.call(myCmd, shell=True, timeout=60)
		except:
			logger.info("{}".format(myutil.getErrLine(sys)))
			raise

	
def gogoJob(myjobDic, tempdir=''):
	#file encoding
	global myFileEncoding
	myFileEncoding=myjobDic['c_file_encoding']

	logger.info("start {} {}".format('general' if tempdir == '' else 'urgent', myjobDic))
	start_time = myutil.getToday("%Y%m%d%H%M%S")
	isOK=True

	errMsg = ''
	myfile=""
	mytarget_dir = ""
	client_name = ""
	myDecorator = {}

	myvaldic = {}
	with mydbapi.Mydb("mysql", "myfeed", myfeedmgr_db_host,  _myPort=3306,_myUser='myfeed') as mycon:
		try:
			#open file
			myTarPath = MYHOME+"/data/"+tempdir+myjobDic['c_name']+"/"+str(myjobDic['c_job_num'])
			
			if os.path.isdir(myTarPath) == False:
				try:
					mypath = "/"
					for tmppath in myTarPath.split("/"):
						mypath += tmppath+"/"
						#logger.info("error --- {}".format(mypath))
						if os.path.isdir(mypath) == False:
							os.mkdir(mypath)
				except:
					logger.info("error --- {}".format(myutil.getErrLine(sys)))

			# default data directory of myfeedmgr 
			# treat user define and system special variable
			myfile=myutil.getSpecialStr(myTarPath+"/"+myjobDic['c_file'])
			if myjobDic['tar_file'].strip() != "":
				myfile=myutil.getSpecialStr(myTarPath+"/"+myjobDic['tar_file'])


			# unpack act_packages
			# get client info
			sql = """select ci.sel_act_packages, ci.act_package_qry_args, ci.client_name, ci.act_package_alias, ci.join_package_where
				from myfeed.client_info ci
				where ci.c_name = '%s' and ci.c_file = '%s'""" % (myjobDic['c_name'], myjobDic['c_file'].replace("'","\\'"))


			logger.info(sql)
			titems = mycon.exeQry("G1", sql)
			if titems is not None:
				myvaldic['delimiter'] = myjobDic['delimiter']
				# union package
				mypackages_str = ""
				if titems[0] is not None:
					mypackages_str = re.sub("#.*","",titems[0])
					mypackages_str = mypackages_str.replace("\r","").replace("\n","").replace("\t","")
				mypackages = [ x.strip() for x in mypackages_str.split("|") ]
				mypackages_args = []
				if titems[1] is None or titems[1].lower().replace("none",'').strip() == '':
					mypackages_args = ['']*len(mypackages)
				else:
					mypackages_args = re.sub("#.*","",titems[1])
					mypackages_args = mypackages_args.replace("\r",'').replace("\n","").split("|")



				client_name = ""
				if titems[2] is not None:
					client_name = titems[2]


				act_package_alias = ""
				if titems[3] is not None:
					act_package_alias = titems[3].replace("\r","").replace("\n","").replace("\t","").replace("None","").replace("none","").strip()

				package_alias = {}
				# use the index of package 
				# alias
				if act_package_alias != "":
					for aliases in act_package_alias.split(","):
							mypackage_idx, myalias = aliases.split("=")
							package_alias[int(mypackage_idx.strip("{}"))] = myalias


				# use the index of package 
				idx = 1
				package_join = {}
				join_package_where = ""
				if titems[4] is not None:
					join_package_where = titems[4].replace("\r","").replace("\n","").replace("\t","").replace("none","").replace("None","").replace(" ","")
				logger.info("join_pacakge_where -{}".format(join_package_where))

				if myjobDic['myheader'] == "U":
					join_package_where = join_package_where.upper()
				elif myjobDic['myheader'] == "L":
					join_package_where = join_package_where.lower()


				for pack_name in mypackages:
					isJoin = "None"
					# JOIN
					if pack_name.find("+") != -1:
						for join_pack_name in pack_name.split("+"):
							pack_name = join_pack_name	
							if isJoin == "None":
								package_join[idx] = {"type":"std", "mypack_name":pack_name}
							else:
								package_join[idx] = {"type":"left", "mypack_name":pack_name, "join_info":[]}
								for join_info in join_package_where.split(","):
									logger.info("join_info -{}".format(join_info))
									stddrive, leftout = join_info.split("=")
									stddrive_idx, stddrive_col = stddrive.split(".")
									leftout_idx, leftout_col = leftout.split(".")
									stddrive_idx = int(stddrive_idx.strip("{}"))
									leftout_idx = int(leftout_idx.strip("{}"))

									# if it is my join 
									if leftout_idx == idx:
										if len(package_join[idx]['join_info']) == 0:
											package_join[idx]['join_std_pack_idx'] = stddrive_idx

											if stddrive_idx in package_alias:
												package_join[idx]['join_std_pack_name'] = package_alias[stddrive_idx]
											else:
												package_join[idx]['join_std_pack_name'] = package_join[stddrive_idx]['mypack_name']

										package_join[idx]['join_info'].append({"std_col":stddrive_col, "my_col":leftout_col})
							isJoin = "treat"
							idx += 1

					if isJoin == "None":
						idx += 1


				# adjust mypackages list if join exists 
				if len(package_join) > 0: 
					mypackages = []
					for mypackage in mypackages_str.split("+"):
						mypackages.extend(mypackage.split("|"))



				# process the sel_act_packages
				packIdx = 1
				prev_header_filestr = []
				total_row_count = 0
				this_row_count = 0
				max_col_cnt = 0
				jsonOut = {}
				rowJsonData = []


				# only use for package join
				joinJsonData = {}

				# check myheader
				# only after add myheader if excel style file, check it
				file_extension = ""
				if myfile.find(".") != -1:
					file_extension = myfile.split(".")[-1].lower()

				myEOF = ""
				# set EOF
				if myjobDic['use_cr_code'] == 'Y':
					myEOF +="\r"	
				myEOF += '\n'


				# if file transmission protocol
				myFileEncoding = myjobDic['c_file_encoding']
				with open(myfile, 'w', encoding=myFileEncoding) as myfd:
					for mypackage in zip(mypackages, mypackages_args):
						myvaldic['act_package'] = mypackage[0]  # check joins '+' string
						myvaldic['act_package_qry_args'] = mypackage[1]


						#file spec list in sequence order
						sql = """select apm.act_package_seq as colseq
								, replace(replace(apm.act_package_qry_col, "<-",""), "->","") as colnm
								, fs.myreplace as myreplace
								, fs.act_package_block_seq as blockseq
								, case when apm.act_package_qry_col like '%=<-%' then 'Y' else '' end as is_input
								, case when apm.act_package_qry_col like '%=->%' then 'Y' else '' end as re_name
								, case when fs.myreplace = '=${{INVISIBLE}}' then  'Y' else '' end as invisible
								from myfeed.file_spec fs, myfeed.act_package_master apm
								where fs.c_name = '{c_name}' and fs.c_file = '{c_file}' and fs.sel_act_package_idx = {package_idx} and apm.act_package = '{act_package}' and fs.act_package_seq = apm.act_package_seq 
								and apm.act_package_qry_col <> ''
								order by fs.c_seq""".format(c_name=myjobDic['c_name'], c_file=myjobDic['c_file'].replace("'","\\'"), act_package=myvaldic['act_package'], package_idx=packIdx)
						#print(sql)
						logger.info(sql)
						#file_spec_list = [(myval[0], myval[1].split("=")[0]+"="+myutil.getSpecialStr(myval[1].split("=")[1]).lower(), myval[2], myval[3]) if myval[1].find("=") != -1 else (myval[0], myval[1].lower(),myval[2], myval[3]) for myval in mycon.exeQry("G", sql)]
						file_spec_list = [{'colseq':x['colseq'], 'colnm':x['colnm'].split("=")[0]+"="+myutil.getSpecialStr(x['colnm'].split("=",1)[1]).replace("\n","").replace("\r","").replace("\t",""), 'myreplace':x['myreplace'], 'blockseq':x['blockseq'], 'is_input':x['is_input'], 're_name':x['re_name'], 'invisible':x['invisible']} if x['colnm'].find("=") != -1 else x for x in mycon.exeQry("G", sql, useDict=True)]

						if len(file_spec_list) == 0:
							errMsg = "error -{}".format(myvaldic['act_package']+" 의 패키지 구성 마스터가 비어 있있거나, \n 파일 ".encode(myEncoding).decode(myEncoding)+myjobDic['c_file']+" 과 연결이 안되어 있습니다.".encode(myEncoding).decode(myEncoding,'replace'))
							logger.info(errMsg)
							raise
							


						# write in the file	
						titems = None
						mycol = None
						useComposite_OutBlock = {}
						colSensitive = myjobDic['myheader']
						setLower = False
						isDecorator = False
						invisibleDic = {}
						mod_myreplace = {}


						if myvaldic['act_package'] == '더미파일용'.encode(myEncoding).decode(myEncoding,'replace'):
							titems = []
							mycol = []
						elif myvaldic['act_package'] == '@DECORATOR': # decorator
							isDecorator = True
							titems =[{'row_count':this_row_count, 'date_time':myutil.getToday("%Y%m%d%H%M%S"), 'total_row_count':total_row_count}]
							myvaldic.update({'myencoding':'system'})
							logger.info("--->{}".format("@DECORATOR"))
						else:
							sql = """
								select apq.*
								from myfeed.act_package_qry apq
								where apq.act_package = '%s'""" % (myvaldic['act_package'])
							tmp_titems = mycon.exeQry("G", sql, useDict=True)
							logger.info("--->{}".format(sql))
							for myval in tmp_titems:
								myvaldic.update(myval)
							
			
							# if myfeedmgr server equals dbms server	
							if myvaldic['host'].lower() == socket.gethostname().lower():
								myvaldic['host'] = 'localhost'
	
							myArgDic = {}
							regexp_def_style=""".[^=]+=\".[^\"]*\"\s?(?:,|$)|.[^=]+=.[^\"]+\s?(?:,|$)|.[^=]+=\"\"|.[^=]+=$"""
							# system arguments
							if myvaldic['act_package_qry_args'] is not None and myvaldic['act_package_qry_args'].strip() != "":
								myArgStr = myutil.getSpecialStr(myvaldic['act_package_qry_args'].replace("\t","").replace("\n","").replace("\r",""))
								myArgDic = dict([(x[:x.find("=")].strip(", "), x[x.find("=")+1:].strip(",' ") if x[x.find("=")+1:].strip(", ").startswith("'") else x[x.find("=")+1:].strip(",\" ")) for x in re.findall(regexp_def_style, myArgStr)])


							# mycrontab modify arguments
							if myjobDic['mod_package_args'] is not None and myjobDic['mod_package_args'].strip() != "":
								myJobArgsList = myjobDic['mod_package_args'].strip().split("|")
								myJobArgs = myJobArgsList[0] # default
								myArgStr = myutil.getSpecialStr(myJobArgs.replace("\t","").replace("\n","").replace("\r",""))
								myArgDic.update(dict([(x[:x.find("=")].strip(", "), x[x.find("=")+1:].strip(",' ") if x[x.find("=")+1:].strip(", ").startswith("'") else x[x.find("=")+1:].strip(",\" ")) for x in re.findall(regexp_def_style, myArgStr)]))
								if len(myJobArgsList) > 1:
									try:
										myJobArgs = myJobArgsList[packIdx-1]
										#print(packIdx, myJobArgs)
										myArgStr = myutil.getSpecialStr(myJobArgs.replace("\t","").replace("\n","").replace("\r",""))
										myArgDic.update(dict([(x[:x.find("=")].strip(", "), x[x.find("=")+1:].strip(",' ") if x[x.find("=")+1:].strip(", ").startswith("'") else x[x.find("=")+1:].strip(",\" ")) for x in re.findall(regexp_def_style, myArgStr)]))
									except:
										pass


								

							# user arguments
							if myjobDic['qry_args'] is not None:
								regexp_def_style=""".[^=]+=\".[^\"]*\"\s?(?:,|$)|.[^=]+=.[^\"]+\s?(?:,|$)|.[^=]+=\"\"|.[^=]+=$"""

								myJobArgsList = myjobDic['qry_args'].strip().split("|")
								myJobArgs = myJobArgsList[0] # default
								# check default argument 
								myArgStr = myutil.getSpecialStr(myJobArgs.replace("\t","").replace("\n","").replace("\r",""))
								myArgDic.update(dict([(x[:x.find("=")].strip(", "), x[x.find("=")+1:].strip(",' ") if x[x.find("=")+1:].strip(", ").startswith("'") else x[x.find("=")+1:].strip(",\" ")) for x in re.findall(regexp_def_style, myArgStr)]))
								if len(myJobArgsList) > 1:
									try:
										myJobArgs = myJobArgsList[packIdx-1]
										print(packIdx, myJobArgs)
										myArgStr = myutil.getSpecialStr(myJobArgs.replace("\t","").replace("\n","").replace("\r",""))
										myArgDic.update(dict([(x[:x.find("=")].strip(", "), x[x.find("=")+1:].strip(",' ") if x[x.find("=")+1:].strip(", ").startswith("'") else x[x.find("=")+1:].strip(",\" ")) for x in re.findall(regexp_def_style, myArgStr)]))
									except:
										pass


							myqry_link_col = {}
							try:
								myqry_link_col = { x.strip("\t\n\r ").split("=")[1]:x.strip("\t\n\r ").split("=")[0] for x in myvaldic['myqry_link_col'].strip("\t\n\r ").split(",") }
							except:
								pass


							# replace involved package items in item of the mod_myreplace at schedule in the mycrontab
							if myjobDic['mod_myreplace'] is not None and myjobDic['mod_myreplace'].strip() != "":
								replace_item = myjobDic['mod_myreplace'].split("|")[packIdx-1].strip("\t\n\r ")
								if replace_item.strip() != "":
									for myreplace in replace_item.split(",@,"):
										try:
											colnum, replaceStr = myreplace.split("=",2)
											colnum = int(colnum.strip("${}"))
											mod_myreplace[colnum] = replaceStr
										except:
											pass

							mycon2 = None
							titems = []
							if myvaldic['dbms'] == 'tio':
								#set the inputDic for tio system
								inputDic = {"TR":myvaldic['dbnm']}
								prevBlockSeq = -1
								for k, v in myArgDic.items():
									myColFullNm = k.strip("\t\r\n ")
									if myColFullNm in myqry_link_col:
										myColFullNm = myqry_link_col[myColFullNm]

									blockNmStr, myCol = myColFullNm.split(".")
									blockNm = blockNmStr
									if blockNmStr.find("[") != -1: # array
										blockNm, blockSeq = blockNmStr.split("[")
										blockSeq = int(blockSeq.split("]")[0])

										if blockNm in inputDic:
											if prevBlockSeq != blockSeq:
												inputDic[blockNm].append({myCol:v})
											else:
												inputDic[blockNm][blockSeq][myCol] = v
										else:
											inputDic[blockNm] = [{myCol:v}]
										prevBlockSeq = blockSeq
									else:
										if blockNm in inputDic:
											inputDic[blockNm][myCol] = v
										else:
											inputDic[blockNm] = {myCol:v}

								# request tio
								isDev=(True if myvaldic['host'] == 'dev' else False)
								r = myutil.getTIO(inputDic, isDev=isDev)
								logger.info("getTIO-dev[{}]-->input--->{}".format(isDev, r['input']))
								logger.info("getTIO--->itsOut--->{}".format(r['output']))
								itsOut = r['output']

								if ('msg' in itsOut and itsOut['msg'] == 'error') or 'TR' not in itsOut:
									errMsg += "패키지의 tio 값을 불러올 수 없습니다. {}".format(inputDic)
									logger.info(errMsg)
									itsOut = {}
									#raise

								if len(itsOut) > 0:
									for myOutBlock in  [ x.split(",")[1].strip("/n/r/t ") for x in re.findall("OUTPUT,.+", myvaldic['myqry']) ]:
										if type(itsOut[myOutBlock]) == type([]):
											tmpList = []
											for retList in itsOut[myOutBlock]:
												tmpDic = {}
												for k,v in retList.items():
													#tmpDic[(myOutBlock+"[]."+k).lower()] = str(v)
													tmpDic[(myOutBlock+"[]."+k)] = str(v)
												tmpList.append(tmpDic)

											titems.append(tmpList)
										else:
											tmpDic = {}
											for k,v in itsOut[myOutBlock].items():
												tmpDic[(myOutBlock+"."+k)] = str(v)
											titems.append(tmpDic)

								# check composite block
								prevBlockNm = file_spec_list[0]['colnm']
								for x in file_spec_list:
									if prevBlockNm.split(".")[0] != x['colnm'].split(".")[0]:
										for ttitems in titems:
											if type(ttitems) != type([]):
												useComposite_OutBlock.update(ttitems)

							else: # general dbms
								# set the default string format item
								sql = myutil.getSpecialStr(myvaldic[myjobDic['myqry_type']], myjobDic)
								for myFormatItem in re.findall("{.[^{}\\\\]*}", sql):
									if myFormatItem[1:-1] not in myArgDic:
										if myFormatItem[1:-1].find("{") != -1 or myFormatItem[1:-1].find("}") != -1:
											pass
										else:
											myArgDic[myFormatItem[1:-1]]=""
								
									
								for modArgCol, oriArgCol in myqry_link_col.items():
									if modArgCol in myArgDic:
										myArgDic[oriArgCol] = myArgDic[modArgCol]
										for argCol, argVal in myArgDic.items():
											if argVal.find("{"+modArgCol+"}") != -1:
												myArgDic[argCol] = eval(argVal.replace("{"+modArgCol+"}", myArgDic[modArgCol]).strip("${}"))
												
										del myArgDic[modArgCol]

									
								sql = sql.format(**myArgDic)
								if 'add_myqry' in myjobDic:
									sql = sql.strip(";") + " " + myjobDic['add_myqry']

								# multi statements
								p_quote = re.compile("\'")
								p_dquote = re.compile("\"")

								exeQryStmt = ""

								mycon2 = None
								if myvaldic['port'] == -1:	
									mycon2 = mydbapi.Mydb(myvaldic['dbms'], myvaldic['dbnm'], myvaldic['host'], _myEncoding=myvaldic['myencoding'])
								else:
									mycon2 = mydbapi.Mydb(myvaldic['dbms'], myvaldic['dbnm'], myvaldic['host'],  _myPort=myvaldic['port'], _myEncoding=myvaldic['myencoding'])
								
								# have to preorder function and proceudre 
								try:
									for qryblock in re.findall(".[^;]*;*", sql):
										#print(qryblock)
										exeQryStmt += qryblock
										if len(p_quote.findall(exeQryStmt)) % 2 == 0 and len(p_dquote.findall(exeQryStmt)) % 2 == 0:
											exeQryStmt = exeQryStmt.replace("\\n","").replace("\\r","")
											if exeQryStmt.strip() == "":
												pass
											else:
												try:
													# treat empty alias in select
													exeQryStmt = re.sub(r",\s+as ", ", 'nodata' as ", exeQryStmt)
													exeQryStmt = re.sub(r"select \s+as ", "select 'nodata' as ", exeQryStmt)

													# execute sql
													#exeQryStmt = myutil.getSpecialStr(exeQryStmt, myjobDic)
													myjobDic['sql'] = exeQryStmt
													logger.info("{}".format("======================================"))
													logger.info("sql--->{}".format(exeQryStmt))
													titems = mycon2.exeQry("G", exeQryStmt, useDictLow=True)
													#titems = mycon2.exeQry("G", exeQryStmt, useDict=True)
													setLower = True
													logger.info("{}".format("======================================"))
												except:
													errMsg += "error -{}".format(myutil.getErrLine(sys))
													logger.info(errMsg)
												finally:
													exeQryStmt = ""
								except:
									errMsg += "error -{}".format(myutil.getErrLine(sys))
									logger.info(errMsg)
								finally:
									mycon2.close()
	
						
						# set EOF
						#if myjobDic['use_cr_code'] == 'Y':
						#	myEOF +="\r"	
						#myEOF += '\n'

				


						#############################################################
						#	
						# make a line with header
						#
						#
						jsonKey = []
						# if json , make jsonKey every package
						header_cnt = 0
						if file_extension == 'json':
							for x in file_spec_list:
								if x['invisible'] == 'Y':
									invisibleDic = {x['colnm']:x['colseq']}
									continue

								if x['re_name'] == 'Y':
									colnm = x['colnm'].split("=")[1].strip("\r\n\t ")
								else:
									colnm = x['colnm'].split("=")[0].strip("\r\n\t ")

								if colnm == "":
									colnm = "auto_def"+str(colseq)

								if colSensitive == 'L':
									colnm = colnm.lower()
								elif colSensitive == 'U':
									colnm = colnm.upper()
								header_cnt += 1
								jsonKey.append(colnm)	
						else:
							header_filestr = []
							if myvaldic['act_package'] == '@DECORATOR': # if not decorator, default 
								header_filestr = prev_header_filestr
								header_cnt = len(header_filestr)
							else:
								for x in file_spec_list:
									if x['invisible'] == 'Y':
										invisibleDic = {x['colnm']:x['colseq']}
										continue

									if x['re_name'] == 'Y':
										colnm = x['colnm'].split("=")[1].strip("\r\n\t ")
									else:
										colnm = x['colnm'].split("=")[0].strip("\r\n\t ")

									if colnm == "":
										colnm = "auto_def"+str(x['colseq'])

									if colSensitive == 'L':
										colnm = colnm.lower()
									elif colSensitive == 'U':
										colnm = colnm.upper()
									#jsonKey.append(colnm)	
									header_filestr.append(colnm)
									header_cnt += 1


							# always for excel and others in myheader set
							if myjobDic['myheader'] != 'N' or (packIdx == 1 and (file_extension == 'xls' or file_extension == 'xlsx')):
								if prev_header_filestr != header_filestr:
									myStr = (myvaldic['delimiter'].join(header_filestr)+myEOF).encode(myFileEncoding,'replace').decode(myFileEncoding)
									myfd.write(myStr)
									#myfd.write(myvaldic['delimiter'].join(header_filestr)+myEOF)	

							prev_header_filestr = header_filestr


						isJoin = False
						isCount = True
						# if it is join package
						if len(package_join) > 0 and packIdx in package_join:
							logger.info("join!!!!!!!!!!!!!!------->{}".format(joinJsonData))
							logger.info("packIdx--[{}]---package_join [{}]------!!!!!!!!!!!!!!------->".format(packIdx, package_join))
							isJoin = True
							if package_join[packIdx]['type'] == 'left':
								isCount = False
						else:
							# to reuse next join 
							if len(joinJsonData) > 0:
								logger.info("finish joinJsonData------->{}".format(joinJsonData))
								for k, v in joinJsonData.items():
									rowJsonData = v

						#logger.info("titems----->{}".format(titems))
						row_index = 0
						global filestr
						for coldic in titems:
							filestr = []
							if myjobDic['myindex'] == 'Y':
								filestr = [{'data':str(row_index), 'invisible':''}]

							procDicList = []
							# if its tio system has block array
							if type(coldic) == type([]):
								# check blockseq if user define specific array index
								procDicList = coldic
							else:
								# if general api
								procDicList.append(coldic)
							

							#logger.info("procDicList------>"+str(procDicList))
							for procDict in procDicList:
								#print(procDict)
								#logger.info("origin------>"+str(procDict))
								procDict.update(useComposite_OutBlock)
								#logger.info("composite------->"+str(procDict))

								# one line string in file
								#global filestr
								filestr = []
								#if myjobDic['myindex'] == 'Y':
								#	filestr = [str(row_index)]

								#jsonKey = [ x[1].split("=")[0] for x in file_spec_list ]
								#logger.info("colnm list ---->"+str(file_spec_list))
								#procColSeq = []
								#for colseq, colnm, _myreplace, blockseq, isInput in file_spec_list:
								for seq_idx, x in enumerate(file_spec_list,1):
									procDic = procDict.copy()
									colseq = x['colseq']
									colnm = x['colnm']

									#print(colseq, colnm, x['invisible'])

									# use lower colnm to default in intersystem
									if setLower:
										colnm = colnm.lower()

									#logger.info("check[{}]---->".format(colnm))
									_myreplace = x['myreplace']
									blockseq = x['blockseq']
									is_input = x['is_input']

									# use sub array data
									if int(blockseq) > -1:
										procDic = procDicList[blockseq]

									tmpstr = ""
									#userdefine = colnm.find("=")

									# check user define colnm
									colnm_list = colnm.split("=")
									if len(colnm_list) > 1:
										if colnm_list[0] == "" and x['invisible'] != 'Y': # very define
											#tmpstr = colnm[userdefine+1:]
											tmpstr = colnm_list[1]
										else:
											try:
												if x['re_name'] == 'Y':
													#tmpstr = procDic[colnm.split("=")[0]]
													#logger.info("re_name [{}]----[{}]---->".format(procDic, colnm_list[0]))
													tmpstr = procDic[colnm_list[0]]
												elif len(colnm_list) == 3: #redefine col value
													#logger.info("redefine [{}]----[{}]---->".format(colnm_list[2], procDic[colnm_list[1]].strip().replace("\n","").replace("\r",""),replace("\t","")))
													try:
														tmpstr = json.loads(colnm_list[2])[procDic[colnm_list[1]].strip()]
													except:
														# no define in dict
														tmpstr = procDic[colnm_list[1]]
												else:
													tmpstr = colnm_list[1]
													# input arg
													if is_input == 'Y':
														#logger.info("inputcol check---[{}]----[{}]---->".format(str(colnm), str(myArgDic)))
														tmpstr = myArgDic[tmpstr]
													else:
														tmpstr = procDic[tmpstr]
											except:
												logger.info("[{}] no colnm[{}] in list-->".format(myutil.getErrLine(sys), str(colnm)))
												continue
									else:
										#colnm = colnm.lower()
										if colnm not in procDic:
											#print("no colnm[{}] in list-->".format(str(colnm)))
											logger.info("no colnm[{}] in list-->".format(str(colnm)))
											continue

										if procDic[colnm] is None:
											tmpstr = ""
										else:
											__tmpstr = ""

											if "decimal" in str(type(procDic[colnm])):
												__tmpstr = str(float(procDic[colnm]))
											elif "str" in str(type(procDic[colnm])):
												__tmpstr = procDic[colnm].encode(myEncoding,"replace").decode(myEncoding,'replace').strip()
											else:
												try:
													__tmpstr = str(procDic[colnm]).strip()
												except:
													__tmpstr = procDic[colnm].encode(myEncoding,"replace").decode(myEncoding,'replace').strip()
											# remove null	
											tmpstr = __tmpstr.rstrip('\x00').strip()


									# check replace	
									if _myreplace is None or _myreplace.strip() == "" or tmpstr.strip() == "" or x['invisible'] == 'Y':
										pass
									else:
										_myreplace = myutil.getSpecialStr(_myreplace)
										if _myreplace.startswith("{") and _myreplace.endswith("}"): # is a json style
											myrepldic = eval(_myreplace)
											if tmpstr in myrepldic:
												tmpstr = myrepldic[tmpstr]
										else: # is a code 
											global _myreplace_ 
											_myreplace_ = tmpstr

											# check system set
											exeSmt = re.sub("\${([0-9]+)}", r"filestr[\1-1]['data']", _myreplace)
											# check user set
											exeSmt = re.sub("\$[a-zA-Z]+[_0-9a-zA-Z]*", "_myreplace_", exeSmt)
											try:
												exec(exeSmt, globals())
											except ValueError:
												tmpstr = exeSmt.replace("long(_myreplace_)", "long(float(_myreplace_))").replace("int(_myreplace_)","int(float(_myreplace_))")
												exec(tmpstr, globals())
											except:
												logger.info("{}-{} error --- {}".format(colnm, procDic[colnm], "replace format error"))
												logger.info("{} error --- {}".format(myutil.getErrLine(sys),_myreplace_))
												errMsg += str(myutil.getErrLine(sys))
												isOK=False
											tmpstr = str(_myreplace_)

									#encoding
									if myvaldic['myencoding'] == 'utf8' and myFileEncoding == 'euckr':
										for utf8_chr, euckr_chr in sp_utf8_euckr.items():
											tmpstr= tmpstr.replace(utf8_chr, euckr_chr)

									filestr.append({'data':tmpstr, 'invisible':x['invisible']})

								if myvaldic['act_package'] == '@DECORATOR' or isCount == False: # no count row if decorator or join package
									row_index = 0
								else:
									row_index +=1

								this_row_count = row_index

								# replace involved package items in item of the mod_myreplace at schedule in the mycrontab
								if len(mod_myreplace) > 0:
									for filestr_seq, replaceStr in mod_myreplace.items():
										filestr_seq -= 1
										tmpstr = filestr[filestr_seq]['data']
										#print(filestr_seq, replaceStr, tmpstr)
										_myreplace = myutil.getSpecialStr(replaceStr)
										if _myreplace.startswith("{") and _myreplace.endswith("}"): # is a json style
											myrepldic = eval(_myreplace)
											if tmpstr in myrepldic:
												tmpstr = myrepldic[tmpstr]
										elif _myreplace == "=${INVISIBLE}":
											filestr[filestr_seq]['invisible'] = 'Y'
										else: # is a code
											global _myreplace2_ 
											_myreplace2_ = tmpstr
											# check system set
											exeSmt = re.sub("\${([0-9]+)}", r"filestr[\1-1]['data']", _myreplace)
											exeSmt = "_myreplace2_="+exeSmt
											try:
												exec(exeSmt, globals())
											except ValueError:
												tmpstr = exeSmt.replace("long(_myreplace2_)", "long(float(_myreplace2_))").replace("int(_myreplace2_)","int(float(_myreplace2_))")
												exec(tmpstr, globals())
											except:
												#print("error--->", myutil.getErrLine(sys))
												logger.info("{} error --- {}".format(myutil.getErrLine(sys),_myreplace2_))
										
											tmpstr = str(_myreplace2_)

										filestr[filestr_seq]['data'] = tmpstr
									#print(filestr)
								filestr = [x['data'] for x in filestr if x['invisible'] != 'Y']

								if header_cnt != len(filestr) and myvaldic['act_package'] != '@DECORATOR':
									#print("skip [header_cnt]", header_cnt, "[filestr]", filestr)
									#logger.info("skip [header_cnt] ---{} ---[filestr]--{}".format(header_cnt, filestr))
									pass
									#no write
								else:
									# write a line in the file
									try:
										# set max_col
										if max_col_cnt < len(filestr):
											max_col_cnt = len(filestr)

										if len(filestr) > 0:
											if file_extension == 'json':
												jsonROW = dict(zip(jsonKey, filestr))
												#logger.info("jsonROW--- {}".format(jsonROW))
												if isDecorator:
													jsonOut.update(jsonROW)
												else:
													# check join package
													if isJoin == True:
														packNm = myvaldic['act_package']
														if package_join[packIdx]['type'] == 'std':
															#logger.info("join packNm std {}!!!!!!!!!!!!--".format(packNm))
															# root
															if packNm in joinJsonData:
																joinJsonData[packNm].append(jsonROW)
															else:
																joinJsonData[packNm] = [jsonROW]
														else:
															if packIdx in package_alias:
																packNm = package_alias[packIdx]

															# check joining package is std package(root)
															# or left join package
															logger.info("join left packNm {}!!!!!!!!!!!!--".format(packNm))
															# left join
															for rootPackNm, v in joinJsonData.items():
																for idx, stddrive_package in enumerate(v):
																	goJoin = False
																	joinPackName = package_join[packIdx]['join_std_pack_name']
																	logger.info("join - PackName-{} ---------rootPackNm-{}!!!!!!!!!!!!--".format(joinPackName,rootPackNm))
																	if joinPackName == rootPackNm:
																		for join_info in package_join[packIdx]['join_info']:
																			#logger.info("join - join_info------{}!!!!!!!!!!!!--".format(join_info))
																			#logger.info("join - stddrive_package------{}!!!!!!!!!!!!--".format(stddrive_package))
																			#logger.info("join - check col[{}] ".format(join_info['my_col']))
																			#logger.info("join - -check_val-[{}]--".format(jsonROW[join_info['my_col']]))
																			#logger.info("join - --stddrive_col[{}]-".format(join_info['std_col']))
																			#logger.info("join - --stddrive_val-[{}]!!!!!!!!!!!!--".format(stddrive_package[join_info['std_col']]))
																			if jsonROW[join_info['my_col']] == stddrive_package[join_info['std_col']]:
																				goJoin = True
																				del jsonROW[join_info['my_col']] 
																			else:
																				goJoin = False
																				break

																		if goJoin:
																			#logger.info("join - goJoin ------!!!!!!!!!!!!--")
																			if packNm in stddrive_package:
																				joinJsonData[rootPackNm][idx][packNm].append(jsonROW)
																			else:
																				joinJsonData[rootPackNm][idx].update({packNm:[jsonROW]})
																				#logger.info("join - goJoin ------{}!!!!!!!!!!!!--".format(joinJsonData[rootPackNm][idx]))
																	else:
																		joinJsonData[rootPackNm][idx] = getJoinData()
													else:
														rowJsonData.append(jsonROW)
											else:
												# need for checking the join package
												if isJoin == True:
													#print(isJoin, header_filestr)
													jsonROW = dict(zip(header_filestr, filestr))

													#print(jsonROW)
													packNm = myvaldic['act_package']
													if package_join[packIdx]['type'] == 'std':
														#logger.info("join packNm std {}!!!!!!!!!!!!--".format(packNm))
														# root
														if packNm in joinJsonData:
															joinJsonData[packNm].append(jsonROW)
														else:
															joinJsonData[packNm] = [jsonROW]
													else:
														if packIdx in package_alias:
															packNm = package_alias[packIdx]

														defaultJsonROW = {}
														for k in jsonROW.keys():
															for join_info in package_join[packIdx]['join_info']:
																if k != join_info['my_col']:
																	defaultJsonROW[k] = ""
														# check joining package is std package(root)
														# or left join package
														#logger.info("join left packNm {}!!!!!!!!!!!!--".format(packNm))
														# left join
														for rootPackNm, v in joinJsonData.items():
															for idx, stddrive_package in enumerate(v):
																goJoin = False
																joinPackName = package_join[packIdx]['join_std_pack_name']
																#logger.info("join - PackName-{} ---------rootPackNm-{}!!!!!!!!!!!!--".format(joinPackName,rootPackNm))
																if joinPackName == rootPackNm:
																	if packNm not in joinJsonData[rootPackNm][idx]:
																		joinJsonData[rootPackNm][idx].update({packNm:[defaultJsonROW]})


																	for join_info in package_join[packIdx]['join_info']:
																		#print(jsonROW[join_info['my_col']] , stddrive_package[join_info['std_col']])
																		if jsonROW[join_info['my_col']] == stddrive_package[join_info['std_col']]:
																			goJoin = True
																			del jsonROW[join_info['my_col']] 
																		else:
																			goJoin = False
																			break

																	if goJoin:
																		joinJsonData[rootPackNm][idx].update({packNm:[jsonROW]})
																		#if packNm in stddrive_package:
																		#	joinJsonData[rootPackNm][idx][packNm].append(jsonROW)
																		#else:
																		#	joinJsonData[rootPackNm][idx].update({packNm:[jsonROW]})
																		#	#logger.info("join - goJoin ------{}!!!!!!!!!!!!--".format(joinJsonData[rootPackNm][idx]))

																else:
																	joinJsonData[rootPackNm][idx] = getJoinData()
												else:
													#print(joinJsonData)
													#rowJsonData.append(jsonROW)
													#flush and very write
													myStr = (myvaldic['delimiter'].join(filestr)+myEOF).encode(myFileEncoding,'replace').decode(myFileEncoding)
													myfd.write(myStr)
													#myfd.write(myvaldic['delimiter'].join(filestr)+myEOF)	
									except:
										logger.info("error -{}".format(myutil.getErrLine(sys)))
										logger.info("-{}".format(filestr))

						myDecorator = {'row_count':this_row_count, 'date_time':myutil.getToday("%Y%m%d%H%M%S"), 'total_row_count':total_row_count}
						total_row_count += this_row_count
						packIdx +=1
				#with open(myfile, 'w', encoding=myjob['c_file_encoding']) as myfd:
			#print("joinJsonData--->", joinJsonData)
			if isJoin and file_extension != 'json': # all join table
				with open(myfile, 'a', encoding=myFileEncoding) as fd:
					for rk,rv in joinJsonData.items():
						for dict_items in rv:		
							filestr = []
							for k,v in dict_items.items():
								if type(v) == dict or type(v) == list:
									getListItem(filestr, v)
								else:
									filestr.append(v)	
							# if not null as the white space in the default		

							myStr = (myvaldic['delimiter'].join(filestr)+myEOF).encode(myFileEncoding,'replace').decode(myFileEncoding)
							fd.write(myStr)
							#fd.write(myvaldic['delimiter'].join(filestr)+myEOF)	
						




			#print(rowJsonData)
			#treat excel style
			# and inline jpg in email
			adjust_files = []
			if file_extension == 'json':
				with open(myfile, 'w', encoding=myFileEncoding) as myfd:
					if len(jsonOut) > 0:
						jsonOut.update({"data":rowJsonData})
						json.dump(jsonOut, myfd, ensure_ascii=False)
					else:
						json.dump(rowJsonData, myfd, ensure_ascii=False)

			elif file_extension == 'xls' or file_extension == 'xlsx' or myjobDic['use_image'] == 'Y':
				import pandas as pd
				from selenium import webdriver

				if myjobDic['use_image'] == 'Y':
					with open(myfile, 'r', encoding=myFileEncoding) as fd:
						filenum = 0
						firstTime = True
						fileNm = myfile+"_"+str(filenum)
						#default size
						adjust_files.append({"fileNm":fileNm, "size":"800,300"})
						fd_sub = open(fileNm, 'w', encoding=myFileEncoding)
						for myline in fd.readlines():
							if myline.find("@DECORATOR") != -1:
								#check_myline = myline.split()[0]
								check_myline = myline
								rest_myline = myline.split(myvaldic['delimiter'])[1:]
								myline, myOpts = check_myline.split("@DECORATOR")
								myline = myline+myvaldic['delimiter']+myvaldic['delimiter'].join(rest_myline).encode(myFileEncoding,'replace').decode(myFileEncoding)
								
								if firstTime:
									firstTime=False
									fd_sub.write(myline)
								else:
									# close 
									fd_sub.close()
									filenum +=1
									fileNm = myfile+"_"+str(filenum)
									adjust_files.append({"fileNm":fileNm, "size":"800,300"})
									fd_sub = open(fileNm, 'w', encoding=myFileEncoding)	
									fd_sub.write(myline)

								if myOpts != '':
									for myOpt in myOpts.split("@"):
										if myOpt.find("SIZE=") != -1:
											adjust_files[filenum].update({'size':myOpt.split("SIZE=")[1]})
							else:
								fd_sub.write(myline)
						# the very last
						fd_sub.close()

						

				
					for adjusted_file_info in adjust_files:
						adjusted_file = adjusted_file_info['fileNm']
						mySize = adjusted_file_info['size']	

						df = pd.DataFrame(pd.read_csv(adjusted_file,encoding=myFileEncoding, sep=myvaldic['delimiter']))
						df.columns = df.columns.map(lambda x : x.lower().split("unnamed:")[0])

						#myhtml_inline = df.to_html(index=False, header=(True if myjobDic['myheader'] == 'Y' else False), classes='myclass')
						myhtml_inline = df.to_html(index=False, header=True, classes='myclass')

						# if need, merge the header 
						if myhtml_inline.find("<th></th>") != -1:
							myhtml_inline = myhtml_inline.replace("<th></th>","")
							myhtml_inline = myhtml_inline.replace("<th>",'<th colspan="'+str(max_col_cnt)+'">')

						# make html file
						myFileNm = adjusted_file
						with open(myFileNm+".html", 'w', encoding=myFileEncoding) as fd:
							myhtml = """
								<!DOCTYPE html>
								<meta charset="UTF-8">
								<html>
									<head>
										<link rel="stylesheet" href="%s/css/myfeed_inline.css?ver=%s" />
									</head>
									<body>""" % (MYHOME, myutil.getToday("%Y%m%d%H%M%S"))
							fd.write(myhtml)
							fd.write(myhtml_inline.replace("&gt;", ">").replace(">>",""))
							myhtml = """
									</body>
								</html>"""
							fd.write(myhtml)
							

						myopts = webdriver.ChromeOptions()
						myopts.add_argument('--headless')
						myopts.add_argument('window-size='+mySize)
						with webdriver.Chrome(chrome_options=myopts) as mydr:
							mydr.get('file:'+myFileNm+".html")
							mydr.save_screenshot(myFileNm+".jpg")
			
				if file_extension == 'xls' or file_extension == 'xlsx':	
					# excel	
					rowStyle = {}
					fileNm = myfile+"_"
					with open(myfile, 'r', encoding=myFileEncoding) as fd:
						#default size
						with open(fileNm, 'w', encoding=myFileEncoding) as fd_sub:
							for i, myline in enumerate(fd.readlines()):
								check_myline = myline.split(myvaldic['delimiter'])[0]
								rest_myline = myline.split(myvaldic['delimiter'])[1:]
								if check_myline.find("@DECORATOR@") != -1:
									ckCMD = check_myline.split("@")
									myLabel = ckCMD[0]
									myJOB = ckCMD[2]
									
									if myJOB == "job:excelrow":
										try:
											rowStyle[i-1] = ckCMD[3]
										except:
											logger.info("error----{}".format(myutil.getErrLine(sys)))
									myline = myLabel+myvaldic['delimiter']+myvaldic['delimiter'].join(rest_myline).encode(myFileEncoding,'replace').decode(myFileEncoding)
								fd_sub.write(myline)

					df = pd.DataFrame(pd.read_csv(fileNm,encoding=myFileEncoding, sep=myvaldic['delimiter']))
					df.columns = df.columns.map(lambda x : x.lower().split("unnamed:")[0])

					def MyColorCell(row):
						if row.name in rowStyle:
							print(rowStyle[row.name])
							return [rowStyle[row.name]]*len(row) 
						else: 
							return ['']*len(row)

					#styled_df = df.style.applymap(MyColorCell)	
					styled_df = df.style.apply(MyColorCell, axis=1)	
					styled_df.to_excel(myfile, engine='openpyxl', index=False, header=(True if myjobDic['myheader'] == 'Y' else False))
					#df.to_excel(myfile, index=False, header=(True if myjobDic['myheader'] == 'Y' else False))
		
					
			# send local dir	
			if myjobDic['tar_localhost'] == '':
				if myjobDic['is_urgent'] == 'ok' and (myjobDic['c_localhost'] == '' or myjobDic['c_localhost'] is None):
					myjobDic['c_localhost'] = socket.gethostname()
			else:
				myjobDic['c_localhost'] = myjobDic['tar_localhost'] 


			if myjobDic['c_localhost'] is not None and myjobDic['c_localhost'].strip() != '' and myjobDic['c_localdir'].strip() != '':
				myhost = socket.gethostname()

				#myhost = "ftp1"

				# tidy the directory
				for mylocalhost in myjobDic['c_localhost'].split(","):
					if myjobDic['is_urgent'] == 'no': # general
						if str(myjobDic['c_local_retain_days']) == "-1":
							pass
						else:
							checkdir = myjobDic['c_localdir']
							if myjobDic['c_localdir'].find("$") != -1: # check special dir
								checkdir = myjobDic['c_localdir'].split("$")[0]

							# check retain days
							myCmd = "find "+checkdir+ " -mindepth 1 -mtime +"+str(myjobDic['c_local_retain_days'])+" -exec rm -rf {} \;"
							if myhost.lower().strip() != mylocalhost.lower().strip():
								myCmd = "ssh "+mylocalhost+" \""+myCmd+"\""
							
							logger.info("{}".format(myCmd))
							try:
								myout = subprocess.check_output(myCmd, shell=True, stderr=subprocess.STDOUT)
							except subprocess.CalledProcessError as e:
								myout = e.output.decode(myEncoding)
								logger.info("{}--->{}".format(myCmd, myout))

					

					mytarget_dir  = myutil.getSpecialStr(myjobDic['c_localdir'])+"/"

					copy2Target(myfile, myhost, mylocalhost, mytarget_dir)

			# send a file to ultimate place by using Protocol
			if 'issent' in myjobDic and myjobDic['issent']:
				if myjobDic['c_url'] is not None and myjobDic['c_url'].strip() != '':
					send2Client(mycon, myfile, file_extension, myjobDic, adjust_files)
		except:
			try:
				errMsg += str(myutil.getErrLine(sys)).replace("'","\'").replace("(","\(").replace(")","\)").replace("<","\<").replace(">","\>")
			except:
				pass
			logger.info("error -----------------{}".format(errMsg))
			logger.info("---{}".format(myjobDic))
			logger.info("---{}".format(myvaldic))
			logger.info("{}".format("---------------------------"))
			isOK=False
		finally:
			procDate ={}
			procDateStr = myjobDic['isGO']['procDate']
			start_time = ''
			end_time = ''
			next_time = ''
			latest_status = ''


			if myjobDic['isGO']['isGO']:
				procDate['c_now'] = procDateStr
				procDate['c_year'] = procDateStr[0:4]
				procDate['c_mon'] = procDateStr[4:6]
				procDate['c_day'] = procDateStr[6:8]
				procDate['c_hour'] = procDateStr[8:10]
				procDate['c_min'] = procDateStr[10:12]
				procDate['c_week'] = procDateStr[12:13]
				cklist = ['c_min', 'c_hour', 'c_day', 'c_mon']
				ckdic = {k:{'list':myjobDic[k].split(","), 'nextgo': False, 'val':None} for k in cklist}
				nextTime = ''

				procDic = {}
				procDic.update(myjobDic['isGO'])
				#'c_min':1, 'c_hour':0, 'c_day':0, 'c_mon':0, 'c_week':0}

				for i in range(0, len(cklist)):
					myck = cklist[i]
					thisIdx = procDic[myck]
					procVal = ckdic[myck]['list'][thisIdx].strip()
					thisValTime = int(procDate[myck]) 
					nextValTime = int(procDate[myck])+1 


					if procVal.find("-") == -1 and procVal.find("*") == -1 and (i == 0 or (i > 0 and ckdic[cklist[i-1]]['nextgo'])) and len(ckdic[myck]['list']) <= thisIdx+1: # last next time tier
						procVal = ckdic[myck]['list'][0].strip()
						ckdic[myck]['nextgo'] = True


					if procVal.find("-") != -1:
						myRange = procVal.split("-")
						checkValTime = thisValTime
						#isNext = False
						if i == 0: # min
							checkValTime = nextValTime
						elif i > 0 and ckdic[cklist[i-1]]['nextgo']: # previous job is all circulated
							checkValTime = nextValTime
							#isNext = True

						if checkValTime >= int(myRange[0]) and checkValTime <= int(myRange[1]):
							ckdic[myck]['val'] = '%02d' % (checkValTime) 	
						else:
							#if isNext:
							thisIdx +=1

							if len(ckdic[myck]['list']) <= thisIdx+1: # last next time tier
								ckdic[myck]['val'] = '%02d' % (int(ckdic[myck]['list'][0].split("-")[0]))
								ckdic[myck]['nextgo'] = True
							else:
								ckdic[myck]['val'] = '%02d' % (int(ckdic[myck]['list'][thisIdx].split("-")[0]))
							
					elif procVal.find("*") != -1: # check range
						checkValTime = thisValTime
						isNext = False
						if i == 0: # min
							checkValTime = nextValTime
						elif i > 0 and ckdic[cklist[i-1]]['nextgo']: # previous job is all circulated
							checkValTime = nextValTime
							isNext = True

						if myck == 'c_min':
							checkValTime = nextValTime
							if checkValTime == 60:
								ckdic[myck]['val'] = '00'	
								ckdic[myck]['nextgo'] = True
							else:
								ckdic[myck]['val'] = "%02d" %(nextValTime)
						elif myck == 'c_hour':
							if checkValTime == 24:
								ckdic[myck]['val'] = '00'
								ckdic[myck]['nextgo'] = True
							else:
								ckdic[myck]['val'] = '%02d' %(nextValTime)
						elif myck == 'c_day':
							if isNext:
								if myjobDic['c_week'] == '*':
									nextValTime = myutil.getDeltaDate(procDate['c_year']+procDate['c_mon']+procDate['c_day'], 'd', 1)
								else:
									isWeek = False
									for weekstr in myjobDic['c_week'].split(","):
										if weekstr.find("-") != -1:
											for dayplus in range(1, 8):
												nextValTime = myutil.getDeltaDate(procDate['c_year']+procDate['c_mon']+procDate['c_day'], 'd', dayplus)
												myweek = myutil.getWeek(nextValTime)
												if int(myweek) >= int(weekstr.split("-")[0]) and int(myweek) <= int(weekstr.split("-")[1]):
													isWeek=True
													break
										else: 
											for dayplus in range(1, 8):
												nextValTime = myutil.getDeltaDate(procDate['c_year']+procDate['c_mon']+procDate['c_day'], 'd', dayplus)
												myweek = myutil.getWeek(nextValTime)
												if myweek == weekstr and procDate['c_week'] != myweek:
													isWeek=True
													break
										if isWeek:
											break

								if nextValTime[4:6] != procDate['c_mon']:
									ckdic[myck]['val'] = '01'
									ckdic[myck]['nextgo'] = True
								else:
									ckdic[myck]['val'] = nextValTime[6:8]
							else:
								ckdic[myck]['val'] = procDate[myck]
						elif myck == 'c_mon':
							if checkValTime == 13:
								ckdic[myck]['val'] = '%04d01' % (int(procDate['c_year'])+1)
							else:
								ckdic[myck]['val'] = '%s%02d' % (procDate['c_year'], checkValTime)

					#elif procVal.find("*") != -1: # check range
					else:
						if ckdic[myck]['nextgo']:
							ckdic[myck]['val'] = "%02d" % (int(ckdic[myck]['list'][0].split("-")[0]))
						else:
							# if low tier in time, not adding index
							if i==0 or (i>0 and ckdic[cklist[i-1]]['nextgo']):
								ckdic[myck]['val'] = "%02d" % (int(ckdic[myck]['list'][thisIdx+1].split("-")[0]))
							else:
								ckdic[myck]['val'] = "%02d" % (int(ckdic[myck]['list'][thisIdx].split("-")[0]))

				mytime = ''
				for i in range(0, len(cklist)):
					myck = cklist[i]
					mytime = ckdic[myck]['val']+mytime
					next_time=mytime+'00'

			start_time=procDateStr[:-1]+'00'
			end_time=myutil.getToday("%Y%m%d%H%M%S")

			latest_status='F' # Fail
			if isOK:
				latest_status='O' # OK

	
			# set the series of schedule information
			nextTimeStr=""
			if next_time != "":
				nextTimeStr = ", next_time = '%s'" % (next_time)
		
			sql = "update myfeed.mycrontab set start_time = '{}', end_time = '{}' {} , latest_status = '{}' where c_name = '{}' and c_job_num = {}".format(start_time,end_time,nextTimeStr,latest_status, myjobDic['c_name'], myjobDic['c_job_num'])
			mycon.exeQry("G", sql)
			logger.info("---[{}]---------------".format(sql))



			# if success
			# check end_job	
			if isOK:
				if myjobDic['issent'] == True:
					if myjobDic['end_job'] != '':
						try:
							for end_job_name in myjobDic['end_job'].split(","):
								sql = "select c_file from mycrontab where end_job = '{}' and latest_status <> 'O'".format(end_job_name)
								items = mycon.exeQry("G", sql)
								logger.info("---check end_job  [{}]---------------".format(sql))
								if len(items) == 0: #very last
									sql = "select * from end_job_info where end_job = '{}'".format(end_job_name)
									end_job = mycon.exeQry("G1", sql, useDict=True)
									logger.info("---check end_job  [{}]---------------".format(sql))

									end_file_name = myutil.getSpecialStr(end_job['end_file_name'].strip())
									c_localhost_list = myutil.getSpecialStr(end_job['c_localhost'].strip())
									if c_localhost_list == "":
										c_localhost_list = myutil.getSpecialStr(myjobDic['c_localhost'].strip())

									c_localdir_list = myutil.getSpecialStr(end_job['c_localdir'].strip())
									if c_localdir_list == "":
										c_localdir_list = myutil.getSpecialStr(myjobDic['c_localdir'].strip())

									end_command = myutil.getSpecialStr(end_job['end_command'].strip())

									# local
									for mylocalhost in c_localhost_list.split(","):
										for mylocaldir in c_localdir_list.split(","):
											if end_file_name != "":
												# myTarPath is myfeed system dir
												# myfile is myfeed system dir and file name temporary

												# mytarget_dir is c_localdir
												end_job_myfile = ""
												if end_job['increment'] == 'Y':
													end_job_myfile = myutil.getSpecialStr(mylocaldir.strip("/")+"/"+myutil.getSpecialStr(end_file_name))

													copy2Target(myfile, myhost, mylocalhost, mylocaldir, inc_target=os.path.basename(end_job_myfile), inc_skip=end_job['cut_headline_with_inc'])
												else:
													end_job_myfile = myutil.getSpecialStr(myTarPath+"/"+myutil.getSpecialStr(end_file_name))
													with open(end_job_myfile, 'w', encoding=myFileEncoding) as fd:
														# decorator
														if end_job['decorator'] == 'Y':
															end_file = {}
															tmpstr = ""
															sql = """select apm.act_package_qry_col
																	, fs.c_name as end_c_name
																	, fs.c_file as end_c_file
																	, fs.sel_act_package_idx as end_sel_act_package_idx
																	, fs.c_seq as end_c_seq
																	, fs.act_package_seq as end_act_package_seq
																	, fs.myreplace as end_myreplace
																	from myfeed.file_spec fs, myfeed.act_package_master apm
																	where fs.c_name = '{}' and c_file = '{}'
																	and apm.act_package = '@DECORATOR'
																	and apm.act_package_seq = fs.act_package_seq""".format(end_job['end_job'], end_job['end_file_name'])
															logger.info("---check end_job  [{}]---------------".format(sql))
															try:
																end_file = mycon.exeQry("G1", sql, useDict=True)
																end_file['send_file'] = myfile
																if end_file['end_act_package_seq'] == 4: # user define
																	#global _myreplace_ 
																	myjobDic.update(end_file)
																	_myreplace = myutil.getSpecialStr(end_file['end_myreplace'])
																	_myreplace = myutil.getSpecialStr(_myreplace, myjobDic)
																	exeSmt = _myreplace
																	exeSmt = re.sub("\$[a-zA-Z]+[_0-9a-zA-Z]*", "_myreplace_", exeSmt)
																	exec(exeSmt, globals())
																	tmpstr = str(_myreplace_)
																	fd.write(tmpstr)
															except:
																print("error--->", myutil.getErrLine(sys))
																fd.write("")
														else:
															fd.write("")

													copy2Target(end_job_myfile, myhost, mylocalhost, mylocaldir)


									# send the file to ultimate place
									if 'issent' in myjobDic and myjobDic['issent']:
										if myjobDic['c_url'] is not None and myjobDic['c_url'].strip() != '':
											#myCmd = myCmd.replace("$myfile", end_job_myfile)
											send2Client(mycon, end_job_myfile, file_extension, myjobDic, adjust_files)

									if end_command != "":
											myCmd = myutil.getSpecialStr(end_command) # system variable
											for checkStr in [ x.strip("${}") for x in re.findall("\${.[^{}]+}", myCmd) ]:
												if checkStr in myjobDic:
													myCmd = myCmd.replace("${"+checkStr+"}", myutil.getSpecialStr(myjobDic[checkStr]))
											logger.info("{}".format(myCmd))
											#exec(myCmd, globals())	
											#subprocess.call(myCmd, shell=True)


									mycon.exeQry("G", "update end_job_info set latest_status = 'O' where end_job = '{}'".format(end_job_name))
						except:
							isOK = False
							errMsg += str(myutil.getErrLine(sys)).replace("'","\'").replace("(","\(").replace(")","\)").replace("<","\<").replace(">","\>")
							logger.info("error -----------------{}".format(errMsg))


						
			# if fail
			# send the message to involved members
			if not isOK: 
				sql ="select members from myfeed.watchgroup where name in({}) union select alias as members from myfeed.watchmember where alias in({})".format("'"+"','".join(myjobDic['watchgroup'].split(','))+"'", "'"+"','".join(myjobDic['watchgroup'].split(','))+"'")
				items = mycon.exeQry("G", sql, useDict=True)
				for item in items:
					logger.info("isOK---[{}]---------------".format(item))
					logger.info("isOK---[{}]---------------".format(myvaldic))
					for contact in item['members'].replace(" ","").split(","):
						try:
							sql = "select etc from myfeed.watchmember where alias ='{}'".format(contact)
							hookurl=mycon.exeQry("G1", sql)[0]+"/botName=피드매니저"
							sms = "다음 스케줄 처리가 실패  했습니다. 작업 내용을 확인해 주세요 !!\n"\
								"url : https://myfeed.einfomax.co.kr \n"\
								"client_name : {} \n"\
								"c_name : {} \n"\
								"c_job_num : {} \n".format(client_name, myjobDic['c_name'], myjobDic['c_job_num'])

							try:
								sms += "c_file : "
								sms += "{} \n".format(myutil.getSpecialStr(myjobDic['c_file']))
							except:
								sms += "error ---> {} \n".format(sys.exc_info())

							try:
								sms += "act_package: "
								sms += "{} \n".format(myvaldic['act_package'])
							except:
								sms += "{} \n".format(sys.exc_info())

								
							if myjobDic['c_localhost'].strip() != "":
								sms += "c_localhost :	{} \n".format(myjobDic['c_localhost'])
								sms += "c_localdir :	{} \n".format(myjobDic['c_localdir'])
							if myjobDic['c_url'].strip() != "":
								sms += "c_url :	{} \n".format(myjobDic['c_url'])
							#sms += "error  : {} \n".format(errMsg)
							#sms = sms.encode(myEncoding).decode(myEncoding,'replace')
							#logger.info("send error messange---[{}]---------------".format(sms))

							#myutil.sms(hookurl, sms, reverse=True)
						except:
							sms += "send error messange---[{}]---------------".format(sys.exc_info())
							logger.info("send error messange---[{}]---------------".format(sys.exc_info()))
						finally:
							sms += "error  : {} \n".format(errMsg)
							sms = sms.encode(myEncoding).decode(myEncoding,'replace')
							myutil.sms(hookurl, sms, reverse=True)

def myUsage():
	print("""-g : general schedule
		-t : test schedule
		[-j <c_name job_num>]
		[-qry <tmp_qry1>] 	
		[-qry_arg \"arg1=\\"val1\\",arg2=\\"val2\\",arg3=val3...\"]
		[-e <where sitemcd ='005930'>]
		[-localhost <ftp2,ftp1...>]
		[-localdir </abc/123/kkk>]
		[-f filename]
		[-send] : send to client by using protocol""")

	
def testJob(myarg):
	job_status = "RUN"
	c_job = []
	myqry_type="myqry"
	add_myqry=""
	mod_local_dir=""
	mod_local_host=""
	qry_args = None
	fromreq = None
	tar_file = ""
	issent = False

	isGo = False


	

	# mode
	# nornal
	if '-run' in myarg:
		isGo = True
		job_status = "RUN"
		print("normal op")
	else:
		# force 
		if '-j' in myarg: # require job number
			c_job.append(myarg[myarg.index("-j")+1])
			c_job.append(myarg[myarg.index("-j")+2])
			isGo = True

			if '-qry' in myarg:
				myqry_type= myarg[myarg.index("-qry")+1]
				isGo = True

			if '-qry_arg' in myarg:
				qry_args = myarg[myarg.index("-qry_arg")+1]
				print(qry_args)

			if '-fromreq' in myarg:
				fromreq = myarg[myarg.index("-fromreq")+1]
				print(fromreq)

			if '-e' in myarg:
				add_myqry = myarg[myarg.index("-e")+1]
				isGo = True
			
			if '-f' in myarg:
				tar_file = myarg[myarg.index("-f")+1]
				isGo = True
			
			if '-localhost' in myarg:
				mod_local_host = myarg[myarg.index("-localhost")+1]

			if '-localdir' in myarg:
				mod_local_dir = myarg[myarg.index("-localdir")+1]
				isGo = True

			if '-send' in myarg:
				issent = True

	mycon = mydbapi.Mydb('mysql', 'myfeed',  myfeedmgr_db_host, _myPort=3306,_myUser='myfeed')

	# reset status if new day
	if myutil.getToday("%H") == '00': 
		mycon.exeQry("G", "update myfeed.mycrontab set latest_status = 'R' where latest_status <> 'F'")
		mycon.exeQry("G", "update myfeed.end_job_info set latest_status = 'R' where latest_status <> 'F'")


	# gogo
	if isGo:
		if len(c_job) > 1: # urgent job!
			myvaldic = {}
			sql = """select m.* from myfeed.mycrontab m 
				where m.c_name = '{}' and m.c_job_num = {}""".format(c_job[0], c_job[1])
			myvaldic = mycon.exeQry("G1", sql, useDict=True)
			mycon.close()

			if myvaldic is None:
				sms = sql + "-------> is not c_name with c_job_num there!!!"
				logger.info("error {} ".format(sms))
				myutil.sms("상우", sms)
			else:
				myvaldic['myqry_type'] = myqry_type
				myvaldic['add_myqry'] = add_myqry
				myvaldic['qry_args'] = qry_args
				myvaldic['issent'] = issent
				myvaldic['tar_file'] = tar_file
				myvaldic['tar_localhost'] = mod_local_host 
				myvaldic['is_urgent'] = 'ok'
				myvaldic['fromreq'] = fromreq

				if mod_local_dir != "":
					myvaldic['c_localdir'] = mod_local_dir


				if myvaldic['mgr_host'].lower() == socket.gethostname().lower():
					myvaldic.update({'isGO':{'procDate':myutil.getToday("%Y%m%d%H%M%w"), 'isGO':False}})	
					gogoJob(myvaldic, tempdir="temp/")
				else:
					print("this schedule have to run in server --> {} ,but your current server is ---> {}".format(myvaldic['mgr_host'].lower(),  socket.gethostname().lower()))
					logger.info("need my server --> {} , in your server {}".format(myvaldic['mgr_host'].lower(),  socket.gethostname().lower()))
		else:
			processes= []
			sql = """
					select cr.*, replace(ifnull(rs.target_date,'00000000'), '-','') as target_date, replace(ifnull(rs.target_until_time,'000000'), ':','') as target_until_time, rs.myadjust_time, rs.myadjust_timetype
					from myfeed.mycrontab cr
					left outer join myfeed.re_schedule rs on cr.re_schedule = rs.myname
					where job_status = '%s'
			""" % (job_status)
			titems = mycon.exeQry("G", sql, IS_PRINT=False, useDict=True)
			mycon.close()
			for myvaldic in titems:
				myvaldic['myqry_type'] = myqry_type
				myvaldic['add_myqry'] = add_myqry
				myvaldic['issent'] = True
				myvaldic['qry_args'] = qry_args
				myvaldic['tar_file'] = tar_file
				myvaldic['tar_localhost'] = mod_local_host 
				myvaldic['is_urgent'] = 'no'

				# check time and gojob
				try:
					checkDate = myvaldic['target_date']
					checkTime = myvaldic['target_until_time']
					adTime = myvaldic['myadjust_time']
					adType = myvaldic['myadjust_timetype']


					# check delay time
					if checkDate is not None and checkDate.strip() != "" and int(checkDate) == int(myutil.getToday("%Y%m%d")):
						if  checkTime is not None and checkTime.strip() != "" and int(checkTime) >= int(myutil.getToday("%H%M%S")):
							#print(checkDate, checkTime)
							#print("gogogo!")
							if adType == 'H':
								if myvaldic['c_hour'] != '*':
									myCheckHour = myvaldic['c_hour']
									myvaldic['c_hour'] = re.sub(r'\d+', lambda x:str(int(x.group(0))+int(adTime)), myCheckHour)
							elif adType == 'M':
									myCheckMin = myvaldic['c_min']
									myvaldic['c_min'] = re.sub(r'\d+', lambda x:str(int(x.group(0))+int(adTime)), myCheckMin)
							else:
								myutil.sms("상우", "there is not the type of adjust time in re_schedule. please check it")

					reVal = myutil.checkCrontab(myvaldic)
					if reVal['isGO']:
						if myvaldic['mgr_host'].lower() == socket.gethostname().lower():
							logger.info("isGO -- {}".format(reVal))
							myvaldic.update({'isGO':reVal})	
							t = Process(target=gogoJob, args=(myvaldic,))
							processes.append(t)
				except:
					logger.info("error {} myvaldic -- {}".format(myutil.getErrLine(sys), str(myvaldic)))
					myutil.sms("상우", str(myvaldic))
		
			for t in processes:
				t.start()
		
			for t in processes:
				t.join()
	else:
		myUsage()
	

def main(myarg):
	testJob(myarg)
if __name__ == "__main__":
	main(sys.argv)
