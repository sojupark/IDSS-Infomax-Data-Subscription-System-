#-*-coding: utf-8-*-
import asyncio
import signal
import multiprocessing as mp
from websockets.server import serve
from websockets.exceptions import ConnectionClosedOK
from websockets.exceptions import ConnectionClosedError
import json
import ast
import sys,os,socket
import queue
import threading
import websocket
import re

import mydbapi3 as mydbapi
import myutil3 as myutil

servINFO = {}
connectedID = {}
connectedWS = {}
userDic = {}
MYHOME = os.getcwd()
MYHOSTNAME = socket.gethostname()
MYIP = socket.gethostbyname(socket.gethostname())
userDicToken = {}
#set the log

#q = queue.Queue()

myQ = {}

mySubQ = {}

subscriber = {}

file_spec = {}
client_info = {}

serviceList = {}

sublist_by_pub = {}

myEncoding=os.environ.get("LANG").split('.')[1].lower()

socket_process = None
myfeedmgr_db_host = 'ftp2'
if socket.gethostname().lower() == myfeedmgr_db_host:
	myfeedmgr_db_host = 'localhost'


def signal_term_handler(signal, frame):
	socket_process.join()
	mylogger.log("stop server")
	myutil.sms("상우", "stop myfeedmgr_stream_broker server[{}]".format(servINFO))
	#sys.exit(0)

signal.signal(signal.SIGTERM, signal_term_handler)

#mysock = myutil.MyMultiCast('myfeed', isSend=False)

class MyBroker_sys(threading.Thread):
	def __init__(self, name=None, myqueue=None):
		super(MyBroker_sys,self).__init__()
		#self.q = myqueue
		self.mysock = myutil.MyMultiCast('mysys', isSend=False)
		self.mylogger = myutil.MyLogger('myfeedmgr_stream_broker_ws', MYHOME+'/log/', mySizeMB=200, myCount=10)
		self.mylogger.log("start!!!")
		self.mylogger.log(sublist_by_pub)
		
	def __exit__(self):
		pass

	def run(self):
		proc_file_spec = False
		proc_client_info = {}
		while True:
			mydata = self.mysock.recv()
			mydata = mydata.decode("utf8")
			
			try:
				#mydata = ast.literal_eval(mydata)
				mydata = json.loads(mydata)
				self.mylogger.log(mydata)
				if 'c_name' in mydata and 'c_file' in mydata and 'act_package' in mydata:
					fsname = mydata['c_name']+'@'+mydata['c_file']+'@'+mydata['act_package']
					if mydata["myStat"] == "I":
						if mydata["myJob"] == "file_spec":
							if fsname in file_spec:
								for i, item in enumerate(file_spec[fsname]): # update
									if str(item['c_seq']) == str(mydata['c_seq']):
										for k, v in mydata.items():
											if k in item:
												item[k] = v
										proc_file_spec = True
										file_spec[fsname][i] = item
								if not proc_file_spec: # Insert
									self.mylogger.log("this is insert job")
									mySeq = int(mydata['c_seq']) - 1
									file_spec[fsname].insert(mySeq, mydata)
								self.mylogger.log(file_spec[fsname])
							else:
								self.mylogger.log("first time insert job")
								file_spec[fsname] = [mydata]
							self.mylogger.log("complete insert ---->>>file_spec >>>>>>>>>>>>>{}".format(file_spec))

						elif mydata["myJob"] == "client_info":
							#self.mylogger.log("fsname --> {}-->{}".format(fsname, proc_client_info))
							if fsname not in proc_client_info:
								proc_client_info[fsname] = []

							if len(mydata["data"]) > 0:
								proc_client_info[fsname].append(mydata["data"])

						elif mydata["myJob"] == "mycrontab":
							#myPubName = mydata['sel_act_packages']
							myPubName = mydata['act_package']
							myToken = mydata['token']
							if mydata["ip"] == MYIP:
								if myToken in subscriber:
									subscriber[myToken][myPubName] = mydata
								else:
									subscriber[myToken] = {myPubName: mydata}

								if myPubName in sublist_by_pub:
									self.mylogger.log("sublist_by_pub[{}] ==>{}".format(myPubName, sublist_by_pub[myPubName]))
									sublist_by_pub[myPubName][mydata['token']] = mydata
									#gotit = False
									#for i, sublist in enumerate(sublist_by_pub[myPubName]):
									#	self.mylogger.log("check {} usblist token {} --- your input token  [{}] ".format(i, sublist['token'], mydata['token']))
									#	if sublist['token'] == mydata['token']:
									#		sublist_by_pub[myPubName][i] = mydata
									#		self.mylogger.log("got it!!!!!!!!")
									#		break
									#if not gotit:
									#	sublist_by_pub[myPubName].append(mydata)
								else:
									#sublist_by_pub[myPubName] = [mydata]
									sublist_by_pub[myPubName] = {mydata['token'] : mydata}
							else:
								rmToken = ''
								for ckToken in list(subscriber.keys()):
									if subscriber[ckToken][myPubName]['c_url'] == mydata['c_url']:
										rmToken = ckToken
										del subscriber[rmToken][myPubName]
										break
								if len(subscriber[rmToken]) == 0:
									del subscriber[rmToken]

								if rmToken != '':
									if myPubName in sublist_by_pub:
										if rmToken in sublist_by_pub[myPubName]:	
											del sublist_by_pub[myPubName][rmToken]
									#	for i, sublist in enumerate(sublist_by_pub[myPubName]):
									#		if sublist['token'] == rmToken:
									#			del sublist_by_pub[myPubName][i]
									#			break

							self.mylogger.log("complete insert/update ---->>>subscriber >>>>>>>>>>>>>{}".format(subscriber))
							self.mylogger.log("complete insert/update ---->>>sublilst_by_pub[{}]>>>>>>>>>>>>>{}".format(myPubName, sublist_by_pub))
							 
					elif mydata["myStat"] == "D":
						if mydata["myJob"] == "file_spec":
							for i, item in enumerate(file_spec[fsname]):
								if str(item['c_seq']) == str(mydata['c_seq']):
									del file_spec[fsname][i]
									proc_file_spec = True
									self.mylogger.log("complete delete ---->>>file_spec >>>>>>>>>>>>>{}".format(file_spec))
									break

						elif mydata["myJob"] == "client_info":
							if fsname in client_info:
								del client_info[fsname]

							self.mylogger.log("complete delete ---->>>client_info>>>>>>>>>>>>>{}".format(client_info))
							myPubName = mydata['act_package']
							for myToken in list(sublist_by_pub[myPubName].keys()):
								if sublist_by_pub[myPubName][myToken]['fsname'] == fsname:
									del sublist_by_pub[myPubName][myToken]
									self.mylogger.log("complete delete >>sublist_by_pub[{}[{}]>>>>>>>>>>>>>{}".format(myPubName, myToken, sublist_by_pub[myPubName]))


						elif mydata["myJob"] == "mycrontab":
							myPubName = mydata['act_package']
							myToken = mydata['token'] 
							if myToken in subscriber:
								del subscriber[myToken]

							#for myPubName in list(sublist_by_pub.keys()):
							#	#for i, sublist in enumerate(sublist_by_pub[myPubName]):
							#	#	if sublist['token'] == myToken:
							#	#		del sublist_by_pub[myPubName][i]
							#	#		break
							#	if myToken in sublist_by_pub[myPubName]:
							#		del sublist_by_pub[myPubName][myToken]
						
							if myToken in sublist_by_pub[myPubName]:
								del sublist_by_pub[myPubName][myToken]

							self.mylogger.log("complete delete ---->>>subscriber >>>>>>>>>>>>>{}".format(subscriber))
							self.mylogger.log("complete insert/update ---->>>sublilst_by_pub[{}]>>>>>>>>>>>>>{}".format(myPubName, sublist_by_pub[myPubName]))

					elif mydata["myStat"] == "end":
						self.mylogger.log("end--------> client insert---->>>>>>>>>>>>>>>>{}".format(proc_client_info))
						if fsname in proc_client_info:
							client_info[fsname] = proc_client_info[fsname]
							self.mylogger.log("client insert---->>>>>>>>>>>>>>>>{}".format(client_info[fsname]))
							del proc_client_info[fsname]

						proc_file_spec = False

			except:
				self.mylogger.log(myutil.getErrLine(sys))
				
	


class MyBroker_mc(threading.Thread):
	def __init__(self, name=None, myqueue=None):
		super(MyBroker_mc,self).__init__()
		self.q = myqueue
		self.mysock = myutil.MyMultiCast('myfeed1', isSend=False)
		self.mylogger = myutil.MyLogger('myfeedmgr_stream_broker_mc', MYHOME+'/log/', mySizeMB=200, myCount=10)

	def __exit__(self):
		self.mysock.close()

	def run(self):
		while True:
			mydata = self.mysock.recv()
			self.q.put(mydata)
			self.mylogger.log(json.loads(mydata))

class MyBroker_pub(threading.Thread):
	def __init__(self, group=None, target=None, name=None,args=(), kwargs=None, verbose=None, myqueue=None):
		super(MyBroker_pub,self).__init__()
		self.target = target
		self.name = name
		self.q = myqueue
		self.mylogger = myutil.MyLogger('myfeedmgr_stream_broker_pub', MYHOME+'/log/', mySizeMB=200, myCount=10)

	def run(self):
		while True:
			mydata = self.q.get()
			mydata = json.loads(mydata)
			self.mylogger.log("{}--{}".format(id(mydata), mydata))

			myJob = mydata["@mypub@"]
			if myJob in myQ:
				myQ[myJob].put(mydata)
			else:
				# new vendor
				socket_process = MyBroker_sub(name=myJob)
				socket_process.start()
						
class MyBroker_sub(threading.Thread):
	def __init__(self, group=None, target=None, name=None,args=(), kwargs=None, verbose=None):
		super(MyBroker_sub,self).__init__()
		self.target = target
		self.name = name

		self.mylogger = myutil.MyLogger('myfeedmgr_stream_broker_sub_'+self.name, MYHOME+'/log/', mySizeMB=200, myCount=10)

		# queue for vendor
		self.q = queue.Queue()
		myQ[self.name] = self.q


	def run(self):
		while True:
			mydata = self.q.get()
			#self.mylogger.log("{}--{}".format(id(mydata), mydata))
			# check subscriber
			try:
				if self.name in sublist_by_pub:
					#self.mylogger.log("sublist_by_pub[{}]-----{}".format(self.name, sublist_by_pub[self.name]))
					for token in sublist_by_pub[self.name]:
						#myToken = sendlist['token']
						#subQname = myToken+"@"+fsname
						#subQname = sendlist['token']
						myProtocol = sublist_by_pub[self.name][token]['c_proto'].split("_")[0]
						fsname = sublist_by_pub[self.name][token]['fsname'].lower()
						subQname = token+'@'+myProtocol+'@'+fsname
						self.mylogger.log("{}--{}".format(subQname, mydata))
						if subQname in mySubQ:
							mySubQ[subQname].put(mydata)
						else:
							# new subscriber
							#myProtocol = sublist_by_pub[self.name][token]['c_proto'].split("_")[0]
							#fsname = sublist_by_pub[self.name][token]['fsname'].lower()
							#myProtocol = sendlist['c_proto'].split("_")[0]
							#fsname = sendlist['fsname'].lower()
							if myProtocol == 'WS':
								#fsname = sendlist['fsname'].lower()
								if fsname.find(".json") != -1:
									socket_process = MyBroker_subscriber_ws_json(myQName=subQname, myToken=token, myProto=myProtocol, fsname=fsname, myPubNm=self.name)
									socket_process.start()
								elif fsname.find(".fix") != -1:
									socket_process = MyBroker_subscriber_ws_fix(myQName=subQname, myToken=token, myProto=myProtocol, fsname=fsname, myPubNm=self.name)
									socket_process.start()
								else: # general string with deleiter
									socket_process = MyBroker_subscriber_ws_gstr(myQName=subQname, myToken=token, myProto=myProtocol, fsname=fsname, myPubNm=self.name)
									socket_process.start()
							elif myProtocol == 'TCP':
								if fsname.find(".json") != -1:
									socket_process = MyBroker_subscriber_tcp_json(myQName=subQname, myToken=token, myProto=myProtocol, fsname=fsname, myPubNm=self.name)
									socket_process.start()
								elif fsname.find(".fix") != -1:
									socket_process = MyBroker_subscriber_tcp_fix(myQName=subQname, myToken=token, myProto=myProtocol, fsname=fsname, myPubNm=self.name)
									socket_process.start()
								else: # general string with deleiter
									socket_process = MyBroker_subscriber_tcp_gstr(myQName=subQname, myToken=token, myProto=myProtocol, fsname=fsname, myPubNm=self.name)
									socket_process.start()
			except:
				self.mylogger.log("error--->{}".format(myutil.getErrLine(sys)))
					
class MyBroker_subscriber_ws_gstr(threading.Thread):
	def __init__(self, **kwargs):
		super(MyBroker_subscriber_ws_gstr,self).__init__()
		#self.target = target
		self.name = kwargs['myname']
		self.myToken = kwargs['myToken']
		self.mylogger = myutil.MyLogger('myfeedmgr_stream_broker_subscriber_'+self.myToken, MYHOME+'/log/', mySizeMB=200, myCount=10)

		# queue for vendor
		self.q = queue.Queue()
		mySubQ[self.name] = self.q

		# connection
		#self.myINFO = subscriber[self.name]
		#self.conURL = serviceList[self.myINFO['c_host']]['url']
		#self.mylogger.log(self.conURL+"----->"+self.name)

	def __exit__(self):
		self.mylogger.log("exit----->"+self.name)


	def run(self):
		while True:
			mydata = self.q.get().copy()
			self.mylogger.log("{}--{}-".format(id(mydata), mydata))
			isSend=False
			try:
				#mydata_r = {}
				#mydata_r_header_list = []
				mydata_r_list = []
				if 'alive' in mydata:
					mydata_r_list.append(mydata['alive'])
					isSend = True
				else:
					for myFILTER in client_info[subscriber[self.myToken]['fsname']]:
						isGO = True
						for k,v in myFILTER.items():
							if k in mydata:
								if v.find("%%") != -1:
									if re.match(v.replace("%%",".*").lower(), mydata[k].lower()) is None:
										isGO = False
										break
								else:
									if mydata[k].lower() != v.lower():
										isGO = False
										break
							else:
								isGO = False
								break
						if isGO:
							isSend=True
							break

					if isSend:
						# check items
						for item in file_spec[subscriber[self.myToken]['fsname']]:
							ckCol = item['act_package_qry_col']
						
							# check speical user define
							isSel = False
							#global tmpstr 
							tmpstr = ''
							if ckCol.find("=") != -1:
								mySet = ckCol.split("=")[1]
								ckCol = myutil.getSpecialStr(mySet)
								ckCol = eval(ckCol)
								#exec("tmpstr="+mySet, globals())
								isSel = True
							elif ckCol in mydata:
								#global tmpstr
								tmpstr = mydata[ckCol]
								isSel = True

							if isSel:
								_myreplace = item['myreplace']
								if _myreplace is None or _myreplace.strip('\r\n ') == "":
									pass
								else:
									_myreplace = myutil.getSpecialStr(_myreplace)
									if _myreplace.startswith("{") and _myreplace.endswith("}"): # is a json style
										myrepldic = eval(_myreplace)
										if tmpstr in myrepldic:
											tmpstr = myrepldic[tmpstr]
									else: # is a code 
										#global _myreplace_
										_myreplace_ = tmpstr
										isEQUAL = _myreplace.split("=")
										if len(isEQUAL) > 1:
											_myreplace = isEQUAL[1]
											# check system set
											exeSmt = re.sub("\${([0-9]+)}", r"filestr[\1-1]['data']", _myreplace)
											#exeSmt = _myreplace
											# check user set
											exeSmt = re.sub("\$[a-zA-Z]+[_0-9a-zA-Z]*", "_myreplace_", exeSmt)
											try:
												_myreplace_ = eval(exeSmt)
											except ValueError:
												exeSmt = exeSmt.replace("long(_myreplace_)", "long(float(_myreplace_))").replace("int(_myreplace_)","int(float(_myreplace_))")
												_myreplace_ = eval(exeSmt)
										tmpstr = str(_myreplace_)

								#mydata_r[ckCol] = tmpstr	
								#mydata_r_header_list.append(ckCol)
								mydata_r_list.append(tmpstr)
								#self.mylogger.log("ok go ---{}".format(tmpstr))
					
								
				if isSend:
					sendData = str({"token":"infomax_sys@systemmgr","to":self.myToken,"data":str(mydata_r)})
					self.mylogger.log("{} message sent!!--->{}".format(serviceList[subscriber[self.myToken]['c_host']]['url'], sendData))
					myFEP_WS = websocket.create_connection(serviceList[subscriber[self.myToken]['c_host']]['url'])
					myFEP_WS.send(sendData)
					myFEP_WS.close()
			except:
				self.mylogger.log(myutil.getErrLine(sys))


class MyBroker_subscriber_tcp_gstr(threading.Thread):
	def __init__(self, **kwargs):
		super(MyBroker_subscriber_tcp_gstr,self).__init__()
		#self.target = target
		self.myQName = kwargs['myQName']
		self.myToken = kwargs['myToken']
		self.myPubNm = kwargs['myPubNm']
		self.mylogger = myutil.MyLogger('myfeedmgr_stream_broker_subscriber_tcp_gstr_'+self.myQName, MYHOME+'/log/', mySizeMB=200, myCount=10)

		# queue for vendor
		self.q = queue.Queue()
		mySubQ[self.myQName] = self.q
		self.mylogger.log("myQueue --> {}".format(self.myQName))
		self.mysock = {}
		#for myPubNm, v in subscriber[self.myToken].items():
		pipeline = serviceList[subscriber[self.myToken][self.myPubNm]['c_host']]['pipeline']
		myurl = serviceList[subscriber[self.myToken][self.myPubNm]['c_host']]['url']
		myAddr = [serviceList[pipeline]['ip'], serviceList[pipeline]['port']]
		self.mylogger.log("my tcp send --> {}-{}-{}-{}".format(self.myPubNm, pipeline, myurl, myAddr))
		self.mysock[myurl] = myutil.MyMultiCast('', isSend=True, myAddr=myAddr)

		self.mylogger.log("complete init")

		# connection
		#self.myINFO = subscriber[self.name]
		#self.conURL = serviceList[self.myINFO['c_host']]['url']

	def __exit__(self):
		self.mylogger.log("exit----->"+self.myQName)


	def run(self):
		while True:
			mydata = self.q.get().copy()
			self.mylogger.log("{}--{}-".format(id(mydata), mydata))
			isSend=False
			try:
				# default data length
					
				mydata_r = ["0000"]
				#mydata_r_header_list = []
				myPubNm = mydata['@mypub@']
				if 'alive' in mydata:
					mydata_r.append("0000")
					mydata_r.append(mydata['alive'].replace("-",""))
					#mydata_r[0] = "%.04d" % (len(mydata['alive']))
					isSend = True
				else:
					myFILE_SPEC = subscriber[self.myToken][myPubNm]['fsname']
					for myFILTER in client_info[myFILE_SPEC]:
						isGO = True
						for k,v in myFILTER.items():
							if k in mydata:
								if v.find("%%") != -1:
									if re.match(v.replace("%%",".*").lower(), mydata[k].lower()) is None:
										isGO = False
										break
								else:
									if mydata[k].lower() != v.lower():
										isGO = False
										break
							else:
								isGO = False
								break
						if isGO:
							isSend=True
							break

					if isSend:
						# check items
						items = file_spec[myFILE_SPEC]
						for item in items:
							ckCol = item['act_package_qry_col']
							
							# check speical user define
							#isSel = False
							tmpstr = None
							if ckCol.find("=") != -1:
								mySet = ckCol.split("=")[1]
								ckCol = myutil.getSpecialStr(mySet)
								ckCol = eval(ckCol)
							elif ckCol in mydata:
								tmpstr = mydata[ckCol]

							_myreplace = item['myreplace']
							if _myreplace is None or _myreplace.strip('\r\n ') == "" or _myreplace.strip('\r\n ').lower() == 'none':
								pass
							else:
								_myreplace = myutil.getSpecialStr(_myreplace)
								if _myreplace.startswith("{") and _myreplace.endswith("}"): # is a json style
									myrepldic = eval(_myreplace)
									if tmpstr in myrepldic:
										tmpstr = myrepldic[tmpstr]
								else: # is a code 
									#global _myreplace_
									_myreplace_ = tmpstr
									isEQUAL = _myreplace.split("=")
									if len(isEQUAL) > 1:
										_myreplace = isEQUAL[1]
										# check system set
										exeSmt = re.sub("\${([0-9]+)}", r"mydata_r[items[\1-1]['act_package_qry_col']]", _myreplace)
										# check user set
										exeSmt = re.sub("\$[a-zA-Z]+[_0-9a-zA-Z]*", "_myreplace_", exeSmt)
										try:
											_myreplace_ = eval(exeSmt)
										except ValueError:
											exeSmt = exeSmt.replace("long(_myreplace_)", "long(float(_myreplace_))").replace("int(_myreplace_)","int(float(_myreplace_))")
											_myreplace_ = eval(exeSmt)
									tmpstr = _myreplace_

							mydata_r.append(tmpstr)
							#mydata_r_header_list.append(ckCol)
							#mydata_r_list.append(tmpstr)
							#self.mylogger.log("ok go ---{}".format(tmpstr))
									
				if isSend:
					# default data length
					if subscriber[self.myToken][myPubNm]['use_cr_code'] == 'Y':
						mydata_r.insert(0, chr(0x02))
						mydata_r.append(chr(0x03))
						mydata_r[1] = "%.04d" % (len(subscriber[self.myToken][myPubNm]['delimiter'].join(mydata_r)))
					else:
						mydata_r[0] = "%.04d" % (len(subscriber[self.myToken][myPubNm]['delimiter'].join(mydata_r)))
					sendData = str({"token":"infomax_sys@systemmgr","to":self.myToken,"data":str(subscriber[self.myToken][myPubNm]['delimiter'].join(mydata_r))})
					self.mylogger.log("{} message sent!!--->{}".format(serviceList[subscriber[self.myToken][myPubNm]['c_host']]['url'], sendData))
					self.mysock[serviceList[subscriber[self.myToken][myPubNm]['c_host']]['url']].send(sendData.encode("utf8"))
			except:
				self.mylogger.log(myutil.getErrLine(sys))



class MyBroker_subscriber_tcp_json(threading.Thread):
	def __init__(self, **kwargs):
		super(MyBroker_subscriber_tcp_json,self).__init__()
		#self.target = target
		self.myQName = kwargs['myQName']
		self.myToken = kwargs['myToken']
		self.myPubNm = kwargs['myPubNm']
		self.mylogger = myutil.MyLogger('myfeedmgr_stream_broker_subscriber_tcp_json_'+self.myQName, MYHOME+'/log/', mySizeMB=200, myCount=10)

		# queue for vendor
		self.q = queue.Queue()
		mySubQ[self.myQName] = self.q
		self.mylogger.log("myQueue --> {}".format(self.myQName))
		self.mysock = {}
		#for myPubNm, v in subscriber[self.myToken].items():
		pipeline = serviceList[subscriber[self.myToken][self.myPubNm]['c_host']]['pipeline']
		myurl = serviceList[subscriber[self.myToken][self.myPubNm]['c_host']]['url']
		myAddr = [serviceList[pipeline]['ip'], serviceList[pipeline]['port']]
		self.mylogger.log("my tcp send --> {}-{}-{}-{}".format(self.myPubNm, pipeline, myurl, myAddr))
		self.mysock[myurl] = myutil.MyMultiCast('', isSend=True, myAddr=myAddr)

		self.mylogger.log("complete init")

		# connection
		#self.myINFO = subscriber[self.name]
		#self.conURL = serviceList[self.myINFO['c_host']]['url']

	def __exit__(self):
		self.mylogger.log("exit----->"+self.myQName)


	def run(self):
		while True:
			mydata = self.q.get().copy()
			self.mylogger.log("{}--{}-".format(id(mydata), mydata))
			isSend=False
			try:
				mydata_r = {}
				#mydata_r_header_list = []
				#mydata_r_list = []
				myPubNm = mydata['@mypub@']
				if 'alive' in mydata:
					#del mydata["@mypub@"]
					mydata_r["alive"] = mydata['alive']
					isSend = True
				else:
					myFILE_SPEC = subscriber[self.myToken][myPubNm]['fsname']
					for myFILTER in client_info[myFILE_SPEC]:
						isGO = True
						for k,v in myFILTER.items():
							if k in mydata:
								if v.find("%%") != -1:
									if re.match(v.replace("%%",".*").lower(), mydata[k].lower()) is None:
										isGO = False
										break
								else:
									if mydata[k].lower() != v.lower():
										isGO = False
										break
							else:
								isGO = False
								break
						if isGO:
							isSend=True
							break

					if isSend:
						# check items
						items = file_spec[myFILE_SPEC]
						for item in items:
							ckCol = item['act_package_qry_col']
							
							# check speical user define
							#isSel = False
							tmpstr = None
							if ckCol.find("=") != -1:
								mySet = ckCol.split("=")[1]
								ckCol = myutil.getSpecialStr(mySet)
								ckCol = eval(ckCol)
							elif ckCol in mydata:
								tmpstr = mydata[ckCol]

							_myreplace = item['myreplace']
							if _myreplace is None or _myreplace.strip('\r\n ') == "" or _myreplace.strip('\r\n ').lower() == 'none':
								pass
							else:
								_myreplace = myutil.getSpecialStr(_myreplace)
								if _myreplace.startswith("{") and _myreplace.endswith("}"): # is a json style
									myrepldic = eval(_myreplace)
									if tmpstr in myrepldic:
										tmpstr = myrepldic[tmpstr]
								else: # is a code 
									#global _myreplace_
									_myreplace_ = tmpstr
									isEQUAL = _myreplace.split("=")
									if len(isEQUAL) > 1:
										_myreplace = isEQUAL[1]
										# check system set
										exeSmt = re.sub("\${([0-9]+)}", r"mydata_r[items[\1-1]['act_package_qry_col']]", _myreplace)
										#exeSmt = _myreplace
										#self.mylogger.log("{} -- exeSmt = _myreplace---> {} ---> ".format(ckCol, exeSmt))
										# check user set
										exeSmt = re.sub("\$[a-zA-Z]+[_0-9a-zA-Z]*", "_myreplace_", exeSmt)
										#self.mylogger.log("replace[{}]---> {} ---> ".format(_myreplace_, exeSmt))
										try:
											#self.mylogger.log("before -- exeSmt = {} -- ".format(exeSmt))
											_myreplace_ = eval(exeSmt)
											#self.mylogger.log("after -- exeSmt = {} -- {}---> ".format(tmpstr, _myreplace_))
										except ValueError:
											exeSmt = exeSmt.replace("long(_myreplace_)", "long(float(_myreplace_))").replace("int(_myreplace_)","int(float(_myreplace_))")
											#exec(tmpstr, globals())
											_myreplace_ = eval(exeSmt)
									tmpstr = _myreplace_

							mydata_r[ckCol] = tmpstr	
							#mydata_r_header_list.append(ckCol)
							#mydata_r_list.append(tmpstr)
							#self.mylogger.log("ok go ---{}".format(tmpstr))
									
				if isSend:
					sendData = str({"token":"infomax_sys@systemmgr","to":self.myToken,"data":str(mydata_r)})
					self.mylogger.log("{} message sent!!--->{}".format(serviceList[subscriber[self.myToken][myPubNm]['c_host']]['url'], sendData))
					self.mysock[serviceList[subscriber[self.myToken][myPubNm]['c_host']]['url']].send(sendData.encode())
			except:
				self.mylogger.log(myutil.getErrLine(sys))


	
class MyBroker_subscriber_ws_json(threading.Thread):
	def __init__(self, **kwargs):
		super(MyBroker_subscriber_ws_json,self).__init__()
		self.myQName = kwargs['myQName']
		self.myToken = kwargs['myToken']
		self.mylogger = myutil.MyLogger('myfeedmgr_stream_broker_subscriber_ws_json_'+self.myQName, MYHOME+'/log/', mySizeMB=200, myCount=10)

		# queue for vendor
		self.q = queue.Queue()
		mySubQ[self.myQName] = self.q
		self.mylogger.log("myQueue --> {}".format(self.myQName))

		# connection
		#self.myINFO = subscriber[self.name]
		#self.conURL = serviceList[self.myINFO['c_host']]['url']

	def __exit__(self):
		self.mylogger.log("exit----->"+self.myQName)


	def run(self):
		while True:
			mydata = self.q.get().copy()
			self.mylogger.log("{}--{}-".format(id(mydata), mydata))
			isSend=False
			try:
				mydata_r = {}
				#mydata_r_header_list = []
				#mydata_r_list = []
				myPubNm = mydata['@mypub@']
				if 'alive' in mydata:
					#del mydata["@mypub@"]
					mydata_r["alive"] = mydata['alive']
					isSend = True
				else:
					myFILE_SPEC = subscriber[self.myToken][myPubNm]['fsname']
					for myFILTER in client_info[myFILE_SPEC]:
						isGO = True
						for k,v in myFILTER.items():
							if k in mydata:
								if v.find("%%") != -1:
									if re.match(v.replace("%%",".*").lower(), mydata[k].lower()) is None:
										isGO = False
										break
								else:
									if mydata[k].lower() != v.lower():
										isGO = False
										break
							else:
								isGO = False
								break
						if isGO:
							isSend=True
							break

					if isSend:
						# check items
						items = file_spec[myFILE_SPEC]
						for item in items:
							ckCol = item['act_package_qry_col']
							
							# check speical user define
							#isSel = False
							tmpstr = None
							if ckCol.find("=") != -1:
								mySet = ckCol.split("=")[1]
								ckCol = myutil.getSpecialStr(mySet)
								ckCol = eval(ckCol)
							elif ckCol in mydata:
								tmpstr = mydata[ckCol]

							_myreplace = item['myreplace']
							if _myreplace is None or _myreplace.strip('\r\n ') == "" or _myreplace.strip('\r\n ').lower() == 'none':
								pass
							else:
								_myreplace = myutil.getSpecialStr(_myreplace)
								if _myreplace.startswith("{") and _myreplace.endswith("}"): # is a json style
									myrepldic = eval(_myreplace)
									if tmpstr in myrepldic:
										tmpstr = myrepldic[tmpstr]
								else: # is a code 
									#global _myreplace_
									_myreplace_ = tmpstr
									isEQUAL = _myreplace.split("=")
									if len(isEQUAL) > 1:
										_myreplace = isEQUAL[1]
										# check system set
										exeSmt = re.sub("\${([0-9]+)}", r"mydata_r[items[\1-1]['act_package_qry_col']]", _myreplace)
										#exeSmt = _myreplace
										#self.mylogger.log("{} -- exeSmt = _myreplace---> {} ---> ".format(ckCol, exeSmt))
										# check user set
										exeSmt = re.sub("\$[a-zA-Z]+[_0-9a-zA-Z]*", "_myreplace_", exeSmt)
										#self.mylogger.log("replace[{}]---> {} ---> ".format(_myreplace_, exeSmt))
										try:
											#self.mylogger.log("before -- exeSmt = {} -- ".format(exeSmt))
											_myreplace_ = eval(exeSmt)
											#self.mylogger.log("after -- exeSmt = {} -- {}---> ".format(tmpstr, _myreplace_))
										except ValueError:
											exeSmt = exeSmt.replace("long(_myreplace_)", "long(float(_myreplace_))").replace("int(_myreplace_)","int(float(_myreplace_))")
											#exec(tmpstr, globals())
											_myreplace_ = eval(exeSmt)
									tmpstr = _myreplace_

							mydata_r[ckCol] = tmpstr	
							#mydata_r_header_list.append(ckCol)
							#mydata_r_list.append(tmpstr)
							#self.mylogger.log("ok go ---{}".format(tmpstr))
									
				if isSend:
					sendData = str({"token":"infomax_sys@systemmgr","to":self.myToken,"data":str(mydata_r)})
					self.mylogger.log("{} message sent!!--->{}".format(serviceList[subscriber[self.myToken][myPubNm]['c_host']]['url'], sendData))
					myFEP_WS = websocket.create_connection(serviceList[subscriber[self.myToken][myPubNm]['c_host']]['url'])
					myFEP_WS.send(sendData)
					myFEP_WS.close()
			except:
				self.mylogger.log(myutil.getErrLine(sys))


def setProcessData(clientDNS, c_name='',c_job_num=''):
	with mydbapi.Mydb('mysql', 'myfeed',  myfeedmgr_db_host, _myPort=3306,_myUser='myfeed') as mycon:
		sql = """ select distinct concat(ci.c_name, '@', ci.c_file, '@', apq.act_package) as fsname
			, ci.sel_act_packages, apq.act_package
			,ci.client_name, ci.c_name, ci.c_file, myc.c_job_num, myc.c_url
			, myc.delimiter, myc.use_cr_code
               	        from myfeed.client_info ci, myfeed.act_package_qry apq, myfeed.mycrontab myc, myfeed.client_dns cdns, ws_status wss
               	        where ci.sel_act_packages like concat('%', apq.act_package, '%')
               	        and apq.dbms in('stream','rio') and myc.c_name = ci.c_name and ci.c_file = myc.c_file
               	        and myc.c_url like concat('%',cdns.c_url,'%') and wss.name = cdns.c_host
			and wss.ip = '{ip}'""".format(ip=MYIP)
	
		for items in mycon.exeQry("G", sql, IS_PRINT=True, useDict=True):
			myPubName = items['act_package'].replace("\n","").replace("\r","").replace("\t","").strip()
			for myURL in items['c_url'].split(","):
				subItems = items.copy()
				if myURL in clientDNS:
					urlINFO = clientDNS[myURL]
					subItems['c_job_num'] = str(subItems['c_job_num'])
					subItems.update(urlINFO)
					myToken = '@'.join([subItems['c_id'],subItems['c_passwd'],subItems['c_name'],subItems['c_job_num'],subItems['c_host']])

					subItems['token'] = myToken
					subItems['c_url'] = myURL

					if myToken in subscriber:
						# verify only one
						subscriber[myToken][myPubName] = subItems
					else:
						subscriber[myToken] = {myPubName : subItems}

	
					if myPubName in sublist_by_pub:
						#gotit = False
						#for i, ckToken in enumerate(sublist_by_pub[myPubName]):
						#	if ckToken['token'] == subItems['token']:
						#		sublist_by_pub[myPubName][i] = subItems
						#		gotit = True
						#		break
						#if not gotit:
						#	sublist_by_pub[myPubName].append(subItems)
						sublist_by_pub[myPubName][subItems['token']] = subItems
					else:
						sublist_by_pub[myPubName] = {subItems['token'] : subItems}

		sql = """select concat(ci.c_name,'@',ci.c_file, '@',apq.act_package) as fsname, ci.*, apq.*
                        from myfeed.act_package_qry apq, myfeed.client_info ci
                        where apq.dbms in('stream','rio')
                        and ci.sel_act_packages like concat('%',apq.act_package,'%')"""
		regexp_def_style=""".[^=]+=\".[^\"]*\"\s?(?:,|$)|.[^=]+=.[^\"]+\s?(?:,|$)|.[^=]+=\"\"|.[^=]+=$"""
		for items in mycon.exeQry("G", sql, IS_PRINT=True, useDict=True):
			act_package_list = items['sel_act_packages'].replace("\t","").replace("\n","").replace("\r","").split('|')
			act_package_qry_args_list = items['act_package_qry_args'].replace("\t","").replace("\n","").replace("\r","").split('|')

			print(act_package_list, act_package_qry_args_list)

			#kv = dict(zip(act_package_list,act_package_qry_args_list))
			act_package_qry_args = dict(zip(act_package_list,act_package_qry_args_list))[items['act_package']]

			myFilter = []
			myArgStr = myutil.getSpecialStr(act_package_qry_args.replace("\t","").replace("\n","").replace("\r",""))
			for myARG in re.split(r'\][^\[,]*,[^\[]*\[', myArgStr.replace("\n","").strip('[] ')):
				myFilter.append(dict([(x[:x.find("=")].strip(", "), x[x.find("=")+1:].strip(",' ") if x[x.find("=")+1:].strip(", ").startswith("'") else x[x.find("=")+1:].strip(",\" ")) for x in re.findall(regexp_def_style, myARG)]))

			client_info[items['fsname']] = myFilter

			#file spec
			sql = """select concat(fs.c_name,'@',fs.c_file, '@', apq.act_package) as fsname, fs.*, apm.*
                	        from myfeed.file_spec fs, myfeed.act_package_master apm, myfeed.act_package_qry apq 
                	        where apq.dbms in('stream','rio')
                	        and apq.act_package = apm.act_package
                	        and apm.act_package = '{act_package}'
                	        and fs.c_name = '{c_name}' and fs.c_file = '{c_file}'
                	        and apm.act_package_seq = fs.act_package_seq
				order by fsname, sel_act_package_idx, c_seq
				""".format(act_package=items['act_package'], c_name=items['c_name'], c_file=items['c_file'])
			for itemss in mycon.exeQry("G", sql, IS_PRINT=True, useDict=True):
				myFileSpecName = itemss['fsname']
				if myFileSpecName in file_spec:
					file_spec[myFileSpecName].append(itemss)
				else:
					file_spec[myFileSpecName] = [itemss]


	print(subscriber)
	#print("sublist_by_pub---------------->",sublist_by_pub)
	#print("client_info---------------->", client_info)
	#print("file_spec--------------------->", file_spec)
				



if __name__ == "__main__":

	clientDNS = {}
	myServTp = ''
	try:
		myServTp = sys.argv[1].lower()
	except:
		pass
	# set userDic
	userDicToken = {"infomax_sys@systemmgr":"", "infomax_sys_cmd@systemmgr":""}

	

	with mydbapi.Mydb('mysql', 'myfeed',  myfeedmgr_db_host, _myPort=3306,_myUser='myfeed') as mycon:
		sql = """select cdns.*, wss.*
			from client_dns cdns, ws_status wss
			where cdns.c_proto in('WS_CLIENT','TCP_CLIENT')
			and cdns.c_host = wss.name"""
		for item in mycon.exeQry("G", sql, IS_PRINT=True, useDict=True):
			clientDNS[item['c_url']] = item

		sql = """select * from ws_status"""
		for items in mycon.exeQry("G", sql, useDict=True):
			serviceList[items['name']] = items

	setProcessData(clientDNS)

	for k,v in serviceList.items():
		if v['ip'] == MYIP:
			servINFO[k] = serviceList[k]

	q = queue.Queue()
	if len(servINFO) == 0:
		mylogger.log("{}".format("no service information"))
		myutil.sms("상우", "no service information ---!!!!! myfeedmgr_stream_broker server[{}-{}]".format(MYIP,MYHOSTNAME))
	else:	
		socket_process = MyBroker_sys(name="myb_sys", myqueue=q)
		socket_process.start()

		socket_process = MyBroker_mc(name="myb_mc", myqueue=q)
		socket_process.start()

		socket_process = MyBroker_pub(name="myb_pub", myqueue=q)
		socket_process.start()
		myutil.sms("상우", "start myfeedmgr_stream_broker server[{}-{}]".format(MYIP,MYHOSTNAME))

