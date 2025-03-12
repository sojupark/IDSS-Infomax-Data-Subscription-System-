# import socket programming library
import socket, os, sys
import signal
import ast
import json

# import thread module
from _thread import *
import threading

from queue import Queue
import mydbapi3 as mydbapi
import myutil3 as myutil

servINFO = {}
connectedID = {}
connectedWS = {}
connectedWSToken = {}

userDicID = {}
userDicToken = {}

MYHOME = os.getcwd()

closed = -1

myEncoding=os.environ.get("LANG").split('.')[1].lower()
MYIP = socket.gethostbyname(socket.gethostname())

myfeedmgr_db_host = 'ftp2'
if socket.gethostname().lower() == myfeedmgr_db_host:
	myfeedmgr_db_host = 'localhost'

mylog = myutil.MyLogger("myfeedmgr_tcp", MYHOME+"/log/", mySizeMB=200, myCount=10)


def signal_term_handler(signal, frame):
	mylog.log("stop server")
	myutil.sms("상우", "stop myfeedmgr_tcp server[{}]".format(servINFO))

signal.signal(signal.SIGTERM, signal_term_handler)
	
q = Queue()
#print_lock = threading.Lock()

def auth_check(mysocket):
	okGO = False
	while True:
		data = mysocket.recv(1024)
		mylog.log("{}".format(data))
		if not data:
			mylog.log("bye")	
			break
		try:
			myauth = json.loads(data.decode("utf8"))
			#if 'token' in myauth and myauth["token"] in ['infomax_sys@systemmgr','infomax_sys_cmd@systemmgr']:

			if 'c_name' in myauth and 'c_job_num' in myauth:
				for ckToken, v in userDicToken.items():
					if 'c_name' in v and 'c_job_num' in v and v['c_name'] == myauth['c_name'] and v['c_job_num'] == myauth['c_job_num'] and v['job_status'] != 'STOP':
						connectedWS[mysocket]['token'] = ckToken
						if ckToken in connectedWSToken:
							if mysocket not in connectedWSToken[ckToken]:
								connectedWSToken[ckToken].append(mysocket)
						else:
							connectedWSToken[ckToken] = [mysocket]

						okGO = True
						break

				if okGO:
					mysocket.send(json.dumps({"subscription":"ready to receive.."}).encode("utf8"))
				else:
					mysocket.send(json.dumps({"subscription":"not allowed"}).encode("utf8"))

				# reday to self queue
				break	

			elif 'id' in myauth and 'passwd' in myauth:
				myid = myauth['id'] 
				mypasswd = myauth['passwd']

				checkID = False
				print(userDicID)
				if myid in userDicID and userDicID[myid]['c_passwd'] == mypasswd:
					checkID = True

				mylog.log("checkID [{}][{}]--> -- {}".format(myid, mypasswd, checkID))
				authStr = "not allowed"
				if checkID:
					authStr = "approved"
					if myid in connectedID:
						if int(userDicID[myid]['connection']) < len(connectedID[myid])+1:
							authStr = "not allowed"
							# check real connection list
							for myconnection in connectedID[myid]:
								if myconnection.fileno() == closed:
									connectedID[myid].remove(myconnection)
									if myconnection in connectedWS:
										del connectedWS[myconnection]

							# recheck
							if int(userDicID[myid]['connection']) < len(connectedID[myid])+1:
								authStr = "there have been reached your limit [{}] connections!!".format(len(connectedID[myid]))
							else:
								authStr = "approved"
				mysocket.send(json.dumps({"auth":authStr}).encode("utf8"))
				if authStr == 'approved':
					if myid in connectedID:
						connectedID[myid].append(mysocket)
					else:
						connectedID[myid] = [mysocket]
					connectedWS[mysocket] = {"myid":myid, "token":""}
					mylog.log("new connection [{}][{}]".format(myauth, mysocket))
				else:
					print("here")
					mysocket.close()
					if mysocket in connectedWS:
						del connectedWS[mysocket]
						try:
							connectedID[connectedWS[mysocket]['id']].remove(mysocket)
						except:
							pass
					break
		except:
			mylog.log(myutil.getErrLine(sys))	
			mysocket.close()
			break



	if mysocket.fileno() == closed:
		print(mysocket)


	if okGO:
		mylog.log("gogogogo!!!!!!!")
		#start_new_thread(dataRecv, (mysocket, ))

def myTCPMain(port):
	port = int(port)
	#auth_queue = Queue()
	#start_new_thread(auth_check, (auth_queue, ))
	#recv_queue = Queue()
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	#s.setblocking(False)
	s.bind(("127.0.0.1", port))
	print("socket binded to port", port)
	s.listen()
	print("socket is listening")

	while True:
		c, addr = s.accept()
		print('Connected to :', addr[0], ':', addr[1])
		#auth_queue.put(c)
		start_new_thread(auth_check, (c, ))
		#start_new_thread(myRecv, (c, recv_queue))
	s.close()


class MyBroker_recv(threading.Thread):
	def __init__(self, name=None):
		super(MyBroker_recv,self).__init__()
		self.mylogger = myutil.MyLogger('myfeedmgr_tcp_recv', MYHOME+'/log/', mySizeMB=200, myCount=10)

	def __exit__(self):
		self.mysock.close()

	def run(self):
		while True:
			try:
				mydata = json.loads(json.dumps(ast.literal_eval(q.get())))
				for ckToken in mydata['to'].split(","):
					self.mylogger.log("try to [{}]".format(ckToken))

					#self.mylogger.log("-connectedWSToken[{}]".format(connectedWSToken))
					if ckToken in connectedWSToken:
						myid = userDicToken[ckToken]['c_id']
						#self.mylogger.log("-myid[{}]".format(myid))
						for my_socket in connectedWSToken[ckToken].copy():
							self.mylogger.log("---------->sending to {} message[{}]".format(ckToken, mydata['data']))
							try:
								my_socket.send(json.dumps(mydata['data']).encode("utf8"))
							except:
								self.mylogger.log("----error-----{}".format(myutil.getErrLine(sys)))
								try:
									if my_socket in connectedWS:
										del connectedWS[my_socket]
		
									try:
										connectedWSToken[ckToken].remove(my_socket)
									except:
										self.mylogger.log("----error2-----{}".format(myutil.getErrLine(sys)))
								except:
									self.mylogger.log("----error3-----{}".format(myutil.getErrLine(sys)))
			except:
				self.mylogger.log("-error---recv mydata-----{}".format(myutil.getErrLine(sys)))
								

						
				


class MyBroker_mc(threading.Thread):
	def __init__(self, name=None, myAddr=[]):
		super(MyBroker_mc,self).__init__()
		self.mysock = myutil.MyMultiCast('', isSend=False, myAddr=myAddr)
		self.mylogger = myutil.MyLogger('myfeedmgr_tcp_mc', MYHOME+'/log/', mySizeMB=200, myCount=10)

	def __exit__(self):
		self.mysock.close()

	def run(self):
		while True:
			mydata = self.mysock.recv()
			q.put(mydata.decode("utf8"))
			self.mylogger.log("{}--".format(json.loads(json.dumps(mydata.decode("utf8")))))


class MyBroker_sys(threading.Thread):
	def __init__(self, name=None):
		super(MyBroker_sys,self).__init__()
		self.mysock = myutil.MyMultiCast('mysys', isSend=False)
		self.mylogger = myutil.MyLogger('myfeedmgr_tcp_sys', MYHOME+'/log/', mySizeMB=200, myCount=10)
		self.mylogger.log("start!!!")
		
	
	def __exit__(self):
		pass

	def run(self):
		while True:
			try:
				msgDic = json.loads(json.dumps(ast.literal_eval(self.mysock.recv().decode("utf8"))))
				#print(self.mysock.recv().decode("utf8"))
				#msgDic = json.loads(mydata)
				self.mylogger.log(msgDic)

				if msgDic['myJob'] == 'client_dns':
					myid = msgDic['c_id']
					if 'ip' in msgDic and msgDic['ip'] == MYIP:
						if msgDic['myStat'] == 'I':
							userDicID[myid] = msgDic
						else:
							if myid in userDicID:
								del userDicID[myid]
						self.mylogger.log("complete to myStat[{}] about myid[{}]----> userDicID---> {}".format(msgDic['myStat'], myid, userDicID))
					else:
						self.mylogger.log("this is not my setting to myip[{}] message[{}]--------".format(MYIP, msgDic))
				elif msgDic['myJob'] == 'mycrontab':
					if 'ip' in msgDic and msgDic['ip'] == MYIP:
						myToken = msgDic['token']
						if msgDic['myStat'] == 'I':
							# check the client_DNS name
							isChange = False
							for ckToken in list(x for x in userDicToken.keys() if x not in ['infomax_sys@systemmgr','infomax_sys_cmd@systemmgr']):
								if userDicToken[ckToken]['c_url'] == msgDic['c_url']:
									self.mylogger.log("remove---myToken[{}]----> userDicToken---> {}".format(myToken, userDicToken))

									del userDicToken[ckToken]
									isChange = True
									userDicToken[myToken] = msgDic
									break

							if not isChange:
								userDicToken[myToken] = msgDic

						else:
							if myToken in userDicToken:
								del userDicToken[myToken]
							if myToken in connectedWSToken:
								del connectedWSToken[myToken]

						self.mylogger.log("complete to myStat[{}] about myToken[{}]----> userDicToken---> {}".format(msgDic['myStat'], myToken, userDicToken))
					else:
						self.mylogger.log("this is not my setting to myip[{}] message[{}]--------".format(MYIP, msgDic))
			except:
				self.mylogger.log("general error  {}".format(myutil.getErrLine(sys)))

							



if __name__ == '__main__':
	myServTp = ''
	try:
		myServTp = sys.argv[1].lower()
	except:
		pass

	# set userDic
	#userDic = {"skt":"1234", "kt":"234", "infomax_sys":"190.1.152.101"}
	#userDic = {"infomax_sys@systemmgr":{'passwd':"infomax_sysinfomax!@"}, "infomax_sys_cmd":{'passwd':"infomax_sysinfomax!@"}}
	userDic = {"infomax_sys@systemmgr":"", "infomax_sys_cmd@systemmgr":""}
	userDicToken = {"infomax_sys@systemmgr":"", "infomax_sys_cmd@systemmgr":""}


	serviceList = {}
	client_dns = {}
	with mydbapi.Mydb('mysql', 'myfeed',  myfeedmgr_db_host, _myPort=3306,_myUser='myfeed') as mycon:
		#sql = """select cdns.* 
		#	, case when cdns.c_proto = "REST_API_CLIENT" then 'restapi' else 'stream' end as type
		#	from myfeed.client_dns cdns where cdns.c_proto in('WS_CLIENT','REST_API_CLIENT')"""
		sql = """select
                                                                                case when cdns.c_proto = "REST_API_CLIENT" then 'restapi' else 'stream' end as type
                                                                                , cdns.*
                                                                                , wss.* 
                                                                                from client_dns cdns, ws_status wss
                                                                                where cdns.c_proto like '%_CLIENT' and wss.url like 'tcp://%'
                                                                                and cdns.c_host = wss.name
										and ip = '{}'""".format(MYIP)
		for item in mycon.exeQry("G", sql, useDict=True):
			client_dns[item['c_url']] = item
			userDicID[item['c_id']] = item
		#sql = """
		#	select concat(cdns.c_id, '@',cdns.c_passwd, '@', mycrt.c_name, '@', mycrt.c_job_num, '@', cdns.c_host) as token
		#	, case when cdns.c_proto = "REST_API_CLIENT" then 'restapi' else 'stream' end as type
		#	, cdns.c_url, cdns.c_id, cdns.c_passwd, cdns.connection, mycrt.c_name, mycrt.c_job_num, cdns.c_proto
		#	from client_dns cdns, mycrontab mycrt
		#	where cdns.c_proto in('WS_CLIENT','REST_API_CLIENT')
		#	and mycrt.c_url like concat('%', cdns.c_url, '%')
		#"""
		sql = """select 
			 mycrt.* 
			from mycrontab mycrt
			where mycrt.c_url <> '' and lower(mycrt.c_url) <> 'none'
			"""
		for item in mycon.exeQry("G", sql, useDict=True):
			item["c_job_num"] = str(item["c_job_num"])
			for ckURL in item['c_url'].replace("\n","").replace("\r","").split(","):
				setItem = item.copy()
				if ckURL in client_dns:
					client_dns[ckURL]
					urlINFO = client_dns[ckURL]
					myToken = '@'.join([urlINFO['c_id'], urlINFO['c_passwd'], item['c_name'], item['c_job_num'], urlINFO['c_host']])
					mylog.log("gotit!!!!!!!! {}----".format(urlINFO))
					setItem.update(urlINFO)
					setItem['c_url'] = ckURL
					if setItem['mgr_host'] != 'stream':
						setItem['type'] = 'restapi'

					userDicToken[myToken] = setItem

		

		mylog.log("{}".format(userDicToken))


		sql = """select ws.* 
			, ws_sub.name as p_name, ws_sub.ip as p_ip, ws_sub.port as p_port
			from ws_status ws
			left outer join ws_status ws_sub on ws.pipeline = ws_sub.name
			where ws.url like 'tcp://%' """
		for items in mycon.exeQry("G", sql, useDict=True):
			serviceList[items['name']] = items
			#{'ip':items['ip'], 'port':items['port'], 'url':items['url'], 'name':items['name']}


	if myServTp.upper() == 'DEV': #devlop
		servINFO = serviceList[myServTp.upper()]
	else:
		for k,v in serviceList.items():
			if k.upper() != 'DEV':
				if v['ip'] == socket.gethostbyname(socket.gethostname()):
					servINFO = serviceList[k]
					break

	if len(servINFO) == 0:
		mylog.log("{}".format("no service information"))
	else:	
		# start server
		socket_process = MyBroker_sys(name="myb_sys")
		socket_process.start()

		socket_process = MyBroker_mc(name="myb_mc", myAddr=[servINFO['p_ip'], servINFO['p_port']])
		socket_process.start()

		socket_process = MyBroker_recv(name="myb_recv")
		socket_process.start()

		mylog.log("start server!!!")
		mylog.log("{}".format(userDic))
		myutil.sms("상우", "start myfeedmgr_tcp server[{}]".format(servINFO))

		myTCPMain(servINFO['port'])
		myutil.rsleep(3,5)



		#try:
		#	asyncio.get_event_loop().run_forever()
		#except:
		#	mylog.info("{}".format(myutil.getErrLine(sys)))
		#finally:
		#	socket_process.join()
		#	mylog.info("stop server")
		#	myutil.sms("상우", "stop myfeedmgr_tcp server[{}]".format(servINFO))

