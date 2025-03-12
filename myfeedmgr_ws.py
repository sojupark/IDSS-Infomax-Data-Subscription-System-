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

import concurrent

import logging
import logging.handlers

import mydbapi3 as mydbapi
import myutil3 as myutil

servINFO = {}
connectedID = {}
connectedWS = {}
connectedWSToken = {}
userDicID = {}
userDicToken = {}
MYHOME = os.getcwd()

#set the log
logger = myutil.MyLogger('myfeedmgr_ws', MYHOME+'/log/', mySizeMB=200, myCount=10)

myEncoding=os.environ.get("LANG").split('.')[1].lower()
MYIP = socket.gethostbyname(socket.gethostname())

socket_process = None
myfeedmgr_db_host = 'ftp2'
if socket.gethostname().lower() == myfeedmgr_db_host:
	myfeedmgr_db_host = 'localhost'


def signal_term_handler(signal, frame):
	socket_process.join()
	logger.log("stop server")
	myutil.sms("상우", "stop myfeedmgr_ws server[{}]".format(servINFO))
	#sys.exit(0)

signal.signal(signal.SIGTERM, signal_term_handler)


class multiWSServer():
	def __init__(self, *args, **kwargs):
		self.servINFO = kwargs['servINFO']
		del kwargs['servINFO']
		super().__init__(*args, **kwargs)
		self._send_queue = asyncio.Queue()
		# database pool
		self.mypool = []
		for i in range(10):
			self.mypool.append({'mycon':self.getDBCon(), 'avail':True})
			self.mypool[i]['mycon'].exeQry("G","set wait_timeout = 9999999")

	def __exit__(self):
		for item in self.mypool:
			item['mycon'].close()

	def run(self):
		asyncio.ensure_future(self.task_consumer())
		#asyncio.run(self.main())# >= python 3.8
		asyncio.get_event_loop().run_until_complete(self.main())

	def getDBCon(self):
		return mydbapi.Mydb('mysql', 'myfeed',  myfeedmgr_db_host, _myPort=3306,_myUser='myfeed') 

	async def acquireCon(self):
		getit = False
		while not getit:
			for item in self.mypool: 
				if item['avail']:
					try:
						item['mycon'].exeQry("G", "select 1")
					except:
						logger.log("acquireCon---{}".format(myutil.getErrLine(sys)))
						if item['mycon']:
							item['mycon'].close()
						item['mycon'] = self.getDBCon()

					item['avail'] = False
					getit = True
					break
		logger.log("acquireCon get connection{}".format(item['mycon']))
		return item['mycon']

	async def releaseCon(self, mycon):
		for item in self.mypool:
			if item['mycon'] == mycon:
				item['avail'] = True
				break
		logger.log("releaseCon connection{}".format(mycon))

	def send(self, item):
		self._send_queue.put(item)

	async def cmd2mgr(self, reqUser):
		items = None
		mycon = await self.acquireCon()
		items = mycon.exeQry("G1", "select t.mgr_host, i.ip, i.dir from myfeed.mycrontab t, myfeed.mgr_host i where t.job_status = 'RUN' and t.c_name = '{}' and t.c_job_num ='{}' and t.mgr_host = i.hostname".format(reqUser['c_name'], reqUser['c_job_num']), useDict=True)
		await self.releaseCon(mycon)

		logger.log("cmd2mgr------------------->")
		if items is not None and len(items) > 0:
			cmdList = ["-j"]
			cmdList.append(reqUser['c_name'])
			cmdList.append(reqUser['c_job_num'])

			# use where restapi is
			if 'fromreq' in reqUser:
				cmdList.append('-fromreq')
				cmdList.append(reqUser['fromreq'])

			if 'qry_arg' in reqUser:
				cmdList.append('-qry_arg')
				qry_arg = ''
				for k,v in reqUser['qry_arg'].items():
					qry_arg += k+"="+v+","
				cmdList.append(qry_arg.strip(", "))

			cmdList.append("-send")
			start_proc = None
			if socket.gethostname().lower() == items['mgr_host']:
				logger.log("python3 myfeedmgr3.py {}".format(cmdList))
				start_proc = await asyncio.create_subprocess_exec("python3", "myfeedmgr3.py", *cmdList, cwd=MYHOME, env=os.environ, shell=False)
			else:
				cmdList = [items['mgr_host'], items['dir']]+cmdList
				logger.log("cmd_remote_mgr.sh {}".format(cmdList))
				start_proc = await asyncio.create_subprocess_exec("/bin/sh", "cmd_remote_mgr.sh", *cmdList, cwd=MYHOME, env=os.environ, shell=False)
			await start_proc.wait()
			logger.log("finish client_command")
		else:
			logger.log("error command is not there client_command {} - {}".format(reqUser['c_name'], reqUser['c_job_num']))



	# auth and request from a client
	async def consumer_handler(self, websocket, path):
		try:
			async for message in websocket:
				logger.log("Consumer_Received message---[{}]-- [{}]".format(self._send_queue.qsize(), message))
				try:
					reqUser = ast.literal_eval(message)

					if websocket in connectedWS:
						# treat client's request 
						myToken = connectedWS[websocket]["token"]

						# system
						if myToken in ['infomax_sys@systemmgr','infomax_sys_cmd@systemmgr']:	
							await self._send_queue.put(message)
						else:
							#myid = ""
							if myToken == "": # check subscription 
								#myid = connectedWS[websocket]["myid"]
								if "c_name" in reqUser and "c_job_num" in reqUser:
									okGO = False
									if 'type' in reqUser: # only restapi
										for ckToken, v in userDicToken.items():
											#print(ckToken, v, reqUser)
											logger.log("check---->{}-{}---> reqUser[{}]".format(ckToken, v, reqUser))
											if 'c_name' in v and 'c_job_num' in v and 'type' in v and v['c_name'] == reqUser['c_name'] and v['c_job_num'] == reqUser['c_job_num'] and v['type'] == reqUser['type']:
												connectedWS[websocket]["token"] = ckToken
												if ckToken in connectedWSToken:
													if websocket not in connectedWSToken[ckToken]:
														connectedWSToken[ckToken].append(websocket)
												else:
													connectedWSToken[ckToken] = [websocket]
												logger.log("ok ---> connectedWS [{}]".format(connectedWS[websocket]))
												okGO = True
												break
										if okGO:
											await self.cmd2mgr(reqUser)
										else:
											await websocket.send(json.dumps({"subscription":"not allowed"}))
									else:
										# check stream or restapi style
										isUrgent = False
										fromreq = ''
										#print("check !!!!!!", reqUser)
										for ckToken, v in userDicToken.items():
											#print("check api!!!!!!", ckToken, v)
											#if 'c_name' in v and 'c_job_num' in v and 'type' in v and v['c_name'] == reqUser['c_name'] and v['c_job_num'] == reqUser['c_job_num'] and v['type'] == "stream" and self.servINFO['name'] == v['c_host']:
											#if 'c_name' in v and 'c_job_num' in v and 'type' in v and v['c_name'] == reqUser['c_name'] and v['c_job_num'] == reqUser['c_job_num'] and self.servINFO['name'] == v['c_host']:
											if 'c_name' in v and 'c_job_num' in v and 'type' in v and v['c_name'] == reqUser['c_name'] and v['c_job_num'] == reqUser['c_job_num'] and self.servINFO['name'] == v['c_host']:
												try:
													if (v['type'] != 'stream' or v['mgr_host'] != 'stream') and v['job_status'] != 'STOP':
														fromreq = v['c_url']
														isUrgent = True
			
													connectedWS[websocket]["token"] = ckToken
													if ckToken in connectedWSToken:
														if websocket not in connectedWSToken[ckToken]:
															connectedWSToken[ckToken].append(websocket)
													else:
														connectedWSToken[ckToken] = [websocket]
													if v['job_status'] != 'STOP':
														okGO = True
														break
												except:
													logger.log("{}--->{}".format(myutil.getErrLine(sys), v))
										if okGO:
											await websocket.send(json.dumps({"subscription":"ready to receive.."}))
											if isUrgent:
												#print("urgent", fromreq)
												logger.log("isUrgent!!!!!!")
												reqUser['fromreq'] = fromreq
												await self.cmd2mgr(reqUser)
										else:
											await websocket.send(json.dumps({"subscription":"not allowed"}))
								else:
									await websocket.send(json.dumps({"subscription":"not allowed"}))
							else:
								if myToken in connectedWSToken and myToken not in ['infomax_sys@systemmgr','infomax_sys_cmd@systemmgr']:	
									if websocket not in connectedWSToken[myToken]:
										print("here is!!!!!!!!")
										connectedWSToken[myToken].append(websocket)
								else:
									connectedWSToken[myToken] = [websocket]
					else:
						# at first
						if "token" in reqUser and reqUser["token"] in ['infomax_sys@systemmgr','infomax_sys_cmd@systemmgr']: 
							# system msg 
							await self._send_queue.put(message)
						else:
							myid = reqUser['id']
							mypasswd = reqUser['passwd']

							# authticate to general client identity at first connection
							checkID = False
							if myid in userDicID:
								if userDicID[myid]['c_passwd'] == mypasswd:
									checkID = True
							
							print("myid[{}] mypasswd[{}]-- checkID {}".format(myid, mypasswd,  checkID))		
							logger.log("myid[{}] mypasswd[{}] --- checkID -->--- {}".format(myid, mypasswd, checkID))
							authStr = "not allowed"
							if checkID:
								authStr = "approved"
								if myid in connectedID:
									logger.log("id[{}]---limit[{}]---size[{}] is connectionlist--[{}]".format(myid, userDicID[myid]['connection'], len(connectedID[myid]), connectedID[myid]))
									if int(userDicID[myid]['connection']) < len(connectedID[myid])+1:
										logger.log("not allowed message, socket id[{}][{}]---limit[{}]---size[{}] is".format(myid, websocket.remote_address, userDicID[myid]['connection'], len(connectedID[myid])))
										authStr = "not allowed"
										# check real connection list
										for myconnection in connectedID[myid]:
											logger.log("check alive connection for id[{}]-socket[{}]-!!!".format(myid, myconnection))
											if myconnection.closed:
												connectedID[myid].remove(myconnection)
												logger.log("remove id[{}]-socket[{}]-!!!, now limit[{}]-size[{}] is".format(myid, myconnection, userDicID[myid]['connection'], len(connectedID[myid])))
												await self.remove_websocket_references(myconnection)
										# recheck
										if int(userDicID[myid]['connection']) < len(connectedID[myid])+1:
											logger.log("still id[{}], limit[{}]-size[{}] is".format(myid, userDicID[myid]['connection'], len(connectedID[myid])))
											authStr = "there have been reached your limit [{}] connections!!".format(len(connectedID[myid]))
										else:
											authStr = "approved"
								

							await websocket.send(json.dumps({"auth":authStr}))
							if authStr == 'approved':
								if myid in connectedID:
									connectedID[myid].append(websocket)
								else:
									connectedID[myid] = [websocket]
								connectedWS[websocket] = {"myid":myid, "token":""}
								logger.log("new connection [{}][{}]".format(message, websocket.remote_address))
							else:
								await websocket.close()
								await self.remove_websocket_references(websocket)

								if 'id' in reqUser:
									logger.log("id[{}][{}] not allowed message[{}], socket is closed!".format(myid, websocket.remote_address, message))
								else:
									logger.log("no id[{}] not allowed message[{}], socket is closed!".format(websocket.remote_address, message))
				except:
					logger.log("{}, error not allowed message[{}], socket is closed!".format(myutil.getErrLine(sys), message))
					try:
						await websocket.send(json.dumps({"auth":"not allowed"}))
					except:
						pass
					try:
						await websocket.close()
					except:
						pass

					try:
						await self.remove_websocket_references(websocket)
					except:
						pass
		except (ConnectionClosedError, ConnectionClosedOK):
			await self.remove_websocket_references(websocket)

	# system message to a client
	async def task_consumer(self):
		while True:
			#message = await asyncio.get_event_loop().run_in_executor(None, self._send_queue.get)
			message = await self._send_queue.get()
			logger.log("ready to sending message---[{}]--[{}]".format(self._send_queue.qsize(), message))
			#msgDic = ast.literal_eval(message)
			msgDic = json.loads(json.dumps(ast.literal_eval(message)))
			myToken = msgDic['token']	
			myid = ''
			try:
				# system command
				if myToken == 'infomax_sys_cmd@systemmgr':
					if 'client_dns' in msgDic:
						msgDic = msgDic['client_dns']
						myid = msgDic['c_id']
						if 'ip' in msgDic and msgDic['ip'] == MYIP:
							if msgDic['myStat'] == 'I':
								userDicID[myid] = msgDic
							else:
								if myid in userDicID:
									del userDicID[myid]
							logger.log("complete to myStat[{}] about myid[{}]----> userDicID---> {}".format(msgDic['myStat'], myid, userDicID))
						else:
							logger.log("this is not my setting to myip[{}] message[{}]--------".format(MYIP, msgDic))

					elif 'mycrontab' in msgDic:
						msgDic = msgDic['mycrontab']
						mcToken = msgDic['token']
						if 'ip' in msgDic and msgDic['ip'] == MYIP:
							if msgDic['myStat'] == 'I':
								# check the client_DNS name
								isChange = False
								for ckToken in list(x for x in userDicToken.keys() if x not in ['infomax_sys@systemmgr','infomax_sys_cmd@systemmgr']):
									if userDicToken[ckToken]['c_url'] == msgDic['c_url']:
										del userDicToken[ckToken]
										isChange = True
										userDicToken[mcToken] = msgDic
										break

								if not isChange:
									userDicToken[mcToken] = msgDic
							else:
								if mcToken in userDicToken:
									del userDicToken[mcToken]
								if mcToken in connectedWSToken:
									del connectedWSToken[mcToken]

							logger.log("complete to myStat[{}] about myToken[{}]----> userDicToken---> {}".format(msgDic['myStat'], mcToken, userDicToken))
						else:
							logger.log("this is not my setting to myip[{}] message[{}]--------".format(MYIP, msgDic))
							
				else:
					for ckToken in msgDic['to'].split(","):
						logger.log("try to [{}]----{}-----this list----connectedWSToken[{}]".format(ckToken, message,connectedWSToken.keys()))
						if ckToken in connectedWSToken:
							myid = userDicToken[ckToken]['c_id']
							for my_websocket in list(connectedWSToken[ckToken]):
								logger.log("--------->sending to {}-{} message[{}]---".format(ckToken, my_websocket, message))
								try:
									#await my_websocket.send(json.dumps(ast.literal_eval(msgDic['data'])))
									await my_websocket.send(json.dumps(ast.literal_eval(msgDic['data'])))

									if ckToken.find("@API") != -1:
										await self.remove_websocket_references(my_websocket)
								except (ConnectionClosedError,ConnectionClosedOK):
									await self.remove_websocket_references(my_websocket)
								except:
									logger.log("producer haandler error ----------------------for loop{}".format(myutil.getErrLine(sys)))
			except:
				logger.log("{} - general error  {}".format(myToken, myutil.getErrLine(sys)))



	# clean shared dict
	async def remove_websocket_references(self, websocket):
		if websocket in connectedWS:
			myToken = connectedWS[websocket]['token']
			myid = connectedWS[websocket]['myid']
			del connectedWS[websocket]

			if myid in connectedID and websocket in connectedID[myid]:
				connectedID[myid].remove(websocket)
				if len(connectedID[myid]) == 0:
					del connectedID[myid]
					logger.log("remove_websocket_references ---- connectedID[{}] is removed!!!".format(myid))
				else:
					logger.log("remove_websocket_references ---- connectedID[{}] is {}!!!".format(myid, connectedID[myid]))

			if myToken and myToken in connectedWSToken and websocket in connectedWSToken[myToken]:
				connectedWSToken[myToken].remove(websocket)
				if len(connectedWSToken[myToken]) == 0:
					del connectedWSToken[myToken]
					logger.log("remove_websocket_references ---- connectedWSToken[{}] is removed!!!".format(myToken))
				else:
					logger.log("remove_websocket_references ---- connectedWSToken[{}] is {}!!!".format(myToken, connectedWSToken[myToken]))


	async def del_dict(self, mydict, key):
		await asyncio.sleep(0)
		try:
			del mydict[key]
		except:
			pass

	async def del_list(self, mylist, key):
		await asyncio.sleep(0)
		try:
			mylist.remove(key)
		except:
			pass

	# system message to a client
#	async def producer_handler(self, websocket):
#		while True:
#			message = await asyncio.get_event_loop().run_in_executor(None, self._send_queue.get)
#			logger.log("ready {} to sending message---[{}]--[{}]".format(websocket, self._send_queue.qsize(), message))
#			msgDic = ast.literal_eval(message)
#			myToken = msgDic['token']	
#			myid = ''
#			try:
#				# system command
#				if myToken == 'infomax_sys_cmd@systemmgr':
#					if 'client_dns' in msgDic:
#						cmd = ast.literal_eval(msgDic['client_dns'])
#						myid = cmd['c_id']
#						if 'ip' in cmd and cmd['ip'] == MYIP:
#							if cmd['myStat'] == 'I':
#								userDicID[myid] = cmd
#							else:
#								if myid in userDicID:
#									del userDicID[myid]
#							logger.log("complete to myStat[{}] about myid[{}]----> userDicID---> {}".format(cmd['myStat'], myid, userDicID))
#						else:
#							logger.log("this is not my setting to myip[{}] message[{}]--------".format(MYIP, cmd))
#					elif 'mycrontab' in msgDic:
#						cmd = ast.literal_eval(msgDic['mycrontab'])
#						if 'ip' in cmd and cmd['ip'] == MYIP:
#							myToken = cmd['token']
#							print(cmd, myToken)
#							if cmd['myStat'] == 'I':
#								# check the client_DNS name
#								isChange = False
#								for ckToken in list(x for x in userDicToken.keys() if x not in ['infomax_sys@systemmgr','infomax_sys_cmd@systemmgr']):
#									if userDicToken[ckToken]['c_url'] == cmd['c_url']:
#										del userDicToken[ckToken]
#										isChange = True
#										userDicToken[myToken] = cmd
#										break
#
#								if not isChange:
#									userDicToken[myToken] = cmd
#							else:
#								await self.remove_websocket_references(websocket)
#
#							logger.log("complete to myStat[{}] about myToken[{}]----> userDicToken---> {}".format(cmd['myStat'], myToken, userDicToken))
#						else:
#							logger.log("this is not my setting to myip[{}] message[{}]--------".format(MYIP, msgDic))
#							
#				else:
#					for ckToken in msgDic['to'].split(","):
#						logger.log("try to [{}]----{}-----this list----connectedWSToken[{}]".format(ckToken, message,connectedWSToken.keys()))
#						if ckToken in connectedWSToken:
#							myid = userDicToken[ckToken]['c_id']
#							for my_websocket in connectedWSToken[ckToken].copy():
#								logger.log("--------->sending to {}-{} message[{}]---".format(ckToken, my_websocket, message))
#								try:
#									await my_websocket.send(json.dumps(ast.literal_eval(msgDic['data'])))
#								except (ConnectionClosedError,ConnectionClosedOK):
#									await self.remove_websocket_references(my_websocket)
#								except:
#									logger.log("producer haandler error ----------------------for loop{}".format(myutil.getErrLine(sys)))
#
#			except (ConnectionClosedError,ConnectionClosedOK):
#				await self.remove_websocket_references(websocket)
#			except:
#				logger.log("{} - general error  {}".format(myToken, myutil.getErrLine(sys)))
#
#
#
#	async def handler(self, websocket, path):
#		await asyncio.gather(
#			self.consumer_handler(websocket),
#			self.producer_handler(websocket)
#		)


	async def main(self):
		# start the server
		logger.log("{}---{}".format(self.servINFO['ip'], self.servINFO['port']))
		#async with serve(self.handler, self.servINFO['ip'], self.servINFO['port'], max_size=None, max_queue=32) as server:
		async with serve(self.consumer_handler, self.servINFO['ip'], self.servINFO['port'], max_size=None, max_queue=32) as server:
			#await server.wait_closed()
			await asyncio.Future()


if __name__ == "__main__":

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
                                                                                where cdns.c_proto like '%_CLIENT' and (wss.url like 'ws%' or wss.url like 'http%')
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
					logger.log("gotit!!!!!!!! {}----".format(urlINFO))
					setItem.update(urlINFO)
					setItem['c_url'] = ckURL
					if setItem['mgr_host'] != 'stream':
						setItem['type'] = 'restapi'

					userDicToken[myToken] = setItem

		

		logger.log("{}".format(userDicToken))


		for items in mycon.exeQry("G", "select * from ws_status where (url like 'ws://%' or url like 'wss://%') and url not like 'ws://%broker%'", useDict=True):
			serviceList[items['name']] = {'ip':items['ip'], 'port':items['port'], 'url':items['url'], 'name':items['name']}


	print(userDicID)
	if myServTp.upper() == 'DEV': #devlop
		servINFO = serviceList[myServTp.upper()]
	else:
		for k,v in serviceList.items():
			if k.upper() != 'DEV':
				if v['ip'] == socket.gethostbyname(socket.gethostname()):
					servINFO = serviceList[k]
					break

	if len(servINFO) == 0:
		logger.log("{}".format("no service information"))
	else:	
		# start websocket server
		socket_process = multiWSServer(servINFO=servINFO)
		socket_process.run()

		myutil.rsleep(3,5)

		logger.log("start server!!!")
		logger.log("{}".format(userDic))
		myutil.sms("상우", "start myfeedmgr_ws server[{}]".format(servINFO))


		try:
			#asyncio.get_event_loop().run_forever()
			pass
		except:
			logger.log("{}".format(myutil.getErrLine(sys)))
		finally:
			socket_process.join()
			logger.log("stop server")
			myutil.sms("상우", "stop myfeedmgr_ws server[{}]".format(servINFO))

