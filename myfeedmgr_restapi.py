#-*-encoding:utf8-*-
import uvicorn
from fastapi import FastAPI, Request, Response
#from fastapi.requests import Request
from starlette.responses import JSONResponse
#from fastapi.encoders import jsonable_encoder
#from fastapi.responses import ORJSONResponse
from fastapi.responses import HTMLResponse


import logging
import logging.handlers
import os,sys,socket
sys.path.append("/home/infomax/svc/lib")
import mydbapi3 as mydbapi
import myutil3 as myutil
import json
import re
from decimal import *
import datetime
import websocket

#reload(sys)
#sys.setdefaultencoding("euckr")

# set the log
logger = logging.getLogger('myfeedmgr_restapi')
logger.setLevel(logging.INFO)
FMTER = logging.Formatter('%(asctime)s|%(message)s')
fileMaxByte = 1024 * 1024 * 110 #110MB
FH = logging.handlers.RotatingFileHandler(os.getcwd()+'/log/myfeedmgr_restapi.log', maxBytes=fileMaxByte, backupCount=10)
#FH.setLevel(logging.INFO)
FH.setFormatter(FMTER)
logger.addHandler(FH)

app = FastAPI()

class LoggerWriter:
	def __init__(self, logger, level):
		self.logger = logger
		self.level = level

	def write(self, msg):
		if msg != '\n':
			self.logger.log(self.level, msg)

@app.route('/{name}')
async def myapi(request: Request):
	myauth = request.path_params['name']
	logger.info("c_name-->{}".format(myauth))
	myparam= dict(request.query_params)

	resp = Response()
	logger.info("myparam--->{}".format(myparam))

	## default bad request
	isOK = True
	myReVal = "<html> you need "
	if 'c_name' not in myparam:
		myReVal += "c_name, "
		isOK = False
	if 'c_job_num' not in myparam:
		myReVal += "c_job_num, "
		isOK = False

	if not isOK:	
		myReVal +="for the service..</html>"
		resp.text = myReVal
	else:
		with mydbapi.Mydb("mysql", "myfeed", _myHost="ftp2", _myPort=3306, _myUser='myfeed') as mycon:
			sql = """select myc.*, cdns.c_id, cdns.c_passwd, pl.url, cdns.c_url
					from myfeed.mycrontab myc, myfeed.client_dns cdns, myfeed.ws_status wss, myfeed.ws_status pl
					where myc.c_name ='{c_name}' and myc.c_job_num = '{c_job_num}' and pl.ip = '{myip}'
					and myc.c_url like concat('%',cdns.c_url,'%') and cdns.c_host = wss.name and pl.name = wss.pipeline""".format(c_name=myparam['c_name'], c_job_num=myparam['c_job_num'], myip=socket.gethostbyname(socket.gethostname()))
			logger.info("{}".format(sql))
			items = mycon.exeQry("G1", sql, useDict=True, IS_PRINT=False)
			if items is not None and len(items) > 0:

				# check credencial
				logger.info("check--->myauth {} == {}".format(myauth, items['c_id']+items['c_passwd']))
				if myauth == items['c_id']+items['c_passwd']:
					logger.info("okgogo----->")
					# check qry_arg
					qry_arg = {}
					logger.info("check qry_arg")
					for k,v in myparam.items():
						if k != 'c_name' and k != 'c_job_num':
							logger.info("orrgin qry_arg----->{}------>{}".format(k,v))
							if v.strip() != '':
								v = '"'+v.strip('"')+'"'
								qry_arg[k] = v
								logger.info("after qry_arg----->{}------>{}".format(k,v))

					sendIdentify = {"id":items['c_id'],"passwd":items['c_passwd']}
					sendMsgDic = {"type":"restapi", "c_name":items['c_name'],"c_job_num":str(items['c_job_num']), "fromreq":items['c_url']}

					if len(qry_arg) > 0:
						sendMsgDic["qry_arg"] = qry_arg

					# ok gogo
					try:
						logger.info("connect {}".format(items['url']))
						#print(items['url'])
						#ws = websocket.create_connection(items['url']) 
						ws = websocket.WebSocket()
						ws.connect(items['url'], timeout=20) 
						logger.info("auth send --> ----->{}------>".format(str(sendIdentify)))
						ws.send(str(sendIdentify))
						myReVal = ws.recv()
						#myReVal = json.loads(myReVal.replace("'",'"'))
						myReVal = json.loads(myReVal)
						if myReVal["auth"] == "approved":
							
							logger.info("send --> ----->{}------>".format(str(sendMsgDic)))
							ws.send(str(sendMsgDic))
							myReVal = ws.recv()
							logger.info("ok recv --> ----->")
							#myReVal = json.loads(myReVal.replace("'",'"'))
							myReVal = json.loads(myReVal)
						logger.info("ok close--> ----->")
						#resp.content_type = 'application/json;charset=utf-8'
						#resp.json_body = myReVal 
						resp = JSONResponse(myReVal)
					except:
						print(myutil.getErrLine(sys))
						resp = HTMLResponse("<html> time out for client={c_name}, c_job_num={c_job_num} .</html>".format(c_name=items['c_name'], c_job_num=items['c_job_num']))
					finally:
						ws.close()
						
				else:
					myReVal = "<html> your authorization has not been passed for client={c_name}, c_job_num={c_job_num} ..</html>".format(c_name=items['c_name'], c_job_num=items['c_job_num'])
					resp = HTMLResponse(myReVal)
			else:
				# no schedule
				myReVal = "<html> your authorization has not been passed for client={c_name}, c_job_num={c_job_num} ..</html>".format(c_name=myparam['c_name'], c_job_num=myparam['c_job_num'])
				resp = HTMLResponse(myReVal)

	return resp

class DecimalEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, Decimal):
			return str(obj)

		elif isinstance(obj, datetime.date):
			return str(obj)

		return json.JSONEncoder.default(self, obj)


if __name__ == '__main__':
	global myPort
	global myServ	
	#default
	myPort="7900"
	#myPort=os.getenv("RA_PORT")
	
	myServ = None
	with mydbapi.Mydb("mysql", "myfeed", _myHost="ftp2", _myPort=3306, _myUser='myfeed') as mycon:
		sql = "select * from myfeed.ws_status where url like 'http://%' or url like 'https://%'"
		for item in  mycon.exeQry("G", sql, useDict=True):
			if item['ip'] == socket.gethostbyname(socket.gethostname()):
				myServ = item['url']
				myPort = item['port']
				#"https://myfeed.einfomax.co.kr/api1"
				break
	if myServ is not None:
		try:
			myPort = sys.argv[1]

			if myPort == '1234':
				myServ="https://myfeeddev.einfomax.co.kr"
			else:
				myServ = "http://"+socket.gethostbyname(socket.gethostname())+":"+myPort
			logger.info("myServ--{}----->".format(myServ))
		except:
			pass


	myutil.sms("상우", "start myfeedmgr_restapi server[{}]".format(myServ))

	uvicorn.run(app, host='0.0.0.0', port=int(myPort))
