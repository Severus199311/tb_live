import redis
from config import *
import re
import time
import os
import requests
import json
from logger import logger

class Alarm():
	def __init__(self):
		self.db1 = redis.StrictRedis(host=RHOST, port=RPORT, password=RPASSWORD, decode_responses= True)
		self.db2 = redis.StrictRedis(host=RHOST2, port=RPORT2, password=RPASSWORD2, decode_responses= True)
		self.key1 = 'errors'
		self.key2 = 'workers'
		self.key3 = 'items'
		self.id = ID
		self.errors_dict = ERRORSDICT
		self.modules_dict = MODULESDICT
		self.workers_dict = WORKERSDICT
		self.data_errors = []
		self.connection_errors = []
		self.access_token = 'https://oapi.dingtalk.com/robot/send?access_token=b3fabc2f14dd149b964548409d5c3a62cda207fd1358df64fb3d2b8e0f92b6ba'
		self.contact = '15757120656'

	def other_errors(self): 
		all_codes = self.db2.zrangebyscore(self.key1, 11000, 61999, withscores=True)
		for each in all_codes:
			code = str(int(each[1]))
			status = code[1]
			if status == '1':
				error = code[0]
				if error == '1':
					self.data_errors.append(each)
				elif error == '2':
					self.connection_errors.append(each)
				else:
					module = code[2]
					worker = code[3] + code[4]
					message = '报警：错误代号-{}，错误类型-{}，报错机器-{}，报错模块-{}'.format(code, self.errors_dict.get(error), self.workers_dict.get(worker), self.modules_dict.get(module))
					logger.info(self.id + '-' + message)
					try:
						self.send_messages(message)
						self.db2.srem(self.key2, self.workers_dict.get(worker))
						self.change_error_status(each) #除连接失败以外的错误需要改变redis中的状态。连接失败不改变，只在报警后手动删除。报警后需要手动删除所有错误
						logger.info(self.id + '-已经报警，已经删除报错机器，已经将错误状态修改为已报警')
					except:
						logger.error(self.id + '-报警失败，或者删除报错机器失败，或者修改错误状态为已报警失败')


	def connection_and_data_errors(self): 
		error_list = self.data_errors + self.connection_errors
		error_dict = {}
		for error in error_list:
			error_dict[error[0]] = int(error[1])
		error_code_set = set(error_dict.values())
		for this_error_code in error_code_set:
			this_error_time_list = []
			for error_time, error_code in error_dict.items():
				if error_code == this_error_code:
					this_error_time_list.append(int(time.mktime(time.strptime(error_time,"%Y-%m-%d-%H-%M-%S"))))
			error_module = str(this_error_code)[2]
			if error_module == '5': #如果是get_items模块，就计算20次错误的产生时间，否则就2次
				tolerance = 20
			else:
				tolerance = 2
			if len(this_error_time_list) >= tolerance:
				this_error_time_list.sort()
				duration = this_error_time_list[-1] - this_error_time_list[-tolerance]
				if duration <= 90 * 60: #表示最近60分钟内发生连续失败的次数超过tolerance次
					error_worker = str(this_error_code)[3] + str(this_error_code)[4]
					error_type = str(this_error_code)[0]
					message = '报警：错误代号-{}，错误类型-{}，报错机器-{}，报错模块-{}'.format(str(this_error_code), self.errors_dict.get(error_type), self.workers_dict.get(error_worker), self.modules_dict.get(error_module))
					logger.info(self.id + '-' + message)

					try:
						self.send_messages(message)
						self.db2.srem(self.key2, self.workers_dict.get(error_worker))
						for error in error_list:
							if int(error[1]) == this_error_code:
								self.change_error_status(error)
						logger.info(self.id + '-已经报警，已经删除报错机器，已经将错误状态修改为已报警')
					except:
						logger.error(self.id + '-报警失败，或者删除报错机器失败，或者修改错误状态为已报警失败')

	def data_brimming_errors(self): 
		code = '51000'
		try:
			item_number = self.db1.zcount(self.key3, float("-inf"), float("inf"))
		except:
			logger.error(self.id + '-获取redis中items数量失败')
			item_number = None
		if item_number:
			if time.localtime().tm_hour in [21, 22, 23, 0, 1, 2, 3]:
				if item_number > 650000:
					logger.error('redis的items表数据溢出，现有items' + str(item_number))
					self.report_errors(code)
			else:
				if item_number > 300000:
					logger.error('redis的items表数据溢出，现有items' + str(item_number))
					self.report_errors(code)


	def no_script_errors(self): 
		path = os.path.split(os.path.abspath(__file__))[0]
		all_files = os.listdir(path)

		for file in ['check_users.py', 'config.py', 'get_items.py', 'get_lives.py', 'get_new_users.py', 'get_raw_data.py', 'logger.py', 'mysql_client.py', 'redis_client.py']:
			if file not in all_files:
				file_code = None
				for key, value in self.modules_dict.items():
					if value == file:
						file_code = key
						logger.error(self.id + '缺失脚本' + file)
						break
				worker = self.id[-2:]
				code = '61' + file_code + worker
				self.report_errors(code)


	def change_error_status(self, error):
		value = error[0]
		status = str(int(error[1]))
		status_list = list(status)
		status_list[1] = '0'
		new_score = int(''.join(status_list))
		self.db2.zadd(self.key1, {value: new_score})

	def send_messages(self, message):
		headers={'Content-Type': 'application/json'}
		data = {"msgtype": "text", "text": {"content": message}, "at": {"atMobiles": [self.contact],"isAtAll": False}}
		requests.post(self.access_token, data=json.dumps(data), headers=headers)

	def report_errors(self, code): #data_brimming_errors和no_script_errors需要调用这个方法去将错误记录于redis
		try:
			code_reported = re.match('(\d)\d(\d+)', str(code)).group(1) + '0' + re.match('(\d)\d(\d+)', str(code)).group(2) #code_reported是错误报警过后的编号
			if not str(code)[0] in ['1', '2']: #表示该错误是反复连接失败和反复被拦截以外的错误，不管是否已经报警过，都不能多次存入redis
				if self.db2.zrangebyscore(self.key1, code, code) == [] and self.db2.zrangebyscore(self.key1,code_reported, code_reported) == []:
					self.db2.zadd(self.key1, {time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()): code})
			else: #表示该错误是反复连接失败或反复被拦截，允许多次存入redis，除非已经报警过
				if self.db2.zrangebyscore(self.key1,code_reported, code_reported) == []:
					self.db2.zadd(self.key1, {time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()): code})
		except:
			logger.error(self.id + '-report_errors出错，code-' + str(code))

if __name__ == '__main__':
	alarm = Alarm()
	alarm.data_brimming_errors()
	alarm.no_script_errors()
	alarm.other_errors()
	alarm.connection_and_data_errors()
	logger.info('查看错误一次，若未报警，也未报警失败，一切正常。')