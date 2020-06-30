import redis
from logger import logger
from config import *
import re
import time

class RedisClient(object):
	def __init__(self):
		self.db1 = redis.StrictRedis(host=RHOST, port=RPORT, password=RPASSWORD, decode_responses= True)
		self.db2 = redis.StrictRedis(host=RHOST2, port=RPORT2, password=RPASSWORD2, decode_responses= True)
		self.key1 = 'users'
		self.key2 = 'lives'
		self.key3 = 'items'
		self.key4 = 'errors'
		self.key5 = 'workers'
		self.id = ID

	def add_users(self, user_id, status):
		try:
			self.db1.zadd(self.key1, {user_id: status})
		except TypeError as e:
			logger.error(self.id + '-添加主播失败-user_id-' + user_id)	

	def increase_user_status(self, user_id):
		status = 1
		self.db1.zadd(self.key1, {user_id: status})

	def decrease_user_status(self, user_id): 
		status = 0
		self.db1.zadd(self.key1, {user_id: status})

	def get_users(self, status):
		try:
			return self.db1.zrangebyscore(self.key1, status, status)
		except TypeError as e:
			logger.error(self.id + '-获取主播失败')	
			return []		

	def delete_users(self, user_id):
		try:
			self.db1.zrem(self.key1, user_id)
		except TypeError as e:
			logger.error(self.id + '-删除主播失败-user_id-' + user_id)			

	def add_new_users(self, user_id): #从直播栏抓到的之前没有的主播
		status = 0
		self.db1.zadd(self.key1, {user_id: status}, nx=True)

	def add_lives(self, live_tag):
		self.db1.sadd(self.key2, live_tag)

	def get_lives(self):
		try:
			return list(self.db1.smembers(self.key2))
		except TypeError as e:
			logger.error(self.id + '-live_quota获取失败')
			return []

	def delete_lives(self, live_tag):
		self.db1.srem(self.key2, live_tag)

	def add_items(self, item):
		self.db1.zadd(self.key3, {item.get('item'): item.get('create_time')})

	def get_items(self):
		try:
			#return self.db1.zrevrangebyscore(self.key3, float('inf'), float('-inf'))
			return self.db1.zrangebyscore(self.key3, float('-inf'), float('inf'))
		except TypeError as e:
			logger.error(self.id + '-item_quota获取失败-')
			return []

	def delete_items(self, item):
		self.db1.zrem(self.key3, item)

	def count_items(self): #这个在生产环境中还没有改
		try:
			return self.db1.zcount(self.key3, float("-inf"), float("inf"))
		except TypeError as e:
			logger.error(self.id)

	def get_work_batch(self, my_id):
		my_id_number = int(re.match('MT(\d+)', my_id).group(1))
		try:
			work_batch = 0
			workers = self.db2.smembers(self.key5)
			workers_number = len(workers)
			for worker in workers:
				his_id_number = int(re.match('MT(\d+)', worker).group(1))
				if his_id_number <= my_id_number:
					work_batch += 1
			if work_batch == workers_number:
				work_batch = 0
			return {'workers_number': workers_number, 'work_batch': work_batch}
		except:
			logger.error(self.id + '-get_work_batch出错')
			if my_id_number != 8:
				return {'workers_number': 8, 'work_batch': my_id_number}
			else:
				return {'workers_number': 8, 'work_batch': 0} #这部分改过了，还没部署

	def report_errors(self, code):
		try:
			code_reported = re.match('(\d)\d(\d+)', str(code)).group(1) + '0' + re.match('(\d)\d(\d+)', str(code)).group(2) #code_reported是错误报警过后的编号
			if not str(code)[0] in ['1', '2']: #表示该错误是反复连接失败和反复被拦截以外的错误，不管是否已经报警过，都不能多次存入redis
				if self.db2.zrangebyscore(self.key4, code, code) == [] and self.db2.zrangebyscore(self.key4,code_reported, code_reported) == []:
					self.db2.zadd(self.key4, {time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()): code})
			else: #表示该错误是反复连接失败或反复被拦截，允许多次存入redis，除非已经报警过
				if self.db2.zrangebyscore(self.key4,code_reported, code_reported) == []:
					self.db2.zadd(self.key4, {time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()): code})
		except:
			logger.error(self.id + '-report_errors出错，code-' + str(code))


if __name__ == '__main__':

	redis_client = RedisClient()
	print(redis_client.get_work_batch(redis_client.id))
	#a = redis_client.get_items()
	#for i in a:
	#	if int(i.split('_', 1)[0])%8 == 0:
	#		print(i)