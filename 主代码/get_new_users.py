import gevent
import gevent.monkey
gevent.monkey.patch_all()

import sys
import time
import re
import json
import asyncio
import aiomysql

from get_raw_data import GetRawData
from redis_client import RedisClient
from mysql_client import MysqlClient
from logger import logger
from config import *

class GetNewUsers():
	def __init__(self):
		self.get_raw_data = GetRawData()
		self.redis_client = RedisClient()
		self.id = ID
		self.user_id_list = []
		self.connection_failures = 0
		self.data_failures = 0
		self.redis_failures = 0

	def get_channels(self):  
		channel_list = []
		try:
			homepage_detail = self.get_raw_data.get_homepage_detail()
			menu = homepage_detail.get('data').get('liveHomeMenuDatas')
			for each in menu:
				if each.get('channelId'):
					channel_list.append(each['channelId'])
		except Exception as e:
			logger.error(self.id + '-直播首页获取器连接失败')
			channel_list = ['0', '13', '60', '9', '69', '7', '29', '12', '65', '81', '8', '14', '49', '61', '11', '31']
		return channel_list

	def get_user_ids(self, channel):
		user_ids = 'Data Coming!'
		s = 0
		module_index = 0
		while user_ids != None: 
			try:
				channel_detail = self.get_raw_data.get_channel_detail(str(s), str(channel), str(module_index))
			except Exception as e:
				channel_detail = None
				logger.error(self.id + '-直播channel获取器连接失败')
				self.connection_failures += 1
			if channel_detail:
				#print(str(s), str(channel), str(module_index))
				user_ids = self.parse_channel_detail(channel_detail)
				if user_ids:
					for each in user_ids:
						self.user_id_list.append(each)
				else:
					if not s == 200:
						logger.error(self.id + '-user_ids解析失败。s,channel,module_index-' + str(s) + '-' + str(channel) + '-' + str(module_index))
						self.data_failures += 1
			s += 10
			module_index += 10

	def parse_channel_detail(self, channel_detail):
		try:
			live_list = channel_detail.get('data').get('dataList')
		except (AttributeError, TypeError) as e:
			live_list = None
		if live_list:
			account_ids = []
			for live in live_list:
				if live['type'] == 'feedList':
				#if live['type'] == 'feedList169WithItem':
					account_id = live.get('data').get('data').get('accountList')[0].get('accountId')
					account_ids.append(account_id)
			return account_ids
		else:
			return None

	def into_redis(self):
		if len(self.user_id_list) == 0:
			logger.critical(self.id + '-获取到的新主播人数为0，有可能被封了')
			self.redis_client.report_errors(int('112' + re.match('MT0(\d+)', self.id).group(1)))
		logger.info(self.id + '-共有主播-' + str(len(self.user_id_list)))
		for each in self.user_id_list:
			try:
				self.redis_client.add_new_users(each)
				self.redis_failures = 0
			except Exception as e:
				logger.error(self.id + '-添加主播失败-user_id-' + each)	
				self.redis_failures += 1
				if self.redis_failures >= 50:
					logger.critical(self.id + 'redis失败已连续超过50次！请检查redis连接')
					self.redis_client.report_errors(int('412' + re.match('MT0(\d+)', self.id).group(1)))


	def run(self):
		channel_list = self.get_channels()
		logger.info(self.id + '-准备寻找新的主播')

		tasks = [gevent.spawn(self.get_user_ids, i) for i in channel_list]
		gevent.joinall(tasks)

		if self.connection_failures >= 20:
			logger.error(self.id + '-获取器连接失败已超过20次，程序将退出')
			#self.redis_client.report_errors(int('212' + re.match('MT0(\d+)', self.id).group(1)))
			sys.exit()
		if self.data_failures >= 20:
			logger.critical(self.id + '-user_ids解析失败超过20次，有可能被封了，程序将退出')
			self.redis_client.report_errors(int('112' + re.match('MT0(\d+)', self.id).group(1)))
			sys.exit()

		self.into_redis()

		self.user_id_list.clear()
		self.connection_failures = 0
		self.data_failures = 0

if __name__ == '__main__':
	get_new_users = GetNewUsers()
	get_new_users.run()