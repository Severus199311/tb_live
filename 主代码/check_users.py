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

class CheckUsers():
	def __init__(self):
		self.get_raw_data = GetRawData()
		self.redis_client = RedisClient()
		self.mysql_client = MysqlClient()
		self.id = ID
		self.workers_number = self.redis_client.get_work_batch(self.id).get('workers_number')
		self.work_batch = self.redis_client.get_work_batch(self.id).get('work_batch')
		self.to_be_updated = []
		self.connection_failures = 0
		self.data_failures = 0
		self.redis_failures = 0
		
	def get_live(self, user_id):
		try:
			live_detail = self.get_raw_data.get_live_detail_2(user_id)
		except Exception as e:
			logger.error(self.id + '-直播获取器连接失败。 user_id-' + user_id)
			self.connection_failures += 1
			return None
		try:
			cur_item = live_detail.get('data').get('curItemList')
			status = live_detail.get('data').get('status')
		except Exception as e:
			logger.error(self.id + '-live_info解析失败。 user_id-' + user_id)
			self.data_failures += 1
			return None
		if status != '3' and cur_item and len(cur_item) != 0:
			live_id = live_detail.get('data').get('liveId')
			if live_id:
				self.to_be_updated.append({'live_id': live_id, 'user_id': user_id})

	def update_scores(self):
		for each in self.to_be_updated:
			live_tag = each.get('live_id') + '_' + each.get('user_id')
			try:
				self.redis_client.add_lives(live_tag)
				self.redis_client.increase_user_status(each.get('user_id'))
				self.redis_failures = 0
			except Exception as e:
				logger.error(self.id + '-添加直播并修改主播状态失败-' + e.args + live_tag)
				self.redis_failures += 1
				if self.redis_failures >= 100:
					logger.critical(self.id + '-redis失败已连续超过100次！请检查redis连接')
					self.redis_client.report_errors(int('413' + re.match('MT0(\d+)', self.id).group(1)))

	def get_user_quota(self):
		user_quota = []
		failures = 0
		while True:
			all_users = self.redis_client.get_users(0)
			if len(all_users) == 0:
				failures += 1
				if failures >= 30:
					logger.critical(self.id + '-连续30次从redis获取到未在直播的主播数量为0！')
					self.redis_client.report_errors(int('413' + re.match('MT0(\d+)', self.id).group(1)))
					break
			else:
				break
		for user in all_users:
			if int(user)%self.workers_number == self.work_batch:
				user_quota.append(user)
		logger.info(self.id + '-待查询的主播数量-' + str(len(user_quota)) + '/' + str(len(all_users)))
		return user_quota

	def run(self):
		logger.info('id-' + self.id + '-workers_number-' + str(self.workers_number) + '-work_batch-' + str(self.work_batch))
		user_quota = self.get_user_quota()

		batch_size = 200
		for batch_limit in range(0, len(user_quota), batch_size):
			start = batch_limit
			stop = min(batch_limit+batch_size, len(user_quota))
			logger.info(self.id + '-当前获取主播id序号' + str(start+1) + '-' + str(stop))
			tasks = [gevent.spawn(self.get_live, user_id) for user_id in user_quota[start:stop]]
			gevent.joinall(tasks)
			logger.info(self.id + '-新查询到开始直播的主播数量-' + str(len(self.to_be_updated)))
			self.update_scores()
			self.to_be_updated.clear()
			if self.connection_failures >= 100:
				logger.error(self.id + '-获取器连接失败已超过100次')
				#self.redis_client.report_errors(int('213' + re.match('MT0(\d+)', self.id).group(1)))
				break
			if self.data_failures >= 100:
				logger.critical(self.id + '-live_info解析失败超过100次，有可能被封了')
				self.redis_client.report_errors(int('113' + re.match('MT0(\d+)', self.id).group(1)))
				break

if __name__ == '__main__':
	check_users = CheckUsers()
	check_users.run()