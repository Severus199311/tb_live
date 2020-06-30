import gevent
import gevent.monkey
gevent.monkey.patch_all()

import sys
import json
import time
import random
import re
import pymysql
import asyncio
import aiomysql

from get_raw_data import GetRawData
from redis_client import RedisClient
from mysql_client import MysqlClient
from logger import logger
from config import *

class GetItems():
	def __init__(self):
		self.redis_client = RedisClient()
		self.mysql_client = MysqlClient()
		self.get_raw_data = GetRawData()
		self.id = ID
		self.workers_number = self.redis_client.get_work_batch(self.id).get('workers_number') #这边get_work_batch不宜调两次
		self.work_batch = self.redis_client.get_work_batch(self.id).get('work_batch')
		self.item_info_list = []
		self.invalid_items = []
		self.connection_failures = 0
		self.data_failures = 0

	def get_items(self, item):
		id_list = item.split('_', 1)
		item_id = id_list[0]
		live_id = id_list[1]
		
		try:
			item_detail = self.get_raw_data.get_item_detail(item_id)
			if not item_detail:
				self.invalid_items.append(item)
		except Exception as e:
			item_detail = None
			logger.error(self.id + '-商品获取器连接失败-' + item_id)
			self.connection_failures += 1
		if item_detail:
			try:
				item_info = self.parse_item_detail(item_detail, item_id, live_id)
				self.data_failures = 0
				self.item_info_list.append(item_info)
			except Exception as e:
				logger.error(self.id + '-item_info解析失败-' + item_id)
				self.data_failures += 1
				self.invalid_items.append(item)

	def parse_item_detail(self, item_detail, item_id, live_id):
		item_info = {}

		seller = item_detail.get('data').get('seller')
		api_stack = item_detail.get('data').get('apiStack')
		api_stack_value = json.loads(api_stack[0].get('value'))

		item_info['item_id'] = item_id
		item_info['live_id'] = live_id
		item_info['price'] = api_stack_value.get('price').get('price').get('priceText')

		item_info['sales'] = api_stack_value.get('item').get('sellCount')
		if item_info.get('sales') == None:
			item_info['sales'] = api_stack_value.get('item').get('vagueSellCount')
		
		item_info['shop_id'] = seller.get('shopId')
		item_info['shop_fans'] = seller.get('fans')
		item_info['user_id'] = seller.get('userId')

		if item_info['user_id']:
			item_info['shop_url'] = 'https://shop.m.taobao.com/shop/shop_index.htm?user_id='+ item_info['user_id']
		
		if seller.get('shopIcon'):
			item_info['shop_img_url'] = 'http:' + seller.get('shopIcon')

		shop_name = seller.get('shopName')
		if shop_name:
			for each in ["'", "\"", "\n", "\\", "\t", "\b"]:
				if each in shop_name:
					shop_name = shop_name.replace(each, '/')
			item_info['shop_name'] = shop_name
		
		today = time.strftime('%Y-%m-%d', time.localtime())
		#item_info['date'] = today
		item_info['create_time'] = time.time()

		try:
			item_info['brand'] = item_detail.get('data').get('props').get('groupProps')[0].get('基本信息')[0].get('品牌')
		except Exception as e:
			pass

		#try:
		#	item_info['video_url'] = api_stack_value.get('item').get('videos')[0].get('url')
		#except Exception as e:
		#	pass
			
		return item_info

	def delete_items(self):
		for each in self.mysql_client.to_be_updated:
			self.redis_client.delete_items(each)
		for each in self.invalid_items:
			self.redis_client.delete_items(each)

	def get_item_quota(self):
		item_quota = []
		all_items = self.redis_client.get_items()
		for item in all_items:
			id_list = item.split('_', 1)
			item_id = id_list[0]
			if int(item_id)%self.workers_number == self.work_batch:
				item_quota.append(item)
		logger.info(self.id + '-待获取的商品数量-' + str(len(item_quota)) + '/' + str(len(all_items)))
		return item_quota

	def run(self):
		item_quota = self.get_item_quota()

		if len(item_quota) == 0:
			time.sleep(30)  #使用消息队列之后，这边要改

		else:
			logger.info('id-' + self.id + '-workers_number-' + str(self.workers_number) + '-work_batch-' + str(self.work_batch))		

			loop = asyncio.get_event_loop()
			task = loop.create_task(self.mysql_client.connect_mysql(loop))
			loop.run_until_complete(task)

			batch_size = 20
			batch_number = 0
			for batch_limit in range(0, len(item_quota), batch_size):
				start = batch_limit
				stop = min(batch_limit+batch_size, len(item_quota))
				logger.info(self.id + '-当前爬取item序号-' + str(start+1) + '-' + str(stop))
				tasks = [gevent.spawn(self.get_items, i) for i in item_quota[start:stop]]
				gevent.joinall(tasks)
				if self.connection_failures >= 100:
					logger.error(self.id + '-获取器连接失败已超过100次')
					self.redis_client.report_errors(int('215' + re.match('MT0(\d+)', self.id).group(1)))
					time.sleep(60)
					break
				if self.data_failures >= 50:
					logger.critical(self.id + '-item_info解析连续失败超过50次，有可能被封了')
					self.redis_client.report_errors(int('115' + re.match('MT0(\d+)', self.id).group(1)))
					time.sleep(60)
					break

				batch_number += 1
				if batch_number >= 10 or stop == len(item_quota):
					logger.info(self.id + '-以上' + str(batch_number) + '批获取到并准备入库的item数量-' + str(len(self.item_info_list)))
					tasks = [self.mysql_client.into_mysql_2(loop, i) for i in self.item_info_list]
					loop.run_until_complete(asyncio.wait(tasks))

					self.delete_items()
					self.item_info_list.clear()
					self.mysql_client.to_be_updated.clear()
					self.invalid_items.clear()
					batch_number = 0

				time.sleep(0.5)


if __name__ == '__main__':
	"""
	get_items = GetItems()
	a = get_items.get_item_quota()
	print(len(a))
	"""
	while True:
		get_items = GetItems()
		get_items.run()