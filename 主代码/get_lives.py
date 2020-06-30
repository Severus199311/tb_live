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

class GetLives():
	def __init__(self):
		self.get_raw_data = GetRawData()
		self.redis_client = RedisClient()
		self.mysql_client = MysqlClient()
		self.live_info_list_1 = []
		self.live_info_list_2 =[]
		self.item_list = []
		self.invalid_live_list = []
		self.connection_failures = 0
		self.data_failures = 0
		self.redis_failures_1 = 0
		self.redis_failures_2 = 0
		self.id = ID
		self.workers_number = self.redis_client.get_work_batch(self.id).get('workers_number')
		self.work_batch = self.redis_client.get_work_batch(self.id).get('work_batch')

	def get_lives_and_itemlists(self, live_tag):
		id_list = live_tag.split('_', 1)
		live_id = id_list[0]
		user_id = id_list[1]
		try:
			live_detail = self.get_raw_data.get_live_detail(live_id)
		except Exception as e:
			live_detail = None
			logger.error(self.id + '-直播获取器连接失败-' + live_tag)
			self.connection_failures += 1
		if live_detail: #这样，连接失败不会导致live_tag被判定为无效
			try:
				live_info = self.parse_live_detail(live_detail, live_id, user_id)
			except Exception as e:
				live_info = None
				logger.error(self.id + '-live_info解析失败，live_tag将被删除-' + live_tag)
				self.data_failures += 1
				self.invalid_live_list.append(live_tag)
			if live_info:
				cur_item = live_detail.get('data').get('curItemList') #streamStatus也能判断直播是否结束
				if not cur_item: #说明直播已经结束
					try:
						itemlist_detail = self.get_raw_data.get_itemlist_detail(user_id, live_id)
					except Exception as e:
						itemlist_detail = None
						logger.error(self.id + '-商品获取器连接失败-' + live_tag)
						self.connection_failures += 1
					if itemlist_detail: #这样，连接失败不会导致live_tag被判定为无效
						try:
							start_time = live_info.get('start_time')
							itemlist_info = self.parse_itemlist_detail(itemlist_detail, live_id, start_time)
						except Exception as e:
							itemlist_info = None
							logger.error(self.id + '-itemlist_info解析失败，直播结束的live_tag将被删除-' + live_tag)
							self.data_failures += 1
							self.invalid_live_list.append(live_tag)
						if itemlist_info:
							live_info['item_info'] = json.dumps(itemlist_info, ensure_ascii=False)
							self.live_info_list_2.append(live_info)
				else: #说明直播还在进行
					self.live_info_list_1.append(live_info)

	def parse_live_detail(self, live_detail, live_id, account_id):
		live_info = {}
		data = live_detail.get('data')
		broadcaster_info = data.get('broadCaster')
		if data.get('status') == '3': #status是3的话， 说明主播在开小差，删去直播
			self.invalid_live_list.append(live_id + '_' + account_id)
			return None
		else:
			live_info['live_id'] = live_id
			live_info['live_title'] = data.get('title')
			if data.get('nativeFeedDetailUrl'):
				live_info['live_url'] = 'https:' + data.get('nativeFeedDetailUrl')
			else:
				live_info['live_url'] = ''
			live_info['account_id'] = account_id
			live_info['account_name'] = broadcaster_info.get('accountName')
			if data.get('coverImg'):
				live_info['cover_img_url'] = 'http:' + data.get('coverImg')
			else:
				live_info['cover_img_url'] = ''
			live_info['total_join_count'] = data.get('totalJoinCount')
			live_info['view_count'] = data.get('viewCount')
			live_info['start_time'] = data.get('startTime')
			live_info['create_time'] = time.time()
			#live_info['date'] = time.strftime('%Y-%m-%d', time.localtime())	
			live_info['fans_number'] = broadcaster_info.get('fansNum')
			if broadcaster_info.get('jumpUrl'):
				live_info['shop_url'] = 'http:' + broadcaster_info.get('jumpUrl')
			if broadcaster_info.get('headImg'):
				live_info['head_img_url'] = 'http:' + broadcaster_info.get('headImg')
			return live_info

	def parse_itemlist_detail(self, itemlist_detail, live_id, start_time):
		all_items = []

		item_list = itemlist_detail.get('data').get('itemList')
		for each in item_list:
			item_info = {}
			goods_dict = each.get('goodsList')[0]
			item_id = goods_dict.get('itemId')

			if item_id:
				item_info['item_id'] = item_id
				item_info['item_index'] = each.get('goodsIndex')
				item_info['item_url'] = 'https://item.taobao.com/item.htm?ft=t&id=' + item_info['item_id']
				item_info['buy_count'] = goods_dict.get('buyCount')
				item_name = goods_dict.get('itemName')
				for each in ["'", "\"", "\n", "\\", "\t", "\b"]:
					if each in item_name:
						item_name = item_name.replace(each, "/")
				item_info['item_name'] = item_name
				item_info['item_price'] = goods_dict.get('itemPrice')
				item_info['live_id'] = live_id
				item_pic = goods_dict.get('itemPic')
				if item_pic:
					item_info['item_pic_url'] = 'http:' + item_pic
				else:
					item_info['item_pic_url'] =  ''
			
				extend_val = goods_dict['extendVal']
				item_info['category_level_leaf'] = extend_val.get('categoryLevelLeaf')
				item_info['category_level_leaf_name'] = extend_val.get('categoryLevelLeafName')
				item_info['category_level_one'] = extend_val.get('categoryLevelOne')
				item_info['category_level_one_name'] = extend_val.get('categoryLevelOneName')

				customized_item_rights = extend_val.get('customizedItemRights')
				if customized_item_rights:
					for each in ["'", "\"", "\n", "\\", "\t", "\b"]:
						if each in customized_item_rights:
							customized_item_rights = customized_item_rights.replace(each, "/")
				item_info['customized_item_rights'] = customized_item_rights
			
				all_items.append(item_info)

				if start_time != '0': 
					create_time = int(re.match( "(\d{10}).*", start_time).group(1))
				else: #直播结束一段时间后，start_time这个字段可能会消失，这种情况下，默认其为当前时间往回推3.5小时
					create_time = int(time.time()) - 60*60*3.5
				self.item_list.append({'item': item_id + '_' + live_id, 'create_time': create_time})

		return all_items

	def update_status(self): #如果在user中将状态从1改为0之前，就在lives中将记录删去，那么users中的状态将永远改不回来（因为user中状态为1的记录在改回0之前将永远不会被再次提取）
		for each in self.mysql_client.to_be_updated:
			live_tag = each.get('live_id') + '_' + each.get('user_id')
			try:
				self.redis_client.decrease_user_status(each.get('user_id')) 
				self.redis_client.delete_lives(live_tag)
				self.redis_failures_1 = 0
			except Exception as e:
				logger.error(self.id + '-恢复主播状态并删除直播失败-' + live_tag)
				self.redis_failures_1 += 1
				if self.redis_failures_1 >= 100:
					logger.critical(self.id + '-redis失败已连续超过100次！请检查redis连接')
					self.redis_client.report_errors(int('414' + re.match('MT0(\d+)', self.id).group(1)))
		for live_tag in self.invalid_live_list:
			user_id = live_tag.split('_', 1)[1]
			try:
				self.redis_client.decrease_user_status(user_id) 
				self.redis_client.delete_lives(live_tag)
				self.redis_failures_1 = 0
			except Exception as e:
				logger.error(self.id + '-恢复主播状态并删除直播失败-' + live_tag)
				self.redis_failures_1 += 1
				if self.redis_failures_1 >= 100:
					logger.critical(self.id + '-redis失败已连续超过100次！请检查redis连接')
					self.redis_client.report_errors(int('414' + re.match('MT0(\d+)', self.id).group(1)))

	def save_items(self):
		for each in self.item_list:
			try:
				self.redis_client.add_items(each)
				redis_failures_2 = 0
			except Exception as e:
				logger.error(self.id + '-商品存入redis失败-' + each.get('item'))
				redis_failures_2 += 1
				if redis_failures_2 >= 100:
					logger.critical(self.id + '-redis失败已连续超过100次！请检查redis连接')
					self.redis_client.report_errors(int('414' + re.match('MT0(\d+)', self.id).group(1)))

	def get_live_quota(self):
		live_quota = []
		failures = 0
		while True:
			all_lives = self.redis_client.get_lives()
			if len(all_lives) == 0:
				failures += 1
				if failures >= 30:
					logger.critical(self.id + '-连续30次从redis获取到正在进行的直播数量为0！')
					self.redis_client.report_errors(int('414' + re.match('MT0(\d+)', self.id).group(1)))
					break
			else: 
				break
		for live in all_lives:
			id_list = live.split('_', 1)
			live_id = id_list[0]
			if int(live_id)%self.workers_number == self.work_batch:
				live_quota.append(live)
		logger.info(self.id +'-正在直播的主播数量-' + str(len(live_quota)) + '/' + str(len(all_lives)))
		return live_quota

	def run(self):
		logger.info('id-' + self.id + '-workers_number-' + str(self.workers_number) + '-work_batch-' + str(self.work_batch))
		live_quota = self.get_live_quota()

		loop = asyncio.get_event_loop()
		task = loop.create_task(self.mysql_client.connect_mysql(loop))
		loop.run_until_complete(task)

		batch_size = 200
		for batch_limit in range(0, len(live_quota), batch_size):
			start = batch_limit
			stop = min(batch_limit+batch_size, len(live_quota))
			logger.info(self.id + '-当前爬取直播序号-' + str(start+1) + '-' + str(stop))
			tasks = [gevent.spawn(self.get_lives_and_itemlists, live_tag) for live_tag in live_quota[start:stop]]
			gevent.joinall(tasks)
			time.sleep(0.5)
			if self.connection_failures >= 100:
				logger.error(self.id + '-获取器连接失败已超过100次')
				#self.redis_client.report_errors(int('214' + re.match('MT0(\d+)', self.id).group(1)))
				break
			if self.data_failures >= 100:
				logger.critical(self.id + '-live_info解析失败超过100次，有可能被封了')
				self.redis_client.report_errors(int('114' + re.match('MT0(\d+)', self.id).group(1)))
				break

			live_info_list = self.live_info_list_1 + self.live_info_list_2
			logger.info('{}-当前入库直播数量-{}-正在直播-{}-已结束-{}, 另有无效直播-{}'.format(self.id, str(len(live_info_list)),str(len(self.live_info_list_1)),str(len(self.live_info_list_2)), str(len(self.invalid_live_list))))

			tasks = [self.mysql_client.into_mysql(loop, i) for i in live_info_list]
			loop.run_until_complete(asyncio.wait(tasks))

			self.update_status() #结束了的users中改为0，lives中删除
			self.save_items()

			self.live_info_list_1.clear()
			self.live_info_list_2.clear()
			self.item_list.clear()
			self.mysql_client.to_be_updated.clear()
			self.invalid_live_list.clear()
			self.mysql_client.failures = 0

if __name__ == '__main__':
	get_lives = GetLives()
	get_lives.run()