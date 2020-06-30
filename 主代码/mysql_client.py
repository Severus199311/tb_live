import time
import pymysql
import aiomysql
from logger import logger
from redis_client import RedisClient
from config import *
import re

class MysqlClient():
	def __init__(self):
		self.pool = None
		self.to_be_updated = [] #这个专门给get_lives_and_items模块用了。已结束的直播入库后，方可改变其在redis中的记录
		self.failures = 0
		self.id = ID
		self.redis_client = RedisClient()

	async def connect_mysql(self, loop):
		if self.pool == None:
			connection_failures = 0
			while connection_failures < 30:
				try:
					self.pool = await aiomysql.create_pool(host=MHOST, port=MPORT, user=MUSER, password=MPASSWORD, db=MDB, loop=loop, charset='utf8mb4', autocommit=True)
					#logger.info(self.id + '-连接池已开启')
					return self.pool
				except pymysql.err.OperationalError:
					logger.error(self.id + '-连接池开启失败')
					connection_failures += 1
			if connection_failures >= 30:
				logger.critical(self.id + '-连接池开启失败超过30次！请检查mysql连接')
				self.redis_client.report_errors(int('316' + re.match('MT0(\d+)', self.id).group(1)))
				return self.pool
		else:
			return self.pool

	async def into_mysql(self, loop, live_info): #这个是直播入库使用的，into_mysql_2是商品入库使用的
		pool = await self.connect_mysql(loop)
		async with self.pool.acquire() as db:
			async with db.cursor() as cursor:

				live_info['date'] = time.strftime('%Y-%m-%d', time.localtime())
									
				live_id = live_info.get('live_id')
				user_id = live_info.get('account_id')

				keys = ','.join(live_info.keys())
				values = ','.join(['%s'] * len(live_info))	
				sql = 'insert into %s (%s) values (%s)' %('tb_live_lives', keys, values)

				try:
					await cursor.execute(sql, tuple(live_info.values()))
					await db.commit()
					self.failures = 0
					if live_info.get('item_info'): #这个说明直播结束了
						self.to_be_updated.append({'live_id': live_id, 'user_id': user_id})
				except aiomysql.Error as e:
					logger.error(self.id + '-直播入库失败。 user_id和live_id-' + user_id + '-' + live_id + '\n' + str(e.args[0]) + '-' + e.args[1])
					self.failures += 1
					if self.failures >= 100:
						logger.critical(self.id + '-连续入库失败已超过100次！请检查mysql连接')
						self.redis_client.report_errors(int('316' + re.match('MT0(\d+)', self.id).group(1)))
				except AttributeError as f:
					logger.error(self.id + '-直播入库失败。 user_id和live_id-' + user_id + '-' + live_id)

	async def into_mysql_2(self, loop, item_info):
		pool = await self.connect_mysql(loop)
		async with self.pool.acquire() as db:
			async with db.cursor() as cursor:

				today = time.strftime('%Y-%m-%d', time.localtime())
				item_info['date'] = today

				item_id = item_info['item_id']
				live_id = item_info['live_id']
				item = item_id + '_' + live_id
									
				keys = ','.join(item_info.keys())
				values = ','.join(['%s'] * len(item_info))	
				sql = 'insert into %s (%s) values (%s)' %('tb_live_items', keys, values)

				try:
					await cursor.execute(sql, tuple(item_info.values()))
					await db.commit()
					self.failures = 0
					self.to_be_updated.append(item)
				except aiomysql.Error as e:
					logger.error(self.id + '-商品入库失败。 item-' + item + '\n' + str(e.args[0]) + '-' + e.args[1])
					if e.args[0] == 1406: #data too long的话，就删除这个item（不过1406错误也可能是编码造成的，这点要注意）
						self.to_be_updated.append(item)
					else:
						self.failures += 1
						if self.failures >= 100:
							logger.critical(self.id + '-连续入库失败已超过100次！请检查数据库连接')
							self.redis_client.report_errors(int('316' + re.match('MT0(\d+)', self.id).group(1)))
				except AttributeError as f:
					logger.error(self.id + '-商品入库失败。 是AttributeError，item-' + item)