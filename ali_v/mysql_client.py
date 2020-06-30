import pymysql
import aiomysql

from config import *

class MysqlClient():
	def __init__(self):
		self.pool = None
		self.mysql_client = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DATEBASE, charset='utf8mb4')
		self.cursor = self.mysql_client.cursor()

	async def connect_mysql(self, loop):
		if self.pool == None:
			self.pool = await aiomysql.create_pool(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DATEBASE, loop=loop, charset='utf8mb4', autocommit=True)
			#self.pool = await aiomysql.create_pool(host='115.28.139.51', port=30306, user='bxusr', password='bxdb@TT12', db='bxdb', loop=loop, charset='utf8mb4', autocommit=True)
			print('连接池已开启')
			return self.pool
		else:
			return self.pool

	async def into_mysql(self, loop, table, data):
		pool = await self.connect_mysql(loop)
		async with self.pool.acquire() as db:
			async with db.cursor() as cursor:
									
					keys = ','.join(data.keys())
					values = ','.join(['%s'] * len(data))	
					sql = 'insert into %s (%s) values (%s)' %(table, keys, values)
					try:
						await cursor.execute(sql, tuple(data.values()))
						await db.commit()
					except aiomysql.Error as e:
						if e.args[0] == 1062:
							print('入库失败，原因是重复入库', data['user_id'])
							await db.rollback()
						else:
							print('入库失败。 user_id：' + data['user_id'] + '\n' + str(e.args[0]) + ' ' + e.args[1])
							#print('直播入库失败。 live_id：', live_info['live_id'])
							#print(e.args)
							await db.rollback()

	def truncate_agencies(self):
		sql = 'truncate table agencies'
		self.cursor.execute(sql)

	def truncate_persons(self):
		sql = 'truncate table persons'
		self.cursor.execute(sql)