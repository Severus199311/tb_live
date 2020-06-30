import gevent
import gevent.monkey
gevent.monkey.patch_all()

import requests
from urllib.parse import urlencode, urljoin
from multiprocessing import Pool
import json
import os
import pymysql
import asyncio
import aiomysql
import time

from config import *
from agencies import AgencyCrawler
from persons import PersonCrawler
from mysql_client import MysqlClient

def run():
	agency_crawler = AgencyCrawler()
	person_crawler = PersonCrawler()
	mysql_client = MysqlClient()

	batch_size = 200

	loop = asyncio.get_event_loop()
	task = loop.create_task(mysql_client.connect_mysql(loop))
	loop.run_until_complete(task)


	"""
	print('准备获取机构')
	agency_crawler.run()

	mysql_client.truncate_agencies()
	print('删除旧有机构成功')

	print('准备将机构入库')
	for batch_limit in range(0, len(agency_crawler.user_info_list), batch_size):
		start = batch_limit
		stop = min(batch_limit+batch_size, len(agency_crawler.user_info_list))
		print('当前入库机构序号：' + str(start+1) + ' ' + str(stop))
		tasks = [mysql_client.into_mysql(loop, 'agencies', i) for i in agency_crawler.user_info_list[start:stop]]
		loop.run_until_complete(asyncio.wait(tasks))
		time.sleep(0.5)
	"""


	print('准备获取主播')
	person_crawler.run()

	mysql_client.truncate_persons() #如果每次都只能获取1000多个，那么就不删除了。但那也就没法更新了。
	print('删除旧有主播成功')

	print('准备将主播入库')
	for batch_limit in range(0, len(person_crawler.user_info_list), batch_size):
		start = batch_limit
		stop = min(batch_limit+batch_size, len(person_crawler.user_info_list))
		print('当前入库主播序号：' + str(start+1) + ' ' + str(stop))
		tasks = [mysql_client.into_mysql(loop, 'persons', i) for i in person_crawler.user_info_list[start:stop]]
		loop.run_until_complete(asyncio.wait(tasks))
		time.sleep(0.5)
	print('任务结束')


if __name__ == '__main__':
	run()