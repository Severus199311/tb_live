import gevent
import gevent.monkey
gevent.monkey.patch_all()

import requests
from urllib.parse import urlencode, urljoin
from multiprocessing import Pool
import json
import os
import pymysql

from config import *

class AgencyCrawler():
	def __init__(self):
		self.cookie = COOKIE
		self.spm = SPM
		self.headers = {
			'accept': 'application/json, text/javascript, */*; q=0.01',
			'accept-encoding': 'gzip, deflate, br',
			'accept-language': 'zh-CN,zh;q=0.9',
			'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
			'sec-fetch-mode': 'cors',
			'sec-fetch-site': 'same-origin',
			'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 Safari/537.36',
			'x-requested-with': 'XMLHttpRequest',
			'cookie': self.cookie
		}
		self.db = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATEBASE, MYSQL_PORT, charset='utf8')
		self.cursor = self.db.cursor()
		self.user_id_list = []
		self.user_info_list = []

	def get_user_ids(self, page):
		referer = {'referer': 'https://v.taobao.com/v/content/live?catetype=702'}
		self.headers.update(referer)
		base_url = 'https://v.taobao.com/micromission/req/selectCreatorV3.do?'
		params = {
			'cateType': '702',
			'currentPage': page,
			'_output_charset': 'UTF-8',
			'_input_charset': 'UTF-8',
			'_csrf': '258d9001-3563-4d20-9fe0-6dc1260e893c'
		}
		url = base_url + urlencode(params)
		try:
			response = requests.get(url, headers=self.headers)
			data = response.json().get('data').get('result')
		except Exception as e:
			print('该页数据未获取到数据：', page)
			print(e.args)
			data = None

		if data:
			for item in data:
				user_id = item.get('userId')
				self.user_id_list.append(user_id)

	def get_user_info(self, user_id):
		referer = {'referer': 'https://v.taobao.com/v/home/?spm='+self.spm+'&userId='+str(user_id)}
		self.headers.update(referer)
		base_url = 'https://v.taobao.com/micromission/daren/daren_main_portalv3.do?'
		params = {
			'userId': user_id,
			'spm': self.spm,
			'_csrf': '258d9001-3563-4d20-9fe0-6dc1260e893c'
		}
		url = base_url + urlencode(params)
		try:
			response = requests.get(url, headers=self.headers)
			data = response.json().get('data')
		except Exception as e:
			print('该user_id未获取到机构信息：', user_id)
			print(e.args)
			data = None

		if data:
			user_info = {}
			user_info['user_id'] = str(user_id)
			user_info['area'] = data.get('area') #服务领域
			user_info['intro_summary'] = data.get('introSummary') #机构简介
			user_info['title'] = data.get('darenNick') #机构名称
			user_info['members_count'] = data.get('darenCount') #签约人数

			try:
				user_info['serv_type'] = data.get('darenMissionData').get('servType') #服务类型
			except AttributeError:
				user_info['serv_type'] = ''

			try:
				bigshots_list = data.get('bigShots')
				bigshots_count = len(bigshots_list) #大咖人数
				new_bigshots_list = [] #该机构所有达人，装在一个list里
				for bigshot in bigshots_list:
					bigshot_info = {}
					bigshot_info['达人ID'] = bigshot.get('id') #达人ID
					bigshot_info['粉丝数'] = bigshot.get('fansCount') #粉丝数
					bigshot_info['达人昵称'] = bigshot.get('name') #达人昵称
					new_bigshots_list.append(bigshot_info) #以上三项放入一个dict，在讲dict放入new_bigshost_list
			except TypeError:
				bigshots_count = 0
				new_bigshots_list = []
			user_info['bigshots_count'] = bigshots_count
			user_info['bigshots_overview'] = json.dumps(new_bigshots_list, ensure_ascii=False)

			logo_url = data.get('picUrl')
			logo_url = urljoin('http://img.alicdn.com', logo_url)	
			user_info['logo_url'] = logo_url		
			"""
			response = requests.get(logo_url)
			file_path = '{0}/{1}.{2}'.format('logos', title, 'jpg')
			if not os.path.exists(file_path):
				with open(file_path, 'wb') as f:
					f.write(response.content) #获取并请求logo路径，将logo以机构名命名，并写入‘logos’文件夹
			"""
			try:
				intro_paras = data.get('desc').get('blocks')
				intro = ''
				for para in intro_paras:
					text = para.get('text')
					intro = intro + text #公司介绍
			except AttributeError:
				intro = ''
			user_info['intro'] = intro

			self.user_info_list.append(user_info)

	def into_database(self, info):
		try:
			keys = ','.join(info.keys())
			values = ','.join(['%s'] * len(info))
			sql = 'insert into %s (%s) values (%s)' %(MYSQL_TABLE_AGENCIES, keys, values)
			self.cursor.execute(sql, tuple(info.values()))
			self.db.commit()
		except pymysql.MySQLError as e:
			print(info['user_id'], info['title'],'入库失败')
			print(e.args)
			self.db.rollback()

	def run(self):
		batch_size = 200
		for batch_limit in range(0, AGENCY_PAGES, batch_size):
			start = batch_limit
			stop = min(batch_limit+batch_size, AGENCY_PAGES)
			tasks = [gevent.spawn(self.get_user_ids, i) for i in range(1, AGENCY_PAGES+1)[start:stop]]
			gevent.joinall(tasks)
		print('user_id获取完毕。总共数量：', len(self.user_id_list))
		for batch_limit in range(0, len(self.user_id_list)+1, batch_size):
			start = batch_limit
			stop = min(batch_limit+batch_size, len(self.user_id_list))
			print('当前爬取user_id序号：' + str(start+1) + ' ' + str(stop))
			tasks = [gevent.spawn(self.get_user_info, i) for i in self.user_id_list[start:stop]]
			gevent.joinall(tasks)

if __name__ == '__main__':
	crawler =AgencyCrawler()
	crawler.run()
