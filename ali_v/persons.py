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

class PersonCrawler():
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
		referer = {'referer': 'https://v.taobao.com/v/content/live?catetype=701'}
		self.headers.update(referer)
		base_url = 'https://v.taobao.com/micromission/req/selectCreatorV3.do?'
		params = {
			'cateType': '701',
			'sortField': '2',
			'currentPage': page,
			'_output_charset': 'UTF-8',
			'_input_charset': 'UTF-8',
			'_csrf': '8da43e45-c69a-49d7-902b-0c1aeee774b1'
		}
		url = base_url + urlencode(params)
		response = requests.get(url, headers=self.headers)
		try:
			data = response.json().get('data').get('result')
		except Exception as e:
			print('该页未获取到数据：', page)
			print(e.args)
			data = None

		if data:
			for item in data:
				user_id = item.get('userId')
				#print(user_id)
				self.user_id_list.append(user_id)		

	def get_user_and_fans(self, user_id):
		user_info = self.get_user_info(user_id)
		if user_info:
			fans_info = self.get_fans_info(user_id)
			user_info['fans_categories'] = fans_info
		self.user_info_list.append(user_info)


	def get_user_info(self, user_id):
		referer = {'referer': 'https://v.taobao.com/v/home/?spm='+self.spm+'&userId='+str(user_id)+'&pvid=&scm='}
		self.headers.update(referer)
		base_url = 'https://v.taobao.com/micromission/daren/daren_main_portalv3.do?'
		params = {
			'userId': user_id,
			'spm': self.spm,
			'_csrf': '229caee8-138c-47bc-aed8-cbc07dc0cba6'
		}
		url = base_url + urlencode(params)
		try:
			response = requests.get(url, headers=self.headers)
			data = response.json().get('data')
		except Exception as e:
			print('该user_id未获取到主播信息：', user_id)
			print(e.args)
			data = None

		if data:
			user_info = {}
			user_info['user_id'] = str(user_id)
			user_info['nickname'] = data.get('darenNick')
			user_info['area'] = data.get('area')
			user_info['fans_count'] = data.get('fansCount')
			user_info['score'] = data.get('darenScore')

			try:
				user_info['ser_type'] = data.get('darenMissionData').get('servType')
			except AttributeError:
				user_info['ser_type'] = ''

			try:
				user_info['agency'] = data.get('darenAgency').get('agencyName')
			except AttributeError:
				user_info['agency'] = ''

			try:
				intro_paras = data.get('desc').get('blocks')
				intro = ''
				for para in intro_paras:
					text = para.get('text')
					intro = intro + text
			except AttributeError:
				intro = ''
			user_info['intro'] = intro

			avatar_url = data.get('picUrl')
			avatar_url = urljoin('http://img.alicdn.com', avatar_url)
			user_info['avatar_url'] = avatar_url
			"""
			response = requests.get(avatar_url)
			file_path = '{0}/{1}.{2}'.format('主播', nickname, 'png')
			if not os.path.exists(file_path):
				with open(file_path, 'wb') as f:
					f.write(response.content) 
			"""

			return user_info

		else:
			return None

	def get_fans_info(self, user_id):
		referer = {'referer': 'https://v.taobao.com/v/home/?spm='+self.spm+'&userId='+str(user_id)+'&pvid=&scm='}
		self.headers.update(referer)
		base_url = 'https://v.taobao.com/micromission/daren/qry_fans_portrait.do?'
		params = {
			'userId': user_id,
			'_csrf': '229caee8-138c-47bc-aed8-cbc07dc0cba6'
		}
		url = base_url + urlencode(params)
		#response = requests.get(url, headers=self.headers)
		try:
			response = requests.get(url, headers=self.headers)
			data = response.json().get('data')
			fans_features = data.get('fansFeature')
		except Exception as e:
			print('该user_id未获取到粉丝信息：', user_id)
			print(e.args)
			fans_features = None

		if fans_features:
			fans_feature_dict = {}
			for key in fans_features.keys():
				old_list = fans_features[key]
				new_list = []
				for item in old_list:
					new_dict = {}
					new_dict[item['title']] = item['value']
					new_list.append(new_dict)
				fans_feature_dict[key] = new_list

			return json.dumps(fans_feature_dict, ensure_ascii=False)

		else:
			return None

	def run(self):
		batch_size = 10 #不知道为什么，并发量大了，总会漏掉很多
		for batch_limit in range(0, PERSON_PAGES, batch_size):
			start = batch_limit
			stop = min(batch_limit+batch_size, PERSON_PAGES)
			tasks = [gevent.spawn(self.get_user_ids, i) for i in range(1, PERSON_PAGES+1)[start:stop]]
			gevent.joinall(tasks)
			
		print('user_id获取完毕。总共数量：', len(self.user_id_list))

		self.user_id_list.append('2103587316')

		batch_size = 200
		for batch_limit in range(0, len(self.user_id_list), batch_size):
			start = batch_limit
			stop = min(batch_limit+batch_size, len(self.user_id_list))
			print('当前爬取user_id序号：' + str(start+1) + ' ' + str(stop))
			tasks = [gevent.spawn(self.get_user_and_fans, i) for i in self.user_id_list[start:stop]]
			gevent.joinall(tasks)


if __name__ == '__main__':
	crawler = PersonCrawler()
	crawler.run()
