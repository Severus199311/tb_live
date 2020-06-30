# !/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
import json
import re
import requests
from urllib.parse import quote_plus
from urllib.parse import urlencode
import time
import random
import string
import hashlib

from logger import logger
from redis_client import RedisClient
from config import *

class GetRawData():

	def __init__(self):
		self.appKey = "21646297"
		self.ttid = '1568707896704@taobao_android_9.1.0'
		self.app_ver = "9.1.0"
		#self.us = 'Mozilla/5.0 (Linux; Android 4.1.1; Nexus 7 Build/JRO03D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Safari/535.19'
		self.ua = "MTOPSDK%2F3.1.1.7+%28Android%3B5.1.1"
		self.Cookie = 'imewweoriw=26F%2BYjtzafANebvCqyXl6O3UOKc%2FSQbYdlNefJ%2BiIkk%3D; WAPFDFDTGFG=%2B4cMKKP%2B8PI%2BKK8fqXY43Gu5fvESrw%3D%3D; _w_tb_nick=tb158007394; ockeqeudmj=iDFpf5o%3D; unb=2207270780533; sn=; uc3=vt3=F8dBxdrMblYkywC%2FbBQ%3D&lg2=U%2BGCWk%2F75gdr5Q%3D%3D&nk2=F5REO%2BAJGrF%2FnUM%3D&id2=UUphzWXF%2B9Y3BoeTWw%3D%3D; uc1=cookie15=UIHiLt3xD8xYTw%3D%3D&existShop=false&cookie14=UoTblAO3Q4xV9g%3D%3D&cookie21=U%2BGCWk%2F7oPIg; csg=9ef649ea; lgc=tb158007394; t=45bf18cb9306ea346d7a152e45902075; cookie17=UUphzWXF%2B9Y3BoeTWw%3D%3D; dnk=tb158007394; skt=225c836b051cbae7; munb=2207270780533; cookie2=5ac0e460e73565082b38fe564932e38c; uc4=nk4=0%40FY4PaQKpiIJHQEpX4YHJwXfnh0bYcQ%3D%3D&id4=0%40U2grFnp7p81MoHa32K9LZ%2F0BZT1eato8; tracknick=tb158007394; _cc_=Vq8l%2BKCLiw%3D%3D; ti=; sg=437; _l_g_=Ug%3D%3D; _nk_=tb158007394; cookie1=VAcHmi86xWvtinSpVMukRJ6mH5xg2Q7kGQmtQ0NOWEk%3D; _tb_token_=ee3101701157a; sgcookie=XT55agMLA5aD9XxcgI30DEwfZE9aQaN6QjdUrWLeJbVg; x5sec=7b226d746f703b32223a226231383665636135353235386632313135363938376439643363613565306635434e57496c504546454a4372792b4b707a707a4135774561447a49794d4463794e7a41334f4441314d7a4d374d513d3d227d; cna=DUCrFlxxsBYCAX12+YU4UHQb; isg=Ahoasf5VEDBGxJydAl67RxJtYMk8S54lsI_b4SSTxq14l7rRDNvuNeDlN1v0; imewweoriw=26F%2BYjtzafANebvCqyXl6O3UOKc%2FSQbYdlNefJ%2BiIkk%3D; WAPFDFDTGFG=%2B4cMKKP%2B8PI%2BKK8fqXY43Gu5fvESrw%3D%3D; _w_tb_nick=tb158007394; ockeqeudmj=iDFpf5o%3D; unb=2207270780533; sn=; uc3=vt3=F8dBxdrMblYkywC%2FbBQ%3D&lg2=U%2BGCWk%2F75gdr5Q%3D%3D&nk2=F5REO%2BAJGrF%2FnUM%3D&id2=UUphzWXF%2B9Y3BoeTWw%3D%3D; uc1=cookie15=UIHiLt3xD8xYTw%3D%3D&existShop=false&cookie14=UoTblAO3Q4xV9g%3D%3D&cookie21=U%2BGCWk%2F7oPIg; csg=9ef649ea; lgc=tb158007394; t=45bf18cb9306ea346d7a152e45902075; cookie17=UUphzWXF%2B9Y3BoeTWw%3D%3D; dnk=tb158007394; skt=225c836b051cbae7; munb=2207270780533; cookie2=5ac0e460e73565082b38fe564932e38c; uc4=nk4=0%40FY4PaQKpiIJHQEpX4YHJwXfnh0bYcQ%3D%3D&id4=0%40U2grFnp7p81MoHa32K9LZ%2F0BZT1eato8; tracknick=tb158007394; _cc_=Vq8l%2BKCLiw%3D%3D; ti=; sg=437; _l_g_=Ug%3D%3D; _nk_=tb158007394; cookie1=VAcHmi86xWvtinSpVMukRJ6mH5xg2Q7kGQmtQ0NOWEk%3D; _tb_token_=ee3101701157a; sgcookie=XT55agMLA5aD9XxcgI30DEwfZE9aQaN6QjdUrWLeJbVg; x5sec=7b226d746f703b32223a226231383665636135353235386632313135363938376439643363613565306635434e57496c504546454a4372792b4b707a707a4135774561447a49794d4463794e7a41334f4441314d7a4d374d513d3d227d; cna=DUCrFlxxsBYCAX12+YU4UHQb; isg=Ahoasf5VEDBGxJydAl67RxJtYMk8S54lsI_b4SSTxq14l7rRDNvuNeDlN1v0'
		#self.Cookie = "enc=nsDkgllsoBPb0wIc0Q8sA4xK0DSWUGILWESmQjTZ0N5v9rvesE96OKtukWmMQNlvKtXJmzc1TT6VuFqddpQepw%3D%3D;"
		self.utdid = "iGuuZCilkcOBojXEIkDESMPP"
		self.lat = ""
		self.lng = ""
		#self.sign_server = "http://192.168.31.132:6778/xsign"
		self.sign_server = "http://localhost:6778/xsign"
		self.data_failures = 0
		self.id = ID
		self.redis_client = RedisClient()

	def random_str(self, length):
		letters = string.ascii_letters
		return ''.join(random.choice(letters) for x in range(length))


	def call_gw_api(self, sign_server, api, v, data, use_cookie=False, uid='', sid='', features='27', method='GET'):
		timestamp = time.time()
		t = int(timestamp)
		deviceId = self.random_str(44)
		pageId = "http://h5.m.taobao.com/taolive/video.html"
		pageName = "com.ali.user.mobile.login.ui.UserLoginActivity"
		pre_sign_data = {
			"uid": uid,
			"ttid": self.ttid,
			"data": quote_plus(data),
			"lng": self.lng,
			"utdid": self.utdid,
			"api": api,
			"lat": self.lat,
			"deviceId": deviceId,
			"sid": sid,
			"x-features": features,
			"v": v,
			"t": str(t),
			"pageName": pageName,
			"pageId": pageId
		}
		sign_dic = self.get_sign_dic(sign_server, pre_sign_data)

		body = "data=" + quote_plus(data)
		req_url = "https://guide-acs.m.taobao.com/gw/{0}/{1}/".format(api, v)

		headers = {
			"x-appkey": self.appKey,
			"x-devid": deviceId,
			"x-ttid": quote_plus(self.ttid),
			"x-sign": quote_plus(sign_dic['result']['x-sign']),
			"x-mini-wua": quote_plus(sign_dic['result']['x-mini-wua']),
			"x-sgext": sign_dic['result']['x-sgext'],
			"x-t": str(t),
			"x-location": quote_plus("{0},{1}".format(self.lng, self.lat)),
			"x-app-ver": self.app_ver,
			"content-type": "application/x-www-form-urlencoded;charset=UTF-8",
			"x-pv": "6.3",
			"x-features": features,
			"x-app-conf-v": str(19),
			"x-utdid": self.utdid,
			"User-Agent": self.ua,
		}
		
		if uid != "":
			headers["x-uid"] = uid
			headers["x-sid"] = sid
		if use_cookie:
			headers["Cookie"] = self.Cookie

		if method == 'GET':
			req_url = "https://trade-acs.m.taobao.com/gw/{0}/{1}/?{2}".format(api, v, body)	
			sign_dic = requests.get(req_url, headers=headers, verify=True, allow_redirects=False, timeout=5)
		else:
			sign_dic = requests.post(req_url, data=body, headers=headers, verify=True, timeout=5)

		if sign_dic.status_code == requests.codes.ok:
			#print(sign_dic.text)
			self.data_failures = 0
			return sign_dic.json()
		else:
			#print(sign_dic.text)
			self.data_failures += 1
			if self.data_failures >= 20:
				logger.critical(self.id + '-连续20次以上返回数据无效，有可能被封了，程序将退出-' + sign_dic.text)
				self.redis_client.report_errors(int('111' + re.match('MT0(\d+)', self.id).group(1)))
				sys.exit()

	def get_sign_dic(self, sign_server, payload):
		headers = {
			"content-type": "application/json;charset=utf-8"
		}
		#print("待签名参数:" + json.dumps(payload))
		res = requests.post(sign_server, data=json.dumps(payload), headers=headers, timeout=5)
		res_content = res.content
		#print("签名返回:" + str(res_content))
		sign_dic = {}
		if res.status_code == requests.codes.ok:
			sign_dic = json.loads(res_content)
		return sign_dic

	
	def get_live_detail_2(self, account_id): #由account_id获取live_detail
		#data = "{\"extendJson\":\"{\\\"guardAnchorSwitch\\\":true,\\\"version\\\":\\\"201903\\\"}\",\"ignoreH265\":\"false\",\"liveId\":\"" + live_id + "\"}"
		data="{\"creatorId\":\"" + account_id + "\"}"
		v = "4.0"
		api = "mtop.mediaplatform.live.livedetail"
		return self.call_gw_api(self.sign_server, api, v, data)

	def get_live_detail(self, live_id): #由live_id获取live_detail
		data = "{\"extendJson\":\"{\\\"guardAnchorSwitch\\\":true,\\\"version\\\":\\\"201903\\\"}\",\"ignoreH265\":\"false\",\"liveId\":\"" + live_id + "\"}"
		#data="{\"creatorId\":\"4002376480\"}"
		v = "4.0"
		api = "mtop.mediaplatform.live.livedetail"
		return self.call_gw_api(self.sign_server, api, v, data)

	def get_itemlist_detail(self, creator_id, live_id):
		data = "{\"type\":\"2\",\"creatorId\":\""+ creator_id +"\",\"liveId\":\""+ live_id +"\"}"
		v = '2.0'
		api = "mtop.mediaplatform.video.livedetail.itemlist"
		return self.call_gw_api(self.sign_server, api, v, data)

	"""
	def get_channel_detail(self, s, channel, module_index): #原版的获取channel_detail的api
		data = "{\"PARCELABLE_WRITE_RETURN_VALUE\":\"1\",\"s\":\""+ s +"\",\"channelId\":\""+ channel +"\",\"CONTENTS_FILE_DESCRIPTOR\":\"1\",\"deviceLevel\":\"2\",\"n\":\"10\",\"CREATOR\":\"{}\",\"queryAd\":\"1\",\"moduleIndex\":\""+ module_index +"\",\"haveOnlook\":\"false\",\"version\":\"12\"}"
		v = '5.0'
		api = 'mtop.mediaplatform.live.videolist'
		return self.call_gw_api(self.sign_server, api, v, data)
	"""

	def get_channel_detail(self, s, channel, module_index): #新版的获取channel_detail的api
		data = "{\"PARCELABLE_WRITE_RETURN_VALUE\":\"1\",\"s\":\""+ s +"\",\"channelId\":\""+ channel +"\",\"CONTENTS_FILE_DESCRIPTOR\":\"1\",\"deviceLevel\":\"2\",\"n\":\"10\",\"CREATOR\":\"{}\",\"queryAd\":\"1\",\"moduleIndex\":\""+ module_index +"\",\"haveOnlook\":\"false\",\"version\":\"12\"}"
		v = '1.0'
		api = 'mtop.taobao.iliad.video.list.spare'
		return self.call_gw_api(self.sign_server, api, v, data)	

	def get_item_detail(self, item_id): 
		data = "{\"id\":\""+ item_id +"\",\"itemNumId\":\""+ item_id +"\",\"itemId\":\""+ item_id +"\",\"exParams\":\"{\\\"id\\\":\\\""+ item_id +"\\\"}\",\"detail_v\":\"8.0.0\",\"utdid\":\"1\"}"
		v = '6.0'
		api = 'mtop.taobao.detail.getdetail'
		return self.call_gw_api(self.sign_server, api, v, data)

	def get_homepage_detail(self):
		data = "{\"selectedChnlId\":\"0\",\"menuType\":\"videoMenuV2\",\"channelId\":\"0\",\"subContentId\":\"600461075303_1\",\"deviceLevel\":\"2\",\"contentId\":\"590664141907_1\",\"version\":\"4\"}"
		v = '2.0'
		api = 'mtop.mediaplatform.live.tabmenu'
		return self.call_gw_api(self.sign_server, api, v, data)

	def get_shop_impression(self):
		data = "{\"sellerId\":\"1751573221\",\"shopId\":\"105324592\"}"
		v = '1.0'
		api = 'mtop.taobao.shop.impression.intro.get'
		return self.call_gw_api(self.sign_server, api, v, data)


if __name__ == '__main__':
	get_raw_data = GetRawData() 
	#data =get_raw_data.get_homepage_detail()
	#data = get_raw_data.get_live_detail('266716494774')
	#data = get_raw_data.get_live_detail_2('272625895')
	#data = get_raw_data.get_item_detail('614304935096')
	#data = get_raw_data.get_itemlist_detail('2206893483723', '260597000787')
	#data = get_raw_data.get_channel_detail('0', '0', '0')
	data = get_raw_data.get_shop_impression()
	
	print(json.dumps(data, ensure_ascii=False))
	
	"""
	api_stack = data.get('data').get('apiStack')
	api_stack_value = json.loads(api_stack[0].get('value'))
	video_url = api_stack_value.get('item').get('videos')[0].get('url')
	print(video_url)
	"""
	
