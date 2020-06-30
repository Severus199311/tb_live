import pymysql

#mysql_client = pymysql.connect(host='47.105.101.249', port=30306, user='bxdb_admin', password='Dz7cGRZx4Tu7Zek@2D6MM', db='bxdb', charset='utf8mb4')
mysql_client = pymysql.connect(host='47.114.166.130', port=13306, user='bxusr', password='bxdb@TT12', db='bxdb', charset='utf8mb4')
cursor = mysql_client.cursor()
#sql = "SELECT live_id, start_time, create_time, sync_time, item_info From tb_live_lives WHERE live_title = '%s' AND item_info IS NOT NULL" %'刘涛【刘一刀】直播'
sql = "SELECT item_id, sales, create_time, sync_time FROM tb_live_items WHERE live_id = '267875656841'"
#sql = "SELECT live_title, item_info FROM tb_live_lives WHERE live_id = '265880593745'"
#sql = "SELECT ldr_id, cate_lvl_2_name, avg_lsw_trd_itm_qty_cnt, avg_lsw_trd_amt, avg_watch_people_cnt, fan_cnt_td, avg_pit_itm_qty_cnt, avg_pit_trd_amt WHERE avg_watch_people_cnt >= 30000"
cursor.execute(sql)
row = cursor.fetchone()
while row:
	for each in  row:
		print(each)
	row = cursor.fetchone()