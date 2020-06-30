

# crontab
* */2 * * * Path='/data/data/com.termux/files/usr/bin/bash' /data/data/com.termux/files/home/schedule/check_users_main.sh >> /data/data/com.termux/files/home/schedule/log/check_users_main_`date "+%Y%m%d"`.log 2>&1

cat << 'eof' > /data/data/com.termux/files/home/schedule/check_users_main.sh
# test_main.sh
prc_name="check_users.py"
work_path="/data/data/com.termux/files/home/tb_live"

if [ `ps -ef|grep ${prc_name}|grep -v grep|wc -l` -gt 0 ] ;then
ps -ef | grep ${prc_name} | grep -v grep | awk '{print $2}' | xargs kill -9
fi
python -u ${work_path}/${prc_name}  &
eof

chmod 777 /data/data/com.termux/files/home/schedule/check_users_main.sh

#get_lives

cat << 'eof' > /data/data/com.termux/files/home/schedule/get_lives_main.sh
# test_main.sh
prc_name="get_lives.py"
work_path="/data/data/com.termux/files/home/tb_live"

if [ `ps -ef|grep ${prc_name}|grep -v grep|wc -l` -gt 0 ] ;then
ps -ef | grep ${prc_name} | grep -v grep | awk '{print $2}' | xargs kill -9
fi
python -u ${work_path}/${prc_name}  &
eof

chmod 777 /data/data/com.termux/files/home/schedule/get_lives_main.sh

* */2 * * * Path='/data/data/com.termux/files/usr/bin/bash' /data/data/com.termux/files/home/schedule/get_lives_main.sh >> /data/data/com.termux/files/home/schedule/log/get_lives_main_`date "+%Y%m%d"`.log 2>&1


#get_items.py 2小时

cat << 'eof' > /data/data/com.termux/files/home/schedule/get_items_main.sh
# test_main.sh
prc_name="get_items.py"
work_path="/data/data/com.termux/files/home/tb_live"

if [ `ps -ef|grep ${prc_name}|grep -v grep|wc -l` -gt 0 ] ;then
ps -ef | grep ${prc_name} | grep -v grep | awk '{print $2}' | xargs kill -9
fi
python -u ${work_path}/${prc_name}  &
eof

chmod 777 /data/data/com.termux/files/home/schedule/get_items_main.sh

* */2 * * * Path='/data/data/com.termux/files/usr/bin/bash' /data/data/com.termux/files/home/schedule/get_items_main.sh >> /data/data/com.termux/files/home/schedule/log/get_items_main_`date "+%Y%m%d"`.log 2>&1


#get_new_users.py 1小时

cat << 'eof' > /data/data/com.termux/files/home/schedule/get_new_users_main.sh
# test_main.sh
prc_name="get_new_users.py"
work_path="/data/data/com.termux/files/home/tb_live"

if [ `ps -ef|grep ${prc_name}|grep -v grep|wc -l` -gt 0 ] ;then
ps -ef | grep ${prc_name} | grep -v grep | awk '{print $2}' | xargs kill -9
fi
python -u ${work_path}/${prc_name}  &
eof

chmod 777 /data/data/com.termux/files/home/schedule/get_new_users_main.sh

* */1 * * * Path='/data/data/com.termux/files/usr/bin/bash' /data/data/com.termux/files/home/schedule/get_new_users_main.sh >> /data/data/com.termux/files/home/schedule/log/get_new_users_main_`date "+%Y%m%d"`.log 2>&1

#alarm
cat << 'eof' > /data/data/com.termux/files/home/schedule/alarm_main.sh
# test_main.sh
prc_name="alarm.py"
work_path="/data/data/com.termux/files/home/tb_live"

if [ `ps -ef|grep ${prc_name}|grep -v grep|wc -l` -gt 0 ] ;then
ps -ef | grep ${prc_name} | grep -v grep | awk '{print $2}' | xargs kill -9
fi
python -u ${work_path}/${prc_name}  &
eof

chmod 777 /data/data/com.termux/files/home/schedule/alarm_main.sh

*/5 * * * * Path='/data/data/com.termux/files/usr/bin/bash' /data/data/com.termux/files/home/schedule/alarm_main.sh >> /data/data/com.termux/files/home/schedule/log/alarm_main_`date "+%Y%m%d"`.log 2>&1