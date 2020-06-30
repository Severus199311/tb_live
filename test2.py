import time

time_stamp = 1590580342
time_array = time.localtime(time_stamp)
structured_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
print(structured_time)