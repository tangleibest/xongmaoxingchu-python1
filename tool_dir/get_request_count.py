# coding:utf-8
import calendar
import datetime
import re
import time
from collections import Counter

"""
统计接口访问次数
"""
# 昨天结束时间戳
today_start_time = int(time.mktime(time.strptime(str(datetime.date.today()), '%Y-%m-%d')))


fileReadObj = open("C:/Users/tl/Desktop/门店数据/moblie.txt", encoding='UTF-8')

fileLineText = fileReadObj.readline()
request_list=[]
while ('' != fileLineText):
    string = fileLineText

    fileLineText = fileReadObj.readline().strip()
    if 'GET /api/' in fileLineText:
        starpattern = re.compile('"GET (.*) HTTP/1.1" 200 -')
        pattern_time=re.compile('\[(.*)\]')
        str_time = pattern_time.findall(fileLineText)

        li = list(calendar.month_abbr).index(str(str_time[0]).split('/')[1])
        tran_time=str(str_time[0]).split('/')[0]+'-'+str(li)+'-'+str(str_time[0]).split('/')[2]

        # 先转换为时间数组
        timeArray = time.strptime(tran_time, "%d-%m-%Y %H:%M:%S")
        # 转换为时间戳
        timeStamp = int(time.mktime(timeArray))

        if timeStamp>=today_start_time:
            star = starpattern.findall(fileLineText)
            print(fileLineText)
            if len(star)>0:
                pattern = re.compile('(.*)\?')
                if '?' in star[0]:
                    result = pattern.findall(star[0])

                    request_list.append(result[0])
                else:
                    request_list.append(star[0])
fileReadObj.close()

count=Counter(request_list)
for i in count:
    print(i,count.get(i))