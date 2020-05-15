# !/usr/bin/python
# -*- coding: UTF-8 -*-

import smtplib
from email.mime.text import MIMEText
from email.header import Header
from DBUtils.PooledDB import PooledDB
import pymysql
from datetime import datetime, date, timedelta

yesterday = (date.today() + timedelta(days=-1)).strftime("%Y%m%d")

pool = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com',
                user='tanglei',
                passwd='tanglei', db='data_visualization', port=3306)
db = pool.connection()
cur = db.cursor()
sql = "SELECT '不为0的数据',channel_type,COUNT(*) from merchant_sales_statistics where stream_date ='%s' and order_count !=0 GROUP BY " \
      "channel_type union ALL SELECT '全部的数据',channel_type,COUNT(*) from merchant_sales_statistics where stream_date ='%s'  " \
      "GROUP BY channel_type" % (yesterday, yesterday)
cur.execute(sql)
results = cur.fetchall()
is_zero_all = 0
not_zero_all = 0
list_all = []
for row in results:
    if row[1] == 100:
        channel = "堂食"
    elif row[1] == 200:
        channel = "美团"
    elif row[1] == 300:
        channel = "饿了么"

    if row[0] == "不为0的数据":
        not_zero_all += row[2]
    else:
        is_zero_all += row[2]
    list_all.append([row[0], row[1], row[2]])

# eqfzbivawwrnbadh
# 第三方 SMTP 服务
mail_host = "smtp.qq.com"  # 设置服务器
mail_user = "1220149242@qq.com"  # 用户名
mail_pass = "eqfzbivawwrnbadh"  # 口令

sender = '1220149242@qq.com'
receivers = ['1220149242@qq.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

mail_msg = """
<p>每日报表</p>
<font color=red>每日报表:<br>饿了么不为0数据：%s<br>饿了么全部数据：%s<br>美团不为0数据：%s<br>美团全部数据：%s<br>堂食不为0数据：%s<br>堂食全部数据：%s<br>汇总不为0数据：%s<br>汇总全部数据：%s</font>
""" % (
    results[0][2], results[3][2], results[1][2], results[4][2], results[2][2], results[5][2], not_zero_all, is_zero_all)

message = MIMEText(mail_msg, 'html', 'utf-8')
message['From'] = Header(yesterday, 'utf-8')
message['To'] = Header("tanglei", 'utf-8')

subject = '%s同步数据情况' % yesterday
message['Subject'] = Header(subject, 'utf-8')

try:
    smtpObj = smtplib.SMTP()
    smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
    smtpObj.login(mail_user, mail_pass)
    smtpObj.sendmail(sender, receivers, message.as_string())
    print("邮件发送成功")
except smtplib.SMTPException:
    print("Error: 无法发送邮件")
