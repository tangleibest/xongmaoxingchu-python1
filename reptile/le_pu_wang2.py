import time

import requests
from bs4 import BeautifulSoup
from lxml import etree
import csv
import codecs
import pandas as pd

"""
爬取乐铺网支付方式
"""


def get_content(shop_id):
    headers = {
        #上海
        # 'Cookie': 'city_id=f9be1bb0dccf6859448c425d91238725317a9583c0602311ee3adb22f441f944a%3A2%3A%7Bi%3A0%3Bs%3A7%3A%22city_id%22%3Bi%3A1%3Bi%3A2%3B%7D; city=7936c9abf8ecf41a5df29c31458fadffdd10dcb2aaaea293bb88b68d99b8c150a%3A2%3A%7Bi%3A0%3Bs%3A4%3A%22city%22%3Bi%3A1%3Bi%3A2%3B%7D; UM_distinctid=16f0c812b6d210-0d98e2163bc01c-4c302b7a-1fa400-16f0c812b6ec3; Hm_lvt_71f10bf513bb45a54c36988ce8479afb=1585533088; CNZZDATA1266216096=923335564-1576457766-https%253A%252F%252Fwww.baidu.com%252F%7C1585557901; shop_list_url_cookie=8c08e0988995b5d75ca1e58371b312b4c4da9c71ebafd1b55e1e5126a6cec6eaa%3A2%3A%7Bi%3A0%3Bs%3A20%3A%22shop_list_url_cookie%22%3Bi%3A1%3Bs%3A0%3A%22%22%3B%7D; Hm_lpvt_71f10bf513bb45a54c36988ce8479afb=1585559119; PHPSESSID=qublliri3qbgb5l65n45gd7h55',
        # 杭州
        'Cookie': 'city_id=43017e32619152f217994f044a31c84f6947ffc78559e3b1f08e896a516f0fd8a%3A2%3A%7Bi%3A0%3Bs%3A7%3A%22city_id%22%3Bi%3A1%3Bi%3A6%3B%7D; city=6791a4c8ca41a789d148871202368aef6ba3560b0e9289d5aca3123465c0ee28a%3A2%3A%7Bi%3A0%3Bs%3A4%3A%22city%22%3Bi%3A1%3Bi%3A6%3B%7D; UM_distinctid=16f0c812b6d210-0d98e2163bc01c-4c302b7a-1fa400-16f0c812b6ec3; Hm_lvt_71f10bf513bb45a54c36988ce8479afb=1585533088; CNZZDATA1266216096=923335564-1576457766-https%253A%252F%252Fwww.baidu.com%252F%7C1585634515; shop_list_url_cookie=8c08e0988995b5d75ca1e58371b312b4c4da9c71ebafd1b55e1e5126a6cec6eaa%3A2%3A%7Bi%3A0%3Bs%3A20%3A%22shop_list_url_cookie%22%3Bi%3A1%3Bs%3A0%3A%22%22%3B%7D; Hm_lpvt_71f10bf513bb45a54c36988ce8479afb=1585637393; PHPSESSID=qublliri3qbgb5l65n45gd7h55',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0'}
    url = "https://www.lepu.cn/hangzhou/shop/detail-%s" % shop_id

    request = requests.get(url)
    html = request.text

    # soup=BeautifulSoup(html,'lxml')
    # con=soup.select('/html/body/div[4]/div[1]/div[1]/div[8]/div[2]/div[2]/ul/li[1]/div/p[1]/span[2]/b')
    html1 = etree.HTML(html)
    # 支付方式
    pay_method = html1.xpath('/html/body/div[4]/div[1]/div[1]/div[7]/div[2]/div[2]/ul/li[1]/div/p[1]/span[2]/b/text()')



    if pay_method is None or len(pay_method) == 0:
        pay_method = ''
    else:
        pay_method = pay_method[0]
    # 押金

    cash_pledge = html1.xpath('/html/body/div[4]/div[1]/div[1]/div[7]/div[2]/div[2]/ul/li[1]/div/p[1]/span[3]/b/text()')
    print(cash_pledge)
    if cash_pledge is None or len(cash_pledge) == 0:
        cash_pledge = ''
    else:
        cash_pledge = cash_pledge[0]
    # 当前租约
    current_renewal = html1.xpath(
        '/html/body/div[4]/div[1]/div[1]/div[8]/div[2]/div[2]/ul/li[1]/div/p[2]/span[1]/b/text()')
    if current_renewal is None or len(current_renewal) == 0:
        current_renewal = ''
    else:
        current_renewal = current_renewal[0]
    # 剩余租约
    cash_pledge1 = html1.xpath(
        '/html/body/div[4]/div[1]/div[1]/div[8]/div[2]/div[2]/ul/li[1]/div/p[2]/span[2]/b/text()')
    if cash_pledge1 is None or len(cash_pledge1) == 0:
        cash_pledge1 = ''
    else:
        cash_pledge1 = cash_pledge1[0]
    # 最长可租
    cash_pledge2 = html1.xpath(
        '/html/body/div[4]/div[1]/div[1]/div[8]/div[2]/div[2]/ul/li[1]/div/p[2]/span[3]/b/text()')
    if cash_pledge2 is None or len(cash_pledge2) == 0:
        cash_pledge2 = ''
    else:
        cash_pledge2 = cash_pledge2[0]
    # 续租方式
    cash_pledge3 = html1.xpath(
        '/html/body/div[4]/div[1]/div[1]/div[8]/div[2]/div[2]/ul/li[1]/div/p[3]/span[1]/b/text()')

    if len(cash_pledge3) == 0 or cash_pledge3 is None:
        cash_pledge3 = '无'
    else:
        cash_pledge3 = cash_pledge3[0]
    # 分时出租
    cash_pledge4 = html1.xpath(
        '/html/body/div[4]/div[1]/div[1]/div[8]/div[2]/div[2]/ul/li[1]/div/p[3]/span[2]/b/text()')
    if cash_pledge4 is None or len(cash_pledge4) == 0:
        cash_pledge4 = ''
    else:
        cash_pledge4 = cash_pledge4[0]
    list1 = [shop_id, pay_method, cash_pledge, current_renewal, cash_pledge1, cash_pledge2, cash_pledge3, cash_pledge4]
    request.close()
    return list1


def data_write_csv(file_name, datas):  # file_name为写入CSV文件的路径，datas为要写入数据列表
    datas = list(map(lambda x: [x], datas))
    file_csv = codecs.open(file_name, 'w+', 'utf-8')  # 追加
    writer = csv.writer(file_csv, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
    for data in datas:
        writer.writerow(data)
    print("保存文件成功，处理结束")


f = open('result_info_hangzhou.csv', 'w', encoding='utf-8', newline='')
csv_writer = csv.writer(f)

with open('result_hangzhou.csv', newline='', encoding='UTF-8') as csvfile:
    rows = csv.reader(csvfile)
    for row in rows:
        data = get_content(row[0])
        csv_writer.writerow(data)
        time.sleep(2)
