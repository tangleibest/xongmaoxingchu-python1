import requests
import json
import csv
import time

"""
爬取乐铺网基本信息
"""
f = open('result_shanghai.csv','w',encoding='utf-8',newline='')
csv_writer = csv.writer(f)
# proxies = {'https': '192.168.28.3'}
header={
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0',
'Cookie': 'PHPSESSID=4tpcgg8r96ibdlvpnnnldstec1; city_id=f9be1bb0dccf6859448c425d91238725317a9583c0602311ee3adb22f441f944a%3A2%3A%7Bi%3A0%3Bs%3A7%3A%22city_id%22%3Bi%3A1%3Bi%3A2%3B%7D; city=7936c9abf8ecf41a5df29c31458fadffdd10dcb2aaaea293bb88b68d99b8c150a%3A2%3A%7Bi%3A0%3Bs%3A4%3A%22city%22%3Bi%3A1%3Bi%3A2%3B%7D; UM_distinctid=16f0c812b6d210-0d98e2163bc01c-4c302b7a-1fa400-16f0c812b6ec3; Hm_lvt_71f10bf513bb45a54c36988ce8479afb=1576462810,1576633853,1576634055,1576639877; Hm_lpvt_71f10bf513bb45a54c36988ce8479afb=1576639885; shop_list_url_cookie=8c08e0988995b5d75ca1e58371b312b4c4da9c71ebafd1b55e1e5126a6cec6eaa%3A2%3A%7Bi%3A0%3Bs%3A20%3A%22shop_list_url_cookie%22%3Bi%3A1%3Bs%3A0%3A%22%22%3B%7D; search_cookie=%7B%22area%22%3A%220%22%2C%22districts%22%3A%220%22%2C%22cost_range%22%3A%220%22%2C%22coss%22%3A%22%22%2C%22cose%22%3A%22%22%2C%22money%22%3A%220%22%2C%22mne%22%3A%22%22%2C%22mns%22%3A%22%22%2C%22shop_area%22%3A%22%22%2C%22sae%22%3A%22%22%2C%22sas%22%3A%22%22%2C%22equipment%22%3A%22%22%2C%22license%22%3A%22%22%2C%22property_type%22%3A%22%22%2C%22business_type%22%3A%22%22%2C%22business_type_sec%22%3A%22%22%2C%22wide%22%3A%220%22%2C%22floor_num%22%3A%22%22%2C%22loop%22%3A%220%22%2C%22keyword%22%3A%22%22%2C%22sort%22%3A%220%22%2C%22shop_tag%22%3A%22%22%7D'
}
num=0
for i in range(1,2004):
    url = "https://api.lepu.cn/app/shop/search?&page=%s" %i
    request=requests.get(url,headers=header)
    json1=json.loads(request.text)
    data=json1.get('data').get('info')
    total=data.get('total')

    list=data.get('list')
    page=data.get('page')
    hasmore=data.get('hasmore')
    for list_info in list:
        id=list_info.get('id')
        tel=list_info.get('tel')
        title=list_info.get('title')
        ctitle=list_info.get('ctitle')
        up_time=list_info.get('up_time')
        city=list_info.get('city').get('name')
        area=list_info.get('area').get('name')
        district=list_info.get('district').get('name')
        money=list_info.get('money')[0]
        money_unit=list_info.get('money')[1]
        day_money=list_info.get('day_money')[0]
        day_money_unit=list_info.get('day_money')[1]
        introduce=list_info.get('introduce')
        property_type=list_info.get('property_type')
        cost=list_info.get('cost')[0]
        cost_unit=list_info.get('cost')[1]
        cbusiness=list_info.get('cbusiness')
        desc=list_info.get('desc')
        state=list_info.get('state')
        created_at=list_info.get('created_at')
        longitude=list_info.get('longitude')
        latitude=list_info.get('latitude')
        autotag=str(list_info.get('autotag'))
        usage_area=list_info.get('usage_area')
        street=list_info.get('street')
        money_stream=list_info.get('money_stream')
        csv_writer.writerow([id,tel,title,ctitle,up_time,city,area,district,money,money_unit,day_money,day_money_unit,introduce,property_type,cost,cost_unit,cbusiness,desc,
            state,created_at,longitude,latitude,autotag,usage_area,street,money_stream])
    request.close()
    time.sleep(3)
    num+=20
    print('上海一共40058条，已经爬了%d条了' % num)






