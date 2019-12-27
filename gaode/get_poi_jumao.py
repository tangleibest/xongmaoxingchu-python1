import requests
import json

import xlwt

"""
搜索高德poi，橘猫
"""

book = xlwt.Workbook(encoding='utf-8')
sheet = book.add_sheet('高德', cell_overwrite_ok=True)
row = 0
for page in range(1,5):
    url = 'https://restapi.amap.com/v3/place/text?keywords=橘猫&city=beijing&output=JSON&offset=20&page=%s&key=4f421f54d160c27a805bf7097730206b&extensions=all' %page
    request = requests.get(url)
    json1=json.loads(request.text)
    pois=json1.get('pois')

    for pois_all in pois:
        name=pois_all.get('name')
        type=pois_all.get('type')
        address=pois_all.get('address')
        location=pois_all.get('location')
        tel=pois_all.get('tel')
        website=pois_all.get('website')
        email=pois_all.get('email')
        cityname=pois_all.get('cityname')
        adname=pois_all.get('adname')
        timestamp=pois_all.get('timestamp')
        business_area=pois_all.get('business_area')
        sheet.write(row, 0, name)
        sheet.write(row, 1, type)
        sheet.write(row, 2, address)
        sheet.write(row, 3, location)
        sheet.write(row, 4, tel)
        sheet.write(row, 5, website)
        sheet.write(row, 6, email)
        sheet.write(row, 7, cityname)
        sheet.write(row, 8, adname)
        sheet.write(row, 9,  )
        sheet.write(row, 10, business_area)
        row+=1
    request.close()
book.save('C:\\Users\\tl\\Desktop\\\\高德橘猫.xlsx' )