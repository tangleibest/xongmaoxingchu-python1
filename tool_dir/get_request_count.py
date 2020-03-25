# coding:utf-8
import calendar
import datetime
import re
import time
from collections import Counter
from typing import Any, Union
import sys

"""
统计接口访问次数
"""
# 门店id对应门店名称
project = {1548233981349868: '自空间',
           1548233981574173: '铸诚大厦',
           1548233981747979: '中关村',
           1548233981922675: '长虹桥',
           1548233982097535: '枣营麦子店',
           1548233982272822: '右安门',
           1548233982792607: '悠乐汇',
           1548233982966194: '雅宝城',
           1548233983142584: '新荟城店',
           1548233983310573: '西直门',
           1548233983513828: '五道口二店',
           1548233983688653: '望京西园',
           1548233983869361: '望京',
           1548233984304802: '驼房营',
           1548233984537722: '四道口',
           1548233984714727: '双井',
           1548233984884458: '十里河',
           1548233985051132: '十里堡二店',
           1548233985222972: '十里堡',
           1548233985397507: '上地',
           1548233985568475: '软件园',
           1548233985739387: '日坛',
           1548233985910790: '梨园',
           1548233986082122: '酒仙桥二店',
           1548233986249609: '酒仙桥',
           1548233986416119: '建国门',
           1548233986646429: '呼家楼二店',
           1548233986814497: '呼家楼',
           1548233986981233: '国展',
           1548233987175462: '国贸',
           1548233987348756: '广渠门',
           1548233987535533: '方庄',
           1548233987710366: '东直门',
           1548233987875726: '车公庄',
           1548233988042290: '百脑汇',
           1548233988217556: '大望路',
           1548233988385650: '五道口一店',
           1548233988556195: '石景山',
           1548233988722144: '经开大厦',
           1548233988887855: '星光影视园',
           1548233989054830: '星光影视园2F',
           1553865620850490: '马家堡',
           1557397275412512: '双井二店',
           1557397275473578: '牡丹园',
           1558349108715778: '新荟城二店',
           1558942470683858: '建国门贵友',
           1560944791763246: '望京六佰本',
           1561443493739244: '华贸天地',
           1562054721516782: '理想国',
           1562054722000548: '瑞和国际',
           1564020560015902: '望京SOHO',
           1565241755274960: '曹杨路店',
           1565241755337842: '广中西路店',
           1565241755384308: '国安路店',
           1565241755445630: '国定东路店',
           1565241755491528: '国权路店',
           1565241755542631: '河南北路店',
           1565241755585918: '河南北路二店',
           1565241755634124: '淮海东路店',
           1565241755678564: '淮海东路二店',
           1565241755717343: '江宁路店',
           1565241755757132: '江宁路二店',
           1565241755800953: '金沙江路店',
           1565241755841172: '金沙江路二店',
           1565241755913909: '控江路店',
           1565241755953135: '兰溪路店',
           1565241755994293: '灵石路店',
           1565241756029486: '南京西路店',
           1565241756194463: '四川北路一店',
           1565241756236183: '松江路店',
           1565241756273297: '汶水路店',
           1565241756314452: '吴中路店',
           1565241756360840: '武宁路店',
           1565241756422264: '协和路二店',
           1565241756464886: '协和路一店',
           1565241756504308: '延长路店',
           1565241756543794: '叶家宅路店',
           1565330730609659: '东直门二店',
           1565330730659182: '六里桥',
           1565330730700257: '新华百货',
           1565560737178944: '古墩路店',
           1565560737533882: '龙舌路店',
           1565560737581370: '莫干山路店',
           1565560737621990: '庆春路店',
           1565560737652104: '秋涛北路店',
           1565560737696267: '文二路店',
           1565560737740236: '文三路店',
           1565560737781179: '文一路店',
           1565560737823266: '星光大道店',
           1565560750566423: '梅华路店',
           1565560750621836: '深南东路店',
           1565560750663948: '深南中路店',
           1565597638438726: '国美第一城',
           1566268131084473: '朝阳路红星美凯龙',
           1566268131111237: '长虹桥二店',
           1566455383731236: '劲松',
           1566456449065193: '三元桥',
           1566996964187782: '天山西路店',
           1566996964211654: '淞沪路店',
           1566996964232353: '天山路店',
           1566996964250366: '曹杨路二店',
           1566996964270974: '古北路店',
           1568811664722306: '凯旋路店',
           1570790085493204: '中山西路店',
           1570790085525149: '商城路店',
           1571395003064816: '汇海广场',
           1572674723524265: '斜徐路店'}

request_dict = {
    '/api/getOfficeInfo': '返回写字楼数据',
    '/api/getHousingInfo': '返回小区数据',
    '/api/getBaicInfo': '返回周边数据下周边配套数据及商场、酒店、高校、医院详情',
    '/api/getShopBasic': '返回月销量统计',
    '/api/getAveMoney': '返回北京总客单价及区域客单价',
    '/api/getCate': '返回周边品类榜',
    '/api/getFood': '返回菜品销量榜',
    '/api/getLineChart': '返回周边数据、门店数据下销售趋势',
    '/api/getCateShop': '返回品类下商户',
    '/api/getRace': '获取竞对数据',
    '/api/getProjectList': '返回项目列表',
    '/api/getStalls': '返回档口统计数据',
    '/api/getXmxcShop': '返回商户信息',
    '/api/getCateRecommend': '返回推荐品类',
    '/api/getCateRecommendShop': '返回推荐品类下商户',
    '/api/getCateAveSection': '返回推荐品类下客单价区间',
    '/api/getCateAveLine': '返回推荐品类下客单价趋势',
    '/api/getCateRecommendLine': '返回推荐品类下品类走势图',
    '/api/getCateList': '返回品类列表',
    '/api/getCatShopRank': '找店',
    '/api/getTwoShopCompare': '两个点对比'
}

# 昨天结束时间戳
today_start_time = int(time.mktime(time.strptime(str(datetime.date.today()), '%Y-%m-%d')))

fileReadObj = open("C:/Users/tl/Desktop/门店数据/moblie.log", encoding='UTF-8')

fileLineText = fileReadObj.readline()
request_list = []
while ('' != fileLineText):
    string = fileLineText

    fileLineText = fileReadObj.readline().strip()
    if 'GET /api/' in fileLineText:
        starpattern = re.compile('"GET (.*) HTTP/1.1" 200 -')
        pattern_time = re.compile('\[(.*)\]')
        str_time = pattern_time.findall(fileLineText)

        li = list(calendar.month_abbr).index(str(str_time[0]).split('/')[1])
        tran_time = str(str_time[0]).split('/')[0] + '-' + str(li) + '-' + str(str_time[0]).split('/')[2]

        # 先转换为时间数组
        timeArray = time.strptime(tran_time, "%d-%m-%Y %H:%M:%S")
        # 转换为时间戳
        timeStamp = int(time.mktime(timeArray))

        if timeStamp >= today_start_time:
            star = starpattern.findall(fileLineText)

            if len(star) > 0:
                pattern = re.compile('(.*)\?')

                if sys.argv[1] == 'project':
                    if '?' in star[0]:
                        result = pattern.findall(star[0])
                        if '/api/getCateRecommend' in result:

                            project_id_pattern = re.compile('project_id=(.*)&platform')
                            project_id = project_id_pattern.findall(star[0])

                            if len(project_id) > 0:
                                project_name = project.get(int(project_id[0]))
                                request_list.append(project_name)

                elif sys.argv[1] == 'all':
                    if '?' in star[0]:
                        result = pattern.findall(star[0])
                        request_list.append(request_dict.get(result[0]))
                    else:
                        request_list.append(request_dict.get(star[0]))
fileReadObj.close()

count = Counter(request_list)
result_list = []
for i in count:
    result_list.append((i, count.get(i)))
    # print(i,count.get(i))

result_list1 = sorted(result_list, key=(lambda x: x[1]), reverse=True)
for row in result_list1:
    print(row)
