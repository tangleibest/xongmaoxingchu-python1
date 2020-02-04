# coding:utf-8
import calendar
import math

from DBUtils.PooledDB import PooledDB
from flask import Flask, request, Blueprint
from flask_docs import ApiDoc
import pymysql
from flask_cors import *
import json
import time
import datetime
import pandas as pd
import redis
import numpy as np

app = Flask(__name__)
CORS(app, supports_credentials=True)
app.config['API_DOC_MEMBER'] = ['api', 'platform']

ApiDoc(app)

api = Blueprint('api', __name__)
platform = Blueprint('platform', __name__)

xm_cate_dict = {"粉面馆": "米粉面馆",
                "中式快餐": "快餐便当",
                "韩式炸鸡": "韩式料理",
                "沙拉": "轻食沙拉",
                "烧烤": "龙虾烧烤",
                "家常菜": "地方菜系",
                "肉夹馍": "夹馍饼类",
                "焖锅": "香锅干锅",
                "炸鸡": "炸鸡炸串",
                "麻辣香锅": "香锅干锅",
                "凉皮": "米粉面馆",
                "粥店": "包子粥店",
                "炸串": "炸鸡炸串",
                "西北菜": "地方菜系",
                "麻辣烫": "麻辣烫冒菜",
                "粤菜": "地方菜系",
                "西式快餐": "西式料理",
                "驴肉火烧": "夹馍饼类",
                "汤类": "暖胃粉丝汤",
                "熟食": "鸭脖卤味",
                "咖喱饭": "快餐便当",
                "中式炸鸡": "炸鸡炸串",
                "生煎": "包子粥店",
                "江浙菜": "地方菜系",
                "饺子馆": "饺子馄饨",
                " 台湾菜": "地方菜系",
                "台湾菜": "地方菜系",
                "包子": "包子粥店",
                "果汁": "甜品饮品",
                "韩式快餐": "韩式料理",
                "煎饼": "夹馍饼类",
                "比萨": "意面披萨",
                "咖啡": "甜品饮品",
                "冰淇淋": "甜品饮品",
                "粉面": "米粉面馆",
                "寿司": "日料寿司",
                "海鲜": "其他品类",
                "越南菜": "地方菜系",
                "粥": "包子粥店",
                "饮品": "甜品饮品",
                "轻食": "轻食沙拉",
                "中餐馆": "快餐便当",
                "日式快餐": "日料寿司",
                "日式料理": "日料寿司",
                "披萨": "意面披萨",
                "日本菜": "日料寿司",
                "小吃": "特色小吃",
                "汉堡": "汉堡薯条",
                "黄焖鸡": "特色小吃",
                "咖喱": "东南亚菜",
                "饮料": "甜品饮品",
                "东北菜": "地方菜系",
                "川菜": "地方菜系",
                "快餐": "快餐便当",
                "奶茶": "甜品饮品",
                "便当": "快餐便当",
                "米线": "米粉面馆",
                "龙虾": "龙虾烧烤",
                "米粉": "米粉面馆",
                "轻食沙拉": "轻食沙拉",
                "粉": "米粉面馆",
                "焖饭": "快餐便当",
                "粥类": "包子粥店",
                "水果": "甜品饮品",
                "中式快餐炒饭": "快餐便当",
                "馄饨店": "饺子馄饨",
                "": "其他品类",
                "日本料理": "日料寿司",
                "陕西菜": "地方菜系",
                "湘菜": "地方菜系",
                "意大利菜": "西式料理",
                "法国菜": "西式料理",
                "鲁菜": "地方菜系",
                "蛋糕西点": "甜品饮品",
                "湖北菜": "地方菜系",
                "素菜": "轻食沙拉",
                "牛排": "西式料理",
                "徽菜": "地方菜系",
                "煲仔饭": "快餐便当",
                "云南菜": "地方菜系",
                "中东料理": "东南亚菜",
                "德国菜": "西式料理",
                "火锅": "火锅串串",
                "蟹煲饭": "快餐便当",
                "烤鸭": "龙虾烧烤",
                "面包甜点": "甜品饮品",
                "泰国菜": "东南亚菜",
                "印尼风味": "东南亚菜",
                "日式铁板烧": "日料寿司",
                "日式面条": "日料寿司",
                "自助餐": "其他品类",
                "新疆菜": "地方菜系",
                "甜品饮品": "甜品饮品",
                "闽南菜": "地方菜系",
                "茶饮小吃": "甜品饮品",
                "北京菜": "地方菜系",
                "烤鱼": "龙虾烧烤",
                "串串香": "火锅串串",
                "贵州菜": "地方菜系",
                "甜点饮品": "甜品饮品",
                "臭豆腐": "特色小吃",
                "粉面粥": "米粉面馆",
                "印度菜": "东南亚菜",
                "韩国菜": "韩式料理",
                "手抓饼": "特色小吃",
                "清真菜": "地方菜系",
                "未知": "其他品类",
                "河南菜": "地方菜系",
                "中式便当": "快餐便当",
                "馄饨": "饺子馄饨",
                "北京城": "地方菜系",
                "简餐": "快餐便当",
                "江西菜": "地方菜系",
                "东南亚菜": "东南亚菜",
                "中餐馆陕西菜": "地方菜系",
                "日式简餐": "日料寿司",
                "健康餐": "轻食沙拉",
                "请选择二级": "其他品类",
                "日式烧烤": "龙虾烧烤",
                "菲律宾菜": "东南亚菜",
                "拉美烧烤": "龙虾烧烤",
                "俄罗斯菜": "西式料理",
                "水果捞": "甜品饮品",
                "水果店": "甜品饮品",
                "果切": "甜品饮品"
                }

xm_project_dict = {"百脑汇": ["北京", "北京市朝阳区朝外大街99号二层", "116.445409,39.923854"],
                   "曹杨路店": ["上海", "上海市普陀区曹杨路458、460、462号地下一层04室", "121.420233,31.236898"],
                   "曹杨路二店": ["上海", "上海市普陀区曹杨路店1040弄2号楼三楼03室", "121.405638,31.245883"],
                   "朝阳路红星美凯龙": ["北京", "北京市朝阳区高井村甲8号朝阳路红星美凯龙五层E8001-E8007", "116.540271,39.914985"],
                   "车公庄": ["北京", "北京市西城区车公庄大街9号五栋大楼C座1楼", "116.349191,39.933619"],
                   "大望路": ["北京", "朝阳区西大望路3号院2号楼蓝堡国际", "116.477324,39.912110"],
                   "东直门": ["北京", "北京市东城区东直门外大街42号宇飞大厦地上三层商业301-05", "116.437370,39.940220"],
                   "东直门二店": ["北京", "北京市东城区东直门外大街42号三层", "116.437566,39.940551"],
                   "方庄": ["北京", "朝阳区方庄东路饮马井45号院", "116.445719,39.866932"],
                   "古北路店": ["上海", "上海市闵行区古北路1699号B2层261室", "121.402820,31.188528"],
                   "古墩路店": ["杭州", "浙江省杭州市西湖区古墩路701号紫金广场B座501室", "120.095120,30.308657"],
                   "广渠门": ["北京", "北京市东城区广渠门内大街29号二层", "116.434295,39.893905"],
                   "广中西路店": ["上海", "上海市静安区广中西路777弄91、99号B1层B区", "121.438338,31.278491"],
                   "国安路店": ["上海", "上海市杨浦区国霞路345号203-206室", "121.505422,31.310684"],
                   "国定东路店": ["上海", "上海市国定东路293号", "121.520749,31.296839"],
                   "国贸": ["北京", "北京市朝阳区永安西里10号", "116.447587,39.906753"],
                   "国美第一城": ["北京", "北京市朝阳区青年路西里2号院11号楼商业01一层消防通道至（15）轴", "116.513105,39.932906"],
                   "国权路店": ["上海", "上海市杨浦区国权路43号地下1层01、02、03、06、07、08、09、10室", "121.518986,31.287088"],
                   "国展": ["北京", "北京市朝阳区静安西街10号国展宾馆内二层", "116.445096,39.960612"],
                   "河南北路店": ["上海", "上海市河南北路445号", "121.479984,31.249157"],
                   "河南北路二店": ["上海", "上海市静安区河南北路441号105室", "121.479738,31.249139"],
                   "呼家楼": ["北京", "北京市朝阳区呼家楼北街7号B1层", "116.466348,39.922827"],
                   "呼家楼二店": ["北京", "北京市朝阳区呼家楼北街7号B1层", "116.466348,39.922827"],
                   "华贸天地": ["北京", "北京市朝阳区清河营南街7号院8号楼02层", "116.430537,40.048231"],
                   "淮海东路店": ["上海", "上海市黄浦区淮海东路99号5楼506室", "121.480955,31.225097"],
                   "淮海东路二店": ["上海", "上海市黄浦区淮海东路99号5楼502室", "121.480955,31.225097"],
                   "汇海广场": ["深圳", "深圳市龙华区龙华街道三联社区三联创业路19号弓村新城商业中心B栋1层102/103/105室", "113.969517,22.703670"],
                   "建国门": ["北京", "北京市朝阳区建国门外大街16号东方瑞景小区", "116.442863,39.906467"],
                   "建国门贵友": ["北京", "北京市朝阳区建国门外大街甲5号", "116.450913,39.908757"],
                   "江宁路店": ["上海", "上海市普陀区江宁路1400号5幢一层-7", "121.440830,31.247180"],
                   "江宁路二店": ["上海", "上海市普陀区江宁路1400号5幢一层1室-6室", "121.440830,31.247180"],
                   "金沙江路店": ["上海", "上海市普陀区金沙江路788号4F", "121.398628,31.232255"],
                   "金沙江路二店": ["上海", "上海市普陀区金沙江路1759号1幢C1-108,C1-109室", "121.376396,31.232101"],
                   "劲松": ["北京", "北京市朝阳区南磨房路37号2层202", "116.466887,39.884817"],
                   "经开大厦": ["北京", "北京市大兴区地盛北街1号", "116.495400,39.783364"],
                   "酒仙桥": ["北京", "北京市朝阳区酒仙桥东路18号尚科办公室社区地下1层", "116.500218,39.974830"],
                   "酒仙桥二店": ["北京", "北京市朝阳区酒仙桥东路18号尚科办公室社区地上2层", "116.500218,39.974830"],
                   "凯旋路店": ["上海", "上海市徐汇区凯旋路2588号501-1B室A区", "121.429156,31.185246"],
                   "控江路店": ["上海", "上海市杨浦区控江路1555号B1层", "121.518979,31.274719"],
                   "兰溪路店": ["上海", "上海市普陀区兰溪路141号4楼C区", "121.404623,31.239418"],
                   "梨园": ["北京", "通州区云景东路1号魔方公寓4楼", "116.666818,39.885136"],
                   "理想国": ["北京", "北京朝阳区双花园南里二区11号楼#201", "116.451634,39.898776"],
                   "灵石路店": ["上海", "上海市静安区灵石路851号6幢二层T、S、R、", "121.433442,31.278409"],
                   "六里桥": ["北京", "北京市丰台区西三环南路10-3号", "116.312850,39.879153"],
                   "龙舌路店": ["杭州", "浙江省杭州市上城区龙舌路46号3楼", "120.176335,30.215449"],
                   "马家堡": ["北京", "北京市丰台区角门路19号院2号楼3层3F-2号商铺", "116.378545,39.836516"],
                   "梅华路店": ["深圳", "深圳市福田区梅华路103号光荣大厦（又名南鹏大厦）1楼及2楼东", "114.056271,22.564189"],
                   "莫干山路店": ["杭州", "浙江省拱墅区莫干山路1177号星尚发展大厦1幢222-226室", "120.138545,30.296969"],
                   "牡丹园": ["北京", "北京市海淀区花园路甲2号院4号楼2层", "116.366772,39.979986"],
                   "南京西路店": ["上海", "上海市黄浦区南京西路388号仙乐斯广场4F414-417", "121.469046,31.231442"],
                   "庆春路店": ["杭州", "浙江省杭州市下城区庆春路38号金龙财富中心3层305室", "120.182414,30.259049"],
                   "秋涛北路店": ["杭州", "浙江省杭州市秋涛北路451号4楼", "120.197465,30.289827"],
                   "日坛": ["北京", "北京市朝阳区神路街39号10幢1至2层4段39、50", "116.443404,39.919294"],
                   "软件园": ["北京", "北京市海淀区西北旺东路10号院东区1号楼B105", "116.281745,40.042669"],
                   "瑞和国际": ["北京", "北京市朝阳区定福庄北街甲15号院7号楼F3-301", "116.553348,39.922238"],
                   "瑞和国际二店": ["北京", "北京市朝阳区定福庄北街甲15号院7号楼F3-301", "116.553348,39.922238"],
                   "三元桥": ["北京", "北京市朝阳区霄云路霄云里6号一层东南侧", "116.464190,39.961761"],
                   "商城路店": ["上海", "上海市浦东新区商城路665号一幢地下一层B103", "121.517848,31.231031"],
                   "上地": ["北京", "北京市海淀区农大南路一号院1号楼-1层B102-2室", "116.311730,40.028740"],
                   "深南东路店": ["深圳", "深圳市罗湖区南湖街道深南东路4003号世界金融中心裙楼三层302", "114.114366,22.542001"],
                   "深南中路店": ["深圳", "深圳市福田区深南中路2020号茂业百货店4楼", "114.086164,22.544956"],
                   "十里堡": ["北京", "北京市朝阳区八里庄北里118号楼1层1000", "116.496839,39.928378"],
                   "十里堡二店": ["北京", "北京市朝阳区八里庄北里118号楼1层1018", "116.496839,39.928378"],
                   "十里河": ["北京", "朝阳区大羊坊路85号汇金中心B座108", "116.471395,39.860050"],
                   "石景山": ["北京", "北京市石景山区石景山路54号院7幢-1层-101", "116.185292,39.906454"],
                   "双井": ["北京", "北京市朝阳区广渠路66号院甲19号楼1层A1", "116.465519,39.891299"],
                   "双井二店": ["北京", "北京市朝阳区广渠路66号院甲19号楼2层", "116.465519,39.891299"],
                   "四川北路一店": ["上海", "上海市虹口区四川北路1885号地下一层A单元", "121.483640,31.261840"],
                   "四道口": ["北京", "北京市海淀区皂君庙14号院鑫雅苑5号楼二层", "116.338391,39.961843"],
                   "松江路店": ["上海", "上海市松江区新松江路1292弄", "121.211358,31.039823"],
                   "淞沪路店": ["上海", "上海杨浦区淞沪路161号5楼", "121.513400,31.303436"],
                   "天山路店": ["上海", "上海市长宁区天山路900号6F", "121.405047,31.211373"],
                   "天山西路店": ["上海", "上海市长宁区天山西路1068号3幢A栋B1-A、B", "121.354330,31.220256"],
                   "驼房营": ["北京", "朝阳区将台乡驼房营路与将台路交叉路口西南角京客隆超市", "116.469337,39.922377"],
                   "望京": ["北京", "北京市朝阳区望京街9号商业楼B1层08-1", "116.482756,39.989440"],
                   "望京SOHO": ["北京", "北京市朝阳区阜通东大街3号楼3层1301、1302、1303、1305、1306房屋", "116.480437,39.989732"],
                   "望京六佰本": ["北京", "北京市朝阳区望京广顺北大街六佰本", "116.466726,40.008103"],
                   "望京西园": ["北京", "北京市朝阳区望京西园三区312号楼商业楼地下一层", "116.477833,39.999570"],
                   "文二路店": ["杭州", "浙江省杭州市西湖区西溪街道文二路126号第一层及兰庭公寓2幢第二层", "120.138891,30.283350"],
                   "文三路店": ["杭州", "杭州市西湖区文三路535号莱茵达大厦二层", "120.123939,30.275992"],
                   "文一路店": ["杭州", "浙江省杭州市西湖区西溪街道文一路122-13号地下一层B1-20", "120.136831,30.289213"],
                   "汶水路店": ["上海", "上海市静安区汶水路40号39栋2F", "121.458957,31.293694"],
                   "吴中路店": ["上海", "上海市闵行区吴中路1050号5幢东楼124-125", "121.386823,31.178439"],
                   "五道口二店": ["北京", "海淀区暂安处1号蓟鑫大厦C区", "116.340098,39.988341"],
                   "五道口一店": ["北京", "北京市海淀区暂安处1号蓟鑫大厦D区301", "116.340098,39.988341"],
                   "武宁路店": ["上海", "上海市普陀区中山北路2688号238A、238B、238C室", "121.420360,31.241786"],
                   "西直门": ["北京", "西直门北大街41号天兆家园", "116.353350,39.953020"],
                   "协和路二店": ["上海", "上海市长宁区协和路788号二层202室", "121.356079,31.216720"],
                   "协和路一店": ["上海", "上海市长宁区协和路788号", "121.356079,31.216720"],
                   "斜徐路店": ["上海", "黄浦区斜徐路595号206-207室2F-4F层", "121.472622,31.206286"],
                   "新华百货": ["北京", "北京市西城区新街口北大街1号6层部分", "116.371505,39.947200"],
                   "新荟城店": ["北京", "北京市朝阳区望京东园一区120号楼-2至6层101内五层5F-6b", "116.482311,39.998540"],
                   "新荟城二店": ["北京", "北京市朝阳区望京东园一区120号新荟城", "116.481863,39.998482"],
                   "星光大道店": ["杭州", "浙江省杭州市滨江区星光大道星光国际广场1幢4层402-405", "120.209191,30.207159"],
                   "星光影视园": ["北京", "北京市大兴区春和路39号院1号楼1单元101B、102B、103B、105B、107B、及106", "116.359053,39.773025"],
                   "雅宝城": ["北京", "北京市朝阳区日坛北路19号楼B1层餐厅", "116.441679,39.918641"],
                   "延长路店": ["上海", "上海市静安区延长路50号2幢2层203室", "121.460767,31.273521"],
                   "叶家宅路店": ["上海", "上海市普陀区叶家宅100号-创享塔园区10幢-145-159单元", "121.432608,31.237550"],
                   "悠乐汇": ["北京", "北京望京园610号2层202", "116.477499,39.990678"],
                   "右安门": ["北京", "北京市丰台区右安门外大街2号迦南大厦二层", "116.365739,39.868642"],
                   "枣营麦子店": ["北京", "北京朝阳区麦子店街枣营北里19号", "116.471202,39.946293"],
                   "长虹桥": ["北京", "北京市朝阳区农展馆南路12号通广大厦4层", "116.463062,39.934231"],
                   "长虹桥二店": ["北京", "北京市朝阳区农展馆南路12号2号楼4层4001、4003室", "116.462630,39.934120"],
                   "中关村": ["北京", "北京市海淀区中关村三街6号中科资源大厦地下1层南侧B12", "116.328217,39.984981"],
                   "中山西路店": ["上海", "上海市中山西路2368号106室", "121.431328,31.181053"],
                   "铸诚大厦": ["北京", "北京中关村南大街甲6号", "116.324353,39.964480"],
                   "自空间": ["北京", "北京自空间写字园E座", "116.504842,39.902526"],
                   "星光影视园2F": ["北京", "北京市大兴区春和路39号院1号楼1单元101B、102B、103B、105B、107B、及106", "116.359053,39.773025"],
                   "星光影视园1F": ["北京", "北京市大兴区春和路39号院1号楼1单元101B、102B、103B、105B、107B、及106", "116.359053,39.773025"]
                   }


# 两个点对比返回数据
def get_two_date(results, lat1, lng1):
    first_3km_list = []
    first_2km_list = []
    first_1km_list = []
    for row in results:
        month_sale = int(row[0])
        latitude = row[1]
        longitude = row[2]
        dis = getDistance(float(latitude), float(longitude), float(lat1), float(lng1))
        if dis <= 1:
            first_3km_list.append(month_sale)
            first_2km_list.append(month_sale)
            first_1km_list.append(month_sale)
        elif dis <= 2 and dis > 1:
            first_3km_list.append(month_sale)
            first_2km_list.append(month_sale)
        elif dis <= 3 and dis > 2:
            first_3km_list.append(month_sale)
    first_3km_list = sorted(first_3km_list)
    first_2km_list = sorted(first_2km_list)
    first_1km_list = sorted(first_1km_list)
    if len(first_3km_list) == 0:
        first_median_3km = 0.0
    else:
        first_median_3km = np.median(first_3km_list)

    if len(first_2km_list) != 0:
        first_median_2km = np.median(first_2km_list)
    else:
        first_median_2km = 0.0

    if len(first_1km_list) != 0:
        first_median_1km = np.median(first_1km_list)
    else:
        first_median_1km = 0.0

    first_sum_3km = np.sum(first_3km_list)
    first_sum_2km = np.sum(first_2km_list)
    first_sum_1km = np.sum(first_1km_list)
    first_shop_count_3km = len(first_3km_list)
    first_shop_count_2km = len(first_2km_list)
    first_shop_count_1km = len(first_1km_list)
    dict_first_3km = {}
    dict_first_3km['sale_median'] = first_median_3km
    dict_first_3km['sale_sum'] = float(first_sum_3km)
    dict_first_3km['shop_count'] = float(first_shop_count_3km)
    if float(first_shop_count_3km) != 0.0:
        shop_sale_ave = int(float(first_sum_3km) / float(first_shop_count_3km))
    else:
        shop_sale_ave = 0
    dict_first_3km['shop_sale_ave'] = shop_sale_ave

    dict_first_2km = {}
    dict_first_2km['sale_median'] = first_median_2km
    dict_first_2km['sale_sum'] = float(first_sum_2km)
    dict_first_2km['shop_count'] = float(first_shop_count_2km)
    if float(first_shop_count_2km) != 0.0:
        shop_sale_ave = int(float(first_sum_2km) / float(first_shop_count_2km))
    else:
        shop_sale_ave = 0
    dict_first_2km['shop_sale_ave'] = shop_sale_ave

    dict_first_1km = {}
    dict_first_1km['sale_median'] = first_median_1km
    dict_first_1km['sale_sum'] = float(first_sum_1km)
    dict_first_1km['shop_count'] = float(first_shop_count_1km)
    if float(first_shop_count_1km) != 0.0:
        shop_sale_ave = int(float(first_sum_1km) / float(first_shop_count_1km))
    else:
        shop_sale_ave = 0
    dict_first_1km['shop_sale_ave'] = shop_sale_ave

    first_list = [dict_first_3km, dict_first_2km, dict_first_1km]
    return first_list


# 两个点对比下的建筑物信息
def get_two_data_buildings(results, lat1, lng1):
    first_3km = 0
    first_2km = 0
    first_1km = 0
    for row in results:
        longitude = row[0]
        latitude = row[1]
        dis = getDistance(float(latitude), float(longitude), float(lat1), float(lng1))
        if dis <= 1:
            first_1km += 1
            first_2km += 1
            first_3km += 1
        elif dis <= 2 and dis > 1:
            first_2km += 1
            first_3km += 1
        elif dis <= 3 and dis > 2:
            first_3km += 1
    return [first_3km, first_2km, first_1km]


# 两个点对比返回客单价
def get_two_ave(results3, lat1, lng1):
    sum_income_3km = 0
    month_sale_3km = 0
    sum_income_2km = 0
    month_sale_2km = 0
    sum_income_1km = 0
    month_sale_1km = 0
    for row3 in results3:
        month_sale = int(row3[0])
        latitude = row3[1]
        longitude = row3[2]
        ave = float(row3[4])

        dis = getDistance(float(latitude), float(longitude), float(lat1), float(lng1))
        if dis <= 1:
            income = month_sale * ave
            sum_income_3km += income
            month_sale_3km += month_sale
            sum_income_2km += income
            month_sale_2km += month_sale
            sum_income_1km += income
            month_sale_1km += month_sale
        elif dis <= 2 and dis > 1:
            income = month_sale * ave
            sum_income_3km += income
            month_sale_3km += month_sale
            sum_income_2km += income
            month_sale_2km += month_sale
        elif dis <= 3 and dis > 2:
            income = month_sale * ave
            sum_income_3km += income
            month_sale_3km += month_sale
    if month_sale_3km != 0:
        first_ave_money_3km = round(sum_income_3km / month_sale_3km, 2)
    else:
        first_ave_money_3km = 0

    if month_sale_2km != 0:
        first_ave_money_2km = round(sum_income_2km / month_sale_2km, 2)
    else:
        first_ave_money_2km = 0

    if month_sale_1km != 0:
        first_ave_money_1km = round(sum_income_1km / month_sale_1km, 2)
    else:
        first_ave_money_1km = 0

    li = [first_ave_money_3km, first_ave_money_2km, first_ave_money_1km]
    return li


# 获取日期
def getdate(beforeOfDay, strf):
    today = datetime.datetime.now()
    # 计算偏移量
    offset = datetime.timedelta(days=-beforeOfDay)
    # 获取想要的日期的时间
    re_date = (today + offset).strftime(strf)
    return re_date


# 获前几个月第一天
def getTheMonth(n, strf):
    date = datetime.datetime.today()
    month = date.month
    year = date.year
    for i in range(n):
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
    return datetime.date(year, month, 1).strftime(strf)


# 把格式化转化成时间戳
def str_to_timestamp(str_time=None, format='%Y-%m-%d'):
    if str_time:
        time_tuple = time.strptime(str_time, format)  # 把格式化好的时间转换成元祖
        result = time.mktime(time_tuple)  # 把时间元祖转换成时间戳
        return int(result)
    return int(time.time())


# 获取上个月第一天时间戳
up_time = int(str_to_timestamp(getTheMonth(1, '%Y-%m-%d')))
# 获取上个月最后一天时间戳
to_time = int(time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month, 1).timetuple()))

# 获取上个月第一天格式化时间
fist = getTheMonth(1, '%Y-%m-%d')
# six_month_time = int(str_to_timestamp(getTheMonth(1,'%Y-%m-%d')))
# last_month = datetime.date.today().month - 1


# 计算两个经纬度之间的距离
EARTH_REDIUS = 6378.137


def rad(d):
    return d * math.pi / 180.0


def getDistance(lat1, lng1, lat2, lng2):
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1 - radLat2
    b = rad(lng1) - rad(lng2)
    s = 2 * math.asin(math.sqrt(math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(
        math.sin(b / 2), 2)))
    s = s * EARTH_REDIUS
    return s


# redis连接池
pool = redis.ConnectionPool(host='139.199.112.205', port=6379, password='xmxc1234', db=1, decode_responses=True)
redis_conn = redis.Redis(connection_pool=pool)
# 数据库连接池
pool_mapmarkeronline = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                                db='mapmarkeronline', port=62864)

pool_test = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                     db='test', port=62864)

pool_project = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei',
                        passwd='tanglei', db='commerce',
                        port=3306)

pool_statistics = PooledDB(pymysql, 5, host='39.104.130.52', user='root', passwd='xmxc1234', db='statistics',
                           port=3520)


# 返回写字楼数据
@app.route('/api/getOfficeInfo')
def get_office_info():
    """返回写字楼详细列表

        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
        |    distance    |    半径    |    string   |    1.5    |
        |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）    |    string   |    1   |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    office_name    |    写字楼名称    |    string   |
        |    distance    |    与所选点距离    |    double   |

        #### return
        - ##### json
        > [{"office_name": "恒通商务园", "office_rent": "5.5", "distance": 0.3893995993948515}, {"office_name": "瀚海国际大厦 ", "office_rent": "6.9", "distance": 0.5507685602422158}]
        @@@
        """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    city_id = request.args.get('city_id')
    coordinate = request.args.get('coordinate')
    distance = request.args.get('distance')

    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    city = ''
    if city_id == '1':
        city = '北京市'
    elif city_id == '2':
        city = '上海市'
    elif city_id == '3':
        city = '杭州市'
    elif city_id == '4':
        city = '深圳市'
    up_lat = float(lat) + 0.04
    down_lat = float(lat) - 0.04
    up_lng = float(lng) + 0.05
    down_lng = float(lng) - 0.05
    sql = "SELECT b_name,longitude,latitude from t_map_office_building where longitude !='不明' and b_city='%s'  and latitude BETWEEN '%s' and " \
          "'%s' and longitude BETWEEN '%s' and '%s'" % (city, down_lat, up_lat, down_lng, up_lng)
    sq = []
    cur.execute(sql)
    results = cur.fetchall()
    for row in results:
        data = {}
        b_name = row[0]
        longitude = row[1]
        latitude = row[2]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            # office_count=office_count+1
            data['office_name'] = b_name
            data['distance'] = dis
            sq.append(data)
    sq = sorted(sq, key=lambda x: x['distance'])
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回小区数据
@app.route('/api/getHousingInfo')
def get_housing_info():
    """返回小区详细数据列表

            @@@
            #### 参数列表

            | 参数 | 描述 | 类型 | 例子 |
            |--------|--------|--------|--------|
            |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
            |    distance    |    半径    |    string   |    1.5    |
            |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）    |    string   |    1   |

            #### 字段解释

            | 名称 | 描述 | 类型 |
            |--------|--------|--------|--------|
            |    housing_name    |    小区名称    |    string   |
            |    distance    |    与所选点距离    |    double   |

            #### return
            - ##### json
            > [{"housing_name": "芳园里", "distance": 0.5681041154328037}, {"housing_name": "银河湾", "distance": 0.8682477990692311}]
            @@@
            """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    city_id = request.args.get('city_id')
    coordinate = request.args.get('coordinate')

    distance = request.args.get('distance')

    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    city = ''
    if city_id == '1':
        city = '北京市'
    elif city_id == '2':
        city = '上海市'
    elif city_id == '3':
        city = '杭州市'
    elif city_id == '4':
        city = '深圳市'
    up_lat = float(lat) + 0.04
    down_lat = float(lat) - 0.04
    up_lng = float(lng) + 0.05
    down_lng = float(lng) - 0.05
    sql = "SELECT uptown_name,longitude,latitude from t_map_lianjia_uptown where longitude !='不明' and " \
          "city='%s' and latitude BETWEEN '%s' and '%s' and longitude BETWEEN '%s' and '%s'" % (
              city, down_lat, up_lat, down_lng, up_lng)
    sq = []
    cur.execute(sql)
    results = cur.fetchall()
    # 遍历结果
    for row in results:
        data = {}
        uptown_name = row[0]
        longitude = row[1]
        latitude = row[2]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            data['housing_name'] = uptown_name
            data['distance'] = dis
            sq.append(data)
    sq = sorted(sq, key=lambda x: x['distance'])
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回周边数据下周边配套数据及商场、酒店、高校、医院详情(餐饮数量是两个平台总和)
@app.route('/api/getBaicInfo')
def get_baic_info():
    """返回周边数据下周边配套数据及商场、酒店、高校、医院详情(餐饮数量是两个平台总和)

        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
        |    distance    |    半径    |    string   |    1.5    |
        |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）    |    string   |    1   |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    baic_info    |    周边配套数量统计    |    object   |
        |    └house_count    |    小区数量    |    int   |
        |    └office_count    |    写字楼数量    |    int   |
        |    └food_count    |    餐饮数量    |    int   |
        |    └hospital_count    |    医院数量    |    int   |
        |    └school_count    |    学校数量    |    int   |
        |    └market_count    |    商场数量    |    int   |
        |    └hotel_count    |    酒店数量    |    int   |
        |    hotel_info[]    |    酒店列表    |    list   |
        |    └hotel_name    |    酒店名称    |    string   |
        |    └hotel_dis   |    酒店与所选点距离    |    double   |
        |    market_info[]   |    商场列表    |    list   |
        |    └market_name   |    商场名称    |    string   |
        |    └market_dis   |    商场与所选点距离    |    double   |
        |    school_info[]   |    学校列表    |    list   |
        |    └school_name   |    学校名称    |    string   |
        |    └school_dis   |    学校与所选点距离    |    double   |

        #### return
        - ##### json
        > [{"hotel_info": [{{"hotel_name": "秋果酒店(798艺术区店)", "hotel_dis": 1.3007605484401805}, {"hotel_name": "北京酒仙酒店公寓", "hotel_dis": 1.1329057421469126}], "market_info": [{"market_name": "颐堤港", "market_dis": 0.7547364429446455}{"market_name": "北京华联(颐堤港超市)", "market_dis": 0.7700639203387806}], "baic_info": {"house_count": 66, "office_count": 48, "food_count": 505, "hospital_count": 5, "school_count": 1, "market_count": 12, "hotel_count": 35}, "school_info": [{"school_name": "北京信息职业技术学院", "school_dis": 1.0214379968385598}], "hospital_info": [{"hospital_name": "朝阳区将台地区将府家园社区卫生服务站", "hospital_dis": 0.8824207876096655}]}]
        @@@
        """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    city_id = request.args.get('city_id')
    coordinate = request.args.get('coordinate')

    distance = request.args.get('distance')

    cheak_key = '/api/getBaicInfo' + city_id + coordinate + distance
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondatar = redis_conn.get(cheak_key)
    else:
        lat = str(coordinate).split(",")[1]
        lng = str(coordinate).split(",")[0]
        up_lat = float(lat) + 0.04
        down_lat = float(lat) - 0.04
        up_lng = float(lng) + 0.05
        down_lng = float(lng) - 0.05
        city_name = ""
        city = ''
        if city_id == "1":
            city_name = "北京市"
            city = 'beijing'
        elif city_id == "2":
            city_name = "上海市"
            city = 'shanghai'
        elif city_id == "3":
            city_name = "杭州市"
            city = 'hangzhou'
        elif city_id == "4":
            city_name = "深圳市"
            city = 'shenzhen'
        sql_house = "SELECT longitude,latitude from t_map_lianjia_uptown where longitude !='不明' and city='%s'" % city_name
        sql_office = "SELECT longitude,latitude from t_map_office_building where longitude !='不明' and b_city='%s'" % city_name
        sql_food = "SELECT latitude,longitude from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and latitude BETWEEN '%s' and  '%s' and longitude BETWEEN '%s' and '%s' union all" \
                   " SELECT latitude,longitude from t_map_client_elm_%s_mark where update_time BETWEEN %s and %s and latitude BETWEEN '%s' and  '%s' and longitude BETWEEN '%s' and '%s'" % (
                       city, up_time, to_time, down_lat, up_lat, down_lng, up_lng, city, up_time, to_time, down_lat,
                       up_lat, down_lng, up_lng)
        sq = []

        # 执行小区sql，获取数据
        cur.execute(sql_house)
        results_house = cur.fetchall()
        # 遍历结果
        house_count = 0
        all_data = {}

        for row in results_house:
            longitude = row[0]
            latitude = row[1]
            dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
            if dis <= float(distance):
                house_count = house_count + 1
        # 执行写字楼sql，获取数据
        cur.execute(sql_office)
        results_office = cur.fetchall()
        office_count = 0
        for row in results_office:
            longitude = row[0]
            latitude = row[1]
            dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
            if dis <= float(distance):
                office_count = office_count + 1

        # 获取餐饮信息
        cur.execute(sql_food)
        results_food = cur.fetchall()
        food_count = 0
        for row in results_food:
            dis = getDistance(float(row[0]), float(row[1]), float(lat), float(lng))
            if dis <= float(distance):
                food_count = food_count + 1

        # 获取医院个数
        sql_hospital = "SELECT hospital_lat,hospital_lng,hospital_name from t_map_hospital_info WHERE hospital_lat !='不明'"
        cur.execute(sql_hospital)
        results_hospital = cur.fetchall()
        hospital_count = 0
        hospital_info = []
        for row_hos in results_hospital:
            hospital_data = {}
            dis = getDistance(float(row_hos[0]), float(row_hos[1]), float(lat), float(lng))
            if dis <= float(distance):
                hospital_count += 1
                hospital_data['hospital_name'] = row_hos[2]
                hospital_data['hospital_dis'] = dis
                hospital_info.append(hospital_data)
        hospital_info = sorted(hospital_info, key=lambda x: x['hospital_dis'])
        # 获取学校信息
        sql_school = "SELECT school_lat,school_lng,school_name from t_map_school_info WHERE school_city='%s' and  school_lng !='不明'" % city_name
        cur.execute(sql_school)
        results_school = cur.fetchall()
        school_count = 0
        school_info = []
        for row_hos in results_school:
            school_data = {}
            dis = getDistance(float(row_hos[0]), float(row_hos[1]), float(lat), float(lng))
            if dis <= float(distance):
                school_count += 1
                school_data["school_name"] = row_hos[2]
                school_data["school_dis"] = dis
                school_info.append(school_data)
        school_info = sorted(school_info, key=lambda x: x['school_dis'])
        # 获取酒店、商场信息
        city_name2 = city_name[0:2]
        sql_buildings = "SELECT buildings_latitude,buildings_longitude,total_type,buildings_name from t_map_buildings where city_name='%s'" % city_name2
        cur.execute(sql_buildings)
        results_buildings = cur.fetchall()
        market_count = 0
        hotel_count = 0
        market_info = []
        hotel_info = []
        for row_hos in results_buildings:
            dis = getDistance(float(row_hos[0]), float(row_hos[1]), float(lat), float(lng))
            market_data = {}
            hotel_data = {}
            if dis <= float(distance):
                if row_hos[2] == '商场':
                    market_count += 1
                    market_data["market_name"] = row_hos[3]
                    market_data["market_dis"] = dis
                    market_info.append(market_data)
                elif row_hos[2] == '酒店':
                    hotel_count += 1
                    hotel_data['hotel_name'] = row_hos[3]
                    hotel_data['hotel_dis'] = dis
                    hotel_info.append(hotel_data)
        market_info = sorted(market_info, key=lambda x: x['market_dis'])
        hotel_info = sorted(hotel_info, key=lambda x: x['hotel_dis'])

        # 整和数据返回
        baic_info = {}
        baic_info['house_count'] = house_count
        baic_info['office_count'] = office_count
        baic_info['food_count'] = food_count
        baic_info['hospital_count'] = hospital_count
        baic_info['school_count'] = school_count
        baic_info['market_count'] = market_count
        baic_info['hotel_count'] = hotel_count

        all_data['baic_info'] = baic_info
        all_data['hotel_info'] = hotel_info
        all_data['market_info'] = market_info
        all_data['school_info'] = school_info
        all_data['hospital_info'] = hospital_info

        sq.append(all_data)
        jsondatar = json.dumps(sq, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondatar)
        redis_conn.expire(cheak_key, 2592000)
        db.close()
    return jsondatar


# 返回月销量统计(数量是两个平台总和)
@app.route('/api/getShopBasic')
def get_shop_basic():
    """月销量统计(数量是两个平台总和)

            @@@
            #### 参数列表

            | 参数 | 描述 | 类型 | 例子 |
            |--------|--------|--------|--------|
            |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
            |    distance    |    半径    |    string   |    1.5    |
            |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）    |    string   |    1   |



            #### 字段解释

            | 名称 | 描述 | 类型 |
            |--------|--------|--------|--------|
            |    update_time    |    更新时间    |    int   |
            |    city_sale_money    |    全城月销量    |    int   |
            |    city_sale_count    |    全城店铺数   |    int   |
            |    dis_sale_money    |    区域销量总数    |    int   |
            |    dis_sale_count    |    区域商铺总数   |    int   |
            |    dis_ave_shop_sale    |    区域单店均销量    |    double   |

            #### return
            - ##### json
            > [{"city_sale_money": 37508086, "dis_sale_money": 482470, "dis_sale_count": 422, "dis_ave_shop_sale": 1143.2938388625591}]
            @@@
            """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    cur1 = db.cursor()
    city_id = request.args.get('city_id')
    coordinate = request.args.get('coordinate')
    distance = request.args.get('distance')
    cheak_key = '/api/getShopBasic' + city_id + coordinate + distance
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondatar = redis_conn.get(cheak_key)
    else:
        city = ''
        if city_id == '1':
            city = 'beijing'
        elif city_id == '2':
            city = 'shanghai'
        elif city_id == '3':
            city = 'hangzhou'
        elif city_id == '4':
            city = 'shenzhen'

        lat = str(coordinate).split(",")[1]
        lng = str(coordinate).split(",")[0]
        up_lat = float(lat) + 0.04
        down_lat = float(lat) - 0.04
        up_lng = float(lng) + 0.05
        down_lng = float(lng) - 0.05
        sql = "SELECT month_sale_num,latitude,longitude,update_time  from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and month_sale_num!=0 and own_set_cate not in ('其他品类','超市便利') and latitude BETWEEN '%s' and  '%s' and longitude BETWEEN '%s' and '%s' union all " \
              "SELECT month_sale_num,latitude,longitude,update_time  from t_map_client_elm_%s_mark where update_time BETWEEN %s and %s and month_sale_num!=0 and own_set_cate not in ('其他品类','超市便利') and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  '%s' and longitude BETWEEN '%s' and '%s'" % (
                  city, up_time, to_time, down_lat, up_lat, down_lng, up_lng, city, up_time, to_time, down_lat, up_lat,
                  down_lng, up_lng)
        # sql = "SELECT month_sale_num,latitude,longitude,update_time  from t_map_client_mt_%s_mark where update_count=11 and month_sale_num!=0 and own_set_cate not in ('其他品类','超市便利') and latitude BETWEEN '%s' and  '%s' and longitude BETWEEN '%s' and '%s' union all " \
        #       "SELECT month_sale_num,latitude,longitude,update_time  from t_map_client_elm_%s_mark where update_time BETWEEN %s and %s and month_sale_num!=0 and own_set_cate not in ('其他品类','超市便利') and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  '%s' and longitude BETWEEN '%s' and '%s'" % (
        #           city, down_lat, up_lat, down_lng, up_lng, city, up_time, to_time, down_lat, up_lat,
        #           down_lng, up_lng)

        cur.execute(sql)
        results = cur.fetchall()
        sq = []
        sql_city = "SELECT sum(month_sale_num),count(*) from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and own_set_cate not in ('其他品类','超市便利') union all SELECT sum(month_sale_num),count(*) from t_map_client_elm_%s_mark where update_time BETWEEN %s and %s and own_set_cate not in ('其他品类','超市便利')" % (
            city, up_time, to_time, city, up_time, to_time)
        cur1.execute(sql_city)
        results1 = cur1.fetchall()
        city_month_sale = int(results1[0][0]) + int(results1[1][0])
        city_shop_count = int(results1[0][1]) + int(results1[1][1])
        if len(results) > 0:
            update_time = results[0][3]
            dis_sale_money = 0
            dis_sale_count = 0
            data = {}
            for row in results:

                month_sale = int(row[0])
                latitude = row[1]
                longitude = row[2]

                dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
                if dis <= float(distance):
                    dis_sale_money = dis_sale_money + month_sale
                    dis_sale_count = dis_sale_count + 1
            dis_ave_shop_sale = dis_sale_money / dis_sale_count

            data['update_time'] = int(update_time)
            data['city_sale_money'] = city_month_sale
            data['city_sale_count'] = city_shop_count
            data['dis_sale_money'] = int(dis_sale_money)
            data['dis_sale_count'] = int(dis_sale_count)
            data['dis_ave_shop_sale'] = float(dis_ave_shop_sale)
            sq.append(data)
        else:
            update_time = int(
                time.mktime(datetime.date(datetime.date.today().year, datetime.date.today().month - 1, 1).timetuple()))
            data = {}
            data['update_time'] = int(update_time)
            data['city_sale_money'] = city_month_sale
            data['city_sale_count'] = city_shop_count
            data['dis_sale_money'] = 0
            data['dis_sale_count'] = 0
            data['dis_ave_shop_sale'] = 0
            sq.append(data)
        jsondatar = json.dumps(sq, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondatar)
        redis_conn.expire(cheak_key, 2592000)
        db.close()
    return jsondatar


# 返回餐饮人均消费及月销量统计下的区域客单价（只计算美团）
@app.route('/api/getAveMoney')
def get_ave_money():
    """返回餐饮人均消费及月销量统计下的区域客单价（只计算美团）
        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
        |    distance    |    半径    |    string   |    1.5    |
        |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）    |    string   |    1   |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    city_ave_shop_sale    |    北京人均    |    double   |
        |    dis_ave_shop_sale    |    周边人均    |    double   |

        #### return
        - ##### json
        > [{"city_ave_shop_sale": 23.536748880587783, "dis_ave_shop_sale": 26.49526066350711}]
        @@@
        """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    cur1 = db.cursor()
    city_id = request.args.get('city_id')
    coordinate = request.args.get('coordinate')
    distance = request.args.get('distance')
    cheak_key = '/api/getAveMoney' + city_id + coordinate + distance
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondatar = redis_conn.get(cheak_key)
    else:
        lat = str(coordinate).split(",")[1]
        lng = str(coordinate).split(",")[0]
        city = ''
        if city_id == '1':
            city = 'beijing'
        elif city_id == '2':
            city = 'shanghai'
        elif city_id == '3':
            city = 'hangzhou'
        elif city_id == '4':
            city = 'shenzhen'

        up_lat = float(lat) + 0.04
        down_lat = float(lat) - 0.04
        up_lng = float(lng) + 0.05
        down_lng = float(lng) - 0.05
        sql = "SELECT average_price,latitude,longitude,month_sale_num from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and latitude BETWEEN '%s' and '%s' and longitude BETWEEN '%s' and '%s' and average_price is not NULL and average_price !='' and own_set_cate !='超市便利'" % (
            city, up_time, to_time, down_lat, up_lat, down_lng, up_lng)

        city_sql = "SELECT SUM(average_price*month_sale_num)/sum(month_sale_num) from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and average_price is not NULL and average_price !=''  and own_set_cate !='超市便利' GROUP BY update_count " % (
            city, up_time, to_time)
        cur.execute(sql)
        results = cur.fetchall()

        cur1.execute(city_sql)
        results1 = cur1.fetchall()
        city_ave_shop_sale = results1[0][0]
        sq = []

        dis_sale_money = 0.0
        dis_sale_count = 0
        data = {}
        for row in results:
            average_price = row[0]
            latitude = row[1]
            longitude = row[2]

            dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
            if dis <= float(distance):
                income = int(row[3]) * float(average_price)
                dis_sale_money += income
                dis_sale_count += row[3]
        if dis_sale_count == 0:
            dis_ave_shop_sale = 0
        else:
            dis_ave_shop_sale = dis_sale_money / dis_sale_count
        data['city_ave_shop_sale'] = city_ave_shop_sale
        data['dis_ave_shop_sale'] = dis_ave_shop_sale
        sq.append(data)
        jsondatar = json.dumps(sq, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondatar)
        redis_conn.expire(cheak_key, 2592000)
        db.close()
    return jsondatar


# 返回周边品类榜（根据参数区分平台，客单价只有美团）
@app.route('/api/getCate')
def get_cate():
    """返回周边品类榜（根据参数区分平台，客单价只有美团）
            @@@
            #### 参数列表

            | 参数 | 参数解释 | 类型 | 例子 |
            |--------|--------|--------|--------|
            |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
            |    distance    |    半径    |    string   |    1.5    |
            |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）    |    string   |    1   |
            |    platform    |    外卖平台（mt、elm）    |    string   |    elm   |

            #### 字段解释

            | 名称 | 解释 | 类型 |
            |--------|--------|--------|--------|
            |    cate_name    |    品类名称    |    string   |
            |    cate_sum    |    该品类销售总单量    |    int   |
            |    cate_count    |    该品类下商户数    |    int   |
            |    cate_ave    |    该品类客单价    |    double   |

            #### return
            - ##### json
            > [{"cate_name": "东北菜", "cate_sum": 18043, "cate_count": 10, "cate_ave": 27.8}, {"cate_name": "云南菜", "cate_sum": 1384, "cate_count": 3, "cate_ave": 40.0}]
            @@@
            """
    city_id = request.args.get('city_id')
    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')
    distance = request.args.get('distance')
    cheak_key = '/api/getCate' + city_id + platform + coordinate + distance
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondatar = redis_conn.get(cheak_key)
    else:
        lat = str(coordinate).split(",")[1]
        lng = str(coordinate).split(",")[0]
        db = pool_mapmarkeronline.connection()
        cur = db.cursor()
        if city_id == '1':
            city = 'beijing'
        elif city_id == '2':
            city = 'shanghai'
        elif city_id == '3':
            city = 'hangzhou'
        elif city_id == '4':
            city = 'shenzhen'

        up_lat = float(lat) + 0.04
        down_lat = float(lat) - 0.04
        up_lng = float(lng) + 0.05
        down_lng = float(lng) - 0.05
        sql = "SELECT latitude,longitude,own_set_cate,month_sale_num FROM t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
              "'%s' and longitude BETWEEN '%s' and '%s' " % (
                  platform, city, up_time, to_time, down_lat, up_lat, down_lng, up_lng)
        cur.execute(sql)
        results = cur.fetchall()

        key_value = []
        data_value = []
        month_sale_list = {}
        for row in results:
            latitude = row[0]
            longitude = row[1]
            dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
            if dis <= float(distance):
                key_value.append(row[2])
                data_value.append(int(row[3]))
        month_sale_list['key'] = key_value
        month_sale_list['data'] = data_value
        df = pd.DataFrame(month_sale_list)
        kk = df.groupby(['key'], as_index=False)['data'].sum()
        kk_count = df.groupby(['key'], as_index=False)['data'].count()
        sq = []
        cate_name = []
        cate_name1 = []
        cate_name2 = []
        cate_sum_list = []
        cate_count_list = []
        kk_values = kk.values
        kk_count_values = kk_count.values
        for a in kk_values:
            cate_name.append(a[0])
            cate_sum_list.append(a[1])
        zip1 = zip(cate_name, cate_sum_list)
        sorted1 = sorted(zip1, key=(lambda x: x[0]))
        for a in kk_count_values:
            cate_name1.append(a[0])
            cate_count_list.append(a[1])
        zip2 = zip(cate_name1, cate_count_list)
        sorted2 = sorted(zip2, key=(lambda x: x[0]))

        if platform == 'mt':
            key_ave = []
            data_ave = []
            ave_list = {}
            sql_ave = "SELECT latitude,longitude,own_set_cate,average_price FROM t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and average_price is not null and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                      "'%s' and longitude BETWEEN '%s' and '%s' " % (
                          city, up_time, to_time, down_lat, up_lat, down_lng, up_lng)
            cur.execute(sql_ave)
            results_ave = cur.fetchall()
            for row in results_ave:
                latitude_ave = row[0]
                longitude_ave = row[1]
                dis_ave = getDistance(float(latitude_ave), float(longitude_ave), float(lat), float(lng))
                if dis_ave <= float(distance):
                    key_ave.append(row[2])
                    data_ave.append(int(row[3]))
            ave_list['key_ave'] = key_ave
            ave_list['data_ave'] = data_ave
            df = pd.DataFrame(ave_list)
            kk_ave = df.groupby(['key_ave'], as_index=False)['data_ave'].mean()

            cate_ave = []
            kk_ave_values = kk_ave.values
            for a in kk_ave_values:
                cate_name2.append(a[0])
                cate_ave.append(a[1])
            zip3 = zip(cate_name2, cate_ave)
            sorted3 = sorted(zip3, key=(lambda x: x[0]))

            sorted1.extend(sorted2)
            sorted1.extend(sorted3)

            d = dict()
            for item in sorted1:
                if item[0] in d:
                    d[item[0]].append(item[1])
                else:
                    d[item[0]] = [item[1]]

            res = []
            res2 = []
            for k, v in d.items():
                v.insert(0, k)
                res.append(v)
            for abc in res:
                if len(abc) <= 3:
                    abc.append(0)
                res2.append(abc)
            res2 = sorted(res2, key=lambda x: x[2], reverse=True)
            for row in res2:
                all_cate = {}
                all_cate['cate_name'] = row[0]
                all_cate['cate_sum'] = row[1]
                all_cate['cate_count'] = row[2]
                all_cate['cate_ave'] = row[3]
                sq.append(all_cate)
            jsondatar = json.dumps(sq, ensure_ascii=False)
            redis_conn.set(cheak_key, jsondatar)
            redis_conn.expire(cheak_key, 2592000)
        elif platform == 'elm':
            sorted1.extend(sorted2)
            d = dict()
            for item in sorted1:
                if item[0] in d:
                    d[item[0]].append(item[1])
                else:
                    d[item[0]] = [item[1]]

            res = []
            res2 = []
            for k, v in d.items():
                v.insert(0, k)
                res.append(v)
            for abc in res:
                if len(abc) <= 3:
                    abc.append(0)
                res2.append(abc)
            res2 = sorted(res2, key=lambda x: x[1], reverse=True)

            for row in res2:
                all_cate = {}
                all_cate['cate_name'] = row[0]
                all_cate['cate_sum'] = row[1]
                all_cate['cate_count'] = row[2]
                sq.append(all_cate)
            jsondatar = json.dumps(sq, ensure_ascii=False)
            redis_conn.set(cheak_key, jsondatar)
            redis_conn.expire(cheak_key, 2592000)
        db.close()
    redis_conn.close()
    return jsondatar


# 返回菜品销量榜（根据参数区分平台）
@app.route('/api/getFood')
def get_food():
    """返回菜品月度销量榜（根据参数区分平台）

        @@@
        #### 参数列表

        | 参数 | 参数解释 | 类型 | 例子 | 备注 |
        |--------|--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |       |
        |    distance    |    半径    |    string   |    1.5    |       |
        |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）    |    string   |    1   |
        |    platform    |    外卖平台（mt、elm）    |    string   |    elm   |       |
        |    cate    |    品类    |    string   |    东北菜   |    需要从周边品类榜中获取   |

        #### 字段解释

        | 名称 | 解释 | 类型 |
        |--------|--------|--------|--------|
        |    food_name    |    菜品名称    |    string   |
        |    food_sale_num    |    菜品销量    |    string   |


        #### return
        - ##### json
        > [{"food_name": "春饼", "food_sale_num": "24000"}, {"food_name": "玉米渣粥", "food_sale_num": "14000"}]
        @@@
        """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    city_id = request.args.get('city_id')

    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')

    distance = request.args.get('distance')
    cate = request.args.get('cate')

    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    city = ''
    if city_id == '1':
        city = 'beijing'
    elif city_id == '2':
        city = 'shanghai'
    elif city_id == '3':
        city = 'hangzhou'
    elif city_id == '4':
        city = 'shenzhen'
    sql = "SELECT latitude,longitude,shop_id,update_count FROM t_map_client_%s_%s_mark where update_time BETWEEN %s and %s  and own_set_cate='%s' and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') " % (
        platform, city, up_time, to_time, cate)
    cur.execute(sql)
    results = cur.fetchall()
    update_count = results[0][3]
    str_shop_id = ''
    for row in results:
        latitude = row[0]
        longitude = row[1]
        dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
        if dis <= float(distance):
            shop_id = row[2]
            str_shop_id = str_shop_id + "'" + shop_id + "',"

    str_shop_id = str_shop_id.strip(',')
    sql_food = ''
    if platform == 'mt':
        sql_food = "SELECT food_name,SUM(month_sale) sum_sale from t_map_client_%s_%s_mark_food where month_sale is not null and update_count=%s and client_id in (%s) GROUP BY food_name order by sum_sale desc limit 50" % (
            platform, city, update_count, str_shop_id)
    elif platform == 'elm':
        sql_food = "SELECT food_name,SUM(month_sale) sum_sale from t_map_client_%s_%s_mark_food where month_sale is not null and update_count=%s and shop_id in (%s) GROUP BY food_name order by sum_sale desc limit 50" % (
            platform, city, update_count, str_shop_id)

    cur.execute(sql_food)
    results_food = cur.fetchall()
    sq = []
    for a in results_food:
        food_dicr = {}
        food_dicr['food_name'] = a[0]
        food_dicr['food_sale_num'] = str(a[1])
        sq.append(food_dicr)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db.close()
    return jsondatar


# 返回周边数据、门店数据下销售趋势（根据参数区分平台）
@app.route('/api/getLineChart')
def get_line_chart():
    """返回周边数据、门店数据下销售趋势（根据参数区分平台）

        @@@
        #### 参数列表

        | 参数 | 参数解释 | 类型 | 例子 | 备注 |
        |--------|--------|--------|--------|--------|
        |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |       |
        |    distance    |    半径    |    string   |    1.5    |       |
        |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）    |    string   |    1   |
        |    platform    |    外卖平台（mt、elm）    |    string   |    elm   |       |
        |    project_id    |    项目id    |    string   |    1548233981574173   |       |

        #### 字段解释

        | 名称 | 解释 | 类型 |
        |--------|--------|--------|--------|
        |    month    |    月份    |    string   |
        |    city_sale_num    |    全城单商户月均销量    |    string   |
        |    dis_sale_num    |    区域单商户月均销量  |    doublo   |
        |    dis_sale_num    |    门店单商户月均销量    |    double   |

        #### return
        - ##### json
        > [{"month": "4月", "city_sale_num": "719.5404", "dis_sale_num": 862.672932330827, "xmxc_sale_num": 4902.0}]
        @@@
        """

    db = pool_mapmarkeronline.connection()
    sta_db = pool_statistics.connection()
    cur_sta = sta_db.cursor()
    cur = db.cursor()
    city_id = request.args.get('city_id')
    platform = request.args.get('platform')
    coordinate = request.args.get('coordinate')
    distance = request.args.get('distance')
    project_id = request.args.get('project_id')
    cheak_key = '/api/getLineChart' + city_id + coordinate + distance + platform + project_id
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondatar = redis_conn.get(cheak_key)
    else:
        city = ''
        if city_id == '1':
            city = 'beijing'
        elif city_id == '2':
            city = 'shanghai'
        elif city_id == '3':
            city = 'hangzhou'
        elif city_id == '4':
            city = 'shenzhen'
        lat = str(coordinate).split(",")[1]
        lng = str(coordinate).split(",")[0]
        shop_to_time = datetime.date(datetime.date.today().year, datetime.date.today().month - 6, 1).strftime('%Y%m%d')
        shop_end_time = str(
            datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)).replace(
            '-', '')
        get_update_sql = "SELECT update_count FROM t_map_client_%s_%s_mark where update_time BETWEEN %s and %s limit 1" % (
            platform, city, up_time, to_time)
        cur.execute(get_update_sql)
        results_get_update = cur.fetchall()
        last_month_update_count = results_get_update[0][0]
        six_month_update_count = last_month_update_count - 5
        get_city_sql = "SELECT update_count,AVG(month_sale_num) from t_map_client_%s_%s_mark where update_count between %s and %s   and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') GROUP BY update_count" % (
            platform, city, six_month_update_count, last_month_update_count)
        cur.execute(get_city_sql)
        results_city = cur.fetchall()
        month_list = []
        city_list = []
        dis_shop_list = []

        for row in results_city:

            if platform == 'elm':
                city_month = str(int(row[0]) - 3) + '月'
            else:
                city_month = str(row[0]) + '月'
            month_list.append(city_month)
            city_list.append(str(row[1]))

        up_lat = float(lat) + 0.04
        down_lat = float(lat) - 0.04
        up_lng = float(lng) + 0.05
        down_lng = float(lng) - 0.05

        get_dis_sql = "SELECT update_count,month_sale_num,latitude,longitude from t_map_client_%s_%s_mark where update_count between %s and %s  and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康')  and latitude BETWEEN '%s' and '%s' and longitude BETWEEN '%s' and '%s'" \
                      % (platform, city, six_month_update_count, last_month_update_count, down_lat, up_lat, down_lng,
                         up_lng)
        cur.execute(get_dis_sql)
        results_dis = cur.fetchall()
        dis_month = []
        dis_month_sale = []
        dis_dict = {}
        for row in results_dis:
            if platform == 'elm':
                city_month = str(int(row[0]) - 3) + '月'
            else:
                city_month = str(row[0]) + '月'
            latitude = row[2]
            longitude = row[3]
            dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
            if dis <= float(distance):
                dis_month.append(city_month)
                dis_month_sale.append(row[1])

        dis_dict['key'] = dis_month
        dis_dict['data'] = dis_month_sale
        df = pd.DataFrame(dis_dict)
        kk = df.groupby(['key'], as_index=False)['data'].mean()
        sq = []
        for a in kk.values:
            dis_shop_list.append(a[1])

        if project_id is not None:
            sql_shop = "SELECT a.project_id, SUM(a.sum_order_count), COUNT(1), RIGHT(a.date,2)  FROM ( SELECT  project_id,  merchant_name,  SUM(order_count) sum_order_count, " \
                       " LEFT (stream_date, 6) date FROM  merchant_statistics WHERE  project_id = %s AND stream_date BETWEEN '%s' and '%s' GROUP BY  merchant_id," \
                       "LEFT (stream_date, 6) ) a GROUP BY a.date" % (project_id, shop_to_time, shop_end_time)
            cur_sta.execute(sql_shop)
            results_cur = cur_sta.fetchall()

            xmxc_list = []
            for row in results_cur:
                shop_ave = 0.0
                if int(row[2] > 0):
                    shop_ave = float(row[1]) / int(row[2])
                xmxc_list.append(shop_ave)

            da = list(zip(month_list, city_list, dis_shop_list, xmxc_list))

            for a in da:
                all_data = {}
                all_data['month'] = a[0]
                all_data['city_sale_num'] = a[1]
                all_data['dis_sale_num'] = a[2]
                all_data['xmxc_sale_num'] = a[3]
                sq.append(all_data)
        elif project_id is None:
            da = list(zip(month_list, city_list, dis_shop_list))

            for a in da:
                all_data = {}
                all_data['month'] = a[0]
                all_data['city_sale_num'] = a[1]
                all_data['dis_sale_num'] = a[2]
                sq.append(all_data)

        jsondatar = json.dumps(sq, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondatar)
        redis_conn.expire(cheak_key, 2592000)
        db.close()
    return jsondatar


# 返回品类下商户（根据参数区分平台，客单价只有美团有）
@app.route('/api/getCateShop')
def get_cate_shop():
    """返回周边配套餐饮商户名单（根据参数区分平台，客单价只有美团有）

    @@@
    #### 参数列表

    | 参数 | 描述 | 类型 | 例子 | 备注 |
    |--------|--------|--------|--------|--------|
    |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |       |
    |    distance    |    半径    |    string   |    1.5    |       |
    |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）    |    string   |    1   |
    |    cate    |    品类    |    string   |    东北菜   |    需要从周边品类榜中获取   |
    |    platform    |    平台（elm、mt）    |    string   |    elm   |       |

    #### 字段解释

    | 名称 | 描述 | 类型 |
    |--------|--------|--------|--------|
    |    shop_name    |    商户名称    |    string   |
    |    shop_month_sale    |    商户月销量    |    int   |
    |    average_price    |    人均 (elm平台不返回)   |    string   |
    |    call_center    |    电话   |    string   |

    #### return
    - ##### json
    > [{"shop_name": "川湘快餐（第1档口+呱呱美食城店）", "shop_month_sale": 9999}, {"shop_name": "张亮麻辣烫（将台路店）", "shop_month_sale": 7645}]
    @@@
    """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    cate = request.args.get('cate')
    coordinate = request.args.get('coordinate')
    distance = request.args.get('distance')
    city_id = request.args.get('city_id')
    platform = request.args.get('platform')

    lat = str(coordinate).split(",")[1]
    lng = str(coordinate).split(",")[0]
    city = ''
    if city_id == '1':
        city = 'beijing'
    elif city_id == '2':
        city = 'shanghai'
    elif city_id == '3':
        city = 'hangzhou'
    elif city_id == '4':
        city = 'shenzhen'
    if platform == 'mt':
        sql = "SELECT client_name,month_sale_num,latitude,longitude,average_price,call_center from t_map_client_mt_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') " \
              "and own_set_cate='%s' order by month_sale_num desc" % (city, up_time, to_time, cate)
        cur.execute(sql)
        reultes = cur.fetchall()
        sql_list = []
        for row in reultes:
            data = {}
            latitude = row[2]
            longitude = row[3]
            dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
            if dis <= float(distance):
                data['shop_name'] = row[0]
                data['month_sale'] = row[1]
                data['latitude'] = row[2]
                data['longitude'] = row[3]
                data['average_price'] = row[4]
                data['call_center'] = row[5]
                sql_list.append(data)
        jsondatar = json.dumps(sql_list, ensure_ascii=False)

    elif platform == 'elm':
        sql = "SELECT client_name,month_sale_num,latitude,longitude,call_center from t_map_client_elm_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') " \
              "and own_set_cate='%s' order by month_sale_num desc" % (city, up_time, to_time, cate)
        cur.execute(sql)
        reultes = cur.fetchall()
        sql_list = []
        for row in reultes:
            data = {}
            latitude = row[2]
            longitude = row[3]
            dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))
            if dis <= float(distance):
                data['shop_name'] = row[0]
                data['month_sale'] = row[1]
                data['latitude'] = row[2]
                data['longitude'] = row[3]
                data['call_center'] = row[4]
                sql_list.append(data)
        jsondatar = json.dumps(sql_list, ensure_ascii=False)
    db.close()
    return jsondatar


# 获取竞对数据
@app.route('/api/getRace')
def get_race():
    """获取竞对数据

    @@@
    #### 参数列表

    | 参数 | 描述 | 类型 | 例子 |
    |--------|--------|--------|--------|
    |    coordinate    |    经纬度    |    string   |    116.494325,39.976051    |
    |    distance    |    半径    |    string   |    1.5    |

    #### 字段解释

    | 名称 | 描述 | 类型 |
    |--------|--------|--------|--------|
    |    mark_name    |    商户名称    |    string   |
    |    area    |    面积    |    string   |
    |    stall_num    |    档口数    |    string   |
    |    seat_num    |    座位数    |    string   |
    |    month_rant    |    档口租金    |    string   |
    |    entry_fee    |    进场费    |    string   |
    |    latitude    |    纬度    |    string   |
    |    longitude    |    经度    |    string   |
    |    address    |    地址    |    string   |

    #### return
    - ##### json
    > [{"mark_name": "餐行者美食广场", "area": null, "stall_num": 19, "seat_num": "10", "month_rant": "8000-08-25 00:00:00", "entry_fee": "20000/8-25"}]
    @@@
    """
    db = pool_mapmarkeronline.connection()
    cur = db.cursor()
    coordinate = request.args.get('coordinate')
    distance = request.args.get('distance')
    cheak_key = '/api/getRace' + coordinate + distance
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondatar = redis_conn.get(cheak_key)
    else:
        lat = str(coordinate).split(",")[1]
        lng = str(coordinate).split(",")[0]
        sql = "SELECT mark_name,latitude,longitude,area,stall_num,seat_num,month_rent,entry_fee,address from t_map_mark"
        cur.execute(sql)
        results = cur.fetchall()

        sq = []
        for row in results:
            data = {}
            latitude = row[1]
            longitude = row[2]
            dis = getDistance(float(latitude), float(longitude), float(lat), float(lng))

            if dis <= float(distance):
                data['mark_name'] = row[0]
                data['latitude'] = row[1]
                data['longitude'] = row[2]
                data['area'] = row[3]
                data['stall_num'] = row[4]
                data['seat_num'] = row[5]
                data['month_rant'] = row[6]
                data['entry_fee'] = row[7]
                data['address'] = row[8]
                sq.append(data)
        jsondatar = json.dumps(sq, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondatar)
        redis_conn.expire(cheak_key, 2592000)
        db.close()
    return jsondatar


# 返回项目列表
@app.route('/api/getProjectList')
def get_project_list():
    """获取门店列表
    @@@
    #### 参数列表
    | 参数 | 描述 | 类型 | 例子 |
    |--------|--------|--------|--------|
    |    city_id    |    城市id （1：北京，2：上海，3：杭州，4：深圳）   |   string    |   1    |
    #### 字段解释
    | 名称 | 描述 | 类型 |    例子   |
    |--------|--------|--------|--------|
    |    project_id    |    项目id    |    long   |    12985728284   |
    |    project_name    |    项目名称    |    string   |    新荟城   |
    |    address    |    地址    |    string   |    北京市朝阳区新荟城   |
    |    latitude    |    纬度    |    string   |    35.356365   |
    |    longitude    |    经度    |    string   |    127.3636336 |
    |    month_sale    |    月销量    |    int   |    3421832   |
    |    shop_count    |    店铺数    |    int   |    3421   |
    |    shop_ave    |    客单价    |    double   |    25.255   |
    #### return
    - ##### json
    > [{"project_id": 1548233981349868, "project_name": "自空间", "address": "北京市朝阳区石门东路", "latitude": "39.9028700000000000", "longitude": "116.5030800000000000"}]
    @@@
    """
    db = pool_project.connection()
    db_map = pool_mapmarkeronline.connection()
    cur_map = db_map.cursor()
    cur = db.cursor()
    city_id = request.args.get('city_id')
    cheak_key = '/api/getProjectList' + city_id
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondu = redis_conn.get(cheak_key)
    else:
        city_name = ''
        if city_id == '1':
            city_name = '北京'
        elif city_id == '2':
            city_name = '上海'
        elif city_id == '3':
            city_name = '杭州'
        elif city_id == '4':
            city_name = '深圳'
        # 查询门店经纬度、id、名称
        sql = "SELECT a.project_id,a.project_name,b.address,b.latitude,b.longitude from project a LEFT JOIN development.project_base_info b on a.project_id=b.tid WHERE b.address is not null and b.latitude is not null and b.longitude  is not null and  a.area_name='%s'" % city_name
        cur.execute(sql)
        results = cur.fetchall()
        # 查询门店范围3km单量和外卖店铺数（饿了么加美团）
        sql_shop_info = "SELECT project_id,sum(month_sale_num_elm_3),sum(month_sale_num_mt_3),sum(shop_num_elm_3),sum(shop_num_mt_3) from t_map_h5_cate where update_time='%s' and cate_name not in ('其他品类','超市便利') GROUP BY project_id" % fist
        cur_map.execute(sql_shop_info)
        results_shop_info = cur_map.fetchall()
        # 查询门店范围3km平均客单价（仅美团）
        sql_ave = "SELECT project_id,SUM(month_sale_num*ave_price)/SUM(month_sale_num) from t_map_h5_shop where update_time='%s' and cate_name not in ('其他品类','超市便利') and ave_price >0 GROUP BY project_id" % fist
        cur_map.execute(sql_ave)
        results_ave = cur_map.fetchall()
        shop_info_dict = {}
        shop_ave_dict = {}

        for row in results_shop_info:
            shop_info_dict[row[0]] = [int(row[1]) + int(row[2]), int(row[3]) + int(row[4])]

        for row in results_ave:
            shop_ave_dict[row[0]] = float(row[1])

        sq = []
        print(results)
        for row in results:
            print(row)
            data = {}
            project_name = row[1]

            project_id = row[0]
            project_list = xm_project_dict.get(project_name)
            address = project_list[1]
            latitude = project_list[2].split(',')[1]
            longitude = project_list[2].split(',')[0]
            month_sale = shop_info_dict.get(project_id, [0, 0])[0]
            shop_count = shop_info_dict.get(project_id, [0, 0])[1]
            shop_ave = shop_ave_dict.get(project_id, 0)
            data['project_id'] = project_id
            data['project_name'] = project_name
            data['address'] = address
            data['latitude'] = latitude
            data['longitude'] = longitude
            data['month_sale'] = month_sale
            data['shop_count'] = shop_count
            data['shop_ave'] = shop_ave
            sq.append(data)
        jsondu = json.dumps(sq, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondu)
        redis_conn.expire(cheak_key, 86400)
        pool_project.close()
        pool_mapmarkeronline.close()
    return jsondu


# 返回档口统计数据
@app.route('/api/getStalls')
def get_stalls():
    """查看项目下档口统计

    @@@
    #### 参数列表

    | 参数 | 描述 | 类型 | 例子 |
    |--------|--------|--------|--------|
    |    project_id    |    项目id    |    string   |    1548233985051132    |

    #### 字段解释

    | 名称 | 描述 | 类型 |
    |--------|--------|--------|--------|
    |    on_business    |    营业中档口    |    int   |
    |    empty    |    空档口    |    int   |
    |    empty    |    新商户    |    int   |


    #### return
    - ##### json
    > [{"on_business": 10, "empty": 2, "new_shop": 0}]
    @@@
    """
    db = pool_project.connection()
    cur = db.cursor()
    project_id = request.args.get("project_id")
    cheak_key = '/api/getStalls' + project_id
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondu = redis_conn.get(cheak_key)
    else:
        sql = "SELECT status,COUNT(1)  from stalls where project_id=%s  GROUP BY status" % project_id
        cur.execute(sql)
        results = cur.fetchall()
        start_time = getdate(30, '%Y-%m-%d')
        end_time = getdate(1, '%Y-%m-%d')
        on_business = 0
        empty = 0
        for row in results:
            status = row[0]
            status_count = row[1]
            if status == 0 or status == 6:
                on_business = on_business + status_count
            elif status == 3 or status == 5:
                empty = empty + status_count
        sql_new_shop = "SELECT COUNT(1) from contract where project_id=%s and enter_time  BETWEEN '%s' and '%s' GROUP BY stall_id" % (
            project_id, start_time, end_time)
        cur.execute(sql_new_shop)
        results_new_shop = cur.fetchall()
        new_shop = 0
        if len(results_new_shop) > 0:
            new_shop = results_new_shop[0][0]

        data = {}
        data['on_business'] = on_business
        data['empty'] = empty
        data['new_shop'] = new_shop
        sq = []
        sq.append(data)
        db.close()
        jsondu = json.dumps(sq, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondu)
        redis_conn.expire(cheak_key, 86400)
    return jsondu


# 返回商户信息
@app.route('/api/getXmxcShop')
def get_xmxc_shop():
    """查看门店数据

    @@@
    #### 参数列表

    | 参数 | 描述 | 类型 | 例子 |
    |--------|--------|--------|--------|--------|
    |    project_id    |    项目id    |    string   |    1548233985051132    |

    #### 字段解释

    | 名称 | 描述 | 类型 |
    |--------|--------|--------|--------|
    |    all_order_count    |    门店总单量    |    string   |
    |    all_all_money    |    门店总金额    |    string   |
    |    ave_shop    |    门店客单价    |    string   |
    |    shop_list[]    |    月度商户排行榜    |    list   |
    |    └merchant_name    |    商户名称    |    string   |
    |    └order_count    |    月销量    |    int   |
    |    └proportion    |    商户单量占比    |    double   |
    |    cate_list    |    月度品类榜    |    list   |
    |    └cate_name    |    品类名称    |    string   |
    |    └cate_count    |     品类销售单量   |    string   |
    |    └cate_ave    |    品类占比    |    string   |


    #### return
    - ##### json
    > [{"all_order_count": "40007", "all_all_money": "813530.04", "ave_shop": "20.334692428824958"}, "shop_list": [{"merchant_name": "轻盒有机", "order_count": 3218, "proportion": 0.08043592371335016}], "cate_list": [{"cate_name": "沙拉", "cate_count": "7194", "cate_ave": "0.1798185317569425350563651361"}]}]
    @@@"""
    db = pool_statistics.connection()
    cur = db.cursor()
    project_id = request.args.get("project_id")
    cheak_key = '/api/getXmxcShop' + project_id
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondu = redis_conn.get(cheak_key)
    else:
        start_time = getdate(30, '%Y%m%d')
        end_time = getdate(1, '%Y%m%d')
        sql = "SELECT merchant_id,merchant_name,SUM(order_count),SUM(sale_amount) from merchant_statistics where project_id=%s and  stream_date BETWEEN '%s' and '%s' GROUP BY merchant_id" % (
            project_id, start_time, end_time)

        cur.execute(sql)
        results = cur.fetchall()
        all_json = []
        sq = []
        all_count = 0
        all_money = 0.0
        for row in results:
            order_count = int(row[2])
            sale_amount = float(row[3])
            all_count += order_count
            all_money += sale_amount

        for row in results:
            data = {}
            merchant_name = row[1]
            order_count = int(row[2])
            proportion = 0.0
            if order_count > 0:
                proportion = order_count / all_count
            data['merchant_name'] = merchant_name
            data['order_count'] = order_count
            data['proportion'] = proportion
            sq.append(data)
        ave_shop = 0
        if all_count > 0:
            ave_shop = all_money / all_count

        sql_cate = "select (select mcc.category_name from commerce.merchants_configuration_category mcc where mcc.tid = mcr.second_category_id),sum(ms.sale_amount)," \
                   "sum(ms.order_count) from merchant_statistics ms left join merchant_category_rela mcr on ms.merchant_id = mcr.merchant_id where ms.project_id =" \
                   " %s and mcr.second_category_id != 0 and ms.stream_date between '%s' and '%s' group by mcr.second_category_id" % (
                       project_id, start_time, end_time)

        cur.execute(sql_cate)
        results_cate = cur.fetchall()
        cate_list = []
        for row in results_cate:
            cata_data = {}
            cate_name = row[0]
            cate_count = row[2]
            cate_ave = 0.0
            if cate_count > 0:
                cate_ave = cate_count / all_count
            cata_data["cate_name"] = cate_name
            cata_data["cate_count"] = str(cate_count)
            cata_data["cate_ave"] = str(cate_ave)
            cate_list.append(cata_data)
        json_dict = {}
        json_dict['all_order_count'] = str(all_count)
        json_dict['all_all_money'] = str(all_money)
        json_dict['ave_shop'] = str(ave_shop)
        json_dict['shop_list'] = sq
        json_dict['cate_list'] = cate_list
        all_json.append(json_dict)

        db.close()
        jsondu = json.dumps(all_json, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondu)
        redis_conn.expire(cheak_key, 86400)
    return jsondu


# 返回推荐品类（根据参数区分平台）
@app.route('/api/getCateRecommend')
def get_cate_recommend():
    """返回推荐品类（根据参数区分平台，客单价只有美团有）
        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    project_id    |    项目id    |    string   |    1548233985051132    |
        |    platform    |    平台(elm、mt)    |    string   |    elm    |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    cate_all_month_sale    |    所有品类总单量（计算单量占比用）    |    int   |
        |    all_shop_ave    |    所有品类均客单价    |    double   |
        |    cate_name    |    品类名称    |    string   |
        |    source    |    品类得分    |    int   |
        |    all_month_sale    |    品类单量    |    int   |
        |    all_shop_count    |    品类商户数    |    int   |
        |    ave_month    |    品类均单量    |    int   |
        |    xmxc_have_count    |    熊猫场地经营的商户数    |    int   |
        #### return
        - ##### json
        > [{"all_month_sale": 6098989, "cate_info": [{"cate_name": "快餐便当", "source": 3332836.5, "all_month_sale": 2145778, "all_shop_count": 2085, "ave_month": 1029.1501199040767}]}]
        @@@
        """
    db = pool_project.connection()
    db_mapmarker = pool_mapmarkeronline.connection()
    # db = pool_statistics.connection()

    last = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)

    cur = db.cursor()
    project_id = request.args.get("project_id")
    platform = request.args.get("platform")
    cheak_key = '/api/getCateRecommend' + project_id + platform
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondu = redis_conn.get(cheak_key)
    else:
        sql = "SELECT * from t_map_h5_cate where cate_name not in ('None','其他品类','超市便利') and update_time BETWEEN '%s' and '%s' and project_id=%s" % (
            fist, last, project_id)

        cur_mapmarker = db_mapmarker.cursor()
        cur_mapmarker.execute(sql)
        results = cur_mapmarker.fetchall()

        sql_all_ave = "SELECT sum(ave_price*month_sale_num)/SUM(month_sale_num) from t_map_h5_shop where project_id=%s and ave_price>0 and platform='mt' and cate_name not in ('None','其他品类','超市便利') and update_time BETWEEN '%s' and '%s'" % (
            project_id, fist, last)

        cur_mapmarker.execute(sql_all_ave)
        results_all_ave = cur_mapmarker.fetchall()

        all_shop_ave = results_all_ave[0][0]

        # sql_shop_ave = "SELECT project_id,cate_name,ave_price,month_sale_num,sum(ave_price*month_sale_num)/SUM(month_sale_num) from t_map_h5_shop where project_id=%s and ave_price>0 and platform='mt' and cate_name not in ('None','其他品类','超市便利') and update_time BETWEEN '%s' and '%s' GROUP BY cate_name " % (project_id,fist, last)
        # cur_mapmarker.execute(sql_shop_ave)
        # results_shop_ave = cur_mapmarker.fetchall()

        sql_cate = "SELECT a.second_classification_name,COUNT(1) from public_sea_pool  a RIGHT JOIN contract b on a.id=b.merchant_id WHERE b.is_delete=0 and b.is_valid=1 and b.project_id=%s GROUP BY a.second_classification_name" % project_id
        cur.execute(sql_cate)
        results_cate = cur.fetchall()

        really_cate_list = {}
        for xmcate_row in results_cate:
            really_cate = xm_cate_dict.get(xmcate_row[0], '其他品类')
            really_cate_list[really_cate] = xmcate_row[1]

        sq = []
        all_month_sale = 0
        data = {}
        for row in results:
            if platform == 'elm':
                cate_list = {}
                all_month_sale += row[3]
                cate_name = row[2]
                month_sale_num_elm_3 = row[3]
                month_sale_num_elm_2 = row[5]
                month_sale_num_elm_1 = row[7]
                if cate_name not in really_cate_list.keys():
                    source = month_sale_num_elm_3 + month_sale_num_elm_2 * 1.5 + month_sale_num_elm_1 * 5
                    cate_all_month_sale = row[3]
                    cate_all_shop_count = row[4]
                    if cate_all_shop_count == 0:
                        ave_month = 0
                    else:
                        ave_month = cate_all_month_sale / cate_all_shop_count
                    cate_list["cate_name"] = cate_name
                    cate_list["source"] = source
                    cate_list["all_month_sale"] = cate_all_month_sale
                    cate_list["all_shop_count"] = cate_all_shop_count
                    cate_list["ave_month"] = ave_month
                    cate_list["xmxc_have_count"] = 0
                    sq.append(cate_list)
                else:
                    source = month_sale_num_elm_3 + month_sale_num_elm_2 * 1.5 + month_sale_num_elm_1 * 5
                    cate_all_month_sale = row[3]
                    cate_all_shop_count = row[4]
                    if cate_all_shop_count == 0:
                        ave_month = 0
                    else:
                        ave_month = cate_all_month_sale / cate_all_shop_count
                    cate_list["cate_name"] = cate_name
                    cate_list["source"] = source
                    cate_list["all_month_sale"] = cate_all_month_sale
                    cate_list["all_shop_count"] = cate_all_shop_count
                    cate_list["ave_month"] = ave_month
                    cate_list["xmxc_have_count"] = really_cate_list.get(cate_name)
                    sq.append(cate_list)
            elif platform == 'mt':
                cate_list = {}
                all_month_sale += row[9]
                cate_name = row[2]
                if cate_name not in really_cate_list.keys():
                    source = row[9] + row[11] * 1.5 + row[13] * 5
                    cate_all_month_sale = row[9]
                    cate_all_shop_count = row[10]
                    if cate_all_shop_count == 0:
                        ave_month = 0
                    else:
                        ave_month = cate_all_month_sale / cate_all_shop_count
                    cate_list["cate_name"] = cate_name
                    cate_list["source"] = source
                    cate_list["all_month_sale"] = cate_all_month_sale
                    cate_list["all_shop_count"] = cate_all_shop_count
                    cate_list["ave_month"] = ave_month
                    cate_list["xmxc_have_count"] = 0
                    sq.append(cate_list)
                else:
                    source = row[9] + row[11] * 1.5 + row[13] * 5
                    cate_all_month_sale = row[9]
                    cate_all_shop_count = row[10]
                    if cate_all_shop_count == 0:
                        ave_month = 0
                    else:
                        ave_month = cate_all_month_sale / cate_all_shop_count
                    cate_list["cate_name"] = cate_name
                    cate_list["source"] = source
                    cate_list["all_month_sale"] = cate_all_month_sale
                    cate_list["all_shop_count"] = cate_all_shop_count
                    cate_list["ave_month"] = ave_month
                    cate_list["xmxc_have_count"] = really_cate_list.get(cate_name)
                    sq.append(cate_list)
        sq1 = sorted(sq, key=lambda x: x["source"], reverse=True)

        data["cate_all_month_sale"] = all_month_sale
        data["all_shop_ave"] = all_shop_ave
        data["cate_info"] = sq1
        jsondu = json.dumps(data, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondu)
        redis_conn.expire(cheak_key, 86400)
    return jsondu


# 返回推荐品类下商户（只有美团）
@app.route('/api/getCateRecommendShop')
def get_cate_recommend_shop():
    """返回推荐品类下商户（只有美团）

            @@@
            #### 参数列表

            | 参数 | 描述 | 类型 | 例子 |
            |--------|--------|--------|--------|
            |    project_id    |    项目id    |    string   |    1548233985051132    |
            |    cate_name    |    品类名称    |    string   |    快餐便当    |

            #### 字段解释

            | 名称 | 描述 | 类型 |
            |--------|--------|--------|--------|
            |    all_month_sale    |    所有品类总单量（计算单量占比用）    |    int   |
            |    shop_name    |    商户名称    |    string   |
            |    month_sale    |    商户单量    |    int   |
            |    ave_price    |    商户客单价    |    int   |
            |    cate_ave    |    品类均客单价    |    double   |
            |    distance    |    商户据项目距离(单位千米)    |    double   |
            |    call_center    |    商户电话    |    string   |
            #### return
            - ##### json
            > [{"all_month_sale": 614305, "shop_list": [{"shop_name": "闽南好粥道（中山西路店）", "month_sale": 9999, "ave_price": 12.0, "distance": 1.06448}]}]
            @@@
            """

    db_mapmarker = pool_mapmarkeronline.connection()

    last = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)
    cur_mapmarker = db_mapmarker.cursor()
    project_id = request.args.get("project_id")
    cate_name = request.args.get("cate_name")
    sql = "SELECT * from t_map_h5_shop where platform='mt' and  update_time BETWEEN '%s' and '%s' and  project_id=%s and cate_name='%s'" % (
        fist, last, project_id, cate_name)

    cur_mapmarker.execute(sql)
    results = cur_mapmarker.fetchall()
    all_month_sale = 0
    sq = []
    re_dict = {}
    all_income = 0
    income_month_sale = 0
    for row in results:
        shop_dict = {}
        month_sale = row[5]
        all_month_sale += month_sale
        shop_dict['shop_name'] = row[4]
        shop_dict['call_center'] = row[14]
        shop_dict['month_sale'] = row[5]
        shop_dict['ave_price'] = row[6]
        if row[6] > 0:
            income = row[6] * row[5]
            all_income += income
            income_month_sale += month_sale
        shop_dict['distance'] = row[8]
        sq.append(shop_dict)
    sq1 = sorted(sq, key=lambda x: x["month_sale"], reverse=True)
    if income_month_sale == 0:
        cate_ave = 0
    else:
        cate_ave = all_income / income_month_sale
    re_dict['all_month_sale'] = all_month_sale
    re_dict['cate_ave'] = cate_ave
    re_dict['shop_list'] = sq1

    jsondu = json.dumps(re_dict, ensure_ascii=False)
    return jsondu


# 返回推荐品类下客单价区间(只有美团)
@app.route('/api/getCateAveSection')
def get_cate_ave_section():
    """返回推荐品类下客单价区间(只有美团)
        @@@
        #### 参数列表
        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    project_id    |    项目id    |    string   |    1548233985051132    |
        |    cate_name    |    品类名称    |    string   |    快餐便当    |
        #### 字段解释
        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    all_month_sale    |    所有区间总单量（计算单量占比用）    |    int   |
        |    less15_sale    |    客单价小于15总单量    |    int   |
        |    less15_shop    |    客单价小于15商户数    |    int   |
        |    between15_and25_sale    |    客单价15-25总单量    |    int   |
        |    between15_and25_shop    |    客单价15-25商户数    |    int   |
        |    between25_and35_sale    |    客单价25-35总单量     |    int   |
        |    between25_and35_shop    |    客单价25-35商户数    |    int   |
        |    more35_sale    |    客单价大于35总单量    |    int   |
        |    more35_shop    |    客单价大于35商户数    |    int   |
        #### return
        - ##### json
        > [{"all_month_sale": 614305, "shop_list": [{"shop_name": "闽南好粥道（中山西路店）", "month_sale": 9999, "ave_price": 12.0}]}]
        @@@
        """

    db_mapmarker = pool_mapmarkeronline.connection()

    last = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)
    cur_mapmarker = db_mapmarker.cursor()
    project_id = request.args.get("project_id")
    cate_name = request.args.get("cate_name")
    cheak_key = '/api/getCateAveSection' + project_id + cate_name
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondu = redis_conn.get(cheak_key)
    else:

        sql = "SELECT * from t_map_h5_shop where platform='mt' and update_time BETWEEN '%s' and '%s' and  project_id=%s and cate_name='%s'" % (
            fist, last, project_id, cate_name)

        cur_mapmarker.execute(sql)
        results = cur_mapmarker.fetchall()
        less15_sale = 0
        less15_shop = 0
        between15_and25_sale = 0
        between15_and25_shop = 0
        between25_and35_sale = 0
        between25_and35_shop = 0
        more35_sale = 0
        more35_shop = 0
        all_month_sale = 0
        for row in results:
            if row[6] <= 15 and row[6] > 0:
                less15_sale += row[5]
                less15_shop += 1
                all_month_sale += row[5]
            elif row[6] <= 25 and row[6] > 15:
                between15_and25_sale += row[5]
                between15_and25_shop += 1
                all_month_sale += row[5]
            elif row[6] <= 35 and row[6] > 25:
                between25_and35_sale += row[5]
                between25_and35_shop += 1
                all_month_sale += row[5]
            elif row[6] > 35:
                more35_sale += row[5]
                more35_shop += 1
                all_month_sale += row[5]
        all_dict = {}
        all_dict['all_month_sale'] = all_month_sale
        all_dict['ave_section'] = [{'sale_num': less15_sale, 'shop_num': less15_shop},
                                   {'sale_num': between15_and25_sale, 'shop_num': between15_and25_shop},
                                   {'sale_num': between25_and35_sale, 'shop_num': between25_and35_shop},
                                   {'sale_num': more35_sale, 'shop_num': more35_shop}
                                   ]
        jsondu = json.dumps(all_dict, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondu)
        redis_conn.expire(cheak_key, 86400)
    return jsondu


# 返回推荐品类下客单价趋势（只有美团）
@app.route('/api/getCateAveLine')
def get_cate_ave_line():
    """返回推荐品类下客单价趋势（只有美团）
            @@@
            #### 参数列表
            | 参数 | 描述 | 类型 | 例子 |
            |--------|--------|--------|--------|
            |    project_id    |    项目id    |    string   |    1548233985051132    |
                |    cate_name    |    品类名称    |    string   |    快餐便当    |
            #### 字段解释
            | 名称 | 描述 | 类型 |
            |--------|--------|--------|--------|
            |    less15[]    |    客单价小于等于15列表    |    list   |
            |    between15_and25   |    客单价小于等于25大于15列表    |    list   |
            |    between25_and35   |    客单价小于等于35大于25列表    |    list   |
            |    more35   |    客单价大于35列表    |    list   |
            |    └time    |    统计月份    |    string   |
            |    └shop_num    |    酒店名称    |    string   |
            |    └sale_num    |    酒店名称    |    string   |

            #### return
            - ##### json
            > [{"all_month_sale": 614305, "shop_list": [{"shop_name": "闽南好粥道（中山西路店）", "month_sale": 9999, "ave_price": 12.0}]}]
            @@@
            """

    db_mapmarker = pool_mapmarkeronline.connection()
    first = getTheMonth(4, '%Y-%m-%d')

    month_3 = getTheMonth(3, '%Y-%m-%d')
    month_2 = getTheMonth(2, '%Y-%m-%d')
    month_1 = getTheMonth(1, '%Y-%m-%d')

    last = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)
    cur_mapmarker = db_mapmarker.cursor()
    project_id = request.args.get("project_id")
    cate_name = request.args.get("cate_name")
    cheak_key = '/api/getCateAveLine' + project_id + cate_name
    res = redis_conn.exists(cheak_key)

    if res == 1:
        jsondu = redis_conn.get(cheak_key)
    else:
        sql = "SELECT 'less15',update_time,cate_name,COUNT(*),SUM(month_sale_num) from t_map_h5_shop where platform='mt' and update_time BETWEEN '%s' and '%s' and  project_id=%s and cate_name='%s' and ave_price <=15 and ave_price>0 GROUP BY update_time " \
              "union all SELECT 'between15_and25',update_time,cate_name,COUNT(*),SUM(month_sale_num) from t_map_h5_shop where platform='mt' and update_time BETWEEN '%s' and '%s' and  project_id=%s and cate_name='%s' and ave_price <=25 and ave_price>15 GROUP BY update_time " \
              "union all SELECT 'between25_and35' ,update_time,cate_name,COUNT(*),SUM(month_sale_num) from t_map_h5_shop where platform='mt' and update_time BETWEEN '%s' and '%s' and  project_id=%s and cate_name='%s' and ave_price <=35 and ave_price>25 " \
              "GROUP BY update_time union all  SELECT 'more35', update_time,cate_name,COUNT(*),SUM(month_sale_num) from t_map_h5_shop where platform='mt' and update_time " \
              "BETWEEN '%s' and '%s' and  project_id=%s and cate_name='%s' and ave_price>35 GROUP BY update_time" % (
                  first, last, project_id, cate_name, first, last, project_id, cate_name, first, last, project_id,
                  cate_name, first, last, project_id, cate_name)

        cur_mapmarker.execute(sql)
        results = cur_mapmarker.fetchall()
        time_dict = [str(first), str(month_3), str(month_2), str(month_1)]
        less15 = []
        between15_and25 = []
        between25_and35 = []
        more35 = []
        for row in results:
            less15_dict = {}
            less15_dict['time'] = str(row[1])
            less15_dict['shop_num'] = int(row[3])
            less15_dict['sale_num'] = int(row[4])

            if row[0] == 'less15':
                less15.append(less15_dict)
            elif row[0] == 'between15_and25':
                between15_and25.append(less15_dict)
            elif row[0] == 'between25_and35':
                between25_and35.append(less15_dict)
            elif row[0] == 'more35':
                more35.append(less15_dict)

        b = [time1.get('time') for time1 in less15]

        for time2 in time_dict:
            if time2 not in b:
                less15.append({"time": str(time2), "shop_num": 0, "sale_num": 0})

        c = [time1.get('time') for time1 in between15_and25]
        for time2 in time_dict:
            if time2 not in c:
                between15_and25.append({"time": str(time2), "shop_num": 0, "sale_num": 0})

        d = [time1.get('time') for time1 in between25_and35]
        for time2 in time_dict:
            if time2 not in d:
                between25_and35.append({"time": str(time2), "shop_num": 0, "sale_num": 0})
        e = [time1.get('time') for time1 in more35]
        for time2 in time_dict:
            if time2 not in e:
                more35.append({"time": str(time2), "shop_num": 0, "sale_num": 0})

        less15 = sorted(less15, key=lambda x: x['time'])
        between15_and25 = sorted(between15_and25, key=lambda x: x['time'])
        between25_and35 = sorted(between25_and35, key=lambda x: x['time'])
        more35 = sorted(more35, key=lambda x: x['time'])
        all_dict = {}
        all_dict['less15'] = less15
        all_dict['between15_and25'] = between15_and25
        all_dict['between25_and35'] = between25_and35
        all_dict['more35'] = more35
        jsondu = json.dumps(all_dict, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondu)
        redis_conn.expire(cheak_key, 86400)
    return jsondu


# 返回推荐品类下品类走势图（只有饿了么）
@app.route('/api/getCateRecommendLine')
def get_cate_recommend_line():
    """返回推荐品类下品类走势图（只有饿了么）

        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    project_id    |    项目id    |    string   |    1548233985051132    |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    cate_all_month_sale    |    所有品类总单量（计算单量占比用）    |    int   |
        |    cate_name    |    品类名称    |    string   |
        |    source    |    品类得分    |    int   |
        |    all_month_sale    |    品类单量    |    int   |
        |    all_shop_count    |    品类商户数    |    int   |
        |    ave_month    |    品类均单量    |    int   |
        |    xmxc_have_count    |    熊猫场地经营的商户数    |    int   |
        #### return
        - ##### json
        > [{"all_month_sale": 6098989, "cate_info": [{"cate_name": "快餐便当", "source": 3332836.5, "all_month_sale": 2145778, "all_shop_count": 2085, "ave_month": 1029.1501199040767}]}]
        @@@
        """
    db_mapmarker = pool_test.connection()
    # db = pool_statistics.connection()
    first = getTheMonth(6, '%Y-%m-%d')
    last = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)
    cur_mapmarker = db_mapmarker.cursor()

    project_id = request.args.get("project_id")
    platform = request.args.get("platform")

    cheak_key = '/api/getCateRecommendLine' + project_id + platform
    res = redis_conn.exists(cheak_key)

    if res == 1:
        jsondu = redis_conn.get(cheak_key)
    else:
        if platform == 'elm':
            sql = "SELECT cate_name,month_sale_num_elm_3,shop_num_elm_3,update_time from t_map_h5_cate where cate_name not in ('None','其他品类','超市便利') and update_time BETWEEN '%s' and '%s' and project_id=%s order by update_time" % (
                first, last, project_id)
        elif platform == 'mt':
            sql = "SELECT cate_name,month_sale_num_mt_3,shop_num_mt_3,update_time from t_map_h5_cate where cate_name not in ('None','其他品类','超市便利') and update_time BETWEEN '%s' and '%s' and project_id=%s order by update_time" % (
                first, last, project_id)

        cur_mapmarker.execute(sql)
        results = cur_mapmarker.fetchall()
        dict_cate = {}
        for row in results:

            if row[0] not in dict_cate.keys():
                dict_cate[row[0]] = [{'month_sale': int(row[1]), 'shop_count': int(row[2]), 'time': str(row[3])}]
            else:
                dict_cate[row[0]].append({'month_sale': int(row[1]), 'shop_count': int(row[2]), 'time': str(row[3])})

        # dict_cate=sorted(dict_cate,key=lambda x:x[0])

        jsondu = json.dumps(dict_cate, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondu)
        redis_conn.expire(cheak_key, 86400)
    return jsondu


# 返回品类列表
@app.route('/api/getCateList')
def get_cate_list():
    """返回品类列表

        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |        |        |       |        |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    cate_all_month_sale    |    所有品类总单量（计算单量占比用）    |    int   |
        |    cate_name    |    品类名称    |    string   |
        |    source    |    品类得分    |    int   |
        |    all_month_sale    |    品类单量    |    int   |
        |    all_shop_count    |    品类商户数    |    int   |
        |    ave_month    |    品类均单量    |    int   |
        |    xmxc_have_count    |    熊猫场地经营的商户数    |    int   |
        #### return
        - ##### json
        > [{"all_month_sale": 6098989, "cate_info": [{"cate_name": "快餐便当", "source": 3332836.5, "all_month_sale": 2145778, "all_shop_count": 2085, "ave_month": 1029.1501199040767}]}]
        @@@
        """

    cheak_key = '/api/getCatList'
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondu = redis_conn.get(cheak_key)
    else:
        db_mapmarker = pool_mapmarkeronline.connection()

        last = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)
        cur_mapmarker = db_mapmarker.cursor()
        sql = "SELECT cate_name from t_map_h5_cate  where cate_name not in ('None','其他品类','超市便利') and update_time BETWEEN '%s' and '%s' GROUP BY cate_name" % (
            fist, last)

        cur_mapmarker.execute(sql)
        results = cur_mapmarker.fetchall()
        sq = {}
        a = [i[0] for i in results]
        sq['cate_list'] = a
        jsondu = json.dumps(sq, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondu)
        redis_conn.expire(cheak_key, 86400)
    return jsondu


# 找店（饿了么美团加起来）
@app.route('/api/getCatShopRank')
def get_cate_shop_rank():
    """返回推荐品类下门店推荐（饿了么美团加起来）
        @@@
        #### 参数列表

        | 参数 | 描述 | 类型 | 例子 |
        |--------|--------|--------|--------|
        |    cate_name    |    品类名称    |    string   |    快餐便当    |
        |    city    |    城市id    |    int   |    1    |

        #### 字段解释

        | 名称 | 描述 | 类型 |
        |--------|--------|--------|--------|
        |    project_name    |    项目名称    |    string   |    自空间    |
        |    project_id    |    项目id    |    int   |    1234323543    |
        |    address    |    场地地址    |    string   |    北京市朝阳区自空间    |
        |    latitude    |    纬度    |    string   |    39.543253    |
        |    longitude    |    经度    |    string   |    139.54342354    |
        |    source    |    品类得分    |    int   |    233423    |
        |    cate_all_month_sale    |    品类单量    |    int   |    3223   |
        |    cate_all_shop_count    |    品类商户数    |    int   |    32   |
        |    ave_month    |    品类均单量    |    double   | 152   |

        #### return
        - ##### json
        > [{"all_month_sale": 6098989, "cate_info": [{"cate_name": "快餐便当", "source": 3332836.5, "all_month_sale": 2145778, "all_shop_count": 2085, "ave_month": 1029.1501199040767}]}]
        @@@
        """

    cate_name = request.args.get("cate_name")
    city_id = request.args.get('city_id')
    cheak_key = '/api/getCatShopRank' + cate_name + city_id
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondu = redis_conn.get(cheak_key)
    else:
        db = pool_project.connection()
        db_mapmarker = pool_mapmarkeronline.connection()
        # db = pool_statistics.connection()

        last = datetime.date(datetime.date.today().year, datetime.date.today().month, 1) - datetime.timedelta(1)
        cur_mapmarker = db_mapmarker.cursor()
        cur = db.cursor()
        if city_id == '1':
            city = '北京'
        elif city_id == '2':
            city = '上海'
        elif city_id == '3':
            city = '杭州'
        elif city_id == '4':
            city = '深圳'
        sql_cate = "SELECT b.project_id,a.second_classification_name from public_sea_pool  a RIGHT JOIN contract b on a.id=b.merchant_id WHERE b.is_delete=0 and b.is_valid=1"
        cur.execute(sql_cate)
        results_cate = cur.fetchall()

        pro = '1'
        for xmcate_row in results_cate:
            really_cate = xm_cate_dict.get(xmcate_row[1], '其他品类')
            if really_cate == cate_name:
                pro += str(xmcate_row[0]) + ','
        pro = pro.strip(',')

        sql = "SELECT * from t_map_h5_cate where cate_name not in ('None','其他品类','超市便利') and update_time BETWEEN '%s' and '%s' and cate_name='%s' and project_id not in (%s) and city='%s'" % (
            fist, last, cate_name, pro, city)

        cur_mapmarker.execute(sql)
        results = cur_mapmarker.fetchall()
        sq = []

        for row in results:
            cate_list = {}
            # all_month_sale += row[3] + row[5] + row[7] + row[9] + row[11] + row[13]

            project_name = row[16]

            project_list = xm_project_dict.get(project_name)

            address = project_list[1]
            latitude = project_list[2].split(',')[1]
            longitude = project_list[2].split(',')[0]
            month_sale_num_elm_3 = row[3]
            month_sale_num_elm_2 = row[5]
            month_sale_num_elm_1 = row[7]

            source = month_sale_num_elm_3 + month_sale_num_elm_2 * 1.5 + month_sale_num_elm_1 * 5 + row[9] + row[
                11] * 1.5 + row[13] * 5
            # 美团饿了么3km范围内的加起来
            cate_all_month_sale = row[3] + row[9]
            cate_all_shop_count = row[4] + row[10]
            if cate_all_shop_count == 0:
                ave_month = 0
            else:
                ave_month = cate_all_month_sale / cate_all_shop_count
            cate_list["project_name"] = project_name
            cate_list["project_id"] = row[1]
            cate_list["address"] = address
            cate_list["latitude"] = latitude
            cate_list["longitude"] = longitude
            cate_list["source"] = source
            cate_list["all_month_sale"] = cate_all_month_sale
            cate_list["all_shop_count"] = cate_all_shop_count
            cate_list["ave_month"] = ave_month

            sq.append(cate_list)
        sq1 = sorted(sq, key=lambda x: x["source"], reverse=True)
        jsondu = json.dumps(sq1, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondu)
        redis_conn.expire(cheak_key, 86400)
    return jsondu


# 两个点对比
@app.route('/api/getTwoShopCompare')
def get_two_shop_compare():
    """两个点对比(区分平台)
            @@@
            #### 参数列表

            | 参数 | 描述 | 类型 | 例子 |
            |--------|--------|--------|--------|
            |    cate_name    |    品类名称    |    string   |    快餐便当    |
            |    first_location    |    第一个经纬度    |    string   |    121.41815,31.2378    |
            |    second_location    |    第二个经纬度    |    string   |    121.41815,31.2378    |
            |    platform    |    平台名称（elm、mt）    |    string   |    mt    |
            |    city_id    |    城市id    |    int   |    1    |

            #### 字段解释

            | 名称 | 描述 | 类型 |        |
            |--------|--------|--------|--------|
            |    first_list    |    第一个点数据列表    |    list   |    自空间    |
            |    second_list    |    第二个点数据列表    |    list   |    1234323543    |
            |    sale_median    |    中位值    |    double   |     581.5    |
            |    sale_sum    |    总单量    |    double   |    723665    |
            |    shop_count    |    店铺数    |    double   |    2132    |
            |    shop_sale_ave    |    平均单量    |    double   |    215    |
            |    income_ave    |    平均客单价（仅美团有）    |    double   |    24.54   |
            |    house_count    |    小区数量   |    double   |    24.54   |
            |    office_count    |    写字楼数量    |    int   |    24  |
            |    hospital_count    |    医院数量    |    int   |    24  |
            |    school_count    |    学校数量    |    int   |    24  |
            |    shopping_count    |    商场数量    |    int   |    24   |
            |    hotal_count    |    酒店数量    |    int   |    24  |

            #### return
            - ##### json
            > {"first_list": [{"first_median_3km": 470.0, "first_sum_3km": 555779.0, "first_shop_count_3km": 627.0}, {"first_median_2km": 521.0, "first_sum_2km": 379433.0, "first_shop_count_2km": 409.0}, {"first_median_1km": 535.0, "first_sum_1km": 164797.0, "first_shop_count_1km": 167.0}], "second_list": [{"first_median_3km": 660.5, "first_sum_3km": 590690.0, "first_shop_count_3km": 532.0}, {"first_median_2km": 684.5, "first_sum_2km": 285866.0, "first_shop_count_2km": 238.0}, {"first_median_1km": 808.5, "first_sum_1km": 131732.0, "first_shop_count_1km": 90.0}]}
            @@@
            """
    cate_name = request.args.get("cate_name")
    first_location = request.args.get("first_location")
    second_location = request.args.get("second_location")
    platform = request.args.get("platform")
    city_id = request.args.get("city_id")
    cheak_key = '/api/getTwoShopCompare' + cate_name + first_location + second_location + platform + city_id
    res = redis_conn.exists(cheak_key)
    if res == 1:
        jsondatar = redis_conn.get(cheak_key)
    else:
        db_mapmarker = pool_mapmarkeronline.connection()
        cur_mapmarker = db_mapmarker.cursor()

        if city_id == "1":
            city_name = "北京市"
            city = 'beijing'
        elif city_id == "2":
            city_name = "上海市"
            city = 'shanghai'
        elif city_id == "3":
            city_name = "杭州市"
            city = 'hangzhou'
        elif city_id == "4":
            city_name = "深圳市"
            city = 'shenzhen'
        lat1 = str(first_location).split(",")[1]
        lng1 = str(first_location).split(",")[0]
        up_lat1 = float(lat1) + 0.04
        down_lat1 = float(lat1) - 0.04
        up_lng1 = float(lng1) + 0.05
        down_lng1 = float(lng1) - 0.05

        lat2 = str(second_location).split(",")[1]
        lng2 = str(second_location).split(",")[0]
        up_lat2 = float(lat2) + 0.04
        down_lat2 = float(lat2) - 0.04
        up_lng2 = float(lng2) + 0.05
        down_lng2 = float(lng2) - 0.05

        # 如果传的cate_name为空字符串就返回全品类
        if cate_name == '':
            # 查询第一个点的数据
            sql = "SELECT month_sale_num,latitude,longitude,update_time  from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  '%s' and longitude BETWEEN '%s' and '%s'  and month_sale_num>0" % (
                platform, city, up_time, to_time, down_lat1, up_lat1, down_lng1, up_lng1,)
            # 查询第二个点的数据
            sql2 = "SELECT month_sale_num,latitude,longitude,update_time  from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                   "'%s' and longitude BETWEEN '%s' and '%s'  and month_sale_num>0" % (
                       platform, city, up_time, to_time, down_lat2, up_lat2, down_lng2, up_lng2,)
        else:
            sql = "SELECT month_sale_num,latitude,longitude,update_time  from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                  "'%s' and longitude BETWEEN '%s' and '%s' and own_set_cate='%s' and month_sale_num>0" % (
                      platform, city, up_time, to_time, down_lat1, up_lat1, down_lng1, up_lng1, cate_name)

            sql2 = "SELECT month_sale_num,latitude,longitude,update_time  from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                   "'%s' and longitude BETWEEN '%s' and '%s' and own_set_cate='%s' and month_sale_num>0" % (
                       platform, city, up_time, to_time, down_lat2, up_lat2, down_lng2, up_lng2, cate_name)

        cur_mapmarker.execute(sql)
        results = cur_mapmarker.fetchall()
        first_list = get_two_date(results, lat1, lng1)

        cur_mapmarker.execute(sql2)
        results2 = cur_mapmarker.fetchall()
        second_list = get_two_date(results2, lat2, lng2)

        # 查询小区
        sql_house = "SELECT longitude,latitude from t_map_lianjia_uptown where longitude !='不明' and city='%s' " % city_name
        cur_mapmarker.execute(sql_house)
        results_house = cur_mapmarker.fetchall()
        first_house = get_two_data_buildings(results_house, lat1, lng1)
        second_house = get_two_data_buildings(results_house, lat2, lng2)
        # 查询写字楼
        sql_office = "SELECT longitude,latitude from t_map_office_building where longitude !='不明' and b_city='%s' " % city_name
        cur_mapmarker.execute(sql_office)
        results_office = cur_mapmarker.fetchall()
        first_office = get_two_data_buildings(results_office, lat1, lng1)
        second_office = get_two_data_buildings(results_office, lat2, lng2)
        # 查询医院个数
        sql_hospital = "SELECT hospital_lng,hospital_lat,hospital_name from t_map_hospital_info WHERE hospital_lat !='不明' "
        cur_mapmarker.execute(sql_hospital)
        results_hospital = cur_mapmarker.fetchall()
        first_hospital = get_two_data_buildings(results_hospital, lat1, lng1)
        second_hospital = get_two_data_buildings(results_hospital, lat2, lng2)
        # 查询学校
        sql_school = "SELECT school_lng,school_lat,school_name from t_map_school_info WHERE school_city='%s' and  school_lng !='不明'" % city_name
        cur_mapmarker.execute(sql_school)
        results_school = cur_mapmarker.fetchall()
        first_school = get_two_data_buildings(results_school, lat1, lng1)
        second_school = get_two_data_buildings(results_school, lat2, lng2)

        # 查询商场
        city_name2 = city_name[0:2]
        sql_shopping = "SELECT buildings_longitude,buildings_latitude,total_type,buildings_name from t_map_buildings where city_name='%s' and total_type='商场'" % city_name2
        cur_mapmarker.execute(sql_shopping)
        results_shopping = cur_mapmarker.fetchall()
        first_shopping = get_two_data_buildings(results_shopping, lat1, lng1)
        second_shopping = get_two_data_buildings(results_shopping, lat2, lng2)
        # 查询酒店
        sql_hotal = "SELECT buildings_longitude,buildings_latitude,total_type,buildings_name from t_map_buildings where city_name='%s' and total_type='酒店'" % city_name2
        cur_mapmarker.execute(sql_hotal)
        results_hotal = cur_mapmarker.fetchall()
        first_hotal = get_two_data_buildings(results_hotal, lat1, lng1)
        second_hotal = get_two_data_buildings(results_hotal, lat2, lng2)

        if platform == 'mt':
            if cate_name != '':
                sql3 = "SELECT month_sale_num,latitude,longitude,update_time,average_price  from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                       "'%s' and longitude BETWEEN '%s' and '%s' and own_set_cate='%s' and month_sale_num>0 and average_price is not null" % (
                           platform, city, up_time, to_time, down_lat1, up_lat1, down_lng1, up_lng1, cate_name)

                sql4 = "SELECT month_sale_num,latitude,longitude,update_time,average_price  from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                       "'%s' and longitude BETWEEN '%s' and '%s' and own_set_cate='%s' and month_sale_num>0 and average_price is not null" % (
                           platform, city, up_time, to_time, down_lat2, up_lat2, down_lng2, up_lng2, cate_name)
            else:
                sql3 = "SELECT month_sale_num,latitude,longitude,update_time,average_price  from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                       "'%s' and longitude BETWEEN '%s' and '%s'  and month_sale_num>0 and average_price is not null" % (
                           platform, city, up_time, to_time, down_lat1, up_lat1, down_lng1, up_lng1)

                sql4 = "SELECT month_sale_num,latitude,longitude,update_time,average_price  from t_map_client_%s_%s_mark where update_time BETWEEN %s and %s and own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and latitude BETWEEN '%s' and  " \
                       "'%s' and longitude BETWEEN '%s' and '%s'  and month_sale_num>0 and average_price is not null" % (
                           platform, city, up_time, to_time, down_lat2, up_lat2, down_lng2, up_lng2)

            cur_mapmarker.execute(sql3)
            results3 = cur_mapmarker.fetchall()
            first_ave_list = get_two_ave(results3, lat1, lng1)

            cur_mapmarker.execute(sql4)
            results4 = cur_mapmarker.fetchall()
            second_ave_list = get_two_ave(results4, lat2, lng2)
            first_list[0]['income_ave'] = first_ave_list[0]
            first_list[0]['house_count'] = first_house[0]
            first_list[0]['office_count'] = first_office[0]
            first_list[0]['hospital_count'] = first_hospital[0]
            first_list[0]['school_count'] = first_school[0]
            first_list[0]['shopping_count'] = first_shopping[0]
            first_list[0]['hotal_count'] = first_hotal[0]
            first_list[1]['income_ave'] = first_ave_list[1]
            first_list[1]['house_count'] = first_house[1]
            first_list[1]['office_count'] = first_office[1]
            first_list[1]['hospital_count'] = first_hospital[1]
            first_list[1]['school_count'] = first_school[1]
            first_list[1]['shopping_count'] = first_shopping[1]
            first_list[1]['hotal_count'] = first_hotal[1]
            first_list[2]['income_ave'] = first_ave_list[2]
            first_list[2]['house_count'] = first_house[2]
            first_list[2]['office_count'] = first_office[2]
            first_list[2]['hospital_count'] = first_hospital[2]
            first_list[2]['school_count'] = first_school[2]
            first_list[2]['shopping_count'] = first_shopping[2]
            first_list[2]['hotal_count'] = first_hotal[2]

            second_list[0]['income_ave'] = second_ave_list[0]
            second_list[0]['house_count'] = second_house[0]
            second_list[0]['office_count'] = second_office[0]
            second_list[0]['hospital_count'] = second_hospital[0]
            second_list[0]['school_count'] = second_school[0]
            second_list[0]['shopping_count'] = second_shopping[0]
            second_list[0]['hotal_count'] = second_hotal[0]
            second_list[1]['income_ave'] = second_ave_list[1]
            second_list[1]['house_count'] = second_house[1]
            second_list[1]['office_count'] = second_office[1]
            second_list[1]['hospital_count'] = second_hospital[1]
            second_list[1]['school_count'] = second_school[1]
            second_list[1]['shopping_count'] = second_shopping[1]
            second_list[1]['hotal_count'] = second_hotal[1]
            second_list[2]['income_ave'] = second_ave_list[2]
            second_list[2]['house_count'] = second_house[2]
            second_list[2]['office_count'] = second_office[2]
            second_list[2]['hospital_count'] = second_hospital[2]
            second_list[2]['school_count'] = second_school[2]
            second_list[2]['shopping_count'] = second_shopping[2]
            second_list[2]['hotal_count'] = second_hotal[2]
        else:
            first_list[0]['house_count'] = first_house[0]
            first_list[0]['office_count'] = first_office[0]
            first_list[0]['hospital_count'] = first_hospital[0]
            first_list[0]['school_count'] = first_school[0]
            first_list[0]['shopping_count'] = first_shopping[0]
            first_list[0]['hotal_count'] = first_hotal[0]

            first_list[1]['house_count'] = first_house[1]
            first_list[1]['office_count'] = first_office[1]
            first_list[1]['hospital_count'] = first_hospital[1]
            first_list[1]['school_count'] = first_school[1]
            first_list[1]['shopping_count'] = first_shopping[1]
            first_list[1]['hotal_count'] = first_hotal[1]

            first_list[2]['house_count'] = first_house[2]
            first_list[2]['office_count'] = first_office[2]
            first_list[2]['hospital_count'] = first_hospital[2]
            first_list[2]['school_count'] = first_school[2]
            first_list[2]['shopping_count'] = first_shopping[2]
            first_list[2]['hotal_count'] = first_hotal[2]

            second_list[0]['house_count'] = second_house[0]
            second_list[0]['office_count'] = second_office[0]
            second_list[0]['hospital_count'] = second_hospital[0]
            second_list[0]['school_count'] = second_school[0]
            second_list[0]['shopping_count'] = second_shopping[0]
            second_list[0]['hotal_count'] = second_hotal[0]

            second_list[1]['house_count'] = second_house[1]
            second_list[1]['office_count'] = second_office[1]
            second_list[1]['hospital_count'] = second_hospital[1]
            second_list[1]['school_count'] = second_school[1]
            second_list[1]['shopping_count'] = second_shopping[1]
            second_list[1]['hotal_count'] = second_hotal[1]

            second_list[2]['house_count'] = second_house[2]
            second_list[2]['office_count'] = second_office[2]
            second_list[2]['hospital_count'] = second_hospital[2]
            second_list[2]['school_count'] = second_school[2]
            second_list[2]['shopping_count'] = second_shopping[2]
            second_list[2]['hotal_count'] = second_hotal[2]

        json_dict = {}

        json_dict['first_list'] = first_list
        json_dict['second_list'] = second_list
        jsondatar = json.dumps(json_dict, ensure_ascii=False)
        redis_conn.set(cheak_key, jsondatar)
        redis_conn.expire(cheak_key, 2592000)
        db_mapmarker.close()

    return jsondatar


app.register_blueprint(api, url_prefix='/')
app.register_blueprint(platform, url_prefix='/platform')

if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host="0.0.0.0", port=5001, debug=True, threaded=True)
