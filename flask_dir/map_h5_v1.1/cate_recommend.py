import math
import pandas as pd
import pymysql
from DBUtils.PooledDB import PooledDB

"""
计算品类推荐页面数据
"""

#elm3m,elm3c,elm2m,elm2c,elm1m,elm1c,mt3m,mt3c,mt2m,mt2c,mt1m,mt1c

EARTH_REDIUS = 6378.137
xm_project_info={"百脑汇":["北京","北京市朝阳区朝外大街99号二层","116.445409,39.923854"],
"曹杨路店":["上海","上海市普陀区曹杨路458、460、462号地下一层04室","121.420233,31.236898"],
"曹杨路二店":["上海","上海市普陀区曹杨路店1040弄2号楼三楼03室","121.405638,31.245883"],
"朝阳路红星美凯龙":["北京","北京市朝阳区高井村甲8号朝阳路红星美凯龙五层E8001-E8007","116.540271,39.914985"],
"车公庄":["北京","北京市西城区车公庄大街9号五栋大楼C座1楼","116.349191,39.933619"],
"大望路":["北京","朝阳区西大望路3号院2号楼蓝堡国际","116.477324,39.912110"],
"东直门":["北京","北京市东城区东直门外大街42号宇飞大厦地上三层商业301-05","116.437370,39.940220"],
"东直门二店":["北京","北京市东城区东直门外大街42号三层","116.437566,39.940551"],
"方庄":["北京","朝阳区方庄东路饮马井45号院","116.445719,39.866932"],
"古北路店":["上海","上海市闵行区古北路1699号B2层261室","121.402820,31.188528"],
"古墩路店":["杭州","浙江省杭州市西湖区古墩路701号紫金广场B座501室","120.095120,30.308657"],
"广渠门":["北京","北京市东城区广渠门内大街29号二层","116.434295,39.893905"],
"广中西路店":["上海","上海市静安区广中西路777弄91、99号B1层B区","121.438338,31.278491"],
"国安路店":["上海","上海市杨浦区国霞路345号203-206室","121.505422,31.310684"],
"国定东路店":["上海","上海市国定东路293号","121.520749,31.296839"],
"国贸":["北京","北京市朝阳区永安西里10号","116.447587,39.906753"],
"国美第一城":["北京","北京市朝阳区青年路西里2号院11号楼商业01一层消防通道至（15）轴","116.513105,39.932906"],
"国权路店":["上海","上海市杨浦区国权路43号地下1层01、02、03、06、07、08、09、10室","121.518986,31.287088"],
"国展":["北京","北京市朝阳区静安西街10号国展宾馆内二层","116.445096,39.960612"],
"河南北路店":["上海","上海市河南北路445号","121.479984,31.249157"],
"河南北路二店":["上海","上海市静安区河南北路441号105室","121.479738,31.249139"],
"呼家楼":["北京","北京市朝阳区呼家楼北街7号B1层","116.466348,39.922827"],
"呼家楼二店":["北京","北京市朝阳区呼家楼北街7号B1层","116.466348,39.922827"],
"华贸天地":["北京","北京市朝阳区清河营南街7号院8号楼02层","116.430537,40.048231"],
"淮海东路店":["上海","上海市黄浦区淮海东路99号5楼506室","121.480955,31.225097"],
"淮海东路二店":["上海","上海市黄浦区淮海东路99号5楼502室","121.480955,31.225097"],
"汇海广场":["深圳","深圳市龙华区龙华街道三联社区三联创业路19号弓村新城商业中心B栋1层102/103/105室","113.969517,22.703670"],
"建国门":["北京","北京市朝阳区建国门外大街16号东方瑞景小区","116.442863,39.906467"],
"建国门贵友":["北京","北京市朝阳区建国门外大街甲5号","116.450913,39.908757"],
"江宁路店":["上海","上海市普陀区江宁路1400号5幢一层-7","121.440830,31.247180"],
"江宁路二店":["上海","上海市普陀区江宁路1400号5幢一层1室-6室","121.440830,31.247180"],
"金沙江路店":["上海","上海市普陀区金沙江路788号4F","121.398628,31.232255"],
"金沙江路二店":["上海","上海市普陀区金沙江路1759号1幢C1-108,C1-109室","121.376396,31.232101"],
"劲松":["北京","北京市朝阳区南磨房路37号2层202","116.466887,39.884817"],
"经开大厦":["北京","北京市大兴区地盛北街1号","116.495400,39.783364"],
"酒仙桥":["北京","北京市朝阳区酒仙桥东路18号尚科办公室社区地下1层","116.500218,39.974830"],
"酒仙桥二店":["北京","北京市朝阳区酒仙桥东路18号尚科办公室社区地上2层","116.500218,39.974830"],
"凯旋路店":["上海","上海市徐汇区凯旋路2588号501-1B室A区","121.429156,31.185246"],
"控江路店":["上海","上海市杨浦区控江路1555号B1层","121.518979,31.274719"],
"兰溪路店":["上海","上海市普陀区兰溪路141号4楼C区","121.404623,31.239418"],
"梨园":["北京","通州区云景东路1号魔方公寓4楼","116.666818,39.885136"],
"理想国":["北京","北京朝阳区双花园南里二区11号楼#201","116.451634,39.898776"],
"灵石路店":["上海","上海市静安区灵石路851号6幢二层T、S、R、","121.433442,31.278409"],
"六里桥":["北京","北京市丰台区西三环南路10-3号","116.312850,39.879153"],
"龙舌路店":["杭州","浙江省杭州市上城区龙舌路46号3楼","120.176335,30.215449"],
"马家堡":["北京","北京市丰台区角门路19号院2号楼3层3F-2号商铺","116.378545,39.836516"],
"梅华路店":["深圳","深圳市福田区梅华路103号光荣大厦（又名南鹏大厦）1楼及2楼东","114.056271,22.564189"],
"莫干山路店":["杭州","浙江省拱墅区莫干山路1177号星尚发展大厦1幢222-226室","120.138545,30.296969"],
"牡丹园":["北京","北京市海淀区花园路甲2号院4号楼2层","116.366772,39.979986"],
"南京西路店":["上海","上海市黄浦区南京西路388号仙乐斯广场4F414-417","121.469046,31.231442"],
"庆春路店":["杭州","浙江省杭州市下城区庆春路38号金龙财富中心3层305室","120.182414,30.259049"],
"秋涛北路店":["杭州","浙江省杭州市秋涛北路451号4楼","120.197465,30.289827"],
"日坛":["北京","北京市朝阳区神路街39号10幢1至2层4段39、50","116.443404,39.919294"],
"软件园":["北京","北京市海淀区西北旺东路10号院东区1号楼B105","116.281745,40.042669"],
"瑞和国际":["北京","北京市朝阳区定福庄北街甲15号院7号楼F3-301","116.553348,39.922238"],
"瑞和国际二店":["北京","北京市朝阳区定福庄北街甲15号院7号楼F3-301","116.553348,39.922238"],
"三元桥":["北京","北京市朝阳区霄云路霄云里6号一层东南侧","116.464190,39.961761"],
"商城路店":["上海","上海市浦东新区商城路665号一幢地下一层B103","121.517848,31.231031"],
"上地":["北京","北京市海淀区农大南路一号院1号楼-1层B102-2室","116.311730,40.028740"],
"深南东路店":["深圳","深圳市罗湖区南湖街道深南东路4003号世界金融中心裙楼三层302","114.114366,22.542001"],
"深南中路店":["深圳","深圳市福田区深南中路2020号茂业百货店4楼","114.086164,22.544956"],
"十里堡":["北京","北京市朝阳区八里庄北里118号楼1层1000","116.496839,39.928378"],
"十里堡二店":["北京","北京市朝阳区八里庄北里118号楼1层1018","116.496839,39.928378"],
"十里河":["北京","朝阳区大羊坊路85号汇金中心B座108","116.471395,39.860050"],
"石景山":["北京","北京市石景山区石景山路54号院7幢-1层-101","116.185292,39.906454"],
"双井":["北京","北京市朝阳区广渠路66号院甲19号楼1层A1","116.465519,39.891299"],
"双井二店":["北京","北京市朝阳区广渠路66号院甲19号楼2层","116.465519,39.891299"],
"四川北路一店":["上海","上海市虹口区四川北路1885号地下一层A单元","121.483640,31.261840"],
"四道口":["北京","北京市海淀区皂君庙14号院鑫雅苑5号楼二层","116.338391,39.961843"],
"松江路店":["上海","上海市松江区新松江路1292弄","121.211358,31.039823"],
"淞沪路店":["上海","上海杨浦区淞沪路161号5楼","121.513400,31.303436"],
"天山路店":["上海","上海市长宁区天山路900号6F","121.405047,31.211373"],
"天山西路店":["上海","上海市长宁区天山西路1068号3幢A栋B1-A、B","121.354330,31.220256"],
"驼房营":["北京","朝阳区将台乡驼房营路与将台路交叉路口西南角京客隆超市","116.469337,39.922377"],
"望京":["北京","北京市朝阳区望京街9号商业楼B1层08-1","116.482756,39.989440"],
"望京SOHO":["北京","北京市朝阳区阜通东大街3号楼3层1301、1302、1303、1305、1306房屋","116.480437,39.989732"],
"望京六佰本":["北京","北京市朝阳区望京广顺北大街六佰本","116.466726,40.008103"],
"望京西园":["北京","北京市朝阳区望京西园三区312号楼商业楼地下一层","116.477833,39.999570"],
"文二路店":["杭州","浙江省杭州市西湖区西溪街道文二路126号第一层及兰庭公寓2幢第二层","120.138891,30.283350"],
"文三路店":["杭州","杭州市西湖区文三路535号莱茵达大厦二层","120.123939,30.275992"],
"文一路店":["杭州","浙江省杭州市西湖区西溪街道文一路122-13号地下一层B1-20","120.136831,30.289213"],
"汶水路店":["上海","上海市静安区汶水路40号39栋2F","121.458957,31.293694"],
"吴中路店":["上海","上海市闵行区吴中路1050号5幢东楼124-125","121.386823,31.178439"],
"五道口二店":["北京","海淀区暂安处1号蓟鑫大厦C区","116.340098,39.988341"],
"五道口一店":["北京","北京市海淀区暂安处1号蓟鑫大厦D区301","116.340098,39.988341"],
"武宁路店":["上海","上海市普陀区中山北路2688号238A、238B、238C室","121.420360,31.241786"],
"西直门":["北京","西直门北大街41号天兆家园","116.353350,39.953020"],
"协和路二店":["上海","上海市长宁区协和路788号二层202室","121.356079,31.216720"],
"协和路一店":["上海","上海市长宁区协和路788号","121.356079,31.216720"],
"斜徐路店":["上海","黄浦区斜徐路595号206-207室2F-4F层","121.472622,31.206286"],
"新华百货":["北京","北京市西城区新街口北大街1号6层部分","116.371505,39.947200"],
"新荟城店":["北京","北京市朝阳区望京东园一区120号楼-2至6层101内五层5F-6b","116.482311,39.998540"],
"新荟城二店":["北京","北京市朝阳区望京东园一区120号新荟城","116.481863,39.998482"],
"星光大道店":["杭州","浙江省杭州市滨江区星光大道星光国际广场1幢4层402-405","120.209191,30.207159"],
"星光影视园":["北京","北京市大兴区春和路39号院1号楼1单元101B、102B、103B、105B、107B、及106","116.359053,39.773025"],
"雅宝城":["北京","北京市朝阳区日坛北路19号楼B1层餐厅","116.441679,39.918641"],
"延长路店":["上海","上海市静安区延长路50号2幢2层203室","121.460767,31.273521"],
"叶家宅路店":["上海","上海市普陀区叶家宅100号-创享塔园区10幢-145-159单元","121.432608,31.237550"],
"悠乐汇":["北京","北京望京园610号2层202","116.477499,39.990678"],
"右安门":["北京","北京市丰台区右安门外大街2号迦南大厦二层","116.365739,39.868642"],
"枣营麦子店":["北京","北京朝阳区麦子店街枣营北里19号","116.471202,39.946293"],
"长虹桥":["北京","北京市朝阳区农展馆南路12号通广大厦4层","116.463062,39.934231"],
"长虹桥二店":["北京","北京市朝阳区农展馆南路12号2号楼4层4001、4003室","116.462630,39.934120"],
"中关村":["北京","北京市海淀区中关村三街6号中科资源大厦地下1层南侧B12","116.328217,39.984981"],
"中山西路店":["上海","上海市中山西路2368号106室","121.431328,31.181053"],
"铸诚大厦":["北京","北京中关村南大街甲6号","116.324353,39.964480"],
"自空间":["北京","北京自空间写字园E座","116.504842,39.902526"],
"星光影视园2F":["北京","北京市大兴区春和路39号院1号楼1单元101B、102B、103B、105B、107B、及106","116.359053,39.773025"]

}
# 计算两个经纬度之间的距离
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

def group_by(key_list,value_list):
    pd_dir = {}
    pd_dir['key'] = key_list
    pd_dir['data'] = value_list
    df = pd.DataFrame(pd_dir)
    kk = df.groupby(['key'], as_index=False)['data'].sum()
    kk_count = df.groupby(['key'], as_index=False)['data'].count()
    cate_name = []
    cate_name1 = []
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
    zip3 = zip(sorted1, sorted2)
    res = []
    for qwe in zip3:
        res.append((qwe[0][0], qwe[0][1], qwe[1][1]))
    return res


city_list=['北京','上海','杭州','深圳']
#[10,13,'2019-11-01'],
time=[[7,10,'2019-08-01']]
# time=[[7,10,'2019-08-01'],[6,9,'2019-07-01'],[8,11,'2019-09-01']]
time=[[8,10,'2019-08-01'],[9,11,'2019-09-01'],[10,12,'2019-10-01'],[11,13,'2019-11-01']]
pool_mapmarkeronline = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                                db='mapmarkeronline', port=62864)

# pool_test = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
#                                 db='test', port=62864)
pool_project = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei', passwd='tanglei', db='commerce',
                        port=3306)
for ti in time:
    for city in city_list:
        if city == '北京':
            city_name = 'beijing'
        elif city == '上海':
            city_name = 'shanghai'
        elif city == '杭州':
            city_name = 'hangzhou'
        elif city == '深圳':
            city_name = 'shenzhen'
        db = pool_project.connection()
        db2 = pool_mapmarkeronline.connection()
        # db3 = pool_test.connection()
        cur = db.cursor()
        sql = "SELECT a.project_id,a.project_name,b.address,b.latitude,b.longitude from project a LEFT JOIN development.project_base_info b on a.project_id=b.tid WHERE a.area_name='%s'" % city
        cur.execute(sql)
        results = cur.fetchall()
        for row in results:
            cate_dir = {"东南亚菜": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "其他品类": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "包子粥店": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "地方菜系": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "夹馍饼类": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "川湘菜": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "快餐便当": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "意面披萨": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "日料寿司": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "暖胃粉丝汤": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "汉堡薯条": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "火锅串串": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "特色小吃": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "甜品饮品": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "米粉面馆": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "西式料理": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "超市便利": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "轻食沙拉": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "韩式料理": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "饺子馄饨": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "香锅干锅": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "鸭脖卤味": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "麻辣烫冒菜": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "龙虾烧烤": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        "炸鸡炸串": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

                        }
            project_id=row[0]
            project_name=row[1]
            project_list = xm_project_info.get(project_name)
            project_latitude = float(project_list[2].split(',')[1])
            project_longitude = float(project_list[2].split(',')[0])
            up_lat = project_latitude + 0.04
            down_lat = project_latitude - 0.04
            up_lng = project_longitude + 0.05
            down_lng = project_longitude - 0.05
            sql_mt = "select month_sale_num,latitude,longitude,own_set_cate from t_map_client_mt_%s_mark where  " \
                  " update_count=%s and month_sale_num!=0  and latitude " \
                  "BETWEEN '%s' and '%s' and longitude BETWEEN '%s' and '%s'" % (city_name,ti[0], down_lat, up_lat, down_lng, up_lng)
            cur2 = db2.cursor()
            cur2.execute(sql_mt)
            results_mt = cur2.fetchall()

            sql_elm = "select month_sale_num,latitude,longitude,own_set_cate from t_map_client_elm_%s_mark where  " \
                     "own_first_cate not in ('商店超市','果蔬生鲜','鲜花绿植','医药健康') and update_count=%s and month_sale_num!=0  and latitude " \
                     "BETWEEN '%s' and '%s' and longitude BETWEEN '%s' and '%s'" % (city_name,ti[1], down_lat, up_lat, down_lng, up_lng)
            cur3 = db2.cursor()
            cur3.execute(sql_elm)
            results_elm = cur3.fetchall()

            key_list3_elm = []
            value_list3_elm = []
            key_list2_elm = []
            value_list2_elm = []
            key_list1_elm = []
            value_list1_elm = []

            for row2 in results_elm:
                month_sale_num = row2[0]
                latitude = float(row2[1])
                longitude = float(row2[2])
                own_set_cate = row2[3]
                distance = getDistance(project_latitude, project_longitude, latitude, longitude)
                if distance <= 1:
                    key_list3_elm.append(own_set_cate)
                    value_list3_elm.append(month_sale_num)
                    key_list2_elm.append(own_set_cate)
                    value_list2_elm.append(month_sale_num)
                    key_list1_elm.append(own_set_cate)
                    value_list1_elm.append(month_sale_num)
                elif distance <= 2 and distance>1:
                    key_list3_elm.append(own_set_cate)
                    value_list3_elm.append(month_sale_num)
                    key_list2_elm.append(own_set_cate)
                    value_list2_elm.append(month_sale_num)
                elif distance <= 3 and distance>2:
                    key_list3_elm.append(own_set_cate)
                    value_list3_elm.append(month_sale_num)
            key_list3_mt = []
            value_list3_mt = []
            key_list2_mt = []
            value_list2_mt = []
            key_list1_mt = []
            value_list1_mt = []

            for row2 in results_mt:
                month_sale_num = row2[0]
                latitude = float(row2[1])
                longitude = float(row2[2])
                own_set_cate = row2[3]
                distance = getDistance(project_latitude, project_longitude, latitude, longitude)
                if distance <= 1:
                    key_list3_mt.append(own_set_cate)
                    value_list3_mt.append(month_sale_num)
                    key_list2_mt.append(own_set_cate)
                    value_list2_mt.append(month_sale_num)
                    key_list1_mt.append(own_set_cate)
                    value_list1_mt.append(month_sale_num)
                elif distance <= 2 and distance > 1:
                    key_list3_mt.append(own_set_cate)
                    value_list3_mt.append(month_sale_num)
                    key_list2_mt.append(own_set_cate)
                    value_list2_mt.append(month_sale_num)
                elif distance <= 3 and distance>2:
                    key_list3_mt.append(own_set_cate)
                    value_list3_mt.append(month_sale_num)

            group_elm_1=group_by(key_list1_elm,value_list1_elm)
            group_elm_2=group_by(key_list2_elm,value_list2_elm)
            group_elm_3=group_by(key_list3_elm,value_list3_elm)

            group_mt_1=group_by(key_list1_mt,value_list1_mt)
            group_mt_2=group_by(key_list2_mt,value_list2_mt)
            group_mt_3=group_by(key_list3_mt,value_list3_mt)

            for gr in group_elm_3:
                cate_dir[gr[0]]=[gr[1],gr[2],0,0,0,0,0,0,0,0,0,0]
            for gr in group_elm_2:
                dict1_value=cate_dir.get(gr[0])
                cate_dir[gr[0]]=[dict1_value[0],dict1_value[1],gr[1],gr[2],0,0,0,0,0,0,0,0]
            for gr in group_elm_1:
                dict2_value = cate_dir.get(gr[0])
                cate_dir[gr[0]] = [dict2_value[0], dict2_value[1], dict2_value[2], dict2_value[3], gr[1], gr[2], 0, 0, 0, 0, 0, 0]
            for gr in group_mt_3:
                print(gr[0])
                dict3_value = cate_dir.get(gr[0])

                cate_dir[gr[0]] = [dict3_value[0], dict3_value[1], dict3_value[2], dict3_value[3], dict3_value[4], dict3_value[5], gr[1], gr[2], 0, 0,0, 0]
            for gr in group_mt_2:
                dict4_value = cate_dir.get(gr[0])
                cate_dir[gr[0]] = [dict4_value[0], dict4_value[1], dict4_value[2], dict4_value[3], dict4_value[4], dict4_value[5], dict4_value[6], dict4_value[7], gr[1], gr[2], 0, 0]
            for gr in group_mt_1:
                dict5_value = cate_dir.get(gr[0])
                cate_dir[gr[0]] = [dict5_value[0], dict5_value[1], dict5_value[2], dict5_value[3], dict5_value[4], dict5_value[5], dict5_value[6], dict5_value[7], dict5_value[8], dict5_value[9], gr[1], gr[2]]
            # print(cate_dir)
            sql_inster="INSERT into t_map_h5_cate (project_id,cate_name,month_sale_num_elm_3,shop_num_elm_3,month_sale_num_elm_2,shop_num_elm_2," \
                       "month_sale_num_elm_1,shop_num_elm_1,month_sale_num_mt_3,shop_num_mt_3,month_sale_num_mt_2,shop_num_mt_2,month_sale_num_mt_1," \
                       "shop_num_mt_1,update_time,project_name,city) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for key_dict in cate_dir.keys():
                value_dict=cate_dir.get(key_dict)
                sql_values=(project_id,key_dict,value_dict[0],value_dict[1],value_dict[2],value_dict[3],value_dict[4],value_dict[5],value_dict[6],value_dict[7],value_dict[8],value_dict[9],value_dict[10],value_dict[11],ti[2],project_name,city)
                cur4 = db2.cursor()
                cur4.execute(sql_inster, sql_values)
            db2.commit()
            cur4.close()

        db.close()
        db2.close()
