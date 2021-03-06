import csv
import math

from DBUtils.PooledDB import PooledDB
import pymysql

project_list = [["北京", "百脑汇", "北京市朝阳区朝外大街99号二层", "116.445409", "39.923854"],
                ["北京", "朝阳路红星美凯龙", "北京市朝阳区高井村甲8号朝阳路红星美凯龙五层E8001-E8007", "116.540271", "39.914985"],
                ["北京", "车公庄", "北京市西城区车公庄大街9号五栋大楼C座1楼", "116.349191", "39.933619"],
                ["北京", "大望路", "朝阳区西大望路3号院2号楼蓝堡国际", "116.477324", "39.91211"],
                ["北京", "东直门", "北京市东城区东直门外大街42号宇飞大厦地上三层商业301-05", "116.43737", "39.94022"],
                ["北京", "东直门二店", "北京市东城区东直门外大街42号三层", "116.437566", "39.940551"],
                ["北京", "方庄", "朝阳区方庄东路饮马井45号院", "116.445719", "39.866932"],
                ["北京", "广渠门", "北京市东城区广渠门内大街29号二层", "116.434295", "39.893905"],
                ["北京", "国贸", "北京市朝阳区永安西里10号", "116.447587", "39.906753"],
                ["北京", "国美第一城", "北京市朝阳区青年路西里2号院11号楼商业01一层消防通道至（15）轴", "116.513105", "39.932906"],
                ["北京", "国展", "北京市朝阳区静安西街10号国展宾馆内二层", "116.445096", "39.960612"],
                ["北京", "呼家楼", "北京市朝阳区呼家楼北街7号B1层", "116.466348", "39.922827"],
                ["北京", "呼家楼二店", "北京市朝阳区呼家楼北街7号B1层", "116.466348", "39.922827"],
                ["北京", "华贸天地", "北京市朝阳区清河营南街7号院8号楼02层", "116.430537", "40.048231"],
                ["北京", "建国门", "北京市朝阳区建国门外大街16号东方瑞景小区", "116.442863", "39.906467"],
                ["北京", "建国门贵友", "北京市朝阳区建国门外大街甲5号", "116.450913", "39.908757"],
                ["北京", "劲松", "北京市朝阳区南磨房路37号2层202", "116.466887", "39.884817"],
                ["北京", "经开大厦", "北京市大兴区地盛北街1号", "116.4954", "39.783364"],
                ["北京", "酒仙桥", "北京市朝阳区酒仙桥东路18号尚科办公室社区地下1层", "116.500218", "39.97483"],
                ["北京", "酒仙桥二店", "北京市朝阳区酒仙桥东路18号尚科办公室社区地上2层", "116.500218", "39.97483"],
                ["北京", "梨园", "通州区云景东路1号魔方公寓4楼", "116.666818", "39.885136"],
                ["北京", "理想国", "北京朝阳区双花园南里二区11号楼#201", "116.451634", "39.898776"],
                ["北京", "六里桥", "北京市丰台区西三环南路10-3号", "116.31285", "39.879153"],
                ["北京", "马家堡", "北京市丰台区角门路19号院2号楼3层3F-2号商铺", "116.378545", "39.836516"],
                ["北京", "牡丹园", "北京市海淀区花园路甲2号院4号楼2层", "116.366772", "39.979986"],
                ["北京", "日坛", "北京市朝阳区神路街39号10幢1至2层4段39、50", "116.443404", "39.919294"],
                ["北京", "软件园", "北京市海淀区西北旺东路10号院东区1号楼B105", "116.281745", "40.042669"],
                ["北京", "瑞和国际", "北京市朝阳区定福庄北街甲15号院7号楼F3-301", "116.553348", "39.922238"],
                ["北京", "瑞和国际二店", "北京市朝阳区定福庄北街甲15号院7号楼F3-301", "116.553348", "39.922238"],
                ["北京", "三元桥", "北京市朝阳区霄云路霄云里6号一层东南侧", "116.46419", "39.961761"],
                ["北京", "上地", "北京市海淀区农大南路一号院1号楼-1层B102-2室", "116.31173", "40.02874"],
                ["北京", "十里堡", "北京市朝阳区八里庄北里118号楼1层1000", "116.496839", "39.928378"],
                ["北京", "十里堡二店", "北京市朝阳区八里庄北里118号楼1层1018", "116.496839", "39.928378"],
                ["北京", "十里河", "朝阳区大羊坊路85号汇金中心B座108", "116.471395", "39.86005"],
                ["北京", "石景山", "北京市石景山区石景山路54号院7幢-1层-101", "116.185292", "39.906454"],
                ["北京", "双井", "北京市朝阳区广渠路66号院甲19号楼1层A1", "116.465519", "39.891299"],
                ["北京", "双井二店", "北京市朝阳区广渠路66号院甲19号楼2层", "116.465519", "39.891299"],
                ["北京", "四道口", "北京市海淀区皂君庙14号院鑫雅苑5号楼二层", "116.338391", "39.961843"],
                ["北京", "驼房营", "朝阳区将台乡驼房营路与将台路交叉路口西南角京客隆超市", "116.502487", "39.970438"],
                ["北京", "望京", "北京市朝阳区望京街9号商业楼B1层08-1", "116.482756", "39.98944"],
                ["北京", "望京SOHO", "北京市朝阳区阜通东大街3号楼3层1301、1302、1303、1305、1306房屋", "116.480437", "39.989732"],
                ["北京", "望京六佰本", "北京市朝阳区望京广顺北大街六佰本", "116.466726", "40.008103"],
                ["北京", "望京西园", "北京市朝阳区望京西园三区312号楼商业楼地下一层", "116.477833", "39.99957"],
                ["北京", "五道口二店", "海淀区暂安处1号蓟鑫大厦C区", "116.340098", "39.988341"],
                ["北京", "五道口一店", "北京市海淀区暂安处1号蓟鑫大厦D区301", "116.340098", "39.988341"],
                ["北京", "西直门", "西直门北大街41号天兆家园", "116.35335", "39.95302"],
                ["北京", "新华百货", "北京市西城区新街口北大街1号6层部分", "116.371505", "39.9472"],
                ["北京", "新荟城", "北京市朝阳区望京东园一区120号楼-2至6层101内五层5F-6b", "116.482311", "39.99854"],
                ["北京", "新荟城二店", "北京市朝阳区望京东园一区120号新荟城", "116.481863", "39.998482"],
                ["北京", "星光影视园", "北京市大兴区春和路39号院1号楼1单元101B、102B、103B、105B、107B、及106", "116.359053", "39.773025"],
                ["北京", "星光影视园2F", "北京市大兴区春和路39号院1号楼1单元101B、102B、103B、105B、107B、及106", "116.359053", "39.773025"],
                ["北京", "雅宝城", "北京市朝阳区日坛北路19号楼B1层餐厅", "116.441679", "39.918641"],
                ["北京", "悠乐汇", "望京园610号2层202", "116.477499", "39.990678"],
                ["北京", "右安门", "北京市丰台区右安门外大街2号迦南大厦二层", "116.365739", "39.868642"],
                ["北京", "枣营麦子店", "朝阳区麦子店街枣营北里19号", "116.471202", "39.946293"],
                ["北京", "长虹桥", "北京市朝阳区农展馆南路12号通广大厦4层", "116.463062", "39.934231"],
                ["北京", "长虹桥二店", "北京市朝阳区农展馆南路12号2号楼4层4001、4003室", "116.46263", "39.93412"],
                ["北京", "中关村", "北京市海淀区中关村三街6号中科资源大厦地下1层南侧B12", "116.328217", "39.984981"],
                ["北京", "铸诚大厦", "中关村南大街甲6号", "116.324353", "39.96448"],
                ["北京", "自空间", "自空间写字园E座", "116.504842", "39.902526"],
                ["杭州", "古墩路店", "浙江省杭州市西湖区古墩路701号紫金广场B座501室", "120.09512", "30.308657"],
                ["杭州", "龙舌路店", "浙江省杭州市上城区龙舌路46号3楼", "120.176335", "30.215449"],
                ["杭州", "莫干山路店", "浙江省拱墅区莫干山路1177号星尚发展大厦1幢222-226室", "120.138545", "30.296969"],
                ["杭州", "庆春路店", "浙江省杭州市下城区庆春路38号金龙财富中心3层305室", "120.182414", "30.259049"],
                ["杭州", "秋涛北路店", "浙江省杭州市秋涛北路451号4楼", "120.197465", "30.289827"],
                ["杭州", "文二路店", "浙江省杭州市西湖区西溪街道文二路126号第一层及兰庭公寓2幢第二层", "120.138891", "30.28335"],
                ["杭州", "文三路店", "杭州市西湖区文三路535号莱茵达大厦二层", "120.123939", "30.275992"],
                ["杭州", "文一路店", "浙江省杭州市西湖区西溪街道文一路122-13号地下一层B1-20", "120.136831", "30.289213"],
                ["杭州", "星光大道店", "浙江省杭州市滨江区星光大道星光国际广场1幢4层402-405", "120.209191", "30.207159"],
                ["上海", "曹杨路店", "上海市普陀区曹杨路458、460、462号地下一层04室", "121.420233", "31.236898"],
                ["上海", "曹杨路二店", "上海市普陀区曹杨路店1040弄2号楼三楼03室", "121.405638", "31.245883"],
                ["上海", "古北路店", "上海市闵行区古北路1699号B2层261室", "121.40282", "31.188528"],
                ["上海", "广中西路店", "上海市静安区广中西路777弄91、99号B1层B区", "121.438338", "31.278491"],
                ["上海", "国安路店", "上海市杨浦区国霞路345号203-206室", "121.505422", "31.310684"],
                ["上海", "国定东路店", "上海市国定东路293号", "121.520749", "31.296839"],
                ["上海", "国权路店", "上海市杨浦区国权路43号地下1层01、02、03、06、07、08、09、10室", "121.518986", "31.287088"],
                ["上海", "河南北路店", "上海市河南北路445号", "121.479984", "31.249157"],
                ["上海", "河南北路二店", "上海市静安区河南北路441号105室", "121.479738", "31.249139"],
                ["上海", "淮海东路店", "上海市黄浦区淮海东路99号5楼506室", "121.480955", "31.225097"],
                ["上海", "淮海东路二店", "上海市黄浦区淮海东路99号5楼502室", "121.480955", "31.225097"],
                ["上海", "江宁路店", "上海市普陀区江宁路1400号5幢一层-7", "121.44083", "31.24718"],
                ["上海", "江宁路二店", "上海市普陀区江宁路1400号5幢一层1室-6室", "121.44083", "31.24718"],
                ["上海", "金沙江路店", "上海市普陀区金沙江路788号4F", "121.398628", "31.232255"],
                ["上海", "金沙江路二店", "上海市普陀区金沙江路1759号1幢C1-108,C1-109室", "121.376396", "31.232101"],
                ["上海", "凯旋路店", "上海市徐汇区凯旋路2588号501-1B室A区", "121.429156", "31.185246"],
                ["上海", "控江路店", "上海区杨浦区控江路1555号B1层", "121.518979", "31.274719"],
                ["上海", "兰溪路店", "上海市普陀区兰溪路141号4楼C区", "121.404623", "31.239418"],
                ["上海", "灵石路店", "上海市静安区灵石路851号6幢二层T、S、R、", "121.433442", "31.278409"],
                ["上海", "南京西路店", "上海市黄浦区南京西路388号仙乐斯广场4F414-417", "121.469046", "31.231442"],
                ["上海", "商城路店", "上海市浦东新区商城路665号一幢地下一层B103", "121.517848", "31.231031"],
                ["上海", "四川北路一店", "上海市虹口区四川北路1885号地下一层A单元", "121.48364", "31.26184"],
                ["上海", "松江路店", "上海市松江区新松江路1292弄", "121.211358", "31.039823"],
                ["上海", "淞沪路店", "上海杨浦区淞沪路161号5楼", "121.5134", "31.303436"],
                ["上海", "天山路店", "上海市长宁区天山路900号6F", "121.405047", "31.211373"],
                ["上海", "天山西路店", "上海市长宁区天山西路1068号3幢A栋B1-A、B", "121.35433", "31.220256"],
                ["上海", "汶水路店", "上海市静安区汶水路40号39栋2F", "121.458957", "31.293694"],
                ["上海", "吴中路店", "上海市闵行区吴中路1050号5幢东楼124-125", "121.386823", "31.178439"],
                ["上海", "武宁路店", "上海市普陀区中山北路2688号238A、238B、238C室", "121.42036", "31.241786"],
                ["上海", "协和路二店", "上海市长宁区协和路788号二层202室", "121.356079", "31.21672"],
                ["上海", "协和路一店", "上海市长宁区协和路788号", "121.356079", "31.21672"],
                ["上海", "斜徐路店", "黄浦区斜徐路595号206-207室2F-4F层", "121.472622", "31.206286"],
                ["上海", "延长路店", "上海市静安区延长路50号2幢2层203室", "121.460767", "31.273521"],
                ["上海", "叶家宅路店", "上海市普陀区叶家宅100号-创享塔园区10幢-145-159单元", "121.432608", "31.23755"],
                ["上海", "中山西路店", "上海市中山西路2368号106室", "121.431328", "31.181053"],
                ["深圳", "汇海广场", "深圳市龙华区龙华街道三联社区三联创业路19号弓村新城商业中心B栋1层102/103/105室", "113.969517", "22.70367"],
                ["深圳", "梅华路店", "深圳市福田区梅华路103号光荣大厦（又名南鹏大厦）1楼及2楼东", "114.056271", "22.564189"],
                ["深圳", "深南东路店", "深圳市罗湖区南湖街道深南东路4003号世界金融中心裙楼三层302", "114.114366", "22.542001"],
                ["深圳", "深南中路店", "深圳市福田区深南中路2020号茂业百货店4楼", "114.086164", "22.544956"]
                ]

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


pool = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                db='mapmarkeronline',
                port=62864)  # 5为连接池里的最少连接数
db = pool.connection()
cur = db.cursor()

sql = "SELECT client_name,month_sale_num,average_price,latitude,longitude from t_map_client_mt_shanghai_mark where update_count=12 and average_price is not NULL and month_sale_num !=0"
cur.execute(sql)
results = cur.fetchall()
with open('data1.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    # writer.writerow(format_list)
    format_list = ['场地', '2km单量', '2km销量', '2km店均实收', '1km单量', '1km销量', '1km店均实收']
    writer.writerow(format_list)
    for project in project_list:
        city = project[0]
        project_name = project[1]
        address = project[2]
        lng = float(project[3])
        lat = float(project[4])
        all_shop_count1 = 0
        all_stream1 = 0
        all_shop_count2 = 0
        all_stream2 = 0
        for row in results:
            month_sale_num = float(row[1])
            average_price = float(row[2])
            stream = month_sale_num * average_price
            dis = getDistance(lat1=lat, lng1=lng, lat2=float(row[3]), lng2=float(row[4]))
            list2 = []
            list1 = []
            if dis <= 1:
                list1.append(row)
                list2.append(row)
                # all_shop_count1 += 1
                # all_stream1 += stream
                # all_shop_count2 += 1
                # all_stream2 += stream
            elif dis <= 2 and dis > 1:
                list2.append(row)
                # all_shop_count2 += 1
                # all_stream2 += stream
            list1=sorted(list1, key=lambda x: x[1], reverse=True)
        format_list = ['场地', '2km单量', '2km店均实收', '1km单量', '1km店均实收']

        writer.writerow([project[1], all_shop_count2, all_stream2, all_shop_count1, all_stream1])
