# coding:utf-8
import datetime
import os

from DBUtils.PooledDB import PooledDB
from flask import Flask, request, Blueprint
import pymysql
import json
import csv
from flask_cors import *
from flask import Response
import xlwt

"""
客户成功部每日数据导出
"""
app = Flask(__name__)
CORS(app, supports_credentials=True)
app.config['API_DOC_MEMBER'] = ['api', 'platform']

api = Blueprint('api', __name__)
platform = Blueprint('platform', __name__)


def getDatesByTimes(sDateStr, eDateStr):
    list = []
    datestart = datetime.datetime.strptime(sDateStr, '%Y%m%d')
    dateend = datetime.datetime.strptime(eDateStr, '%Y%m%d')
    list.append(datestart.strftime('%Y%m%d'))
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        list.append(datestart.strftime('%Y%m%d'))
    return list


def file_iterator(file_path, chunk_size=512):
    """
        文件读取迭代器
    :param file_path:文件路径
    :param chunk_size: 每次读取流大小
    :return:
    """
    with open(file_path, 'rb') as target_file:
        while True:
            chunk = target_file.read(chunk_size)
            if chunk:
                yield chunk
            else:
                break


def to_json(obj):
    """
        放置
    :return:
    """
    return json.dumps(obj, ensure_ascii=False)


# 获取场地所在城市的字典
project_city = {
    "车公庄店": "BJ",
    "大望路店": "BJ",
    "东直门店": "BJ",
    "古墩路店": "HZ",
    "广渠门店": "BJ",
    "国贸店": "BJ",
    "国权路店": "SH",
    "国展店": "BJ",
    "呼家楼店": "BJ",
    "呼家楼二店": "BJ",
    "淮海东路店": "SH",
    "建国门店": "BJ",
    "江宁路店": "SH",
    "酒仙桥店": "BJ",
    "兰溪路店": "SH",
    "庆春路店": "HZ",
    "日坛店": "BJ",
    "上地店": "BJ",
    "十里堡店": "BJ",
    "十里堡二店": "BJ",
    "十里河店": "BJ",
    "双井店": "BJ",
    "四川北路二店": "SH",
    "四川北路一店": "SH",
    "四道口店": "BJ",
    "驼房营店": "BJ",
    "望京店": "BJ",
    "望京西园店": "BJ",
    "武宁路店": "SH",
    "新荟城店": "BJ",
    "雅宝城店": "BJ",
    "延长路店": "SH",
    "悠乐汇店": "BJ",
    "中关村店": "BJ",
    "中关村软件园店": "BJ",
    "铸诚大厦": "BJ",
    "自空间店": "BJ",
    "淮海东路二店": "SH",
    "方庄店": "BJ",
    "五道口二店": "BJ",
    "百脑汇店": "BJ",
    "长虹桥店": "BJ",
    "灵石路店": "SH",
    "西直门店": "BJ",
    "文二路店": "HZ",
    "五道口一店": "BJ",
    "梨园店": "BJ",
    "莫干山路店": "HZ",
    "国定东路店": "SH",
    "河南北路店": "SH",
    "文三路店": "HZ",
    "河南北路二店": "SH",
    "协和路店": "SH",
    "广中西路店": "SH",
    "国安路店": "SH",
    "江宁路二店": "SH",
    "秋涛北路店": "HZ",
    "深南东路店": "SZ",
    "控江路店": "SH",
    "协和路一店": "SH",
    "右安门店": "BJ",
    "南京西路店": "SH",
    "协和路二店": "SH",
    "经开大厦": "BJ",
    "牡丹园店": "BJ",
    "汶水路店": "SH",
    "松江路店": "SH",
    "酒仙桥二店": "BJ",
    "石景山店": "BJ",
    "马家堡店": "BJ",
    "枣营麦子店": "BJ",
    "龙舌路店": "HZ",
    "星光影视园店": "BJ",
    "新荟城二店": "BJ",
    "金沙江路店": "SH",
    "曹杨路店": "SH",
    "文一路店": "HZ",
    "双井二店": "BJ",
    "叶家宅路店": "SH",
    "深南中路店": "SZ",
    "金沙江路二店": "SH",
    "六佰本": "BJ",
    "国美店": "BJ",
    "创客店": "BJ",
    "五道口店": "BJ",
    "三元桥": "BJ",
    "东直门二店": "BJ",
    "六里桥店": "BJ",
    "华贸天地": "BJ",
    "天山西路店": "SH",
    "梅华路店": "SZ",
    "理想国店": "BJ",
    "劲松店": "BJ"
}

pool = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234', db='redsquirrel',
                port=62864)  # 5为连接池里的最少连接数
pool_commerce = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com', user='tanglei',
                         passwd='tanglei', db='commerce', port=3306)
pool_visualization = PooledDB(pymysql, 5, host='rm-hp364ebpsp6649ra0bo.mysql.huhehaote.rds.aliyuncs.com',
                              user='tanglei',
                              passwd='tanglei', db='data_visualization', port=3306)


# 返回门店列表
@app.route('/get_data')
def getDate():
    db = pool.connection()
    cur = db.cursor()

    db2 = pool_visualization.connection()
    cur2 = db2.cursor()

    dimension = request.args.get('dimension')
    first_date = request.args.get('first_date')
    first_date = str(first_date).replace('-', '')
    second_date = request.args.get('second_date')
    second_date = str(second_date).replace('-', '')
    if dimension is None and first_date is None:
        return '请输入参数'
    elif first_date is None:
        return '请输入第一个时间参数'
    elif dimension == 'week' and second_date is None:
        return '请输入第二个时间参数'
    elif dimension is None:
        return '请输入days或者week参数'
    else:
        # 查询商户等级
        sql_shop_level = "SELECT DISTINCT `name`,city_name,level_name from public_sea_pool"
        db_comm = pool_commerce.connection()
        cur_comm = db_comm.cursor()
        cur_comm.execute(sql_shop_level)
        results_comm = cur_comm.fetchall()
        lever_dit = {}
        for row_lever in results_comm:
            shop_name = row_lever[0]
            city_name = row_lever[1]
            lever_name = row_lever[2]
            lever_dit[shop_name] = [city_name, lever_name]
        # 查询商户最早有营业额数据
        sql_time = "SELECT CONCAT(PROJECT_NAME, CLIENT_NAME) na,PROJECT_NAME,CLIENT_NAME,SUBSTRING_INDEX(MIN(STREAM_DATE), ' ', 1)," \
                   "SUBSTRING_INDEX(MAX(STREAM_DATE), ' ', 1) FROM t_shop_info_archive WHERE PROJECT_NAME !='场外商户' and IS_DELETE=0 " \
                   "GROUP BY PROJECT_NAME,CLIENT_NAME"
        cur.execute(sql_time)
        results_time = cur.fetchall()
        time_dict = {}
        for time_row in results_time:
            time_dict[time_row[0]] = time_row[3]
        # 当参数为days时执行sql
        if dimension == 'days':
            # sql="SELECT PROJECT_NAME,CLIENT_NAME,CONCAT(PROJECT_NAME,CLIENT_NAME),STALL_NAME,FIRST_CATEGORY,SECOND_CATEGORY," \
            #     "BREAK_EVEN_AMOUNT/100,SUM(ORDER_AMOUNT),SUM(STREAM) from t_shop_info_archive where IS_DELETE=0 and SUBSTRING_INDEX(STREAM_DATE,' ',1) = '%s'  GROUP BY PROJECT_NAME,CLIENT_NAME,SUBSTRING_INDEX(STREAM_DATE,' ',1)" % first_date
            # 查询实收数据
            sql = "SELECT  c.project_name,  c.merchant_name,  concat_name,  c.stalls_name,  d.first_classification_name,  d.second_classification_name,  c.monthly_rent,  c.order_count,  c.sale_amount, c.plat_type FROM  (   SELECT    q.project_name,    q.stalls_name,    q.merchant_id,    q.merchant_name,    CONCAT(     q.project_name,     q.merchant_name    ) concat_name,    w.order_count,    w.sale_amount,    q.monthly_rent, w.plat_type   FROM    (     SELECT      table2.project_id,table2.project_name,table2.stalls_id,table2.stalls_name,table2.merchant_id,public_sea_pool.`name` merchant_name,table2.area_name,table2.monthly_rent     FROM      (       SELECT        table1.*, contract.merchant_id       FROM        (         SELECT          b.project_id,    b.project_name,    a.stalls_id,    a.stalls_name,    b.area_name,    a.monthly_rent         FROM          commerce.stalls a         LEFT JOIN commerce.project b ON a.project_id = b.project_id        ) table1       LEFT JOIN commerce.contract ON table1.stalls_id = contract.stall_id       WHERE        contract.is_valid = 1       AND contract.is_delete = 0      ) table2     LEFT JOIN commerce.public_sea_pool ON table2.merchant_id = public_sea_pool.id    ) q   LEFT JOIN (    SELECT     l.project_id,     l.project_name,     l.stall_id,     l.stall_name,     l.merchant_id,     l.merchant_name,     l.channel_type,     sum(l.order_count) order_count,     sum(l.sale_amount) sale_amount, l.plat_type    FROM     (      SELECT       project_id, project_name, stall_id, stall_name, merchant_id, merchant_name, channel_type, SUM(order_count) / COUNT(1) order_count, SUM(sale_amount) / COUNT(1) sale_amount, plat_type      FROM       merchant_sales_statistics      WHERE       stream_date = '%s'      AND is_delete = 0      GROUP BY       project_id, merchant_id, channel_type     ) l    GROUP BY     l.project_id,     l.merchant_id   ) w ON q.project_id = w.project_id   AND q.stalls_id = w.stall_id  ) c LEFT JOIN commerce.public_sea_pool d ON c.merchant_id = d.id;" % first_date

            file_name = str(datetime.datetime.strptime(str(first_date), '%Y%m%d')).split(' ')[0]
            # 写入excel的表头
            format_list = ['城市', '门店名称', '商户名', '档口', '一级分类', '二级分类', '月租金', '单量', '实收', '房租+人工+物业', '健康百分比', '商户健康度',
                           '门店和商户', '商户等级', '开始营业时间', '日期', '平台']
        # 当参数为week时执行sql
        elif dimension == 'week':
            sql = "SELECT c.project_name, c.merchant_name, concat_name, c.stalls_name, d.first_classification_name, d.second_classification_name, c.monthly_rent, c.order_count, c.sale_amount, c.plat_type FROM (  SELECT   q.project_name,   q.stalls_name,   q.merchant_id,   q.merchant_name,   CONCAT(    q.project_name,    q.merchant_name   ) concat_name,   w.order_count,   w.sale_amount,   q.monthly_rent, w.plat_type  FROM   (    SELECT     table2.project_id,     table2.project_name,     table2.stalls_id,     table2.stalls_name,     table2.merchant_id,     public_sea_pool.`name` merchant_name,     table2.area_name,     table2.monthly_rent    FROM     (      SELECT       table1.*, contract.merchant_id      FROM       (        SELECT         b.project_id,   b.project_name,   a.stalls_id,   a.stalls_name,   b.area_name,   a.monthly_rent        FROM         commerce.stalls a        LEFT JOIN commerce.project b ON a.project_id = b.project_id       ) table1      LEFT JOIN commerce.contract ON table1.stalls_id = contract.stall_id      WHERE       contract.is_valid = 1      AND contract.is_delete = 0     ) table2    LEFT JOIN commerce.public_sea_pool ON table2.merchant_id = public_sea_pool.id   ) q  LEFT JOIN (   SELECT    l.project_id,    l.project_name,    l.stall_id,    l.stall_name,    l.merchant_id,    l.merchant_name,    l.channel_type,    sum(l.order_count) order_count,    sum(l.sale_amount) sale_amount, l.plat_type   FROM    (     SELECT      project_id,project_name,stall_id,stall_name,merchant_id,merchant_name,channel_type,SUM(order_count) / COUNT(1) order_count,SUM(sale_amount) / COUNT(1) sale_amount, plat_type,stream_date     FROM      merchant_sales_statistics     WHERE      stream_date BETWEEN '%s' and '%s'     AND is_delete = 0     GROUP BY      project_id,merchant_id,channel_type,stream_date    ) l   GROUP BY    l.project_id,    l.merchant_id  ) w ON q.project_id = w.project_id  AND q.stalls_id = w.stall_id ) c LEFT JOIN commerce.public_sea_pool d ON c.merchant_id = d.id;" % (
                first_date, second_date)
            file_name = first_date + '-' + second_date
            format_list = ['城市', '门店名称', '商户名', '档口', '一级分类', '二级分类', '月租金', '单量', '实收', '房租+人工+物业', '健康百分比', '商户健康度',
                           '门店和商户', '商户等级', '开始营业时间', '周次', '平台']
        cur2.execute(sql)
        results = cur2.fetchall()
        # 写入csv文件
        with open('/home/www/python/get_excel/excel_dir/%s.csv' % file_name, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(format_list)
            for row in results:

                project_name = row[0]
                client_name = row[1]
                concat_name = row[2]
                stall_name = row[3]
                first_cate = row[4]
                second_cate = row[5]
                break_amount = row[6]
                if row[7] is None:
                    order_amount = 0
                else:
                    order_amount = row[7]
                if row[8] is None:
                    stream = 0
                else:
                    stream = row[8]
                # order_amount = row[7]
                # stream = row[8]
                city_name = project_city.get(project_name, "BJ")

                if city_name == "HZ":
                    cost = break_amount + 18500
                else:
                    cost = break_amount + 25500
                if stream == 0:
                    healthy_num = 0
                else:
                    healthy_num = 1 - cost / stream
                if healthy_num >= 0.45:
                    healthy = "好"
                elif healthy_num < 0.35:
                    healthy = "差"
                else:
                    healthy = "中"
                shop_lever = lever_dit.get(client_name, "未知等级")[1]
                shop_min_time = time_dict.get(project_name + client_name, '未知')
                if row[9] == 200:
                    source = '外卖邦'
                else:
                    source = '管家端'

                shop_list = [city_name, project_name, client_name, stall_name, first_cate, second_cate, break_amount,
                             order_amount, stream, cost, healthy_num, healthy, project_name + client_name, shop_lever,
                             shop_min_time, file_name, source]
                writer.writerow(shop_list)

        file_path = "/home/www/python/get_excel/excel_dir/%s.csv" % file_name

        filename = os.path.basename(file_path)
        response = Response(file_iterator(file_path))
        response.headers['Content-Type'] = 'application/octet-stream'
        response.headers["Content-Disposition"] = 'attachment;filename="{}"'.format(filename)
        return response


# 导出监控数据
@app.route('/get_control')
def getControl():
    db_commerce = pool_commerce.connection()
    cur_commerce = db_commerce.cursor()

    db_visualization = pool_visualization.connection()
    cur_visualization = db_visualization.cursor()

    first_date = request.args.get('first_date')
    second_date = request.args.get('second_date')
    city_id = request.args.get('city_id')
    date_list = getDatesByTimes(first_date, second_date)
    file_name = first_date + second_date
    dict_stalls = {}
    for date in date_list:

        if city_id == '1':
            city_name = '北京'
        elif city_id == '2':
            city_name = '上海'
        elif city_id == '3':
            city_name = '杭州'
        elif city_id == '4':
            city_name = '深圳'

        if first_date is None or second_date is None:
            return '请输入参数'
        else:
            # 查询该日的营业数据
            sql_project = "SELECT table2.project_id,table2.project_name,table2.stalls_id,table2.stalls_name,table2.merchant_id, " \
                          "public_sea_pool.`name`,table2.area_name from  (SELECT	table1.*, contract.merchant_id FROM (SELECT	" \
                          "b.project_id,b.project_name,a.stalls_id,a.stalls_name,b.area_name FROM stalls a LEFT JOIN project b ON " \
                          "a.project_id = b.project_id ) table1 LEFT JOIN contract ON table1.stalls_id = contract.stall_id WHERE " \
                          "contract.is_valid = 1 and contract.is_delete=0) table2 LEFT JOIN  public_sea_pool on table2.merchant_id=" \
                          "public_sea_pool.id where table2.area_name='%s'" % city_name
            cur_commerce.execute(sql_project)
            results_project = cur_commerce.fetchall()

            # 构造每个商户三个平台的字典
            for row in results_project:
                dict_stalls[str(row[0]) + str(row[2]) + '100' + date] = [row[0], row[1], row[2], row[3], row[4], row[5],
                                                                         row[6],
                                                                         '堂食', '', '无数据', 0.0, 0, 0.0, date]
                dict_stalls[str(row[0]) + str(row[2]) + '200' + date] = [row[0], row[1], row[2], row[3], row[4], row[5],
                                                                         row[6],
                                                                         '美团', '', '无数据', 0.0, 0, 0.0, date]
                dict_stalls[str(row[0]) + str(row[2]) + '300' + date] = [row[0], row[1], row[2], row[3], row[4], row[5],
                                                                         row[6],
                                                                         '饿了么', '', '无数据', 0.0, 0, 0.0, date]
            sql_sale = "SELECT project_id,project_name,stall_id,stall_name,merchant_id,merchant_name,plat_type,channel_type,order_count,sale_amount,average,stream_date from merchant_sales_statistics where stream_date='%s'" % date
            cur_visualization.execute(sql_sale)
            results_visualization = cur_visualization.fetchall()
            # 把有数据的商户在字典中更新
            for row_vis in results_visualization:
                if str(row_vis[0]) + str(row_vis[2]) + str(row_vis[7]) + date in dict_stalls.keys():
                    stall_list = dict_stalls.get(str(row_vis[0]) + str(row_vis[2]) + str(row_vis[7]) + date)
                    if row_vis[6] == 100:
                        stall_list[8] = '店长端录入'
                    elif row_vis[6] == 200:
                        stall_list[8] = '外卖邦'
                    stall_list[9] = '有数据'
                    stall_list[10] = row_vis[9]
                    stall_list[11] = row_vis[8]
                    stall_list[12] = row_vis[10]
                    stall_list[13] = row_vis[11]

    book = xlwt.Workbook(encoding='utf-8')
    sheet1 = book.add_sheet(u'Sheet1', cell_overwrite_ok=True)
    # 构造写入excel的列表
    sheet_list = []
    sheet_list.append(['场地', '档口', '商户', '城市', '平台', '数据来源', '是否有数据', '实收', '单量', '单均', '日期'])
    for row_value in dict_stalls.values():
        sheet_list.append([row_value[1], row_value[3], row_value[5], row_value[6], row_value[7], row_value[8],
                           row_value[9], row_value[10], row_value[11], row_value[12], row_value[13]])
    # 写入excel
    for row, line in enumerate(sheet_list):
        for col, t in enumerate(line):
            sheet1.write(row, col, t)
    book.save('/home/www/python/get_excel/excel_dir/data_control%s.xls' % file_name)
    file_path = "/home/www/python/get_excel/excel_dir/data_control%s.xls" % file_name

    filename = os.path.basename(file_path)

    response = Response(file_iterator(file_path))
    response.headers['Content-Type'] = 'application/octet-stream'
    response.headers["Content-Disposition"] = 'attachment;filename="{}"'.format(filename)
    return response


app.register_blueprint(api, url_prefix='/api')
app.register_blueprint(platform, url_prefix='/platform')

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5010)
