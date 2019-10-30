#coding:utf-8
from flask import Flask, request
import pymysql
import json

app = Flask(__name__)

# pool = PooledDB(pymysql,5,host='bj-cdb-cwu7v42u.sql.tencentcdb.com',user='root',passwd='xmxc1234',db='test',port=62864) #5为连接池里的最少连接数
#返回门店列表
@app.route('/getShopInfo')
def getShopInfo():
    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="test", port=62864)
    cur = db.cursor()
    brandID=request.args.get('brandID')
    sql = "SELECT brand_id,brand_name,brand_address,shop_id,shop_name,shop_icon from t_fla_shop_info  where brand_id=%s" %brandID
    sq = []
    cur.execute(sql)
    results = cur.fetchall()
    # 遍历结果
    for row in results:
            data = {}
            data['brand_id'] = row[0]
            data['brand_name'] = row[1]
            data['brand_address'] = row[2]
            data['shop_id'] = row[3]
            data['shop_name'] = row[4]
            data['shop_icon'] = row[5]
            sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    # print(id)

    db.close()
    return jsondatar

#返回商户基本信息和菜品信息
@app.route('/getFoodInfo')
def getFoodInfo():
    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="test", port=62864)
    cur_food = db.cursor()

    shopID = request.args.get('shopID')
    sql_shop = "SELECT shop_id,shop_name,shop_address,shop_mobile,shopping_time,business_services,shop_icon,ground_level,company_name,company_address," \
               "legal_representative,license_key,business_scope,period_validity,professional_qualifications,managerial_level,examination_date,main_body from t_fla_shop_info  where shop_id=%s" % shopID
    sql_food = "SELECT shop_id,shop_name,food_id,food_name,food_price,making_way,making_time,save_condition,save_time,food_icon from t_fla_food_info where shop_id=%s" % shopID

    sq_food = []
    cur_food.execute(sql_food)
    results_food = cur_food.fetchall()

    cur_food.execute(sql_shop)
    results_shop = cur_food.fetchall()

    # 遍历结果
    for row in results_food:
        data = {}
        data['food_id'] = row[2]
        data['food_name'] = row[3]
        data['food_price'] = row[4]
        data['making_way'] = row[5]
        data['making_time'] = row[6]
        data['save_condition'] = row[7]
        data['save_time'] = row[8]
        data['food_icon'] = row[9]
        sql_greens = "SELECT food_id,greens_name  from t_fla_greens_info where food_id=%s and shop_id=%s" % (row[2],row[0])
        cur_food.execute(sql_greens)
        results_greens = cur_food.fetchall()
        greens_name=""
        for row in results_greens:
            greens_row=row[1]
            greens_name=greens_name+greens_row+"、"
        data["greens_name"]=greens_name.strip("、")
        sq_food.append(data)
    all_data=[]
    shop_bic_info={}
    shop_bic_info['shop_id'] = results_shop[0][0]
    shop_bic_info['shop_name'] = results_shop[0][1]
    shop_bic_info['shop_address'] = results_shop[0][2]
    shop_bic_info['shop_mobile'] = results_shop[0][3]
    shop_bic_info['shopping_time'] = results_shop[0][4]
    shop_bic_info['business_services'] = results_shop[0][5]
    shop_bic_info['shop_icon'] = results_shop[0][6]
    shop_bic_info['ground_level'] = results_shop[0][7]
    shop_bic_info['company_name'] = results_shop[0][8]
    shop_bic_info['company_address'] = results_shop[0][9]
    shop_bic_info['legal_representative'] = results_shop[0][10]
    shop_bic_info['license_key'] = results_shop[0][11]
    shop_bic_info['business_scope'] = results_shop[0][12]
    shop_bic_info['period_validity'] = str(results_shop[0][13])
    shop_bic_info['professional_qualifications'] = results_shop[0][14]
    shop_bic_info['managerial_level'] = results_shop[0][15]
    shop_bic_info['examination_date'] = str(results_shop[0][16])
    shop_bic_info['main_body'] = results_shop[0][17]
    shop_bic_info['shop_level'] = results_shop[0][18]
    print(shop_bic_info)
    data = {}
    data['shop_bic_info'] = shop_bic_info
    data['food_list']=sq_food
    all_data.append(data)
    jsondatar = json.dumps(all_data, ensure_ascii=False)
    db.close()

    return jsondatar


#返回菜品配菜信息
@app.route('/getGreensFrom')
def getGreensFrom():
    db = pymysql.connect(host="bj-cdb-cwu7v42u.sql.tencentcdb.com", user="root",
                         password="xmxc1234", db="test", port=62864)
    db2 = pymysql.connect(host="140.143.78.200", user="root",
                         password="xmxc1234", db="data_visualization", port=3306)
    cur = db.cursor()
    foodID = request.args.get('foodID')
    sql1="SELECT max(SUBSTRING_INDEX(a.buy_time,' ',1)),a.supplier_name,b.second_cate_name from food_cate_buy_info a join second_food_cate b on a.second_cate_id=b.second_cate_id GROUP BY a.second_cate_id"
    sql = "SELECT greens_id,greens_name,greens_from,buy_time FROM t_fla_greens_info where food_id=%s" % foodID

    cur.execute(sql)
    results = cur.fetchall()

    cur2 = db2.cursor()
    cur2.execute(sql1)
    results1 = cur2.fetchall()
    # print(results1)
    name = []
    for row in results1:

        name.append([row[2],str(row[0]),row[1]])
    print(name)
        # print(row[2])
    sq = []
    for row in results:
            data = {}
            data['greens_id'] = row[0]
            greens_name = row[1]
            data['greens_name'] = greens_name
            for i in name:
                if greens_name==i[0]:
                    data['greens_from'] = i[1]
                    data['buy_time'] = i[2]
            sq.append(data)
    jsondatar = json.dumps(sq, ensure_ascii=False)
    db2.close()
    db.close()
    return jsondatar


if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host="0.0.0.0",port=5000)