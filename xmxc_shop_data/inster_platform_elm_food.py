import json
from DBUtils.PooledDB import PooledDB
import pymysql

pool = PooledDB(pymysql, 5, host='bj-cdb-cwu7v42u.sql.tencentcdb.com', user='root', passwd='xmxc1234',
                db='test', port=62864)


def concat_list(list1):
    if list1 is not None and len(list1) > 0:
        concat_str = ''
        for pro in list1:
            concat_str = concat_str + str(pro) + ','
            concat_str.strip(',')
        return concat_str
    else:
        return ""


def get_start_data():
    concat_sql_value = ""
    count_row = 0
    for line in file.readlines():
        dic = json.loads(line)
        count_row += 1
        print(count_row)

        if count_row < 100000:
            appCode = str(dic.get('appCode'))
            catId1 = str(dic.get('catId1'))
            catName1 = str(dic.get('catName1'))
            commentCount = str(dic.get('commentCount'))

            imageUrls = dic.get('imageUrls')
            imageUrls = str(concat_list(imageUrls))
            id = str(dic.get('id'))

            description = str(dic.get('description'))
            goodRatingRatio = str(dic.get('goodRatingRatio'))
            likeCount = str(dic.get('likeCount'))
            monthSaleCount = str(dic.get('monthSaleCount'))
            originPlace = str(dic.get('originPlace'))
            priceOptions = dic.get('priceOptions')
            if priceOptions is not None and len(priceOptions) > 0:
                food_price = priceOptions[0].get('price')
                sale_price = priceOptions[1].get('sale_price')
                market_price = priceOptions[2].get('market_price')
                current_price = priceOptions[3].get('current_price')
                original_price = priceOptions[4].get('original_price')

            else:
                food_price = 0
                sale_price = 0
                market_price = 0
                current_price = 0
                original_price = 0
            rating = str(dic.get('rating'))
            ratingCount = str(dic.get('ratingCount'))
            sellerId = str(dic.get('sellerId'))
            skuOptions = dic.get('skuOptions')
            if skuOptions is not None and len(skuOptions) > 0:
                soldout = str(skuOptions[0].get("soldout"))
                stockSize = str(skuOptions[0].get("stockSize"))
                soldout_id = str(skuOptions[0].get("id"))
                marketPrice = str(skuOptions[0].get("marketPrice"))
                saleCount = str(skuOptions[0].get("saleCount"))
                name = str(skuOptions[0].get("name"))
                packFee = str(skuOptions[0].get("packFee"))
                price = str(skuOptions[0].get("price"))
            else:
                soldout = 0
                stockSize = 0
                soldout_id = 0
                marketPrice = 0
                saleCount = 0
                name = 0
                packFee = 0
                price = 0

            sortId = str(dic.get('sortId'))
            title = str(dic.get('title'))

            sql_value = (
                appCode, catId1, catName1, commentCount, description, rating, ratingCount, goodRatingRatio, id,
                imageUrls,
                name, likeCount, monthSaleCount, originPlace
                , sortId, market_price, food_price, title, imageUrls, sellerId, 15)
            concat_sql_value = concat_sql_value + str(sql_value) + ','
    concat_sql_value = concat_sql_value.strip(',')
    return concat_sql_value


def get_data(start_row, end_row):
    concat_sql_value = ""
    count_row = 0
    for line in file.readlines():
        dic = json.loads(line)
        count_row += 1
        print(count_row)

        if count_row < end_row and count_row >= start_row:
            appCode = str(dic.get('appCode'))
            catId1 = str(dic.get('catId1'))
            catName1 = str(dic.get('catName1'))
            commentCount = str(dic.get('commentCount'))

            imageUrls = dic.get('imageUrls')
            imageUrls = str(concat_list(imageUrls))
            id = str(dic.get('id'))

            description = str(dic.get('description'))
            goodRatingRatio = str(dic.get('goodRatingRatio'))
            likeCount = str(dic.get('likeCount'))
            monthSaleCount = str(dic.get('monthSaleCount'))
            originPlace = str(dic.get('originPlace'))
            priceOptions = dic.get('priceOptions')
            if priceOptions is not None and len(priceOptions) > 0:
                food_price = priceOptions[0].get('price')
                sale_price = priceOptions[1].get('sale_price')
                market_price = priceOptions[2].get('market_price')
                current_price = priceOptions[3].get('current_price')
                original_price = priceOptions[4].get('original_price')

            else:
                food_price = 0
                sale_price = 0
                market_price = 0
                current_price = 0
                original_price = 0
            rating = str(dic.get('rating'))
            ratingCount = str(dic.get('ratingCount'))
            sellerId = str(dic.get('sellerId'))
            skuOptions = dic.get('skuOptions')
            if skuOptions is not None and len(skuOptions) > 0:
                soldout = str(skuOptions[0].get("soldout"))
                stockSize = str(skuOptions[0].get("stockSize"))
                soldout_id = str(skuOptions[0].get("id"))
                marketPrice = str(skuOptions[0].get("marketPrice"))
                saleCount = str(skuOptions[0].get("saleCount"))
                name = str(skuOptions[0].get("name"))
                packFee = str(skuOptions[0].get("packFee"))
                price = str(skuOptions[0].get("price"))
            else:
                soldout = 0
                stockSize = 0
                soldout_id = 0
                marketPrice = 0
                saleCount = 0
                name = 0
                packFee = 0
                price = 0

            sortId = str(dic.get('sortId'))
            title = str(dic.get('title'))

            sql_value = (
                appCode, catId1, catName1, commentCount, description, rating, ratingCount, goodRatingRatio, id,
                imageUrls,
                name, likeCount, monthSaleCount, originPlace
                , sortId, market_price, food_price, title, imageUrls, sellerId, 15)
            concat_sql_value = concat_sql_value + str(sql_value) + ','
    concat_sql_value = concat_sql_value.strip(',')
    return concat_sql_value


def get_end_data():
    concat_sql_value = ""
    count_row = 0
    for line in file.readlines():
        dic = json.loads(line)
        count_row += 1
        print(count_row)

        if count_row >= 2000000:
            appCode = str(dic.get('appCode'))
            catId1 = str(dic.get('catId1'))
            catName1 = str(dic.get('catName1'))
            commentCount = str(dic.get('commentCount'))

            imageUrls = dic.get('imageUrls')
            imageUrls = str(concat_list(imageUrls))
            id = str(dic.get('id'))

            description = str(dic.get('description'))
            goodRatingRatio = str(dic.get('goodRatingRatio'))
            likeCount = str(dic.get('likeCount'))
            monthSaleCount = str(dic.get('monthSaleCount'))
            originPlace = str(dic.get('originPlace'))
            priceOptions = dic.get('priceOptions')
            if priceOptions is not None and len(priceOptions) > 0:
                food_price = priceOptions[0].get('price')
                sale_price = priceOptions[1].get('sale_price')
                market_price = priceOptions[2].get('market_price')
                current_price = priceOptions[3].get('current_price')
                original_price = priceOptions[4].get('original_price')

            else:
                food_price = 0
                sale_price = 0
                market_price = 0
                current_price = 0
                original_price = 0
            rating = str(dic.get('rating'))
            ratingCount = str(dic.get('ratingCount'))
            sellerId = str(dic.get('sellerId'))
            skuOptions = dic.get('skuOptions')
            if skuOptions is not None and len(skuOptions) > 0:
                soldout = str(skuOptions[0].get("soldout"))
                stockSize = str(skuOptions[0].get("stockSize"))
                soldout_id = str(skuOptions[0].get("id"))
                marketPrice = str(skuOptions[0].get("marketPrice"))
                saleCount = str(skuOptions[0].get("saleCount"))
                name = str(skuOptions[0].get("name"))
                packFee = str(skuOptions[0].get("packFee"))
                price = str(skuOptions[0].get("price"))
            else:
                soldout = 0
                stockSize = 0
                soldout_id = 0
                marketPrice = 0
                saleCount = 0
                name = 0
                packFee = 0
                price = 0

            sortId = str(dic.get('sortId'))
            title = str(dic.get('title'))

            sql_value = (
                appCode, catId1, catName1, commentCount, description, rating, ratingCount, goodRatingRatio, id,
                imageUrls,
                name, likeCount, monthSaleCount, originPlace
                , sortId, market_price, food_price, title, imageUrls, sellerId, 15)
            concat_sql_value = concat_sql_value + str(sql_value) + ','
    concat_sql_value = concat_sql_value.strip(',')
    return concat_sql_value


path = "G:\\数据\\外卖数据\\2020年4月\\饿了么\\饿了么4月份店铺数据.json"
# path = "G:\\数据\\外卖数据\\2020年4月\\饿了么\\abc.json"

file = open(path, 'r', encoding='utf-8')
for i in range(2, 20):
    start_row = i * 10000
    end_row = (i + 1) * 10000
    concat_sql = get_data(start_row, end_row)

    sql = "INSERT  into elm_mark_food_data (" \
          "appCode,catId1,catName1,commentCount,description,rating,ratingCount," \
          "goodRatingRatio,id,concat_ws(,, imageUrls),keyValues[0].key,concat_ws(,, keyValues[0].value),likeCount,monthSaleCount," \
          "originPlace,sortId,marketPrice,price,title,url,sellerId,update_count) values " + concat_sql
    # print(sql)
    print("sql拼接完成，开始插入数据！")
    db = pool.connection()
    cur = db.cursor()
    cur.execute(sql)  # 执行sql语句
    db.commit()  # 提交到数据库执行
    print("数据插入成功！！")
