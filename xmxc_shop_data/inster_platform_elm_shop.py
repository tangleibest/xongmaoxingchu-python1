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


path = "G:\\数据\\外卖数据\\2020年4月\\饿了么\\饿了么4月份店铺数据.json"
# path = "G:\\数据\\外卖数据\\2020年4月\\饿了么\\abc.json"

file = open(path, 'r', encoding='utf-8')
concat_sql_value1 = ""
concat_sql_value2 = ""
concat_sql_value3 = ""
count_row = 0
for line in file.readlines():
    dic = json.loads(line)
    count_row += 1
    print(count_row)

    if count_row <= 80000:
        city = dic.get('city')
        commentCount = str(dic.get('commentCount'))
        qualRating = str(dic.get('qualRating'))
        minDeliverFee = str(dic.get('minDeliverFee'))

        bannerUrls = dic.get('bannerUrls')
        bannerUrls = str(concat_list(bannerUrls))
        id = str(dic.get('id'))
        ratingDist = dic.get('ratingDist')
        if ratingDist is not None and len(ratingDist) > 0:
            goodReputation = ratingDist[2].get('value')
            negativeComment = ratingDist[3].get('value')
        else:
            goodReputation = 0
            negativeComment = 0
        state = str(dic.get('state'))
        telephones = str(dic.get('telephones'))
        isChainStore = str(dic.get('isChainStore'))
        saleStatus = str(dic.get('saleStatus'))
        lat = str(dic.get('geoPoint').get('lat'))
        lon = str(dic.get('geoPoint').get('lon'))
        description = str(dic.get('description'))
        deliverProvider = str(dic.get('deliverProvider'))
        promotions = dic.get('promotions')
        promotions_str = str(concat_list(promotions))

        url = str(dic.get('url'))
        tags = dic.get('tags')
        tags_str = str(concat_list(tags))

        district = str(dic.get('district'))
        otherIds = str(dic.get('otherIds'))
        address = str(dic.get('address'))
        monthSaleCount = str(dic.get('monthSaleCount'))
        rating = str(dic.get('rating'))
        servRating = str(dic.get('servRating'))
        deliverFee = str(dic.get('deliverFee'))
        isNewOpening = str(dic.get('isNewOpening'))
        deliverTime = str(dic.get('deliverTime'))
        hasBrandTag = str(dic.get('hasBrandTag'))
        title = str(dic.get('title'))
        ratingRank = str(dic.get('ratingRank'))
        openingHours = dic.get('openingHours')
        openingHours = str(concat_list(openingHours))

        ratingCount = str(dic.get('ratingCount'))
        appCode = str(dic.get('appCode'))
        createDate = str(dic.get('createDate'))
        checkId = str(dic.get('checkId'))
        catName11 = dic.get('catName1')
        catName1 = str(concat_list(catName11))
        catName22 = dic.get('catName2')
        catName2 = str(concat_list(catName22))
        sql_value1 = (
            tags_str, qualRating, isNewOpening, bannerUrls, telephones, rating, state, id, minDeliverFee, title,
            address, openingHours, ratingCount, promotions_str, appCode, description, ratingRank, deliverTime,
            servRating,
            commentCount,
            deliverProvider, deliverFee, monthSaleCount, city, catName2, catName1, district, negativeComment, lat,
            lon,
            hasBrandTag, isChainStore, otherIds, url, goodReputation, 15, 1586317376, createDate)
        concat_sql_value1 = concat_sql_value1 + str(sql_value1) + ','
    elif count_row > 80000 and count_row <= 160000:
        city = dic.get('city')
        commentCount = str(dic.get('commentCount'))
        qualRating = str(dic.get('qualRating'))
        minDeliverFee = str(dic.get('minDeliverFee'))

        bannerUrls = dic.get('bannerUrls')
        bannerUrls = str(concat_list(bannerUrls))
        id = str(dic.get('id'))
        ratingDist = dic.get('ratingDist')
        if ratingDist is not None and len(ratingDist) > 0:
            goodReputation = ratingDist[2].get('value')
            negativeComment = ratingDist[3].get('value')
        else:
            goodReputation = 0
            negativeComment = 0
        state = str(dic.get('state'))
        telephones = str(dic.get('telephones'))
        isChainStore = str(dic.get('isChainStore'))
        saleStatus = str(dic.get('saleStatus'))
        lat = str(dic.get('geoPoint').get('lat'))
        lon = str(dic.get('geoPoint').get('lon'))
        description = str(dic.get('description'))
        deliverProvider = str(dic.get('deliverProvider'))
        promotions = dic.get('promotions')
        promotions_str = str(concat_list(promotions))

        url = str(dic.get('url'))
        tags = dic.get('tags')
        tags_str = str(concat_list(tags))

        district = str(dic.get('district'))
        otherIds = str(dic.get('otherIds'))
        address = str(dic.get('address'))
        monthSaleCount = str(dic.get('monthSaleCount'))
        rating = str(dic.get('rating'))
        servRating = str(dic.get('servRating'))
        deliverFee = str(dic.get('deliverFee'))
        isNewOpening = str(dic.get('isNewOpening'))
        deliverTime = str(dic.get('deliverTime'))
        hasBrandTag = str(dic.get('hasBrandTag'))
        title = str(dic.get('title'))
        ratingRank = str(dic.get('ratingRank'))
        openingHours = dic.get('openingHours')
        openingHours = str(concat_list(openingHours))

        ratingCount = str(dic.get('ratingCount'))
        appCode = str(dic.get('appCode'))
        createDate = str(dic.get('createDate'))
        checkId = str(dic.get('checkId'))
        catName11 = dic.get('catName1')
        catName1 = str(concat_list(catName11))
        catName22 = dic.get('catName2')
        catName2 = str(concat_list(catName22))
        sql_value2 = (
            tags_str, qualRating, isNewOpening, bannerUrls, telephones, rating, state, id, minDeliverFee, title,
            address, openingHours, ratingCount, promotions_str, appCode, description, ratingRank, deliverTime,
            servRating,
            commentCount,
            deliverProvider, deliverFee, monthSaleCount, city, catName2, catName1, district, negativeComment, lat,
            lon,
            hasBrandTag, isChainStore, otherIds, url, goodReputation, 15, 1586317376, createDate)
        concat_sql_value2 = concat_sql_value2 + str(sql_value2) + ','
    elif count_row > 160000:
        city = dic.get('city')
        commentCount = str(dic.get('commentCount'))
        qualRating = str(dic.get('qualRating'))
        minDeliverFee = str(dic.get('minDeliverFee'))

        bannerUrls = dic.get('bannerUrls')
        bannerUrls = str(concat_list(bannerUrls))
        id = str(dic.get('id'))
        ratingDist = dic.get('ratingDist')
        if ratingDist is not None and len(ratingDist) > 0:
            goodReputation = ratingDist[2].get('value')
            negativeComment = ratingDist[3].get('value')
        else:
            goodReputation = 0
            negativeComment = 0
        state = str(dic.get('state'))
        telephones = str(dic.get('telephones'))
        isChainStore = str(dic.get('isChainStore'))
        saleStatus = str(dic.get('saleStatus'))
        lat = str(dic.get('geoPoint').get('lat'))
        lon = str(dic.get('geoPoint').get('lon'))
        description = str(dic.get('description'))
        deliverProvider = str(dic.get('deliverProvider'))
        promotions = dic.get('promotions')
        promotions_str = str(concat_list(promotions))

        url = str(dic.get('url'))
        tags = dic.get('tags')
        tags_str = str(concat_list(tags))

        district = str(dic.get('district'))
        otherIds = str(dic.get('otherIds'))
        address = str(dic.get('address'))
        monthSaleCount = str(dic.get('monthSaleCount'))
        rating = str(dic.get('rating'))
        servRating = str(dic.get('servRating'))
        deliverFee = str(dic.get('deliverFee'))
        isNewOpening = str(dic.get('isNewOpening'))
        deliverTime = str(dic.get('deliverTime'))
        hasBrandTag = str(dic.get('hasBrandTag'))
        title = str(dic.get('title'))
        ratingRank = str(dic.get('ratingRank'))
        openingHours = dic.get('openingHours')
        openingHours = str(concat_list(openingHours))

        ratingCount = str(dic.get('ratingCount'))
        appCode = str(dic.get('appCode'))
        createDate = str(dic.get('createDate'))
        checkId = str(dic.get('checkId'))
        catName11 = dic.get('catName1')
        catName1 = str(concat_list(catName11))
        catName22 = dic.get('catName2')
        catName2 = str(concat_list(catName22))
        sql_value3 = (
            tags_str, qualRating, isNewOpening, bannerUrls, telephones, rating, state, id, minDeliverFee, title,
            address, openingHours, ratingCount, promotions_str, appCode, description, ratingRank, deliverTime,
            servRating,
            commentCount,
            deliverProvider, deliverFee, monthSaleCount, city, catName2, catName1, district, negativeComment, lat,
            lon,
            hasBrandTag, isChainStore, otherIds, url, goodReputation, 15, 1586317376, createDate)
        concat_sql_value3 = concat_sql_value3 + str(sql_value3) + ','

concat_sql_value1 = concat_sql_value1.strip(',')
concat_sql_value2 = concat_sql_value2.strip(',')
concat_sql_value3 = concat_sql_value2.strip(',')
list2 = [concat_sql_value1, concat_sql_value2, concat_sql_value3]
for i in list2:
    sql = "INSERT  into elm_mark_data (" \
          "tags,qualRating,isNewOpening,bannerUrls,telephones,rating,state,id,minDeliverFee,title," \
          "address,openingHours,ratingCount,promotions,appCode,description,ratingRank,deliverTime,servRating,commentCount," \
          "deliverProvider,deliverFee,monthSaleCount,city,catName2,catName1,district,negativeComment,lat,lon," \
          "hasBrandTag,isChainStore,otherIds,url,goodReputation,update_count,update_time,createDate) values " + i
    # print(sql)
    print("sql拼接完成，开始插入数据！")
    db = pool.connection()
    cur = db.cursor()
    cur.execute(sql)  # 执行sql语句
    db.commit()  # 提交到数据库执行
    print("数据插入成功！！")
