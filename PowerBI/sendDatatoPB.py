import mysql.connector
import requests
import time
from _datetime import datetime

def mysqlConnect():
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="NguyenLam2001",
            database="stock")
    except Exception:
        raise Exception("connect failed")

jsonScript = "["\
        "{0}"\
        "\"date\" :{1},"\
        "\"index\" :\"{2}\","\
        "\"closing_price\" :{3},"\
        "\"+-_variation\" :{4},"\
        "\"total_volume\" :{5},"\
        "\"total_value\" :{6},"\
        "\"capitalization\" :{7},"\
        "\"vn30-index\" :{8},"\
        "\"vn30-variation\" :{9},"\
        "\"vn30-value\" :{10},"\
        "\"timestamp\" : \"{11}\""\
        "{12}"\
        "]"

def postToPowerBI():
    url = "https://api.powerbi.com/beta/06f1b89f-07e8-464f-b408-ec1b45703f31/datasets/4780d7c0-bfbe-4050-b0e5-0d93b27d4ebb/rows?cmpid=pbi-home-body-snn-signin&key=iOeYFC0Iyeuts9yc8OVB3vrcv4TzeRwgpHV6q9B%2BGFygMheNzdnHPuIKrnM1j1WkiSxGH%2FFtrS1StKcFG41VHw%3D%3D"
    conn = mysqlConnect()

    with open("PowerBI/markedId.txt", "r") as file:
        marked_id = int(file.read())
    file.close()
    cursor = conn.cursor()
    cursor.execute("select id, cast(`date` as unsigned) as `date`, `index`, closing_price, "\
                   "`+-_variation`, total_volume, total_value, capitalization, "\
                   "(select distinct closing_price from stock.history_price "\
                   "where `index` = \"VN30-Index\" and `date` = "\
                   "(select max(`date`) from stock.history_price where `index` = \"VN30-Index\")) as `vn30-index`, " \
                   "(select `+-_variation` from stock.history_price "\
                   "where `index` = \"VN30-Index\" and `date` = "\
                   "(select max(`date`) from stock.history_price where `index` = \"VN30-Index\")) as `vn30-variation`, "\
                   "(select total_value from stock.history_price "\
                   "where `index` = \"VN30-Index\" and `date` = "\
                   "(select max(`date`) from stock.history_price where `index` = \"VN30-Index\")) as `vn30-value` "\
                   "from stock.history_price where id > {0} and `index` != \"VN30-Index\" limit 50".format(marked_id))
    line = cursor.fetchone()
    while (line != None):
        jsonObject = jsonScript.format("{", line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9],
                                       line[10], str(datetime.now()), "}")
        marked_id = line[0]
        post = requests.post(url=url, data=jsonObject)
        print(post.status_code)
        line = cursor.fetchone()
        time.sleep(0.1)

    with open("PowerBI/markedId.txt", "w") as file:
        file.write(str(marked_id))
    file.close()
