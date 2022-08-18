import mysql.connector
import requests
import time

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
        "\"date\" :\"{1}\","\
        "\"index\" :\"{2}\","\
        "\"reference_price\" :{3},"\
        "\"opening_price\" :{4},"\
        "\"closing_price\" :{5},"\
        "\"max_price\" :{6},"\
        "\"min_price\" :{7},"\
        "\"average_price\" :{8},"\
        "\"+-_variation\" :{9},"\
        "\"%_variation\" :{10},"\
        "\"matching_volume\" :{11},"\
        "\"matching_value\" :{12},"\
        "\"agreement_volume\" :{13},"\
        "\"agreement_value\" :{14},"\
        "\"total_volume\" :{15},"\
        "\"total_value\" :{16},"\
        "\"capitalization\" :{17}"\
        "{18}"\
        "]"

def postToPowerBI():
    url = "https://api.powerbi.com/beta/06f1b89f-07e8-464f-b408-ec1b45703f31/datasets/4780d7c0-bfbe-4050-b0e5-0d93b27d4ebb/rows?key=iOeYFC0Iyeuts9yc8OVB3vrcv4TzeRwgpHV6q9B%2BGFygMheNzdnHPuIKrnM1j1WkiSxGH%2FFtrS1StKcFG41VHw%3D%3D"
    conn = mysqlConnect()

    with open("PowerBI/markedId.txt", "r") as file:
        marked_id = int(file.read())
    file.close()
    cursor = conn.cursor()
    cursor.execute(
        "select id, `date`, `index`, reference_price, opening_price, closing_price, max_price, min_price, average_price," \
        "`+-_variation`, `%_variation`, matching_volume, matching_value, agreement_volume, agreement_value, total_volume," \
        "total_value, capitalization from history_price where id>{0} limit 10".format(marked_id))
    line = cursor.fetchone()
    while (line != None):
        jsonObject = jsonScript.format("{", line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9],
                                       line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], "}")
        marked_id = line[0]
        print(marked_id)
        post = requests.post(url=url, data=jsonObject)
        print(post.status_code)
        line = cursor.fetchone()
        time.sleep(0.1)

    with open("PowerBI/markedId.txt", "w") as file:
        file.write(str(marked_id))
    file.close()

