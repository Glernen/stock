import mysql.connector
from mysql.connector import errorcode
import requests
import time
import hmac
import hashlib
import base64
import urllib.parse
import logging
import datetime

# PushPlus 的配置信息
PUSHPLUS_TOKEN = "935332d730494a87bddafe704c4e6f3c"
PUSHPLUS_TOPIC = "stock"  # 群组编码

def main():
    # 配置日志记录
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
    try:
        # 建立数据库连接，需要将相应的信息修改为你的数据库信息
        cnx = mysql.connector.connect(
            host="10.1.10.88",
            user="root",
            password="root",
            database="instockdb"
        )
        # 使用 with 语句管理 cursor，确保资源自动释放
        with cnx.cursor() as cursor:
            # 编写 SQL 查询语句，将字符串比较修改为数值比较
            query = """
            -- 使用 CTE 封装原始查询
            WITH OriginalQuery AS (
                SELECT
                    csib.CODE,
                    csib.NAME,
                    csib.DATE,
                    cssn.turnoverrate AS 换手率,
                    cssn.industry AS 行业,
                    cssn.popularity_rank AS 股吧人气排名,
                    cssn.browse_rank AS 今日浏览排名,
                    cssn.concept AS 概念,
                    cssn.downnday AS 连跌天数,
                    cssn.change_rate  AS 当日跌幅,
                    csib.rate_1 AS 1日收益率,
                    csib.rate_2 AS 2日收益率,
                    csib.rate_3 AS 3日收益率,
                    csi.macd,
                    csi.kdjk,
                    csi.kdjd,
                    csi.kdjj,
                    csi.rsi_6,
                    csi.rsi_12,
                    csi.cci,
                    csi.mdi,
                    csi.pdi 
                FROM
                    cn_stock_indicators_buy csib
                    JOIN cn_stock_indicators csi ON csib.CODE = csi.CODE AND csib.DATE = csi.DATE 
                    JOIN cn_stock_selection cssn ON cssn.CODE = csib.CODE  AND cssn.DATE = csib.DATE 
                WHERE
                    -- 添加筛选条件
                    csi.name not like '%ST%'
                    AND csi.code like '30%'
                    -- AND csi.code not like '300%'
                    -- AND csi.code not like '301%'
                    AND cssn.downnday > 4
                    AND cssn.industry not regexp '港口航运|化学制药|化学制品|煤炭'
                    AND cssn.popularity_rank > 500
                    AND csib.date = DATE_SUB(CURDATE(), INTERVAL 0 DAY)
            ),
            -- 对每个日期的股吧人气排名进行排名
            PopularityRanked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY DATE ORDER BY 股吧人气排名 DESC) AS popularity_row_num
                FROM
                    OriginalQuery
            ),
            -- 对每个日期的今日浏览排名进行排名
            BrowseRanked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY DATE ORDER BY 今日浏览排名 DESC) AS browse_row_num
                FROM
                    OriginalQuery
            )
            -- 主查询，获取每个日期的符合条件的记录
            SELECT
                p.CODE,
                p.NAME,
                p.DATE,
                p.换手率,
                p.行业,
                p.股吧人气排名,
                p.今日浏览排名,
                p.当日跌幅,
                p.连跌天数
            FROM
                PopularityRanked p
            JOIN
                BrowseRanked b ON p.CODE = b.CODE AND p.DATE = b.DATE
            WHERE
                p.popularity_row_num < 3
                OR b.browse_row_num < 3
            ORDER BY
                p.DATE;
            """
            # 执行查询语句
            cursor.execute(query)
            # 分批获取数据，每次获取 100 条
            message = ""
            while True:
                rows = cursor.fetchmany(100)
                if not rows:
                    break
                for row in rows:
                    # 处理日期元素
                    new_row = []
                    for element in row:
                        if isinstance(element, datetime.date):
                            element = str(element)
                        new_row.append(element)
                    # 生成超链接
                    code = new_row[0]
                    name = new_row[1]
                    turnoverrate = new_row[3]
                    industry = new_row[4]
                    popularity_rank = new_row[5]
                    browse_rank = new_row[6]
                    concept = new_row[7]
                    downnday = new_row[8]

                    if code.startswith(('30', '00')):
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=0&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    else:
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=1&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    link1 = f"[{name}](https://quote.eastmoney.com/{code}.html/)"
                    link3 = f"换手率：{turnoverrate}"
                    link4 = f"行业：{industry}"
                    link5 = f"股吧人气排名：{popularity_rank}"
                    link6 = f"今日浏览排名：{browse_rank}"
                    link7 = f"当日跌幅：{concept}"
                    link8 = f"连跌天数：{downnday}"

                    new_row[0] = link0
                    new_row[1] = link1
                    new_row[3] = link3
                    new_row[4] = link4
                    new_row[5] = link5
                    new_row[6] = link6
                    new_row[7] = link7
                    new_row[8] = link8

                    message += str(new_row) + "\n\n"  # 将结果添加到消息中，每个结果占一行，增加额外的换行符
                    # 打印日志
                    logging.info(f"Stock info: {new_row}")
            if not message:
                message = "暂无推荐的数据\n\n"
            # 发送消息到钉钉机器人
            send_to_dingtalk(message)
            # 发送消息到 PushPlus
            send_to_pushplus(message)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Database does not exist")
        else:
            logging.error(err)


def send_to_dingtalk(message):
    access_token = "2bfbf3c1ec278566e52ddf8821770c640a96c8bd2082b2ccbebe381eaee0b590"
    secret = "SEC804d8c1c199e32330240305b1e84409b97180594d8d00f804d6d9c24b59b6d89"
    timestamp = str(round(time.time() * 1000))
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    url = f"https://oapi.dingtalk.com/robot/send?access_token={access_token}&timestamp={timestamp}&sign={sign}"
    headers = {"Content-Type": "application/json"}
    data = {
        "at": {"isAtAll": True},
        "msgtype": "markdown",
        "markdown": {
            "title": "创业板人气选股信息",
            "text": "创业板人气选股信息\n\n" + message + "超卖选股信息，考虑买入。（盈亏比95%）\n\n系统当前运行时间" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

        }
    }
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 200:
        print("消息发送成功")
    else:
        print("消息发送失败，状态码：", response.status_code)


def send_to_pushplus(message):
    url = "https://www.pushplus.plus/send"
    data = {
        "token": PUSHPLUS_TOKEN,
        "title": "创业板人气选股信息",
        "content": message + "上涨趋势选股信息，考虑买入。（盈亏比90%+） \n\n系统当前运行时间" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
        "template": "markdown",  # 可以根据需要修改为其他模板类型，如 "text"、"markdown" 等
        "topic": PUSHPLUS_TOPIC
    }
    try:
        response = requests.post(url, json=data)
        if response.status_code == 200:
            print("PushPlus 消息发送成功")
        else:
            print("PushPlus 消息发送失败，状态码：", response.status_code)
    except requests.RequestException as e:
        print("PushPlus 消息发送失败，错误：", e)


if __name__ == "__main__":
    main()