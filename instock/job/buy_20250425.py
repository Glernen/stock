#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

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
from decimal import Decimal  # 需在文件开头导入
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset 

# PushPlus 的配置信息
PUSHPLUS_TOKEN = "935332d730494a87bddafe704c4e6f3c"
PUSHPLUS_TOPIC = "stock"  # 群组编码

# A 策略 SQL
query_a = """
WITH latest_date AS (
    SELECT 
        MAX(`date_int`) AS latest_date_int
    FROM 
        `stock_3day_indicators`
),
buy_all AS (
    SELECT 
        `date_int`, 
        `code`, 
        `name`, 
        `strategy`, 
        `close`, 
        `turnover`, 
        `jingliuru_cn`, 
        `industry`, 
        `industry_cci`, 
        `industry_sentiment`,
        CAST(COUNT(*) OVER (PARTITION BY csi.`industry`) * 100.0 / COUNT(*) OVER () AS FLOAT) AS `入选股的行业占比`
    FROM 
        `cn_stock_indicators_buy` csi
    JOIN latest_date ON csi.`date_int` = latest_date.latest_date_int
    WHERE 
        csi.`rate_60` IS NULL 
        -- AND csi.`date_int` = 20250409
        AND csi.`strategy` = 'strategy_a' 
        -- AND csi.`industry_kdj` = '上涨趋势' 
        AND csi.`code` NOT LIKE '688%' 
        AND csi.`code` NOT LIKE '8%' 
    AND (
    -- 大盘强势上涨：大盘指标上涨，行业指标上涨，个股换手率小于20%的精筛股
            (csi.`up_sentiment` > 10
        AND csi.`up_sentiment` < 50
        AND csi.`down_sentiment` = 0
        AND csi.`industry_kdj` = '上涨趋势'
        AND csi.`industry_kdjj` < 60
        AND csi.`industry_sentiment` = '行业震荡中性'
        AND csi.`turnover` > 1
        AND csi.`industry_cci` != '方向不明')
    -- 大盘强势上涨：大盘指标上涨，行业指标上涨
    OR (csi.`up_sentiment` >= 50
        AND csi.`down_sentiment` = 0
        AND csi.`industry_kdj` = '上涨趋势'
        AND csi.`industry_sentiment` = '行业强势看涨'
        and csi.`turnover` > 0.5
        AND csi.`industry_cci` != '方向不明' )
    --     -- 大盘震荡期：大盘指标上涨（指数K值小于60为标准）小于2个指数盘，行业指标上涨，个股换手率小于20%的精筛股
    OR (csi.`up_sentiment` <= 20
        AND csi.`industry_kdj` = '上涨趋势'
        AND csi.`industry_wr` = '持续超买'
        AND csi.`turnover` < 20)
    --     -- 大盘上涨初现：大盘指标上涨，行业指标未墙裂上涨且处于超卖区，个股换手率大于1%的精筛股
    OR (csi.`up_sentiment` >= 30
        AND csi.`down_sentiment` = 0
        AND csi.`industry_kdj` = '下降趋势'
        AND csi.`industry_kdjj` < 20
        AND csi.`industry_sentiment` = '行业震荡中性'
        AND csi.`turnover` > 1)
        )
)
SELECT * 
FROM buy_all 
ORDER BY `入选股的行业占比` DESC  
LIMIT 20;

"""

# B 策略 SQL
query_b = """
WITH latest_date AS (
    SELECT 
        MAX(`date_int`) AS latest_date_int
    FROM 
        `stock_3day_indicators`
),
buy_all AS (
    SELECT 
        `date_int`, 
        `code`, 
        `name`, 
        `strategy`, 
        `close`, 
        `turnover`, 
        `jingliuru_cn`, 
        `industry`, 
        `industry_cci`, 
        `industry_sentiment`,
        CAST(COUNT(*) OVER (PARTITION BY csi.`industry`) * 100.0 / COUNT(*) OVER () AS FLOAT) AS `入选股的行业占比`
    FROM 
        `cn_stock_indicators_buy` csi
    JOIN latest_date ON csi.`date_int` = latest_date.latest_date_int
    WHERE 
        csi.`rate_60` IS NULL 
        -- AND csi.`date_int` = 20250409
        AND csi.`strategy` = 'strategy_b' 
        -- AND csi.`industry_kdj` = '上涨趋势' 
        AND csi.`code` NOT LIKE '688%' 
        AND csi.`code` NOT LIKE '8%' 
    AND (
    -- 大盘强势上涨：大盘指标上涨，行业指标上涨，个股换手率小于20%的精筛股
            (csi.`up_sentiment` > 10 
        AND csi.`up_sentiment` < 50
        AND csi.`down_sentiment` = 0
        AND csi.`industry_kdj` = '上涨趋势'
        AND csi.`industry_kdjj` < 60
        AND csi.`industry_sentiment` = '行业震荡中性'
        AND csi.`turnover` > 1
        AND csi.`industry_cci` != '方向不明')
    -- 大盘强势上涨：大盘指标上涨，行业指标上涨
    OR (csi.`up_sentiment` >= 50
        AND csi.`down_sentiment` = 0
        AND csi.`industry_kdj` = '上涨趋势'
        AND csi.`industry_sentiment` = '行业强势看涨'
        and csi.`turnover` > 0.5
        AND csi.`industry_cci` != '方向不明' )
    --     -- 大盘震荡期：大盘指标上涨（指数K值小于60为标准）小于2个指数盘，行业指标上涨，个股换手率小于20%的精筛股
    OR (csi.`up_sentiment` <= 20
        AND csi.`industry_kdj` = '上涨趋势'
        AND csi.`industry_wr` = '持续超买'
        AND csi.`turnover` < 20)
    --     -- 大盘上涨初现：大盘指标上涨，行业指标未墙裂上涨且处于超卖区，个股换手率大于1%的精筛股
    OR (csi.`up_sentiment` >= 30
        AND csi.`down_sentiment` = 0
        AND csi.`industry_kdj` = '下降趋势'
        AND csi.`industry_kdjj` < 20
        AND csi.`industry_sentiment` = '行业震荡中性'
        AND csi.`turnover` > 1)
        )
)
SELECT * 
FROM buy_all 
ORDER BY `入选股的行业占比` DESC  
LIMIT 20;
"""


def main():
    # 配置日志记录
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
    try:
        # 建立数据库连接，需要将相应的信息修改为你的数据库信息
        cnx = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_database
        )
        # 使用 with 语句管理 cursor，确保资源自动释放
        with cnx.cursor() as cursor:
            # 先获取指数买入推荐占比
            ratio_query = """
                SELECT date_int,指数上涨情绪,指数下跌情绪,操作建议 FROM market_sentiment_a ORDER BY date_int DESC LIMIT 1;
            """
            cursor.execute(ratio_query)
            result = cursor.fetchone()
            if result:
                date_int = result[0]
                up = result[1]
                down = result[2]
                mark = result[3]
                message1 = f"大盘情绪日期：{date_int}\n\n买入情绪：{up}%，卖出情绪：{down}%\n\n{mark}！"
            else:
                message1 = f"暂无指数数据，可能系统崩了"
            logging.info(f"操作建议: {message1}")
            print(f"操作建议: {message1}")

            # 执行 A 策略查询
            cursor.execute(query_a)
            message_a = ""
            while True:
                rows = cursor.fetchmany(100)
                if not rows:
                    break
                for row in rows:
                    # 处理日期元素

                    # 修改代码中的数据处理部分：
                    new_row = []
                    for element in row:
                        if isinstance(element, datetime.date):
                            element = str(element)
                        new_row.append(element)


                    # 生成超链接
                    date = new_row[0]
                    code = new_row[1]
                    name = new_row[2]
                    row2 = new_row[3]
                    row3 = new_row[4]
                    row4 = new_row[5] 
                    row5 = new_row[6]
                    row6 = new_row[7]
                    row7 = new_row[8]
                    row8 = new_row[9]
                    row9 = new_row[10]

                    if code.startswith(('30', '00')):
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=0&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    else:
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=1&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    link1 = f"[{name}](https://quote.eastmoney.com/{code}.html/)"
                    link2 = f"策略：{row2}"
                    link3 = f"当前价格：{row3}"
                    link4 = f"换手率：{row4}%"
                    link5 = f"净流入：{row5}"
                    link6 = f"行业：{row6}"
                    link7 = f"行业CCI：{row7}"
                    link8 = f"行业情绪：{row8}"
                    link9 = f"入选股的行业占比：{row9}%"
                    link100 = f"{date}入选"
                    link110 = f"行业数据：{row8}，{row7}"

                    # new_row[0] = link0
                    # new_row[1] = link1
                    # new_row[2] = link2
                    # new_row[3] = link3
                    # new_row[4] = link4  
                    # new_row[5] = link5
                    # new_row[6] = link6
                    # new_row[7] = link7
                    # new_row[8] = link8
                    # new_row[9] = link9

                    message_a += f"> {link0} {link1}  {link3} | {link4} | {link5} | {link6} | {link110} | {link9}\n\n"   # 将结果添加到消息中，每个结果占一行，增加额外的换行符
                    # 打印日志
                    logging.info(f"A 策略 Stock info: {message_a}")
                    # print(f"A 策略 Stock info: {message_a}")
            if not message_a:
                message_a = "A 策略暂无推荐的数据\n\n"
            else:
                message_a = f"A 策略{link110}推荐信息:\n\n{message_a}\n\n\n\n"

            # 执行 B 策略查询
            cursor.execute(query_b)
            message_b = ""
            while True:
                rows = cursor.fetchmany(100)
                if not rows:
                    break
                for row in rows:
                    # 处理日期元素
                     # 修改代码中的数据处理部分：
                    new_row = []
                    for element in row:
                        if isinstance(element, datetime.date):
                            element = str(element)
                        new_row.append(element)
                    # 生成超链接
                    date = new_row[0]
                    code = new_row[1]
                    name = new_row[2]
                    row2 = new_row[3]
                    row3 = new_row[4]
                    row4 = new_row[5] 
                    row5 = new_row[6]
                    row6 = new_row[7]
                    row7 = new_row[8]
                    row8 = new_row[9]
                    row9 = new_row[10]

                    if code.startswith(('30', '00')):
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=0&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    else:
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=1&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    link1 = f"[{name}](https://quote.eastmoney.com/{code}.html/)"
                    link2 = f"策略：{row2}"
                    link3 = f"当前价格：{row3}"
                    link4 = f"换手率：{row4}%"
                    link5 = f"净流入：{row5}"
                    link6 = f"行业：{row6}"
                    link7 = f"行业CCI：{row7}"
                    link8 = f"行业情绪：{row8}"
                    link9 = f"入选股的行业占比：{row9}%"
                    link100 = f"{date}入选"
                    link110 = f"行业数据：{row8}，{row7}"

                    # new_row[0] = link0
                    # new_row[1] = link1
                    # new_row[2] = link2
                    # new_row[3] = link3
                    # new_row[4] = link4  
                    # new_row[5] = link5
                    # new_row[6] = link6
                    # new_row[7] = link7
                    # new_row[8] = link8
                    # new_row[9] = link9

                    message_b += f"> {link0} {link1}  {link3} | {link4} | {link5} | {link6} | {link110} | {link9}\n\n"   # 将结果添加到消息中，每个结果占一行，增加额外的换行符
                    # 打印日志
                    logging.info(f"B 策略 Stock info: {message_b}")
                    # print(f"B 策略 Stock info: {message_b}")
            if not message_b:
                message_b = "B 策略暂无推荐的数据\n\n"
            else:
                message_b = f"B 策略{link110}推荐信息:\n\n{message_b}\n\n\n\n"

            message = f"{message_a}\n\n===================\n\n{message_b}"
            # print(f"A 策略推荐信息:\n{message_a}\n\n\n\nB 策略推荐信息:\n{message_b}")
            # 发送消息到钉钉机器人
            send_to_dingtalk(message, message1)
            # 发送消息到 PushPlus
            send_to_pushplus(message, message1)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Database does not exist")
        else:
            logging.error(err)


def send_to_dingtalk(message, message1):
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
            "title": "买入盘选股信息",
            "text": f"{message1}\n\n\n\n" + message + "Good Luck。买入盘选股信息 \n\n系统当前运行时间" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        }
    }
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 200:
        print("消息发送成功")
    else:
        print("消息发送失败，状态码：", response.status_code)


def send_to_pushplus(message, message1):
    url = "https://www.pushplus.plus/send"
    data = {
        "token": PUSHPLUS_TOKEN,
        "title": "买入盘选股信息",
        "content": f"{message1}\n\n" + message + "Good Luck。买入盘选股信息 \n\n系统当前运行时间" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
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