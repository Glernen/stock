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
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset 

# PushPlus 的配置信息
PUSHPLUS_TOKEN = "935332d730494a87bddafe704c4e6f3c"
PUSHPLUS_TOPIC = "stock"  # 群组编码

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
            -- 子查询获取最大日期
            WITH max_date AS (
                SELECT MAX(date_int) AS max_date_int FROM index_3day_indicators
            ),
            up_sentiment AS (
                SELECT
                    md.max_date_int AS date_int,
                    ROUND(IFNULL((COUNT(i3di.date_int) / 10.0) * 100, 0)) AS `指数上涨情绪`
                FROM
                    max_date md
                -- 左连接确保即使无符合条件数据也能显示最大日期
                LEFT JOIN index_3day_indicators i3di ON 
                    i3di.date_int = md.max_date_int
                    AND `kdjK` >= kdjk_day1
                    AND `kdjd` >= kdjd_day1
                    AND wr_6 >= wr_6_day1
                    AND cci >= cci_day1
                    AND i3di.code_int IN (1, 300, 510, 399001, 399006, 399293, 688, 852, 932000, 899050)
                    AND (`kdjk` <= 45)
                    AND (`kdjd` <= 45)
                    AND (`kdjj` <= 75)
                GROUP BY
                    md.max_date_int
            ),
            down_sentiment AS (
                SELECT
                    md.max_date_int AS date_int,
                    ROUND(IFNULL((COUNT(i3di.date_int) / 10.0) * 100, 0)) AS `指数下跌情绪`
                FROM
                    max_date md
                -- 左连接确保即使无符合条件数据也能显示最大日期
                LEFT JOIN index_3day_indicators i3di ON 
                    i3di.date_int = md.max_date_int
                    AND `kdjK` <= kdjk_day1
                    AND `kdjd` <= kdjd_day1
                    AND wr_6 <= wr_6_day1
                    AND cci <= cci_day1
                    AND cci_day1 <= cci_day2
                    AND i3di.code_int IN (1, 300, 510, 399001, 399006, 399293, 688, 852, 932000, 899050)
                    AND (`kdjk` >= 70)
                    AND (`kdjd` >= 60)
                    AND (abs(`wr_6`) <= 70)
                GROUP BY
                    md.max_date_int
            )
            SELECT
                us.date_int,
                us.`指数上涨情绪`,
                ds.`指数下跌情绪`,
                CASE
                    WHEN us.`指数上涨情绪` = 100 AND ds.`指数下跌情绪` = 0 THEN '满仓梭哈，韭菜冲锋'
                    WHEN us.`指数上涨情绪` > 0 AND ds.`指数下跌情绪` = 0 THEN '上行趋势，适当建仓'
                    WHEN us.`指数上涨情绪` = 0 AND ds.`指数下跌情绪` = 0 THEN '震荡阶段，谨慎操作'
                    WHEN us.`指数上涨情绪` = 0 AND ds.`指数下跌情绪` > 0 THEN '下行趋势，适当减仓'
                    WHEN us.`指数上涨情绪` = 0 AND ds.`指数下跌情绪` = 100 THEN '一键清仓，韭菜快跑'
                    ELSE '未知状态'
                END AS `市场状态`
            FROM
                up_sentiment us
            JOIN
                down_sentiment ds ON us.date_int = ds.date_int;    
            """
            cursor.execute(ratio_query)
            result = cursor.fetchone()
            if result:
                date_int = result[0]
                up = result[1]
                down = result[2]
                mark = result[3]
                message1 = f"日期：{date_int}\n\n买入情绪：{up}%，卖出情绪：{down}%\n\n{mark}！"
            else:
                message1 = f"暂无指数数据，可能系统崩了"


            # 编写 SQL 查询语句，将字符串比较修改为数值比较
            query = """
            SELECT 
                csi.code,
                csi.name,
            --     csi.date_int,
            --     csi.wr_6,
            --     csi.kdjk,
            --     csi.kdjd,
            --     csi.kdjj,
                s.industry,
            --     t.code_int AS zijin_code_int,
                t.jingliuru AS 净流入,
                t.new_price AS 最新价,
                t.turnover AS 换手率
            FROM stock_3day_indicators csi 
            JOIN cn_stock_info s ON s.code_int = csi.code_int
            JOIN stock_zijin t ON t.code_int = csi.code_int AND t.date_int = csi.date_int
            WHERE 
                csi.kdjk <= '45'
                AND csi.kdjd <= '45'
                AND csi.kdjj <= '0' -- KDJ中J值小于0，超卖现象
                AND csi.cci < '-130' -- CCI值低于-130，超卖现象
                AND rsi_6 <= '30' -- rsi6低于30，超卖现象
                AND ABS(csi.wr_6) >= 90 -- 威廉6日大于90，超卖现象
                AND ABS(csi.wr_10) >= 90 -- 威廉10日大于90，超卖现象
                AND csi.date_int >= (SELECT MAX(date_int) FROM stock_3day_indicators)
                AND csi.name NOT LIKE '%ST%'
                AND s.industry not regexp '酿酒行业|美容护理|农药兽药|食品饮料|光伏设备|煤炭行业|造纸印刷|保险'
            ORDER BY csi.date_int;
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
                    row2 = new_row[2]
                    row3 = new_row[3]
                    row4 = new_row[4] 
                    row5 = new_row[5]
                    # row6 = new_row[6]
                    # downnday = new_row[8]

                    if code.startswith(('30', '00')):
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=0&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    else:
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=1&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    link1 = f"[{name}](https://quote.eastmoney.com/{code}.html/)"
                    link2 = f"行业：{row2}"
                    link3 = f"净流入：{row3}"
                    link4 = f"当前价格：{row4}"  # 不再使用指数买入推荐占比
                    link5 = f"换手率：{row5}%"
                    # link6 = f"行业占比：{concept}%"


                    new_row[0] = link0
                    new_row[1] = link1
                    new_row[2] = link2
                    new_row[3] = link3
                    new_row[4] = link4  
                    new_row[5] = link5
                    # new_row[6] = link7
                    # new_row[8] = link8

                    message += str(new_row) + "\n\n"  # 将结果添加到消息中，每个结果占一行，增加额外的换行符
                    # 打印日志
                    logging.info(f"Stock info: {new_row}")
            if not message:
                message = "暂无推荐的数据\n\n"
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
            "text": f"{message1}\n\n买入盘选股信息\n\n" + message + "玩股票不就为解套吗？买的那一刻你就开启了解套之路，Good Luck。买入盘选股信息 \n\n系统当前运行时间" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
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
        "content": f"{message1}\n\n" + message + "玩股票不就为解套吗？买的那一刻你就开启了解套之路，Good Luck。买入盘选股信息 \n\n系统当前运行时间" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
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