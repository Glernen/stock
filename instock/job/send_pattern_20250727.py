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
from decimal import Decimal

# PushPlus 的配置信息
PUSHPLUS_TOKEN = "935332d730494a87bddafe704c4e6f3c"
PUSHPLUS_TOPIC = "stock"


def main():
    # 配置日志记录
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
    try:
        # 建立数据库连接
        cnx = mysql.connector.connect(
            host="10.1.10.88",
            user="root",
            password="root",
            database="instockdb",
            use_pure=True  # 强制使用纯Python实现
        )
        # 使用 with 语句管理 cursor，确保资源自动释放
        with cnx.cursor() as cursor:
            # 先获取指数数据的最新日期
            ratio_query = """
                SELECT date_int,指数上涨情绪,指数下跌情绪,操作建议 
                FROM market_sentiment_a 
                ORDER BY date_int DESC 
                LIMIT 1;
            """
            cursor.execute(ratio_query)
            result = cursor.fetchone()

            if result:
                market_date = result[0]
                up = result[1]
                down = result[2]
                mark = result[3]
                message1 = f"大盘情绪日期：{market_date}\n\n买入情绪：{up}%，卖出情绪：{down}%\n\n{mark}！"
            else:
                message1 = f"暂无指数数据，可能系统崩了"
                market_date = None

            logging.info(f"操作建议: {message1}")
            print(f"操作建议: {message1}")

            # 如果获取到了市场日期，查询对应日期的阳锤子线数据
            message_content = ""
            if market_date:
                # 使用市场日期查询阳锤子线数据
                query = f"""
                SELECT 
                    pattern_stock.date_int,
                    pattern_stock.code,
                    pattern_stock.name,
                    pattern_stock.turnoverrate,
                    basic_info_stock.`东方财富网行业`,
                    basic_info_stock.`申万1级名`,
                    basic_info_stock.`申万2级名` 
                FROM pattern_stock 
                LEFT JOIN basic_info_stock 
                    ON basic_info_stock.code_int = pattern_stock.code_int 
                WHERE pattern_stock.date_int = {market_date}
                    AND `阳锤子线` = 1
                """

                cursor.execute(query)
                rows = cursor.fetchall()
                total_count = len(rows)  # 获取总数

                for row in rows:
                    # 处理日期元素
                    new_row = []
                    for element in row:
                        if isinstance(element, datetime.date):
                            element = str(element)
                        new_row.append(element)

                    # 生成超链接
                    date = new_row[0]
                    code = new_row[1]
                    name = new_row[2]
                    turnover = new_row[3]
                    industry1 = new_row[4]
                    industry2 = new_row[5]
                    industry3 = new_row[6]

                    if code.startswith(('30', '00')):
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=0&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    else:
                        link0 = f"[{code}](https://wzq.tenpay.com/mp/v2/index.html?stat=Obx51p007x002&_buildh5ver=202502192144&stat_data=Obx51p007x002#/trade/stock_detail.shtml?scode={code}&type=1&holder=&frombroker=&remindtype=choose&act_flow_id=stk_wave)"
                    link1 = f"[{name}](https://quote.eastmoney.com/{code}.html/)"

                    message_content += f"> {link0} {link1} | 换手率: {turnover}% | 东方财富行业: {industry1} | 申万1级: {industry2} | 申万2级: {industry3}\n\n"
                    logging.info(f"Stock info: {link0} {link1}")

                if not message_content:
                    message_content = f"日期 {market_date} 暂无阳锤子线形态的股票数据\n\n"
                else:
                    # 添加总数显示
                    message_content = f"阳锤子线选股信息 (日期: {market_date}, 总数: {total_count}只):\n\n{message_content}\n\n\n\n"
            else:
                message_content = "无法获取市场日期，跳过阳锤子线查询\n\n"

            # 发送消息到钉钉机器人
            send_to_dingtalk(message_content, message1)
            # 发送消息到 PushPlus
            send_to_pushplus(message_content, message1)
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
            "title": "阳锤子线选股信息",
            "text": f"{message1}\n\n\n\n" + message + "Good Luck。阳锤子线选股信息 \n\n系统当前运行时间" + time.strftime(
                '%Y-%m-%d %H:%M:%S', time.localtime())
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
        "title": "阳锤子线选股信息",
        "content": f"{message1}\n\n" + message + "Good Luck。阳锤子线选股信息 \n\n系统当前运行时间" + time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime()),
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