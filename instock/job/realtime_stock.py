#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import json
import re
import math
import requests
import numpy as np
import pandas as pd
import time
import datetime
import mysql.connector
import pandas_market_calendars as mcal
from mysql.connector import Error
from typing import List, Dict
from sqlalchemy import DATE, VARCHAR, FLOAT, BIGINT, SmallInteger, DATETIME, INT
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm



########################################################################

# 获取新浪接口实时股票数据
# 网址：https://vip.stock.finance.sina.com.cn/mkt/#hs_a
# 接口：https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=55&num=100&sort=symbol&asc=0&node=hs_a&symbol=
# hs_a，沪深A股，最多55页（包含北京）

def fetch_sina_stock_data(page):
    """从新浪接口获取股票数据"""
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=100&sort=symbol&asc=0&node=hs_a"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://vip.stock.finance.sina.com.cn/mkt/'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        # 新浪返回的是JSONP格式，实际是JSON字符串
        data_str = response.text.strip()

        # 处理特殊JSON格式（无引号的key）
        try:
            # 尝试直接解析
            return json.loads(data_str)
        except json.JSONDecodeError:
            # 修复非法JSON：给key加双引号
            fixed_json = re.sub(r'(\w+):', r'"\1":', data_str)
            return json.loads(fixed_json)

    except Exception as e:
        print(f"第{page}页请求失败: {str(e)}")
        return None


def stock_hs_a_spot_sina():
    """从新浪接口获取所有股票实时数据"""
    print("开始获取新浪实时股票数据...")
    start_time = time.time()

    total_pages = 55  # 沪深A股总页数
    all_stocks = []  # 存储所有股票数据

    # 使用tqdm显示进度条
    with tqdm(total=total_pages, desc="获取新浪股票数据") as pbar:
        for page in range(1, total_pages + 1):
            stock_data = fetch_sina_stock_data(page)
            if stock_data:
                all_stocks.extend(stock_data)
                pbar.set_postfix({"当前页": page, "总记录": len(all_stocks)})

            # 更新进度条
            pbar.update(1)
            time.sleep(0.5)  # 添加延迟避免被封IP

    # 转换为DataFrame
    if not all_stocks:
        print("未获取到任何股票数据")
        return None

    temp_df = pd.DataFrame(all_stocks)
    # print(f"成功获取 {len(temp_df)} 条股票数据，耗时 {time.time() - start_time:.2f}秒")

    # 转换数值类型（去除逗号）
    numeric_cols = ['trade', 'pricechange', 'changepercent', 'buy', 'sell',
                    'settlement', 'open', 'high', 'low', 'volume', 'amount',
                    'turnoverratio', 'nmc', 'mktcap', 'pb', 'per']

    for col in numeric_cols:
        if col in temp_df.columns:
            # 处理可能的字符串类型（包含逗号）
            temp_df[col] = temp_df[col].apply(lambda x: float(str(x).replace(',', '')) if pd.notnull(x) else None)

    # 单位转换
    # 1. 成交量：股 → 手 (除以100)
    temp_df['成交量(手)'] = temp_df['volume'] / 100

    # 2. 成交额：元 → 万元 (除以10000)
    temp_df['成交额(万元)'] = temp_df['amount'] / 10000

    # 3. 市值转换：万元 → 亿元 (除以10000)
    temp_df['流通市值(亿元)'] = temp_df['nmc'] / 10000
    temp_df['总市值(亿元)'] = temp_df['mktcap'] / 10000

    # 获取上证交易所日历
    sh_cal = mcal.get_calendar('SSE')
    latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime(
        "%Y-%m-%d")
    temp_df.loc[:, "date"] = latest_trade_date
    temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

    # 准备新浪实时股票数据
    realtime_stock_sina = pd.DataFrame({
        "date": temp_df["date"],
        "date_int": temp_df["date_int"],
        "code": temp_df["code"],
        "code_int": temp_df["code"].astype(int),
        "symbol": temp_df["symbol"],
        "name": temp_df["name"],
        "开盘价": temp_df["open"],
        "最高价": temp_df["high"],
        "最低价": temp_df["low"],
        "收盘价": temp_df["trade"],
        "昨收价": temp_df["settlement"],
        "成交量(手)": temp_df["成交量(手)"],
        "换手率(%)": temp_df["turnoverratio"],
        "涨跌幅(%)": temp_df["changepercent"],
        "涨跌额": temp_df["pricechange"],
        "成交额(万元)": temp_df["成交额(万元)"],
        "总市值(亿元)": temp_df["总市值(亿元)"],
        "流通市值(亿元)": temp_df["流通市值(亿元)"]
    })

    # print(realtime_stock_sina.head())

    # 生成批量SQL
    sql_batches = sql_batch_generator(
        table_name='realtime_stock_sina',
        data=realtime_stock_sina,
        batch_size=6000  # 根据实际情况调整
    )

    # 执行批量插入
    execute_batch_sql(sql_batches)
    print(f"[Success] 新浪股票实时数据写入完成，数据量：{len(realtime_stock_sina)}，耗时 {time.time() - start_time:.2f}秒")
    return realtime_stock_sina


########################################################################

# 获取腾讯接口实时股票数据
# https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList?_appver=11.17.0&board_code=aStock&sort_type=price&direct=down&offset=0&count=200
# （来源网址：https://stockapp.finance.qq.com/mstats/#mod=list&id=hs_hsj&module=hs&type=hsj）

def fetch_tencent_stock_data(offset=0, count=200):
    """获取单页股票数据"""
    url = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList"
    params = {
        "_appver": "11.17.0",
        "board_code": "aStock",
        "sort_type": "price",
        "direct": "down",
        "offset": offset,
        "count": count
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data['code'] == 0:
                return data['data']['rank_list'], data['data']['total']
        return [], 0
    except Exception as e:
        print(f"请求失败: {e}")
        return [], 0


def get_tencent_all_stocks():
    """从新浪接口获取所有股票实时数据"""
    print("开始获取腾讯实时股票数据...")
    start_time = time.time()

    """获取所有股票数据"""
    all_stocks = []
    count = 200  # 每页固定200条
    first_page_data, total = fetch_tencent_stock_data(offset=0, count=count)
    all_stocks.extend(first_page_data)

    if total > count:
        total_pages = (total + count - 1) // count  # 计算总页数
        for page in tqdm(range(1, total_pages), desc="获取腾讯股票数据"):
            offset = page * count
            page_data, _ = fetch_tencent_stock_data(offset=offset, count=count)
            all_stocks.extend(page_data)

            # 在每次请求后添加延时
            time.sleep(0.5)  # 添加延时避免被封IP

    # 转换为DataFrame
    if not all_stocks:
        print("未获取到任何股票数据")
        return None


    temp_df = pd.DataFrame(all_stocks)
    # print(f"成功获取 {len(temp_df)} 条股票数据，耗时 {time.time() - start_time:.2f}秒")

    # 数值类型转换
    numeric_cols = ['close', 'volume', 'hsl', 'zdf', 'zf', 'zd', 'lb', 'speed',
                    'zljlr', 'turnover', 'zdf_d5', 'zdf_d10', 'zdf_d20', 'zdf_d60',
                    'zdf_w52', 'zdf_y', 'zllc', 'zllr', 'zllc_d5', 'zllr_d5',
                    'zsz', 'ltsz', 'pe_ttm', 'pn']

    for col in numeric_cols:
        if col in temp_df.columns:
            temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')

    # 获取上证交易所日历
    sh_cal = mcal.get_calendar('SSE')
    latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime(
        "%Y-%m-%d")
    temp_df.loc[:, "date"] = latest_trade_date
    temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

    # 提取后6位作为股票代码
    temp_df["stock_code"] = temp_df["code"].str[-6:]

    # 准备腾讯实时股票数据
    realtime_stock_tx = pd.DataFrame({
        "date": temp_df["date"],
        "date_int": temp_df["date_int"],
        "code": temp_df["stock_code"],
        "code_int": temp_df["stock_code"].astype(int),
        "symbol": temp_df["code"],
        "name": temp_df["name"],
        "收盘价": temp_df["zxj"],
        "成交量(手)": temp_df["volume"],
        "换手率(%)": temp_df["hsl"],
        "涨跌幅(%)": temp_df["zdf"],
        "振幅(%)": temp_df["zf"],
        "涨跌额": temp_df["zd"],
        "量比": temp_df["lb"],
        "涨速(%)": temp_df["speed"],
        "主力净流入(万元)": temp_df["zljlr"],
        "成交额(万元)": temp_df["turnover"],
        "5日涨跌幅(%)": temp_df["zdf_d5"],
        "10日涨跌幅(%)": temp_df["zdf_d10"],
        "20日涨跌幅(%)": temp_df["zdf_d20"],
        "60日涨跌幅(%)": temp_df["zdf_d60"],
        "52周涨跌幅(%)": temp_df["zdf_w52"],
        "年初至今涨跌幅(%)": temp_df["zdf_y"],
        "主力流出(万元)": temp_df["zllc"],
        "主力流入(万元)": temp_df["zllr"],
        "5日主力流出(万元)": temp_df["zllc_d5"],
        "5日主力流入(万元)": temp_df["zllr_d5"],
        "总市值(亿元)": temp_df["zsz"],
        "流通市值(亿元)": temp_df["ltsz"],
        "市盈率TTM": temp_df["pe_ttm"],
        "市净率": temp_df["pn"],
        "股票类型": temp_df["stock_type"]

    })

    # print(realtime_stock_tx.head())

    # 生成批量SQL
    sql_batches = sql_batch_generator(
        table_name='realtime_stock_tx',
        data=realtime_stock_tx,
        batch_size=6000  # 根据实际情况调整
    )

    # 执行批量插入
    execute_batch_sql(sql_batches)
    print(f"[Success] 腾讯股票实时数据写入完成，数据量：{len(realtime_stock_tx)}，耗时 {time.time() - start_time:.2f}秒")

    return realtime_stock_tx


########################################################################

# 获取沪市A股+深市A股实时股票数据数据并写入数据库

def stock_zh_a_spot_em() -> pd.DataFrame:
    '''
    东方财富网-沪深京 A 股-实时行情
    https://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 实时行情
    :rtype: pandas.DataFrame
    '''
    start_time = time.time()
    print("开始获取东方财富实时股票数据...")

    page_size = 100
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    # 初始请求参数，先获取总数据量和 page_size
    params = {
        "pn": "1",
        "pz": str(page_size),
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f26,f37,f38,f39,f40,f41,f45,f46,f48,f49,f57,f61,f100,f112,f113,f114,f115,f221",
        "_": str(int(time.time() * 1000)),
    }

    try:
        # 获取总页数
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data_json = response.json()
            total_count = data_json["data"]["total"]
            page_total = math.ceil(total_count / page_size)
            print(f"总数据量: {total_count}, 总页数: {page_total}")
        except Exception as e:
            print(f"获取总页数失败: {str(e)}")
            return pd.DataFrame()

        all_data = []

        # 使用tqdm显示进度条
        with tqdm(total=page_total, desc="获取东方财富股票数据") as pbar:
            for page in range(1, page_total + 1):
                try:
                    # 更新页码参数
                    params["pn"] = str(page)
                    params["_"] = str(int(time.time() * 1000))  # 更新时间戳防止缓存

                    response = requests.get(url, params=params,  timeout=10)
                    response.raise_for_status()

                    data_json = response.json()
                    page_data = data_json["data"]["diff"]
                    all_data.extend(page_data)

                    pbar.set_postfix({"当前页": page, "总记录": len(all_data)})
                    pbar.update(1)

                except Exception as e:
                    print(f"第{page}页请求失败: {str(e)}")

                # 添加请求间隔，避免被封IP
                time.sleep(1)

        if not all_data:
            print("未获取到任何数据")
            return pd.DataFrame()

        temp_df = pd.DataFrame(all_data)

        # 定义数值列清单
        numeric_cols = [
            'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 'f10', 'f11','f13',
            'f15', 'f16', 'f17', 'f18', 'f20', 'f21', 'f22', 'f23', 'f24', 'f25',
            'f37', 'f38', 'f39', 'f40', 'f41', 'f45', 'f46', 'f48', 'f49', 'f57', 'f61',
            'f112', 'f113', 'f114', 'f115'
        ]

        # 执行类型转换
        temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        date_cols = ["f26", "f221"]
        temp_df[date_cols] = temp_df[date_cols].apply(lambda x: pd.to_datetime(x, format='%Y%m%d', errors="coerce"))

        # 获取上证交易所日历
        sh_cal = mcal.get_calendar('SSE')
        latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime(
            "%Y-%m-%d")
        temp_df.loc[:, "date"] = latest_trade_date
        temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

        # temp_df = temp_df.loc[temp_df['new_price'].apply(is_open)]
        # temp_df = temp_df.replace({np.nan: None})
        # 单位转换

        #  成交额：元 → 万元 (除以10000)
        temp_df['成交额(万元)'] = temp_df['f6'] / 10000

        #  总市值：元 → 亿元 (除以100000000)
        temp_df['总市值(亿元)'] = temp_df['f20'] / 100000000

        #  流通市值：元 → 亿元 (除以100000000)
        temp_df['流通市值(亿元)'] = temp_df['f21'] / 100000000

        # 准备东方财富实时股票数据
        realtime_stock_df = pd.DataFrame({
            "date": temp_df["date"],
            "date_int": temp_df["date_int"],
            "code": temp_df["f12"],
            "code_int": temp_df["f12"].astype(int),
            "name": temp_df["f14"],
            "市场标识": temp_df["f13"],
            "开盘价": temp_df["f17"],
            "最高价": temp_df["f15"],
            "最低价": temp_df["f16"],
            "收盘价": temp_df["f2"],
            "昨收价": temp_df["f18"],
            "成交量(手)": temp_df["f5"],
            "成交额(万元)": temp_df["成交额(万元)"],
            "振幅(%)": temp_df["f7"],
            "换手率(%)": temp_df["f8"],
            "涨跌幅(%)": temp_df["f3"],
            "涨跌额": temp_df["f4"],
            "量比": temp_df["f10"],
            "涨速(%)": temp_df["f22"],
            "5分钟涨跌幅(%)": temp_df["f11"],
            "60日涨跌幅(%)": temp_df["f24"],
            "年初至今涨跌幅(%)": temp_df["f25"],
            "动态市盈率": temp_df["f9"],
            "市盈率TTM": temp_df["f115"],
            "静态市盈率": temp_df["f114"],
            "市净率": temp_df["f23"],
            "每股收益": temp_df["f112"],
            "每股净资产": temp_df["f113"],
            "每股公积金": temp_df["f61"],
            "每股未分配利润": temp_df["f48"],
            "加权净资产收益率": temp_df["f37"],
            "毛利率": temp_df["f49"],
            "资产负债率": temp_df["f57"],
            "营业收入": temp_df["f40"],
            "营业收入同比增长": temp_df["f41"],
            "归属净利润": temp_df["f45"],
            "归属净利润同比增长": temp_df["f46"],
            "报告期": temp_df["f221"],
            "总股本": temp_df["f38"],
            "已流通股份": temp_df["f39"],
            "总市值(亿元)": temp_df["总市值(亿元)"],
            "流通市值(亿元)": temp_df["流通市值(亿元)"],
            "所处行业": temp_df["f100"],
            "上市时间": temp_df["f26"]
        })

        # print(realtime_stock_df.head())

        # 生成批量SQL
        sql_batches = sql_batch_generator(
            table_name='realtime_stock_df',
            data=realtime_stock_df,
            batch_size=6000  # 根据实际情况调整
        )

        # 执行批量插入
        execute_batch_sql(sql_batches)
        print(
            f"[Success] 东方财富股票实时数据写入完成，数据量：{len(realtime_stock_df)}，耗时 {time.time() - start_time:.2f}秒")

        return realtime_stock_df

    except Exception as e:
        print(f"东方财富股票实时数据处理失败: {e}")


########################################################################
class DBManager:
    @staticmethod
    def get_new_connection():
        """创建并返回一个新的数据库连接"""
        try:
            connection = mysql.connector.connect(
                # host=db_host,
                # user=db_user,
                # password=db_password,
                # database=db_database,
                # charset=db_charset,
                host="rm-uf6kkirh7qc88ug36fo.mysql.rds.aliyuncs.com",
                user="aheng",
                password="Admin888dashabi",
                database="instockdb",
                charset="utf8mb4",
                use_pure=True  # 强制使用纯Python实现
            )
            return connection
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            return None

    @staticmethod
    def execute_sql(sql: str, params=None):
        """安全执行 SQL 语句"""
        connection = DBManager.get_new_connection()
        if connection:
            try:
                cursor = connection.cursor(buffered=True)
                cursor.execute(sql, params)
                connection.commit()
                cursor.close()
            except Error as e:
                print(f"Error while executing SQL: {e}")
            finally:
                if connection.is_connected():
                    connection.close()


def sql_batch_generator(table_name, data, batch_size=500):
    columns = ', '.join([f"`{col}`" for col in data.columns])
    unique_keys = ['date_int', 'code_int']
    update_clause = ', '.join(
        [f"`{col}`=VALUES(`{col}`)"
         for col in data.columns if col not in unique_keys]
    )

    batches = []
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i + batch_size]
        value_rows = []

        for row in batch.itertuples(index=False):
            values = []
            for item in row:
                if pd.isna(item) or item in ['-', '']:
                    values.append("NULL")
                elif isinstance(item, (datetime.date, datetime.datetime)):
                    values.append(f"'{item.strftime('%Y-%m-%d')}'")
                elif isinstance(item, (int, float)):
                    values.append(str(item))
                else:
                    cleaned = str(item).replace("'", "''").replace("\\", "\\\\")
                    values.append(f"'{cleaned}'")
            value_rows.append(f"({', '.join(values)})")

        sql = f"""INSERT INTO `{table_name}` ({columns}) 
               VALUES {','.join(value_rows)}
               ON DUPLICATE KEY UPDATE {update_clause};"""
        batches.append(sql)

    return batches


def execute_batch_sql(sql_batches, max_retries=3):
    """通用批量执行函数"""
    for batch in sql_batches:
        attempt = 0
        while attempt < max_retries:
            conn = None
            try:
                conn = DBManager.get_new_connection()
                cursor = conn.cursor()

                # 执行单个批次
                cursor.execute(batch)

                # 显式消费结果集
                while True:
                    if cursor.with_rows:
                        cursor.fetchall()
                    if not cursor.nextset():
                        break

                conn.commit()
                print(f"成功插入 {batch.count('VALUES')} 行数据")
                break

            except Error as e:
                attempt += 1
                print(f"第{attempt}次重试失败: {e}")
                if conn:
                    conn.rollback()
                time.sleep(2 ** attempt)  # 指数退避
            finally:
                if conn and conn.is_connected():
                    cursor.close()
                    conn.close()


def main():
    # 创建进程池（最多3个进程）
    with ProcessPoolExecutor(max_workers=3) as executor:
        # 提交三个任务到进程池
        futures = [
            executor.submit(stock_zh_a_spot_em),
            executor.submit(stock_hs_a_spot_sina),
            executor.submit(get_tencent_all_stocks)
        ]

        # 等待所有任务完成（可选添加进度条）
        for future in tqdm(as_completed(futures), total=len(futures), desc="执行数据获取任务"):
            try:
                future.result()  # 获取结果（如有异常会在此抛出）
            except Exception as e:
                print(f"任务执行出错: {str(e)}")


if __name__ == "__main__":
    main()