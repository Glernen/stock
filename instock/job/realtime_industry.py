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
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset


########################################################################

def fetch_tencent_industry_data(board_type='hy', offset=0, count=200):
    """获取单页行业数据"""
    url = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/pt/getRank"
    params = {
        "board_type": board_type,  # hy:一级行业, hy2:二级行业
        "sort_type": "price",
        "direct": "down",
        "offset": offset,
        "count": count
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data['code'] == 0:
                return data['data']['rank_list'], data['data']['total']
        return [], 0
    except Exception as e:
        print(f"请求失败: {e}")
        return [], 0


def get_tencent_all_industries(board_type='hy'):
    """获取所有行业数据（一级或二级）"""
    print(f"开始获取腾讯行业数据({board_type})...")
    start_time = time.time()

    all_industries = []
    count = 200  # 每页固定200条
    first_page_data, total = fetch_tencent_industry_data(board_type=board_type, offset=0, count=count)
    all_industries.extend(first_page_data)

    # 处理分页
    if total > count:
        total_pages = (total + count - 1) // count
        for page in tqdm(range(1, total_pages), desc=f"获取腾讯行业数据({board_type})进度"):
            offset = page * count
            page_data, _ = fetch_tencent_industry_data(board_type=board_type, offset=offset, count=count)
            all_industries.extend(page_data)

            # 添加请求间隔
            time.sleep(0.5)

    # 创建DataFrame
    temp_df = pd.DataFrame(all_industries)

    # 添加当前日期
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    temp_df.loc[:, "date"] = today
    temp_df.loc[:, "date_int"] = temp_df["date"].str.replace('-', '')

    # 展开领涨股信息
    if 'lzg' in temp_df.columns:
        lzg_df = pd.json_normalize(temp_df['lzg'])
        lzg_df.columns = ['lzg_' + col for col in lzg_df.columns]
        temp_df = pd.concat([temp_df.drop('lzg', axis=1), lzg_df], axis=1)

    # 数值类型转换
    numeric_cols = ['close', 'volume', 'hsl', 'zdf', 'zd', 'lb', 'speed',
                    'zljlr', 'turnover', 'zdf_d5', 'zdf_d20', 'zdf_d60',
                    'zdf_w52', 'zdf_y', 'zllc', 'zllr', 'zljlr_d5', 'zljlr_d20',
                    'zsz', 'ltsz', 'pe_ttm', 'pn', 'lzg_zd', 'lzg_zdf', 'lzg_zxj']

    for col in numeric_cols:
        if col in temp_df.columns:
            temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')

    # 获取上证交易所日历
    sh_cal = mcal.get_calendar('SSE')
    latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime(
        "%Y-%m-%d")
    temp_df.loc[:, "date"] = latest_trade_date
    temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

    # 提取后8位作为股票代码
    temp_df["industry_code"] = temp_df["code"].str[-8:]
    temp_df["stock_code"]= temp_df["lzg_code"].str[-6:]


    # 准备腾讯实时行业数据
    realtime_industry_tx = pd.DataFrame({
        "date": temp_df["date"],
        "date_int": temp_df["date_int"],
        "code": temp_df["industry_code"],
        "code_int": temp_df["industry_code"].astype(int),
        "symbol": temp_df["code"],
        "name": temp_df["name"],
        "行业类型": temp_df["stock_type"],
        "收盘价": temp_df["zxj"],
        "成交量(手)": temp_df["volume"],
        "换手率(%)": temp_df["hsl"],
        "涨跌幅(%)": temp_df["zdf"],
        "涨跌额": temp_df["zd"],
        "量比": temp_df["lb"],
        "涨速(%)": temp_df["speed"],
        "主力净流入(万元)": temp_df["zljlr"],
        "成交额(万元)": temp_df["turnover"],
        "5日涨跌幅(%)": temp_df["zdf_d5"],
        "20日涨跌幅(%)": temp_df["zdf_d20"],
        "60日涨跌幅(%)": temp_df["zdf_d60"],
        "52周涨跌幅(%)": temp_df["zdf_w52"],
        "年初至今涨跌幅(%)": temp_df["zdf_y"],
        "主力流出(万元)": temp_df["zllc"],
        "主力流入(万元)": temp_df["zllr"],
        "5日主力净流入(万元)": temp_df["zljlr_d5"],
        "20日主力净流入(万元)": temp_df["zljlr_d20"],
        "总市值(亿元)": temp_df["zsz"],
        "流通市值(亿元)": temp_df["ltsz"],
        "lzg_code": temp_df["stock_code"],
        "lzg_code_int": temp_df["stock_code"].astype(int),
        "lzg_symbol": temp_df["lzg_code"],
        "lzg_name": temp_df["lzg_name"],
        "领涨股涨跌额": temp_df["lzg_zd"],
        "领涨股涨跌幅(%)": temp_df["lzg_zdf"],
        "领涨股收盘价": temp_df["lzg_zxj"],
    })

    # print(realtime_stock_tx.head())

    # 生成批量SQL
    sql_batches = sql_batch_generator(
        table_name='realtime_industry_tx',
        data=realtime_industry_tx,
        batch_size=6000  # 根据实际情况调整
    )

    # 执行批量插入
    execute_batch_sql(sql_batches)
    print(f"[Success] 腾讯股票行业数据写入完成，数据量：{len(realtime_industry_tx)}，耗时 {time.time() - start_time:.2f}秒")

    return realtime_industry_tx


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
        with tqdm(total=page_total, desc="获取股票数据") as pbar:
            for page in range(1, page_total + 1):
                try:
                    # 更新页码参数
                    params["pn"] = str(page)
                    params["_"] = str(int(time.time() * 1000))  # 更新时间戳防止缓存

                    response = requests.get(url, params=params, timeout=10)
                    response.raise_for_status()

                    data_json = response.json()
                    page_data = data_json["data"]["diff"]
                    all_data.extend(page_data)

                    pbar.set_postfix({"当前页": page, "总记录": len(all_data)})
                    pbar.update(1)

                except Exception as e:
                    print(f"第{page}页请求失败: {str(e)}")

                # 添加请求间隔，避免被封IP
                time.sleep(0.5)

        if not all_data:
            print("未获取到任何数据")
            return pd.DataFrame()

        temp_df = pd.DataFrame(all_data)

        # 定义数值列清单
        numeric_cols = [
            'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 'f10', 'f11', 'f13',
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
                host=db_host,
                user=db_user,
                password=db_password,
                database=db_database,
                charset=db_charset,
                # host="rm-uf6kkirh7qc88ug36fo.mysql.rds.aliyuncs.com",
                # user="aheng",
                # password="Admin888dashabi",
                # database="instockdb",
                # charset="utf8mb4",
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
            executor.submit(get_tencent_all_industries, 'hy'),  # 申万一级行业
            executor.submit(get_tencent_all_industries, 'hy2')  # 申万二级行业
            # executor.submit(get_tencent_all_stocks)
        ]

        # 等待所有任务完成（可选添加进度条）
        for future in tqdm(as_completed(futures), total=len(futures), desc="执行数据获取任务"):
            try:
                future.result()  # 获取结果（如有异常会在此抛出）
            except Exception as e:
                print(f"任务执行出错: {str(e)}")


if __name__ == "__main__":
    main()