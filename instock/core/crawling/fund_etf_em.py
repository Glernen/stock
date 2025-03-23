#!/usr/bin/env python
# -*- coding:utf-8 -*-
'''
Date: 2023/1/4 12:18
Desc: 东方财富-ETF 行情
https://quote.eastmoney.com/sh513500.html
'''

from functools import lru_cache
import pandas as pd
import requests
import logging

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fund_etf_spot_em() -> pd.DataFrame:
    """
    东方财富-ETF 实时行情
    https://quote.eastmoney.com/center/gridlist.html#fund_etf
    :return: ETF 实时行情
    :rtype: pandas.DataFrame
    """
    url = "http://88.push2.eastmoney.com/api/qt/clist/get"
    # 初始请求参数，先获取总数据量和 page_size
    params = {
        "pn": "1",
        "pz": "1000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "wbp2u": "|0|0|0|web",
        "fid": "f3",
        "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
        "_": "1672806290972",
    }
    try:
        # 发送第一次请求获取总数据量和 page_size
        r = requests.get(url, params=params)
        r.raise_for_status()
        data_json = r.json()
        total = data_json["data"]["total"]
        # 获取 page_size
        page_size = len(data_json["data"]["diff"])
        total_pages = (total + page_size - 1) // page_size

        all_data = []
        for page in range(1, total_pages + 1):
            params["pn"] = str(page)
            params["pz"] = str(page_size)
            r = requests.get(url, params=params)
            r.raise_for_status()
            data_json = r.json()
            if data_json["data"]["diff"]:
                all_data.extend(data_json["data"]["diff"])

        if not all_data:
            return pd.DataFrame()

        temp_df = pd.DataFrame(all_data)
        temp_df.rename(
            columns={
                "f12": "代码",
                "f14": "名称",
                "f2": "最新价",
                "f3": "涨跌幅",
                "f4": "涨跌额",
                "f5": "成交量",
                "f6": "成交额",
                "f17": "开盘价",
                "f15": "最高价",
                "f16": "最低价",
                "f18": "昨收",
                "f8": "换手率",
                "f21": "流通市值",
                "f20": "总市值",
            },
            inplace=True,
        )
        temp_df = temp_df[
            [
                "代码",
                "名称",
                "最新价",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "开盘价",
                "最高价",
                "最低价",
                "昨收",
                "换手率",
                "流通市值",
                "总市值",
            ]
        ]
        numeric_cols = [
            "最新价", "涨跌幅", "涨跌额", "成交量", "成交额",
            "开盘价", "最高价", "最低价", "昨收", "换手率",
            "流通市值", "总市值"
        ]
        temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        return temp_df
    except requests.RequestException as e:
        logging.error(f"请求出错: {e}")
        return pd.DataFrame()
    except KeyError as e:
        logging.error(f"解析数据出错: {e}，响应数据：{r.json() if 'r' in locals() else '未知'}")
        return pd.DataFrame()


@lru_cache()
def _fund_etf_code_id_map_em() -> dict:
    """
    东方财富-ETF 代码和市场标识映射
    https://quote.eastmoney.com/center/gridlist.html#fund_etf
    :return: ETF 代码和市场标识映射
    :rtype: pandas.DataFrame
    """
    url = "http://88.push2.eastmoney.com/api/qt/clist/get"
    # 初始请求参数，先获取总数据量和 page_size
    params = {
        "pn": "1",
        "pz": "5000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "wbp2u": "|0|0|0|web",
        "fid": "f3",
        "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",
        "fields": "f12,f13",
        "_": "1672806290972",
    }
    try:
        # 发送第一次请求获取总数据量和 page_size
        r = requests.get(url, params=params)
        r.raise_for_status()
        data_json = r.json()
        total = data_json["data"]["total"]
        # 获取 page_size
        page_size = len(data_json["data"]["diff"])
        total_pages = (total + page_size - 1) // page_size

        all_data = []
        for page in range(1, total_pages + 1):
            params["pn"] = str(page)
            params["pz"] = str(page_size)
            r = requests.get(url, params=params)
            r.raise_for_status()
            data_json = r.json()
            if data_json["data"]["diff"]:
                all_data.extend(data_json["data"]["diff"])

        if not all_data:
            return {}

        temp_df = pd.DataFrame(all_data)
        temp_dict = dict(zip(temp_df["f12"], temp_df["f13"]))
        return temp_dict
    except requests.RequestException as e:
        logging.error(f"请求出错: {e}")
        return {}
    except KeyError as e:
        logging.error(f"解析数据出错: {e}，响应数据：{r.json() if 'r' in locals() else '未知'}")
        return {}


def fund_etf_hist_em(
    symbol: str = "159707",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
) -> pd.DataFrame:
    """
    东方财富-ETF 行情
    https://quote.eastmoney.com/sz159707.html
    :param symbol: ETF 代码
    :type symbol: str
    :param period: choice of {'daily', 'weekly', 'monthly'}
    :type period: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param adjust: choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :type adjust: str
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    code_id_dict = _fund_etf_code_id_map_em()
    if symbol not in code_id_dict:
        logging.error(f"未找到 {symbol} 的市场标识映射")
        return pd.DataFrame()
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": "1623766962675",
    }
    try:
        logging.info(f"请求 {symbol} 历史数据，参数：{params}")
        r = requests.get(url, params=params)
        r.raise_for_status()
        data_json = r.json()
        if not (data_json["data"] and data_json["data"]["klines"]):
            logging.info(f"未获取到 {symbol} 从 {start_date} 到 {end_date} 的历史数据，响应数据：{data_json}")
            return pd.DataFrame()
        temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
        temp_df.columns = [
            "日期",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
        temp_df.index = pd.to_datetime(temp_df["日期"])
        temp_df.reset_index(inplace=True, drop=True)

        numeric_cols = [
            "开盘", "收盘", "最高", "最低", "成交量",
            "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"
        ]
        temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        return temp_df
    except requests.RequestException as e:
        logging.error(f"请求出错: {e}，状态码：{r.status_code if 'r' in locals() else '未知'}，响应内容：{r.text if 'r' in locals() else '未知'}")
        return pd.DataFrame()
    except KeyError as e:
        logging.error(f"解析数据出错: {e}，响应数据：{data_json if 'data_json' in locals() else '未知'}")
        return pd.DataFrame()


def fund_etf_hist_min_em(
    symbol: str = "159707",
    start_date: str = "1979-09-01 09:32:00",
    end_date: str = "2222-01-01 09:32:00",
    period: str = "5",
    adjust: str = "",
) -> pd.DataFrame:
    code_id_dict = _fund_etf_code_id_map_em()
    if symbol not in code_id_dict:
        logging.error(f"未找到 {symbol} 的市场标识映射")
        return pd.DataFrame()
    adjust_map = {
        "": "0",
        "qfq": "1",
        "hfq": "2",
    }
    if period == "1":
        url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "ndays": "5",
            "iscr": "0",
            "secid": f"{code_id_dict[symbol]}.{symbol}",
            "_": "1623766962675",
        }
        try:
            logging.info(f"请求 {symbol} 1 分钟历史数据，参数：{params}")
            r = requests.get(url, params=params)
            r.raise_for_status()
            data_json = r.json()
            temp_df = pd.DataFrame(
                [item.split(",") for item in data_json["data"]["trends"]]
            )
            temp_df.columns = [
                "时间",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "成交量",
                "成交额",
                "最新价",
            ]
            temp_df.index = pd.to_datetime(temp_df["时间"])
            temp_df = temp_df[start_date:end_date]
            temp_df.reset_index(drop=True, inplace=True)
            numeric_cols = [
                "开盘", "收盘", "最高", "最低", "成交量",
                "成交额", "最新价"
            ]
            temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
            temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
            return temp_df
        except requests.RequestException as e:
            logging.error(f"请求出错: {e}，状态码：{r.status_code if 'r' in locals() else '未知'}，响应内容：{r.text if 'r' in locals() else '未知'}")
            return pd.DataFrame()
        except KeyError as e:
            logging.error(f"解析数据出错: {e}，响应数据：{data_json if 'data_json' in locals() else '未知'}")
            return pd.DataFrame()
    else:
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "klt": period,
            "fqt": adjust_map[adjust],
            "secid": f"{code_id_dict[symbol]}.{symbol}",
            "beg": "0",
            "end": "20500000",
            "_": "1630930917857",
        }
        try:
            logging.info(f"请求 {symbol} {period} 分钟历史数据，参数：{params}")
            r = requests.get(url, params=params)
            r.raise_for_status()
            data_json = r.json()
            # 检查 data 和 klines 是否存在
            if data_json.get("data") and data_json["data"].get("klines"):
                temp_df = pd.DataFrame(
                    [item.split(",") for item in data_json["data"]["klines"]]
                )
                temp_df.columns = [
                    "时间",
                    "开盘",
                    "收盘",
                    "最高",
                    "最低",
                    "成交量",
                    "成交额",
                    "振幅",
                    "涨跌幅",
                    "涨跌额",
                    "换手率",
                ]
                temp_df.index = pd.to_datetime(temp_df["时间"])
                temp_df = temp_df[start_date:end_date]
                temp_df.reset_index(drop=True, inplace=True)
                numeric_cols = [
                    "开盘", "收盘", "最高", "最低", "成交量",
                    "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"
                ]
                temp_df[numeric_cols] = temp_df[numeric_cols].apply(pd.to_numeric, errors="coerce")
                temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
                temp_df = temp_df[
                    [
                        "时间",
                        "开盘",
                        "收盘",
                        "最高",
                        "最低",
                        "涨跌幅",
                        "涨跌额",
                        "成交量",
                        "成交额",
                        "振幅",
                        "换手率",
                    ]
                ]
                return temp_df
            else:
                logging.info(f"未获取到 {symbol} 从 {start_date} 到 {end_date} 的 {period} 分钟历史数据，响应数据：{data_json}")
                return pd.DataFrame()
        except requests.RequestException as e:
            logging.error(f"请求出错: {e}，状态码：{r.status_code if 'r' in locals() else '未知'}，响应内容：{r.text if 'r' in locals() else '未知'}")
            return pd.DataFrame()
        except KeyError as e:
            logging.error(f"解析数据出错: {e}，响应数据：{data_json if 'data_json' in locals() else '未知'}")
            return pd.DataFrame()


if __name__ == "__main__":
    fund_etf_spot_em_df = fund_etf_spot_em()
    print(fund_etf_spot_em_df)

    fund_etf_hist_hfq_em_df = fund_etf_hist_em(
        symbol="513500",
        period="daily",
        start_date="20000101",
        end_date="",
        adjust="hfq",
    )
    print(fund_etf_hist_hfq_em_df)

    fund_etf_hist_qfq_em_df = fund_etf_hist_em(
        symbol="513500",
        period="daily",
        start_date="20000101",
        end_date="",
        adjust="qfq",
    )
    print(fund_etf_hist_qfq_em_df)

    fund_etf_hist_em_df = fund_etf_hist_em(
        symbol="513500",
        period="daily",
        start_date="20000101",
        end_date="20230201",
        adjust="",
    )
    print(fund_etf_hist_em_df)

    fund_etf_hist_min_em_df = fund_etf_hist_min_em(
        symbol="513500",
        period="5",
        adjust="hfq",
        start_date="2025-03-18 09:32:00",
        end_date="2025-03-19 14:40:00",
    )
    print(fund_etf_hist_min_em_df)
    