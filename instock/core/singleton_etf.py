#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import instock.core.stockfetch as stf
import instock.core.tablestructure as tbs
import instock.lib.trade_time as trd
from instock.lib.singleton_type import singleton_type

__author__ = 'hqm'
__date__ = '2025/3/23 '

# 配置日志记录
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# 读取当天基金数据
class etf_data(metaclass=singleton_type):
    def __init__(self, date):
        try:
            self.data = stf.fetch_etfs(date)
            logging.info(f"成功获取 {date} 的基金数据")
            print(f"成功获取 {date} 的基金数据")
        except Exception as e:
            logging.error(f"singleton_etf.etf_data处理异常：{e}")

    def get_data(self):
        return self.data

# 读取基金历史数据
class etf_hist_data(metaclass=singleton_type):
    def __init__(self, date=None, etfs=None, workers=16):
        if etfs is None:
            try:
                _subset = etf_data(date).get_data()[list(tbs.TABLE_CN_ETF_FOREIGN_KEY['columns'])]
                logging.info(f"singleton_etf.etf_hist_data._subset：{_subset}")
                print(f"singleton_etf.etf_hist_data._subset：{_subset}")
                etfs = [tuple(x) for x in _subset.values]
            except Exception as e:
                logging.error(f"获取 _subset 数据时出错：{e}")
                self.data = None
                return
        if etfs is None:
            self.data = None
            return
        try:
            # print(f"{etfs}")
            date_start, is_cache = trd.get_trade_hist_interval(etfs[0][0])
            date_end = date.strftime("%Y%m%d")
            logging.info(f"trd.get_trade_hist_interval 返回值: date_start={date_start}, is_cache={is_cache}")
            print(f"trd.get_trade_hist_interval 返回值: date_start={date_start}, is_cache={is_cache}")
        except Exception as e:
            logging.error(f"获取交易历史时间间隔时出错：{e}")
            print(f"获取交易历史时间间隔时出错：{e}")
            self.data = None
            return
        _data = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_etf = {executor.submit(stf.fetch_etf_hist, etf, date_start , date_end ,is_cache): etf for etf in etfs}
                # print(f"future_to_etf：{future_to_etf} ")
                for future in concurrent.futures.as_completed(future_to_etf):
                    etf = future_to_etf[future]
                    try:
                        #logging.info(f"开始获取 ETF {etf} 的历史数据")
                        print(f"开始获取 ETF {etf} 的历史数据")
                        __data = future.result()
                        if __data is None:
                            #logging.warning(f"ETF {etf} 返回的数据为 None")
                            print(f"ETF {etf} 返回的数据为 None")
                        else:
                            #logging.info(f"成功获取 ETF {etf} 的数据")
                            print(f"成功获取 ETF {etf} 的数据")
                            _data[etf] = __data
                    except Exception as e:
                        logging.error(f"获取 ETF {etf} 数据时出错: {e}")
                        print(f"获取 ETF {etf} 数据时出错: {e}")
        except Exception as e:
            logging.error(f"singleton_etf.etf_hist_data处理异常：{e}")
            print(f"singleton_etf.etf_hist_data处理异常：{e}")
        if not _data:
            self.data = None
        else:
            self.data = _data

    def get_data(self):
        return self.data
    
