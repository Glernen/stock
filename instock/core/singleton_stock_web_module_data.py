#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import instock.core.tablestructure as tbs
from instock.lib.singleton_type import singleton_type
import instock.core.web_module_data as wmd

__author__ = 'myh '
__date__ = '2023/3/10 '


class stock_web_module_data(metaclass=singleton_type):
    def __init__(self):
        _data = {}
        self.data_list = [wmd.web_module_data(
            mode="query",
            type="综合选股",
            ico="fa fa-desktop",
            name=tbs.TABLE_CN_STOCK_SELECTION['cn'],
            table_name=tbs.TABLE_CN_STOCK_SELECTION['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_SELECTION['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_SELECTION['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_SELECTION['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="基本数据",
            ico="fa fa-book",
            name=tbs.kline_stock['cn'],
            table_name=tbs.kline_stock['name'],
            columns=tuple(tbs.kline_stock['columns']),
            column_names=tbs.get_field_cns(tbs.kline_stock['columns']),
            primary_key=[],
            is_realtime=True
        ), wmd.web_module_data(
            mode="query",
            type="基本数据",
            ico="fa fa-indent",
            name=tbs.kline_etf['cn'],
            table_name=tbs.kline_etf['name'],
            columns=tuple(tbs.kline_etf['columns']),
            column_names=tbs.get_field_cns(tbs.kline_etf['columns']),
            primary_key=[],
            is_realtime=False,
        ), wmd.web_module_data(
            mode="query",
            type="基本数据",
            ico="fa fa-indent",
            name=tbs.kline_index['cn'],
            table_name=tbs.kline_index['name'],
            columns=tuple(tbs.kline_index['columns']),
            column_names=tbs.get_field_cns(tbs.kline_index['columns']),
            primary_key=[],
            is_realtime=False,
        ),wmd.web_module_data(
            mode="query",
            type="基本数据",
            ico="fa fa-book",
            name=tbs.kline_industry['cn'],
            table_name=tbs.kline_industry['name'],
            columns=tuple(tbs.kline_industry['columns']),
            column_names=tbs.get_field_cns(tbs.kline_industry['columns']),
            primary_key=[],
            is_realtime=False,
        ), wmd.web_module_data(
            mode="query",
            type="基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_BONUS['cn'],
            table_name=tbs.TABLE_CN_STOCK_BONUS['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_BONUS['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_BONUS['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_BONUS['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_TOP['cn'],
            table_name=tbs.TABLE_CN_STOCK_TOP['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_TOP['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_TOP['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_TOP['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_BLOCKTRADE['cn'],
            table_name=tbs.TABLE_CN_STOCK_BLOCKTRADE['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_BLOCKTRADE['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['cn'],
            table_name=tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['columns']),
            primary_key=[],
            is_realtime=False,
            order_by=" `fund_amount` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="基本数据",
            ico="fa fa-book",
            name=tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['cn'],
            table_name=tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_FUND_FLOW_CONCEPT['columns']),
            primary_key=[],
            is_realtime=False,
            order_by=" `fund_amount` DESC"
        ),   wmd.web_module_data(
            mode="query",
            type="历史行情数据",
            ico="fa fa-indent",
            name=tbs.CN_STOCK_HIST_DAILY_DATA['cn'],
            table_name=tbs.CN_STOCK_HIST_DAILY_DATA['name'],
            columns=tuple(tbs.CN_STOCK_HIST_DAILY_DATA['columns']),
            column_names=tbs.get_field_cns(tbs.CN_STOCK_HIST_DAILY_DATA['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.CN_STOCK_HIST_DAILY_DATA['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="历史行情数据",
            ico="fa fa-indent",
            name=tbs.CN_ETF_HIST_DAILY_DATA['cn'],
            table_name=tbs.CN_ETF_HIST_DAILY_DATA['name'],
            columns=tuple(tbs.CN_ETF_HIST_DAILY_DATA['columns']),
            column_names=tbs.get_field_cns(tbs.CN_ETF_HIST_DAILY_DATA['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.CN_ETF_HIST_DAILY_DATA['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="历史行情数据",
            ico="fa fa-indent",
            name=tbs.CN_INDEX_HIST_DAILY_DATA['cn'],
            table_name=tbs.CN_INDEX_HIST_DAILY_DATA['name'],
            columns=tuple(tbs.CN_INDEX_HIST_DAILY_DATA['columns']),
            column_names=tbs.get_field_cns(tbs.CN_INDEX_HIST_DAILY_DATA['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.CN_INDEX_HIST_DAILY_DATA['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="股票指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_STOCK_INDICATORS['cn'],
            table_name=tbs.TABLE_CN_STOCK_INDICATORS['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_INDICATORS['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_INDICATORS['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_INDICATORS['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="股票指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_STOCK_INDICATORS_BUY['cn'],
            table_name=tbs.TABLE_CN_STOCK_INDICATORS_BUY['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_INDICATORS_BUY['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"

        ),wmd.web_module_data(
            mode="query",
            type="股票指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_STRATEGY_STOCK_BUY_OPTIMIZATION['cn'],
            table_name=tbs.TABLE_STRATEGY_STOCK_BUY_OPTIMIZATION['name'],
            columns=tuple(tbs.TABLE_STRATEGY_STOCK_BUY_OPTIMIZATION['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_STRATEGY_STOCK_BUY_OPTIMIZATION['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_INDICATORS_BUY['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"

        ), wmd.web_module_data(
            mode="query",
            type="股票指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_STOCK_INDICATORS_SELL['cn'],
            table_name=tbs.TABLE_CN_STOCK_INDICATORS_SELL['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_INDICATORS_SELL['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_INDICATORS_SELL['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_STOCK_INDICATORS_SELL['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ),wmd.web_module_data(
            mode="query",
            type="ETF指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_ETF_INDICATORS['cn'],
            table_name=tbs.TABLE_CN_ETF_INDICATORS['name'],
            columns=tuple(tbs.TABLE_CN_ETF_INDICATORS['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_ETF_INDICATORS['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_ETF_INDICATORS['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="ETF指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_ETF_INDICATORS_BUY['cn'],
            table_name=tbs.TABLE_CN_ETF_INDICATORS_BUY['name'],
            columns=tuple(tbs.TABLE_CN_ETF_INDICATORS_BUY['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_ETF_INDICATORS_BUY['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_ETF_INDICATORS_BUY['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="ETF指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_ETF_INDICATORS_SELL['cn'],
            table_name=tbs.TABLE_CN_ETF_INDICATORS_SELL['name'],
            columns=tuple(tbs.TABLE_CN_ETF_INDICATORS_SELL['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_ETF_INDICATORS_SELL['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_ETF_INDICATORS_SELL['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="指数指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_INDEX_INDICATORS['cn'],
            table_name=tbs.TABLE_CN_INDEX_INDICATORS['name'],
            columns=tuple(tbs.TABLE_CN_INDEX_INDICATORS['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_INDEX_INDICATORS['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_INDEX_INDICATORS['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="指数指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_INDEX_INDICATORS_BUY['cn'],
            table_name=tbs.TABLE_CN_INDEX_INDICATORS_BUY['name'],
            columns=tuple(tbs.TABLE_CN_INDEX_INDICATORS_BUY['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_INDEX_INDICATORS_BUY['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_INDEX_INDICATORS_BUY['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ), wmd.web_module_data(
            mode="query",
            type="指数指标数据",
            ico="fa fa-indent",
            name=tbs.TABLE_CN_INDEX_INDICATORS_SELL['cn'],
            table_name=tbs.TABLE_CN_INDEX_INDICATORS_SELL['name'],
            columns=tuple(tbs.TABLE_CN_INDEX_INDICATORS_SELL['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_INDEX_INDICATORS_SELL['columns']),
            primary_key=[],
            is_realtime=False,
            # order_columns=f"(SELECT `datetime` FROM `{tbs.TABLE_CN_STOCK_ATTENTION['name']}` WHERE `code`=`{tbs.TABLE_CN_INDEX_INDICATORS_SELL['name']}`.`code`) AS `cdatetime`",
            # order_by=" `cdatetime` DESC"
        ),
            wmd.web_module_data(
            mode="query",
            type="股票策略数据",
            ico="fa fa-check-square-o",
            name=tbs.TABLE_CN_STOCK_SPOT_BUY['cn'],
            table_name=tbs.TABLE_CN_STOCK_SPOT_BUY['name'],
            columns=tuple(tbs.TABLE_CN_STOCK_SPOT_BUY['columns']),
            column_names=tbs.get_field_cns(tbs.TABLE_CN_STOCK_SPOT_BUY['columns']),
            primary_key=[],
            is_realtime=False

        )]

        for table in tbs.TABLE_CN_STOCK_STRATEGIES:
            self.data_list.append(
                wmd.web_module_data(
                    mode="query",
                    type="股票策略数据",
                    ico="fa fa-check-square-o",
                    name=table['cn'],
                    table_name=table['name'],
                    columns=tuple(table['columns']),
                    column_names=tbs.get_field_cns(table['columns']),
                    primary_key=[],
                    is_realtime=False
                )
            )
        for tmp in self.data_list:
            _data[tmp.table_name] = tmp
        self.data = _data

    def get_data_list(self):
        return self.data_list

    def get_data(self, name):
        return self.data[name]
