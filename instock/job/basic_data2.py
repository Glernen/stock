import requests
import pandas as pd
import pandas_market_calendars as mcal
from tqdm import tqdm  # 进度条工具，可选


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
    """获取所有股票数据"""
    all_stocks = []
    count = 200  # 每页固定200条
    first_page_data, total = fetch_tencent_stock_data(offset=0, count=count)
    all_stocks.extend(first_page_data)

    if total > count:
        total_pages = (total + count - 1) // count  # 计算总页数
        for page in tqdm(range(1, total_pages), desc="获取股票数据进度"):
            offset = page * count
            page_data, _ = fetch_tencent_stock_data(offset=offset, count=count)
            all_stocks.extend(page_data)

    # 创建DataFrame
    temp_df = pd.DataFrame(all_stocks)

    # 获取上证交易所日历
    sh_cal = mcal.get_calendar('SSE')
    latest_trade_date = sh_cal.schedule(start_date='2020-01-01', end_date=pd.Timestamp.today()).index[-1].strftime(
        "%Y-%m-%d")
    temp_df.loc[:, "date"] = latest_trade_date
    temp_df.loc[:, "date_int"] = temp_df["date"].astype(str).str.replace('-', '')

    # 中文字段名映射
    column_mapping = {
        'date': '日期',
        'code': '股票代码',
        'hsl': '换手率',
        'lb': '量比',
        'ltsz': '流通市值', # (亿元)
        'name': '股票名称',
        'pe_ttm': '动态市盈率',
        'pn': '市净率',
        'speed': '涨速', # (每分钟)
        'state': '状态',
        'stock_type': '股票类型',
        'turnover': '成交额', # (万元)
        'volume': '成交量', # (手)
        'zd': '涨跌额',
        'zdf': '涨跌幅',
        'zdf_d10': '10日涨跌幅',
        'zdf_d20': '20日涨跌幅',
        'zdf_d5': '5日涨跌幅',
        'zdf_d60': '60日涨跌幅',
        'zdf_w52': '52周涨跌幅',
        'zdf_y': '年初至今涨跌幅',
        'zf': '振幅',
        'zljlr': '主力净流入', # (万元)
        'zllc': '主力流出', # (万元)
        'zllc_d5': '5日主力流出', # (万元)
        'zllr': '主力流入', # (万元)
        'zllr_d5': '5日主力流入', # (万元)
        'zsz': '总市值',
        'zxj': '最新价'
    }

    # 重命名列
    temp_df = temp_df.rename(columns=column_mapping)

    return temp_df


# 使用示例
if __name__ == "__main__":
    df = get_tencent_all_stocks()
    print(f"共获取 {len(df)} 条股票数据")
    print(df.head())