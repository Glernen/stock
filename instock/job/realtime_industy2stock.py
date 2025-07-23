import requests
import pandas as pd
import datetime
import time
from tqdm import tqdm  # 进度条工具
import json


# ================ 行业数据获取函数 ================
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

    # 添加请求间隔
    request_interval = 0.5  # 每次请求间隔0.5秒

    # 处理分页
    if total > count:
        total_pages = (total + count - 1) // count
        for page in tqdm(range(1, total_pages), desc=f"获取行业数据({board_type})进度"):
            offset = page * count
            page_data, _ = fetch_tencent_industry_data(board_type=board_type, offset=offset, count=count)
            all_industries.extend(page_data)

            # 添加请求间隔
            time.sleep(request_interval)

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

    # 中文字段名映射
    column_mapping = {
        'date': '日期',
        'code': '行业代码',
        'name': '行业名称',
        'id': '行业标识',  # 新增行业标识字段
        'stock_type': '行业类型',
        'hsl': '换手率(%)',
        'lb': '量比',
        'ltsz': '流通市值(亿元)',
        'speed': '涨速(%)',
        'turnover': '成交额(万元)',
        'volume': '成交量(手)',
        'zd': '涨跌额',
        'zdf': '涨跌幅(%)',
        'zdf_d5': '5日涨跌幅(%)',
        'zdf_d20': '20日涨跌幅(%)',
        'zdf_d60': '60日涨跌幅(%)',
        'zdf_w52': '52周涨跌幅(%)',
        'zdf_y': '年初至今涨跌幅(%)',
        'zljlr': '主力净流入(万元)',
        'zllc': '主力流出(万元)',
        'zllr': '主力流入(万元)',
        'zsz': '总市值(亿元)',
        'zxj': '收盘价',
        'lzg_code': '领涨股代码',
        'lzg_name': '领涨股名称',
        'lzg_zd': '领涨股涨跌额',
        'lzg_zdf': '领涨股涨跌幅(%)',
        'lzg_zxj': '领涨股最新价'
    }

    # 重命名列并选择需要的字段
    temp_df = temp_df.rename(columns=column_mapping)
    valid_columns = [col for col in column_mapping.values() if col in temp_df.columns]

    # 数值类型转换
    numeric_cols = ['换手率(%)', '量比', '流通市值(亿元)', '涨速(%)', '成交额(万元)', '成交量(手)',
                    '涨跌额', '涨跌幅(%)', '5日涨跌幅(%)', '20日涨跌幅(%)', '60日涨跌幅(%)',
                    '52周涨跌幅(%)', '年初至今涨跌幅(%)', '主力净流入(万元)', '主力流出(万元)',
                    '主力流入(万元)', '总市值(亿元)', '收盘价', '领涨股涨跌额', '领涨股涨跌幅(%)', '领涨股最新价']

    for col in numeric_cols:
        if col in temp_df.columns:
            temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')

    print(f"成功获取 {len(temp_df)} 条行业数据({board_type})，耗时 {time.time() - start_time:.2f}秒")
    return temp_df[valid_columns]


# ================ 行业股票数据获取函数 ================
def get_industry_stocks(industry_id, industry_name="", sort_type=1, max_count=500):
    """
    获取指定行业下的关联股票数据

    参数:
    industry_id -- 行业标识 (如: 'pt01801120')
    industry_name -- 行业名称 (可选)
    sort_type -- 排序方式 (1:涨跌幅排序)
    max_count -- 单次请求最大股票数量
    """
    # 从行业标识中提取纯数字代码 (移除'pt'前缀)
    plate_code = industry_id[2:] if industry_id.startswith('pt') else industry_id
    print(f"开始获取行业股票数据: {industry_name}({plate_code})...")
    start_time = time.time()

    url = "https://bisheng.tenpay.com/fcgi-bin/xg_plate_stocks.fcgi"
    all_stocks = []
    offset = 0
    total = 1  # 初始值

    # 分页获取所有股票数据
    with tqdm(desc="获取股票进度") as pbar:
        while offset < total:
            params = {
                "exchange": "12",  # 固定交易所代码
                "plate_code": plate_code,
                "sort_type": sort_type,
                "source": "zxg",
                "stocks_type": "3",
                "time": int(time.time() * 1000),  # 当前时间戳(毫秒)
                "user_type": "4",
                "offset": offset,
                "count": max_count
            }

            try:
                response = requests.get(url, params=params, timeout=15)
                if response.status_code == 200:
                    data = response.json()

                    # 检查返回状态
                    if data.get('retcode') == '0':
                        stocks_data = data.get('stocks', {})
                        stocks_list = stocks_data.get('stocks_list', [])
                        total = stocks_data.get('total', 0)

                        # 添加行业基本信息
                        for stock in stocks_list:
                            stock['industry_id'] = industry_id
                            stock['industry_code'] = plate_code
                            stock['industry_name'] = industry_name

                        all_stocks.extend(stocks_list)
                        offset += len(stocks_list)
                        pbar.update(len(stocks_list))
                        pbar.set_description(f"已获取 {offset}/{total} 只股票")

                        # 判断是否获取完成
                        if offset >= total:
                            break
                    else:
                        print(f"接口返回错误: {data.get('retmsg', '未知错误')}")
                        break
                else:
                    print(f"请求失败，状态码: {response.status_code}")
                    break
            except Exception as e:
                print(f"请求异常: {e}")
                break

            # 请求间隔防止被封
            time.sleep(0.3)

    # 创建DataFrame
    if not all_stocks:
        print("未获取到股票数据")
        return pd.DataFrame()

    df = pd.DataFrame(all_stocks)

    # 添加当前日期
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    df['date'] = today

    # 字段映射 (英文->中文)
    column_mapping = {
        'date': '日期',
        'industry_id': '行业标识',
        'industry_code': '行业代码',
        'industry_name': '行业名称',
        'stock_code': '股票代码',
        'stock_name': '股票名称',
        'price': '最新价',
        'change_percent': '涨跌幅(%)',
        'price_change': '涨跌额',
        'market_cap': '总市值(亿元)',
        'liutong_cap': '流通市值(亿元)',
        'turnover_ratio': '换手率(%)',
        'turnover_money': '成交额(万元)',
        'turnover_amount': '成交量(手)',
        'amplitude': '振幅(%)',
        'quantity_ratio': '量比',
        'main_net_inflow': '主力净流入(元)',
        'pe_ttm': '市盈率(TTM)',
        'weight': '权重',
        'tags': '标签',
        'stock_type': '股票类型'
    }

    # 重命名列
    df.rename(columns=column_mapping, inplace=True)

    # 处理tags字段(列表转字符串)
    if '标签' in df.columns:
        df['标签'] = df['标签'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)

    # 数值类型转换
    numeric_cols = ['最新价', '涨跌幅(%)', '涨跌额', '总市值(亿元)', '流通市值(亿元)', '换手率(%)',
                    '成交额(万元)', '成交量(手)', '振幅(%)', '量比', '主力净流入(元)', '市盈率(TTM)', '权重']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 调整单位: 主力净流入(元)->(万元)
    if '主力净流入(元)' in df.columns:
        df['主力净流入(万元)'] = df['主力净流入(元)'] / 10000
        df.drop('主力净流入(元)', axis=1, inplace=True)

    print(f"成功获取 {len(df)} 只股票，耗时 {time.time() - start_time:.2f}秒")
    return df


# ================ 使用示例 ================
if __name__ == "__main__":
    # 示例1: 获取申万一级行业数据
    df_level1 = get_tencent_all_industries(board_type='hy')
    if not df_level1.empty:
        print("\n一级行业示例:")
        print(df_level1[['行业标识', '行业代码', '行业名称']].head())

        # 示例2: 获取第一个行业的股票数据
        sample_industry = df_level1.iloc[0]
        industry_stocks = get_industry_stocks(
            industry_id=sample_industry['行业标识'],
            industry_name=sample_industry['行业名称']
        )

        if not industry_stocks.empty:
            print("\n行业股票示例:")
            print(industry_stocks[['股票代码', '股票名称', '最新价', '涨跌幅(%)', '行业名称']].head())
            # industry_stocks.to_csv(f"{sample_industry['行业名称']}_stocks.csv",
            #                        index=False, encoding='utf_8_sig')

    # # 保存行业数据
    # df_level1.to_csv('sw_level1_industries.csv', index=False, encoding='utf_8_sig')
    # df_level2 = get_tencent_all_industries(board_type='hy2')
    # df_level2.to_csv('sw_level2_industries.csv', index=False, encoding='utf_8_sig')