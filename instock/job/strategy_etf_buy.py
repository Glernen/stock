#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

# 在项目运行时，临时将项目路径添加到环境变量
import os.path
import sys
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import argparse
import logging
import concurrent.futures
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Table, text
from tqdm import tqdm
import mysql.connector
from sqlalchemy import create_engine, DATE, FLOAT, VARCHAR, INT
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset 
import instock.lib.database as mdb

__author__ = 'hqm'
__date__ = '2025/4/15'

# 在文件顶部定义回测字段配置（替换tbs模块的引用）
RATE_FIELDS_COUNT = 100
BACKTEST_DATA = {
    'columns': {
        f'rate_{i}': {'type': 'FLOAT', 'cn': f'{i}日收益率'} 
        for i in range(1, RATE_FIELDS_COUNT + 1)
    }
}

# sql字段名：up_sentiment,down_sentiment,code,name,date_int,kdjj,jingliuru_cn,close,turnover,industry,industry_kdjj,industry_kdjj_day1,industry_kdj,industry_wr,industry_cci,industry_sentiment
# 在文件顶部新增策略配置
STRATEGY_CONFIG = {
    'strategy_a': {
        'conditions': """
            AND 三日指标.kdjk <= 45 -- k处于超卖区域
            AND 三日指标.kdjd <= 45  -- d处于超卖区域
            AND 三日指标.kdjj <= 0   -- j处于超卖区域
            AND 三日指标.cci < - 100 AND 三日指标.cci > 三日指标.cci_day1 
            AND 三日指标.rsi_6 <= 30
            --                 AND rsi_12 <= 45 
            AND ABS(三日指标.wr_6) >= 90
            AND ABS(三日指标.wr_10) >= 90
        """
    },
    'strategy_b': {
        'conditions': """
            -- 当前K值 > 前1日K值
            AND 三日指标.`kdjK` > 三日指标.kdjk_day1
            AND 三日指标.kdjk_day1 > 三日指标.kdjk_day2
            -- 当前D值 > 前1日D值
            AND 三日指标.`kdjd` > 三日指标.kdjd_day1
            AND 三日指标.kdjd_day1 > 三日指标.kdjd_day2
            -- 前1日D值 > 前2日D值（形成连续上涨趋势）
            AND 三日指标.wr_6 > 三日指标.wr_6_day1
            AND 三日指标.wr_6_day1 > 三日指标.wr_6_day2
            AND 三日指标.cci > 三日指标.cci_day1
            AND 三日指标.cci_day1 > 三日指标.cci_day2
            AND 三日指标.`kdjk` <= 45
            -- 筛选条件2：D值小于等于30（超卖区域）
            AND 三日指标.`kdjd` <= 45
            -- 注意：这里同时满足K值和D值都在超卖区域 
            AND 三日指标.`kdjj` <= 50
        """
    }
}


def validate_date(date_str):
    """验证日期格式是否为YYYYMMDD"""
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return int(date_str)
    except ValueError:
        raise argparse.ArgumentTypeError(f"无效的日期格式: {date_str}，应使用YYYYMMDD格式")

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='股票分析工具')
    parser.add_argument('dates', nargs='*', type=validate_date,
                       help='分析日期(支持1-2个YYYYMMDD格式日期)')
    return parser.parse_args()

def get_default_dates():
    """获取默认日期（最近3个交易日）"""
    # 这里需要实现获取真实交易日逻辑，示例使用静态数据
    today = datetime.now().strftime("%Y%m%d")
    return [int(today)]

def build_date_condition(start_date, end_date=None):
    """构建SQL日期条件"""
    if end_date is None or start_date == end_date:
        return f"三日指标.date_int = {start_date}"
    return f"三日指标.date_int BETWEEN {start_date} AND {end_date}"

def guess_buy(strategy_name, start_date_int, end_date_int=None):
    """核心分析逻辑"""
    try:
        # 获取策略条件
        strategy_cond = STRATEGY_CONFIG.get(strategy_name, {}).get('conditions')
        if not strategy_cond:
            raise ValueError(f"未知策略: {strategy_name}")

        # 确保表存在
        create_optimized_table()
        
        # 构建动态日期条件
        date_condition = build_date_condition(start_date_int, end_date_int)

        sql = f"""
        SELECT
            三日指标.code ,
            三日指标.code_int ,
            三日指标.name ,
            三日指标.date ,
            三日指标.date_int ,
            三日指标.kdjj,
            三日指标.close ,
            三日指标.turnover 

        FROM
            etf_3day_indicators 三日指标
            LEFT JOIN cn_etf_info 基础信息 ON 基础信息.code_int  = 三日指标.code_int
        WHERE 
            {date_condition}
            {strategy_cond}  # 插入策略特定条件
            AND 基础信息.T0 = 1
        ORDER BY
            三日指标.date_int;
        """
        
        with mdb.engine().connect() as conn:
            data = pd.read_sql(text(sql), conn)
        
        if data.empty:
            logging.info("无符合条件的数据")
            return

        # 新增列合并逻辑（关键修改点）
        _columns_backtest = tuple(BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])

        # 添加策略标识
        data['strategy'] = strategy_name

        # 移除调试用的错误print语句，替换为日志输出
        logging.debug(f"处理日期范围: {start_date_int} 至 {end_date_int or start_date_int}")

        # print(f'{data}')

        # 插入数据库逻辑（使用优化后的插入方法）
        optimized_data_insert(data)
        
    except Exception as e:
        logging.error(f"处理异常：{e}")
        raise

# sql字段名：up_sentiment,down_sentiment,code,name,date_int,kdjj,jingliuru_cn,close,turnover,industry,industry_kdjj,industry_kdjj_day1,industry_kdj,industry_wr,industry_cci,industry_sentiment
# 修改表结构（新增字段）
def create_optimized_table():
    table_name = "cn_etf_indicators_buy"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `date` DATE NOT NULL,
        `date_int` INT NOT NULL COMMENT '日期YYYYMMDD格式',
        `code` VARCHAR(6) NOT NULL COMMENT '股票代码',
        `code_int` INT NOT NULL COMMENT '整数格式股票代码',
        `name` VARCHAR(20) COMMENT '股票名称',
        `strategy` VARCHAR(50) NOT NULL COMMENT '策略标识',
        `close` FLOAT COMMENT '收盘价',
        `kdjj` FLOAT COMMENT '股票当天J值',
        `turnover` FLOAT COMMENT '换手率',
        /* 新增回测字段 */
        {' ,'.join([f'`{col}` FLOAT' for col in BACKTEST_DATA['columns']])},
        UNIQUE KEY `idx_unique` (`date_int`,`code_int`,`strategy`),
        KEY `idx_code` (`code_int`),
        KEY `idx_date` (`date_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='选股结果表';
    """
    
    # 使用新的连接检查并创建表
    with mdb.engine().connect() as conn:
        try:
            # 检查表是否存在
            if not conn.dialect.has_table(conn, table_name):
                conn.execute(text(create_table_sql))
                conn.commit()
                logging.info(f"表 {table_name} 创建成功")
            else:
                # 检查并添加缺少的列（可选）
                pass
        except Exception as e:
            logging.error(f"创建表时发生错误: {e}")
            raise


def optimized_data_insert(data):
    table_name = "cn_etf_indicators_buy"
    try:
        with mdb.engine().connect() as conn:
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=conn.engine)
            
            # 调试：打印表结构
            # print("\n[DEBUG] 表结构字段：")
            # for col in table.columns:
            #     print(f"{col.name}: {col.type}")
                
            unique_keys = {'date_int', 'code_int', 'strategy'}
            update_columns = [
                col.name for col in table.columns 
                if col.name not in unique_keys 
                and col.name != 'id'
            ]
            # print(f"\n[DEBUG] 将更新的字段：{update_columns}")

            # 验证数据字段匹配
            missing_columns = set(data.columns) - {col.name for col in table.columns}
            if missing_columns:
                raise ValueError(f"数据包含表中不存在的列: {missing_columns}")

        def upsert_data(table, conn, keys, data_iter):
            from sqlalchemy.dialects.mysql import insert
            
            data_rows = [dict(zip(keys, row)) for row in data_iter]
            if not data_rows:
                return
                
            stmt = insert(table.table).values(data_rows)
            update_dict = {col: stmt.inserted[col] for col in update_columns}
            
            # 调试：打印生成的SQL
            compiled_stmt = stmt.on_duplicate_key_update(**update_dict).compile(
                compile_kwargs={"literal_binds": True}
            )
            # print(f"\n[DEBUG] 执行SQL:\n{compiled_stmt}")
            
            result = conn.execute(stmt.on_duplicate_key_update(**update_dict))
            # print(f"[DEBUG] 影响行数: {result.rowcount}")

        data.to_sql(
            name=table_name,
            con=mdb.engine(),
            if_exists='append',
            index=False,
            chunksize=500,  # 减小chunksize便于调试
            method=upsert_data
        )
        print("数据插入/更新成功")
        logging.info("数据插入/更新成功")
        
    except Exception as e:
        logging.error(f"操作失败，详细错误：{str(e)}", exc_info=True)
        raise




# 优化后的数据插入函数
# def optimized_data_insert(data):
#     table_name = "cn_etf_indicators_buy"
#     try:
#         # 自定义插入方法，使用INSERT IGNORE避免重复
#         def insert_ignore(table, conn, keys, data_iter):
#             from sqlalchemy.dialects.mysql import insert
#             data_rows = [dict(zip(keys, row)) for row in data_iter]
#             if not data_rows:
#                 return
#             stmt = insert(table.table).values(data_rows).prefix_with('IGNORE')
#             conn.execute(stmt)
        
#         # 使用自定义方法插入数据
#         data.to_sql(
#             name=table_name,
#             con=mdb.engine(),
#             if_exists='append',
#             index=False,
#             chunksize=500,
#             method=insert_ignore
#         )
#         logging.info("数据插入完成，重复记录已自动忽略")
#     except Exception as e:
#         logging.error(f"数据插入失败: {e}")
#         raise









CN_ETF_INDICATORS_BUY = {
    'name': 'cn_etf_indicators_buy',
    'columns': {
        'date': {'type': DATE},
        'date_int': {'type': INT},
        'code': {'type': VARCHAR(6)},
        'code_int': {'type': INT},
        'name': {'type': VARCHAR(20)},
        'strategy': {'type':VARCHAR(50) },
        'close': {'type':FLOAT },
        'kdjj': {'type':FLOAT },
        'turnover': {'type':FLOAT },
        **{f'rate_{i}': {'type': FLOAT} for i in range(1, RATE_FIELDS_COUNT+1)}
    }
}



CN_ETF_INDICATORS_SELL = {
    'name': 'cn_etf_indicators_sell',
    'columns': {
        'date': {'type': DATE},
        'date_int': {'type': INT},
        'code': {'type': VARCHAR(6)},
        'code_int': {'type': INT},
        'name': {'type': VARCHAR(20)},
        'strategy': {'type':VARCHAR(50) },
        'close': {'type':FLOAT },
        'kdjj': {'type':FLOAT },
        'turnover': {'type':FLOAT },
        **{f'rate_{i}': {'type': FLOAT} for i in range(1, RATE_FIELDS_COUNT+1)}
    }
}

# 回测字段配置
backtest_columns = [
    'date_int', 'code_int' , 'code', 'date', 'name', 
    'strategy', 'close', 'kdjj', 'turnover'

] + [f'rate_{i}' for i in range(1, 101)]

# 数据库配置
db_config = {
    'host': db_host,
    'user': db_user,
    'password': db_password,
    'database': db_database,
    'charset': 'utf8mb4'
}

class StockHistData:
    def __init__(self):
        self.engine = create_engine(
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )

    def get_recent_data(self, days=60):
        """获取最近N天的股票历史数据"""
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        query = f"""
            SELECT * FROM cn_etf_hist_daily 
            WHERE date_int BETWEEN {start_date} AND {end_date} 
            ORDER BY date_int ASC  
        """
        try:
            df = pd.read_sql_query(query, self.engine)
            # df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
            return df.set_index(['date_int', 'code_int'])
        except Exception as e:
            logging.error(f"数据库查询失败: {str(e)}")
            return None

def prepare():
    # tables = [CN_ETF_INDICATORS_BUY, CN_ETF_INDICATORS_SELL]
    tables = [CN_ETF_INDICATORS_BUY]
    
    # 获取历史数据
    hist_data = StockHistData().get_recent_data()
    if hist_data is None:
        return

    # print(f'{hist_data}')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for table in tables:
            executor.submit(process_table, table, hist_data)

def process_table(table, hist_data):
    table_name = table['name']
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT date, date_int, code , code_int, name, strategy, close, kdjj, turnover
            FROM {table_name}
            WHERE rate_60 IS NULL
        """)
        stocks = cursor.fetchall()
        # print(f"需处理股票: {stocks}")
        print(f"需处理股票数量: {len(stocks)}")

        results = {}
        for idx, stock in enumerate(stocks, 1):
            try:
                date_int = stock[1]
                code_int = stock[3]
                key = (date_int, code_int)
                
                # 从预加载数据中筛选当前股票的数据
                stock_data = hist_data.xs(code_int, level='code_int', drop_level=False)
                # print(f"\n股票{code_int}总数据量：{len(stock_data)}条")
                
                # 找到当前日期在时间序列中的位置
                date_index = stock_data.index.get_loc(key)
                # print(f"当前记录在时间序列中的位置：{date_index}")
                
                # 获取从当前日期开始往后100天的数据
                needed_data = stock_data.iloc[date_index:date_index+100]
                # print(f"可用于计算的数据量：{len(needed_data)}")
                
                rates = calculate_rates(stock, needed_data)
                results[key] = rates

            except Exception as e:
                print(f"处理{code_int}时发生异常: {str(e)}")
                continue

        # 更新数据库
        if results:
            print(f"准备更新{len(results)}条记录")
            update_data = []
            for (date_int, code_int), rates in results.items():
                # 关键修改：从results遍历中获取对应股票数据
                current_stock = next((s for s in stocks if s[1] == date_int and s[3] == code_int), None)
                if not current_stock:
                    print(f"未找到匹配的股票数据 date_int={date_int} code_int={code_int}")
                    continue
                
                # 添加详细日志输出
                # print(f"\n正在构建更新数据：")
                # print(f"日期: {date_int} 代码: {code_int}")
                # print(f"原始数据: {current_stock}")
                # print(f"收益率数据长度: {len(rates)} 前3项: {rates[:3]}")

                # 关键修改1：转换日期格式和数值类型
                formatted_date = current_stock[0].strftime('%Y-%m-%d')  # 转换datetime.date为字符串
                formatted_rates = [float(rate) if rate is not None else None for rate in rates]  # 转换numpy类型

                update_row = [
                    current_stock[2],  # code
                    formatted_date,  #date
                    current_stock[4],  # name  
                    current_stock[5] if len(current_stock) > 5 else '',  # strategy
                    float(current_stock[6]) if len(current_stock) > 6 and current_stock[6] is not None else 0.0,  # close
                    float(current_stock[7]) if len(current_stock) > 7 and current_stock[7] is not None else 0.0,  # kdjj
                    float(current_stock[8]) if len(current_stock) > 8 and current_stock[8] is not None else 0.0,  # turnover
                    *formatted_rates,   # rate_1~rate_100
                ] + [
                    date_int,  # WHERE条件
                    code_int
                ]

                print(f"参数数量验证: {len(update_row)} 应等于: {len(backtest_columns[2:])+2}")
                print(f"示例数据：{update_row[:8]}...{update_row[-3:]}")
                update_data.append(tuple(update_row))

            # 添加SQL调试信息
            print(f"\n生成的UPDATE语句结构：")
            set_clause = ', '.join([f'{col}=%s' for col in backtest_columns[2:]])
            # print(f"SET字段数：{set_clause}")
            print(f"SET子句字段数：{len(backtest_columns[2:])} 实际：{set_clause.count('%s')}")
            print(f"WHERE条件参数数：2")
            print(f"总占位符数：{set_clause.count('%s') + 2}")
            
            update_query = f"""
                UPDATE {table_name}
                SET {set_clause}
                WHERE date_int=%s AND code_int=%s
            """
            print(f"update_query{update_query}")
            try:
                print(f"开始执行批量更新，记录数：{len(update_data)}")
                cursor.executemany(update_query, update_data)
                conn.commit()
                print(f"成功更新{len(update_data)}条记录")
            except Error as e:
                print(f"更新失败: {str(e)}")
                # 打印第一条失败记录的参数
                if update_data:
                    print(f"示例参数：{update_data[0]}")
                    print(f"参数数量：{len(update_data[0])}")


    except Error as e:
        print(f"数据库操作失败: {str(e)}")
        if e.errno == 1213:  # Deadlock处理
            conn.rollback()
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def calculate_rates(stock, stock_data):
    """计算收益率（处理单行数据）"""
    try:
        # 添加空数据检查
        if len(stock_data) == 0:
            print("空数据输入")
            return [None]*100
            
        # 修正索引访问方式
        # print(f"\n当前基准数据：")
        # print(f"日期：{stock_data.index[0][0]} 收盘价：{stock_data.iloc[0]['close']}")
        
        base_close = stock_data.iloc[0]['close']
        rates = []
        
        # 添加边界检查
        max_days = min(len(stock_data)-1, 100)  # 确保最多取100天
        
        for i in range(1, max_days+1):
            row = stock_data.iloc[i]
            pct = round((row['close'] - base_close)/base_close*100, 2)
            rates.append(pct)
            # print(f"第{i}天 {row.name[0]} 收盘价：{row['close']} 收益率：{pct}%")
            
        # 补足剩余天数
        rates += [None]*(100 - len(rates))
        return rates[:100]
        
    except Exception as e:
        logging.error(f"数据索引超出范围,收益率计算失败 {stock[2]}: {str(e)}")
        return [None]*100






def main():
    """主入口函数"""
    try:
        args = parse_arguments()
        
        # 处理日期参数
        if len(args.dates) == 0:
            dates = get_default_dates()
            if len(dates) == 1:
                start_date = end_date = dates[0]
            else:
                start_date, end_date = dates[0], dates[-1]
        elif len(args.dates) == 1:
            start_date = end_date = args.dates[0]
        elif len(args.dates) == 2:
            start_date, end_date = sorted(args.dates)
            if start_date > end_date:
                raise ValueError("开始日期不能晚于结束日期")
        else:
            print("参数错误！支持以下调用方式：")
            print("1. 无参数       -> 自动使用最近交易日")
            print("2. 单日期       -> python script.py 20230101")
            print("3. 日期区间     -> python script.py 20230101 20230105")
            return

        # 定义要运行的策略列表
        strategies = ['strategy_a', 'strategy_b']
        
        # 遍历执行所有策略
        for strategy in strategies:
            logging.info(f"正在执行策略: {strategy}")
            guess_buy(
                strategy_name=strategy,
                start_date_int=start_date,
                end_date_int=end_date
            )

        # 回测
        prepare()
        
    except Exception as e:
        logging.error(f"执行失败: {e}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()