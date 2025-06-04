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
import time
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Table, text
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset 
import instock.lib.database as mdb
import instock.core.tablestructure as tbs

__author__ = 'hqm'
__date__ = '2025/4/15'

table_name = "cn_stock_indicators_sell"

# 在文件顶部定义回测字段配置（替换tbs模块的引用）
RATE_FIELDS_COUNT = 100
TABLE_CN_STOCK_BACKTEST_DATA = {
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
            AND 三日指标.kdjd >= 70  -- d处于超买区域
            AND 三日指标.kdjj >= 85  -- j处于超买区域
            AND 三日指标.cci >= 110 
            AND 三日指标.rsi_6 >= 65
            AND 三日指标.rsi_12 >= 65  
            AND ABS(三日指标.wr_6) <= 5
        """
    },
    'strategy_b': {
        'conditions': """

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
            大盘情绪.指数上涨情绪 AS up_sentiment,
            大盘情绪.指数下跌情绪 AS down_sentiment,
        --     大盘情绪.操作建议,
            三日指标.code ,
            三日指标.code_int ,
            三日指标.name ,
            三日指标.date ,
            三日指标.date_int ,
            三日指标.kdjj,
            资金.jingliuru_cn ,
            三日指标.close ,
            三日指标.turnover ,
            三日指标.industry ,
            行业指标.kdjj AS industry_kdjj,
            行业指标.kdjj_day1 AS industry_kdjj_day1,
        --     行业数据.行业名称,
            行业数据.KDJ趋势 AS industry_kdj,
            行业数据.WR趋势 AS industry_wr,
            行业数据.CCI趋势 AS industry_cci,
        --     行业数据.强烈买入信号,
        --     行业数据.强烈卖出信号,
            行业数据.行业情绪  AS industry_sentiment
        FROM
            stock_3day_indicators 三日指标
            LEFT JOIN cn_stock_info 基础信息 ON 基础信息.code_int = 三日指标.code_int
            LEFT JOIN stock_zijin 资金 ON 资金.code_int = 三日指标.code_int AND 资金.date_int = 三日指标.date_int
            LEFT JOIN market_sentiment_a 大盘情绪 ON 大盘情绪.date_int = 三日指标.date_int
            LEFT JOIN industry_3day_indicators 行业指标 ON 行业指标.name = 三日指标.industry AND 行业指标.date_int = 三日指标.date_int
            LEFT JOIN industry_sentiment_a 行业数据 ON 行业数据.行业名称 = 三日指标.industry AND 行业数据.date_int = 三日指标.date_int
        WHERE 
            {date_condition}
            {strategy_cond}  # 插入策略特定条件
            AND 三日指标.NAME NOT LIKE '%ST%' 
            AND 基础信息.industry NOT REGEXP '酿酒行业|美容护理|农药兽药|食品饮料|光伏设备|煤炭行业|造纸印刷|保险' 
        ORDER BY
            三日指标.date_int;
        """
        
        with mdb.engine().connect() as conn:
            data = pd.read_sql(text(sql), conn)
        
        if data.empty:
            logging.info("无符合条件的数据")
            return

        # 新增列合并逻辑（关键修改点）
        _columns_backtest = tuple(TABLE_CN_STOCK_BACKTEST_DATA['columns'])
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
    # table_name = "cn_stock_indicators_sell"
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
        `jingliuru_cn` VARCHAR(50) COMMENT '净流入',
        `industry` VARCHAR(50) COMMENT '所属行业',
        `up_sentiment` INT COMMENT '大盘上涨情绪',
        `down_sentiment` INT COMMENT '大盘下跌情绪',
        `industry_kdjj` FLOAT COMMENT '行业当天J值',
        `industry_kdjj_day1` FLOAT COMMENT '行业前一天J值',
        `industry_kdj` VARCHAR(50) COMMENT '行业KDJ趋势',
        `industry_wr` VARCHAR(50) COMMENT '行业威廉趋势',
        `industry_cci` VARCHAR(50) COMMENT '行业CCI趋势',
        `industry_sentiment` VARCHAR(50) COMMENT '行业买卖情绪',
        /* 新增回测字段 */
        {' ,'.join([f'`{col}` FLOAT' for col in TABLE_CN_STOCK_BACKTEST_DATA['columns']])},
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
    # table_name = "cn_stock_indicators_sell"
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
        # print("数据插入/更新成功")
        logging.info("数据插入/更新成功")
        
    except Exception as e:
        logging.error(f"操作失败，详细错误：{str(e)}", exc_info=True)
        raise


def main():
    """主入口函数"""
    start_time = time.time()
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
        strategies = ['strategy_a']
        
        # 遍历执行所有策略
        for strategy in strategies:
            logging.info(f"正在执行策略: {strategy}")
            guess_buy(
                strategy_name=strategy,
                start_date_int=start_date,
                end_date_int=end_date
            )
        
    except Exception as e:
        logging.error(f"执行失败: {e}")
    finally:
        print(f"strategy_stock_sell\n🕒 总耗时: {time.time()-start_time:.2f}秒")  # 确保异常时也输出


if __name__ == '__main__':

    main()