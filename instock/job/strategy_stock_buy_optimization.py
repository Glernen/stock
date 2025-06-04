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

# 在文件顶部定义回测字段配置（替换tbs模块的引用）
RATE_FIELDS_COUNT = 100
TABLE_CN_STOCK_BACKTEST_DATA = {
    'columns': {
        f'rate_{i}': {'type': 'FLOAT', 'cn': f'{i}日收益率'} 
        for i in range(1, RATE_FIELDS_COUNT + 1)
    }
}

table_name = "strategy_stock_buy_optimization"

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
        return f"csi.date_int = {start_date}"
    return f"csi.date_int BETWEEN {start_date} AND {end_date}"

def guess_buy(start_date_int, end_date_int=None):
    """核心分析逻辑"""
    try:
        # 获取策略条件
        # strategy_cond = STRATEGY_CONFIG.get(strategy_name, {}).get('conditions')
        # if not strategy_cond:
        #     raise ValueError(f"未知策略: {strategy_name}")

        # 确保表存在
        create_optimized_table()
        
        # 构建动态日期条件
        date_condition = build_date_condition(start_date_int, end_date_int)

        sql = f"""
        WITH latest_date AS (
            SELECT 
                MAX(`date_int`) AS latest_date_int
            FROM 
                `stock_3day_indicators`
        ),
        buy_all AS (
            SELECT *
            FROM 
                `cn_stock_indicators_buy` csi
        --     JOIN latest_date ON csi.`date_int` = latest_date.latest_date_int
            WHERE 
                
                -- AND csi.`date_int` > 20250409
                {date_condition}
                AND csi.`rate_60` IS NULL 
                AND (csi.`strategy` = 'strategy_a' OR csi.`strategy` = 'strategy_b') 
                -- AND csi.`industry_kdj` = '上涨趋势' 
                AND csi.`code` NOT LIKE '688%' 
                AND csi.`code` NOT LIKE '8%' 
            AND (
            -- 大盘强势上涨：大盘指标上涨，行业指标上涨，个股换手率小于20%的精筛股
                    (csi.`up_sentiment` > 10
                AND csi.`up_sentiment` < 50
                AND csi.`down_sentiment` = 0
                AND csi.`industry_kdj` = '上涨趋势'
                AND csi.`industry_kdjj` < 60
                AND csi.`industry_sentiment` = '行业震荡中性'
                AND csi.`turnover` > 1
                AND csi.`industry_cci` != '方向不明')
            -- 大盘强势上涨：大盘指标上涨，行业指标上涨
            OR (csi.`up_sentiment` >= 50
                AND csi.`down_sentiment` = 0
                AND csi.`industry_kdj` = '上涨趋势'
                AND csi.`industry_sentiment` = '行业强势看涨'
                and csi.`turnover` > 0.5
                AND csi.`industry_cci` != '方向不明' )
            --     -- 大盘震荡期：大盘指标上涨（指数K值小于60为标准）小于2个指数盘，行业指标上涨，个股换手率小于20%的精筛股
            OR (csi.`up_sentiment` <= 20
                AND csi.`industry_kdj` = '上涨趋势'
                AND csi.`industry_wr` = '持续超买'
                AND csi.`turnover` < 20)
            --     -- 大盘上涨初现：大盘指标上涨，行业指标未墙裂上涨且处于超卖区，个股换手率大于1%的精筛股
            OR (csi.`up_sentiment` >= 30
                AND csi.`down_sentiment` = 0
                AND csi.`industry_kdj` = '下降趋势'
                AND csi.`industry_kdjj` < 20
                AND csi.`industry_sentiment` = '行业震荡中性'
                AND csi.`turnover` > 1)
                )
        )
        SELECT * 
        FROM buy_all 
        """
        
        with mdb.engine().connect() as conn:
            data = pd.read_sql(text(sql), conn)
        
        if data.empty:
            logging.info("无符合条件的数据")
            return

        # 新增列合并逻辑（关键修改点）
        _columns_backtest = tuple(TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])

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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='选股结果优化表';
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


        guess_buy(
            start_date_int=start_date,
            end_date_int=end_date
        )
        
    except Exception as e:
        logging.error(f"执行失败: {e}")
    finally:
        print(f"strategy_stock_buy_optimization\n🕒 总耗时: {time.time()-start_time:.2f}秒")  # 确保异常时也输出

if __name__ == '__main__':
    main()