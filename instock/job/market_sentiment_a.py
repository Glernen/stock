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
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text
from tqdm import tqdm
from instock.lib.database import db_host, db_user, db_password, db_database, db_charset 
import instock.lib.database as mdb
import instock.core.tablestructure as tbs

__author__ = 'hqm'
__date__ = '2025年4月20日'

'''
主要逻辑：访问数据库，将获取的数据保存到market_sentiment_a数据表中
1、sql获取数据：
sql：WITH date_range AS (
    SELECT DISTINCT date_int
    FROM index_3day_indicators
    WHERE STR_TO_DATE(date_int, '%Y%m%d') >= DATE_SUB(
        (SELECT MAX(STR_TO_DATE(date_int, '%Y%m%d')) FROM index_3day_indicators), 
        INTERVAL 3 MONTH
    )
),
tech_indicators AS (
    SELECT
        date_int,
        -- 三日趋势指标（保持不变）
        SUM(CASE WHEN kdjK > kdjD AND kdjk_day1 <= kdjd_day1 AND kdjk_day2 <= kdjd_day2 THEN 1 ELSE 0 END) AS kdj_golden_cross,
        SUM(CASE WHEN kdjK < kdjD AND kdjk_day1 >= kdjd_day1 AND kdjk_day2 >= kdjd_day2 THEN 1 ELSE 0 END) AS kdj_dead_cross,
        SUM(CASE WHEN wr_6 > wr_6_day1 AND wr_6_day1 > wr_6_day2 THEN 1 ELSE 0 END) AS wr_up_trend,
        SUM(CASE WHEN cci > cci_day1 AND cci_day1 > cci_day2 THEN 1 ELSE 0 END) AS cci_up_trend,
        -- 替代方案：使用KDJ/CCI的即时变化作为信号
        SUM(CASE 
            -- 买入信号：KDJ金叉且CCI突破-100
            WHEN kdjK > kdjD 
            AND kdjK_day1 <= kdjD_day1  -- 当日新金叉
            AND cci > -100 
            AND cci_day1 <= -100 
            THEN 1 ELSE 0 
        END) AS strong_buy_signal,
        SUM(CASE 
            -- 卖出信号：KDJ死叉且CCI下破+100
            WHEN kdjK < kdjD 
            AND kdjK_day1 >= kdjD_day1  -- 当日新死叉
            AND cci < 100 
            AND cci_day1 >= 100 
            THEN 1 ELSE 0 
        END) AS strong_sell_signal
    FROM index_3day_indicators
    WHERE code_int IN (1, 300, 510, 399001, 399006, 399293, 688, 852, 932000, 899050)
    GROUP BY date_int
),
up_sentiment AS (
    SELECT
        dr.date_int,
        ROUND(IFNULL((COUNT(i3di.code_int) / 10.0) * 100, 0)) AS `指数上涨情绪`
    FROM date_range dr
    LEFT JOIN index_3day_indicators i3di 
        ON dr.date_int = i3di.date_int
        AND i3di.`kdjK` >= i3di.kdjk_day1
        AND i3di.`kdjd` >= i3di.kdjd_day1
        AND i3di.wr_6 >= i3di.wr_6_day1
        AND i3di.cci >= i3di.cci_day1
        AND i3di.code_int IN (1, 300, 510, 399001, 399006, 399293, 688, 852, 932000, 899050)
        AND i3di.`kdjk` <= 45
        AND i3di.`kdjd` <= 45
        AND i3di.`kdjj` <= 75
    GROUP BY dr.date_int
),
down_sentiment AS (
    SELECT
        dr.date_int,
        ROUND(IFNULL((COUNT(i3di.code_int) / 10.0) * 100, 0)) AS `指数下跌情绪`
    FROM date_range dr
    LEFT JOIN index_3day_indicators i3di 
        ON dr.date_int = i3di.date_int
        AND i3di.`kdjK` <= i3di.kdjk_day1
        AND i3di.`kdjd` <= i3di.kdjd_day1
        AND i3di.wr_6 <= i3di.wr_6_day1
        AND i3di.cci <= i3di.cci_day1
        AND i3di.cci_day1 <= i3di.cci_day2
        AND i3di.code_int IN (1, 300, 510, 399001, 399006, 399293, 688, 852, 932000, 899050)
        AND i3di.`kdjk` >= 60
        AND i3di.`kdjd` >= 60
        AND ABS(i3di.`wr_6`) <= 70
    GROUP BY dr.date_int
),

final_data AS (
    SELECT
        us.date_int,
        us.`指数上涨情绪`,
        ds.`指数下跌情绪`,
        ti.kdj_golden_cross,
        ti.kdj_dead_cross,
        ti.wr_up_trend,
        ti.cci_up_trend,
        ti.strong_buy_signal,
        ti.strong_sell_signal,
        -- 动态阈值计算（根据实际指数数量）
        (SELECT COUNT(DISTINCT code_int) FROM index_3day_indicators) * 0.6 * 2 AS up_threshold,
        (SELECT COUNT(DISTINCT code_int) FROM index_3day_indicators) * 0.4 * 2 AS down_threshold
    FROM up_sentiment us
    JOIN down_sentiment ds ON us.date_int = ds.date_int
    JOIN tech_indicators ti ON us.date_int = ti.date_int
)

SELECT 
    date_int,
    `指数上涨情绪`,
    `指数下跌情绪`,
    `操作建议`,
    -- 市场情绪字段处理
    CASE
        WHEN `操作建议` IS NOT NULL THEN 
            LEFT(
                REPLACE(REPLACE(`操作建议`, '：', ''), '（', ''),  -- 同时处理冒号和括号
                4
            )
        ELSE '未知情绪'
    END AS `市场情绪`
FROM (
    SELECT 
        date_int,
        `指数上涨情绪`,
        `指数下跌情绪`,
        -- 原操作建议字段定义
        CASE
            WHEN `指数上涨情绪` = 10 OR `指数下跌情绪` = 10 THEN 
                CASE 
                    WHEN strong_buy_signal > 2 THEN CONCAT('观察阶段：趋势可能反转，',strong_buy_signal,'个买点信号')
                    WHEN strong_sell_signal > 2 THEN CONCAT('观察阶段：趋势可能反转，',strong_sell_signal,'个卖点信号')
                    ELSE '观察阶段：趋势可能反转，保持关注'
                END
            WHEN `指数上涨情绪` = 100 AND `指数下跌情绪` = 0 THEN 
                CASE 
                    WHEN strong_buy_signal >= 5 THEN '满仓持有：市场极度乐观，警惕超买风险'
                    ELSE '满仓持有：维持仓位，注意止盈' 
                END
            WHEN `指数上涨情绪` > 0 AND `指数下跌情绪` = 0 THEN 
                CASE 
                    WHEN strong_buy_signal > 0 THEN CONCAT('上行趋势：建议持仓待涨（',strong_buy_signal,'个买点信号）')
                    ELSE '上行趋势：建议分批建仓'
                END
            WHEN `指数上涨情绪` = 0 AND `指数下跌情绪` > 0 THEN 
                CASE 
                    WHEN strong_sell_signal > 0 THEN CONCAT('下行趋势：建议控制仓位（',strong_sell_signal,'个卖点信号）')
                    ELSE '下行趋势：建议反弹减仓'
                END
            WHEN `指数上涨情绪` = 0 AND `指数下跌情绪` = 100 THEN 
                CASE 
                    WHEN strong_sell_signal >= 5 THEN '空仓观望：市场恐慌抛售，等待企稳'
                    ELSE '空仓观望：保持现金仓位' 
                END
            WHEN `指数上涨情绪` = 0 AND `指数下跌情绪` = 0 THEN 
                CASE 
                    WHEN kdj_golden_cross > 0 OR (wr_up_trend + cci_up_trend) >= up_threshold THEN
                        CASE 
                            WHEN strong_buy_signal > 0 THEN CONCAT('上行震荡：建议逢低布局（',strong_buy_signal,'个买点）')
                            ELSE '上行震荡：建议分批建仓'
                        END
                    WHEN kdj_dead_cross > 0 OR (wr_up_trend + cci_up_trend) <= down_threshold THEN
                        CASE 
                            WHEN strong_sell_signal > 0 THEN CONCAT('下行震荡：建议反弹减仓（',strong_sell_signal,'个卖点）')
                            ELSE '下行震荡：建议控制仓位'
                        END
                    ELSE 
                        CASE 
                            WHEN strong_buy_signal > strong_sell_signal THEN CONCAT('多空平衡：买盘占优（',strong_buy_signal,'次信号）')
                            WHEN strong_sell_signal > strong_buy_signal THEN CONCAT('多空平衡：卖盘占优（',strong_sell_signal,'次信号）')
                            ELSE '多空平衡：建议保持观望'
                        END
                END
            ELSE '未知状态：暂时无法识别市场状态'
        END AS `操作建议`
    FROM final_data
) AS sub_query
ORDER BY date_int DESC;

-- 获取的字段为date_int,指数上涨情绪，指数下跌情绪，操作建议，市场情绪

2、market_sentiment_a表的字段：id(自生成主键)，date_int（int类型），指数上涨情绪（FLOAT类型），指数下跌情绪（FLOAT类型），操作建议（VARCHAR(120)），市场情绪（VARCHAR(50)）

'''



def get_market_sentiment_data():
    """核心分析逻辑"""
    try:
        # 确保表存在
        create_optimized_table()

        sql = f"""
        WITH date_range AS (
            SELECT DISTINCT date_int
            FROM index_3day_indicators
            WHERE STR_TO_DATE(date_int, '%Y%m%d') >= DATE_SUB(
                (SELECT MAX(STR_TO_DATE(date_int, '%Y%m%d')) FROM index_3day_indicators), 
                INTERVAL 3 MONTH
            )
        ),
        tech_indicators AS (
            SELECT
                date_int,
                -- 三日趋势指标（保持不变）
                SUM(CASE WHEN kdjK > kdjD AND kdjk_day1 <= kdjd_day1 AND kdjk_day2 <= kdjd_day2 THEN 1 ELSE 0 END) AS kdj_golden_cross,
                SUM(CASE WHEN kdjK < kdjD AND kdjk_day1 >= kdjd_day1 AND kdjk_day2 >= kdjd_day2 THEN 1 ELSE 0 END) AS kdj_dead_cross,
                SUM(CASE WHEN wr_6 > wr_6_day1 AND wr_6_day1 > wr_6_day2 THEN 1 ELSE 0 END) AS wr_up_trend,
                SUM(CASE WHEN cci > cci_day1 AND cci_day1 > cci_day2 THEN 1 ELSE 0 END) AS cci_up_trend,
                -- 替代方案：使用KDJ/CCI的即时变化作为信号
                SUM(CASE 
                    -- 买入信号：KDJ金叉且CCI突破-100
                    WHEN kdjK > kdjD 
                    AND kdjK_day1 <= kdjD_day1  -- 当日新金叉
                    AND cci > -100 
                    AND cci_day1 <= -100 
                    THEN 1 ELSE 0 
                END) AS strong_buy_signal,
                SUM(CASE 
                    -- 卖出信号：KDJ死叉且CCI下破+100
                    WHEN kdjK < kdjD 
                    AND kdjK_day1 >= kdjD_day1  -- 当日新死叉
                    AND cci < 100 
                    AND cci_day1 >= 100 
                    THEN 1 ELSE 0 
                END) AS strong_sell_signal
            FROM index_3day_indicators
            WHERE code_int IN (1, 300, 510, 399001, 399006, 399293, 688, 852, 932000, 899050)
            GROUP BY date_int
        ),
        up_sentiment AS (
            SELECT
                dr.date_int,
                ROUND(IFNULL((COUNT(i3di.code_int) / 10.0) * 100, 0)) AS `指数上涨情绪`
            FROM date_range dr
            LEFT JOIN index_3day_indicators i3di 
                ON dr.date_int = i3di.date_int
                AND i3di.`kdjK` >= i3di.kdjk_day1
                AND i3di.`kdjd` >= i3di.kdjd_day1
                AND i3di.wr_6 >= i3di.wr_6_day1
                AND i3di.cci >= i3di.cci_day1
                AND i3di.code_int IN (1, 300, 510, 399001, 399006, 399293, 688, 852, 932000, 899050)
                AND i3di.`kdjk` <= 45
                AND i3di.`kdjd` <= 45
                AND i3di.`kdjj` <= 75
            GROUP BY dr.date_int
        ),
        down_sentiment AS (
            SELECT
                dr.date_int,
                ROUND(IFNULL((COUNT(i3di.code_int) / 10.0) * 100, 0)) AS `指数下跌情绪`
            FROM date_range dr
            LEFT JOIN index_3day_indicators i3di 
                ON dr.date_int = i3di.date_int
                AND i3di.`kdjK` <= i3di.kdjk_day1
                AND i3di.`kdjd` <= i3di.kdjd_day1
                AND i3di.wr_6 <= i3di.wr_6_day1
                AND i3di.cci <= i3di.cci_day1
                AND i3di.cci_day1 <= i3di.cci_day2
                AND i3di.code_int IN (1, 300, 510, 399001, 399006, 399293, 688, 852, 932000, 899050)
                AND i3di.`kdjk` >= 60
                AND i3di.`kdjd` >= 60
                AND ABS(i3di.`wr_6`) <= 70
            GROUP BY dr.date_int
        ),

        final_data AS (
            SELECT
                us.date_int,
                us.`指数上涨情绪`,
                ds.`指数下跌情绪`,
                ti.kdj_golden_cross,
                ti.kdj_dead_cross,
                ti.wr_up_trend,
                ti.cci_up_trend,
                ti.strong_buy_signal,
                ti.strong_sell_signal,
                -- 动态阈值计算（根据实际指数数量）
                (SELECT COUNT(DISTINCT code_int) FROM index_3day_indicators) * 0.6 * 2 AS up_threshold,
                (SELECT COUNT(DISTINCT code_int) FROM index_3day_indicators) * 0.4 * 2 AS down_threshold
            FROM up_sentiment us
            JOIN down_sentiment ds ON us.date_int = ds.date_int
            JOIN tech_indicators ti ON us.date_int = ti.date_int
        )

        SELECT 
            date_int,
            `指数上涨情绪`,
            `指数下跌情绪`,
            `操作建议`,
            -- 市场情绪字段处理
            CASE
                WHEN `操作建议` IS NOT NULL THEN 
                    LEFT(
                        REPLACE(REPLACE(`操作建议`, '：', ''), '（', ''),  -- 同时处理冒号和括号
                        4
                    )
                ELSE '未知情绪'
            END AS `市场情绪`
        FROM (
            SELECT 
                date_int,
                `指数上涨情绪`,
                `指数下跌情绪`,
                -- 原操作建议字段定义
                CASE
                    WHEN `指数上涨情绪` = 10 OR `指数下跌情绪` = 10 THEN 
                        CASE 
                            WHEN strong_buy_signal > 2 THEN CONCAT('观察阶段：趋势可能反转，',strong_buy_signal,'个买点信号')
                            WHEN strong_sell_signal > 2 THEN CONCAT('观察阶段：趋势可能反转，',strong_sell_signal,'个卖点信号')
                            ELSE '观察阶段：趋势可能反转，保持关注'
                        END
                    WHEN `指数上涨情绪` = 100 AND `指数下跌情绪` = 0 THEN 
                        CASE 
                            WHEN strong_buy_signal >= 5 THEN '满仓持有：市场极度乐观，警惕超买风险'
                            ELSE '满仓持有：维持仓位，注意止盈' 
                        END
                    WHEN `指数上涨情绪` > 0 AND `指数下跌情绪` = 0 THEN 
                        CASE 
                            WHEN strong_buy_signal > 0 THEN CONCAT('上行趋势：建议持仓待涨（',strong_buy_signal,'个买点信号）')
                            ELSE '上行趋势：建议分批建仓'
                        END
                    WHEN `指数上涨情绪` = 0 AND `指数下跌情绪` > 0 THEN 
                        CASE 
                            WHEN strong_sell_signal > 0 THEN CONCAT('下行趋势：建议控制仓位（',strong_sell_signal,'个卖点信号）')
                            ELSE '下行趋势：建议反弹减仓'
                        END
                    WHEN `指数上涨情绪` = 0 AND `指数下跌情绪` = 100 THEN 
                        CASE 
                            WHEN strong_sell_signal >= 5 THEN '空仓观望：市场恐慌抛售，等待企稳'
                            ELSE '空仓观望：保持现金仓位' 
                        END
                    WHEN `指数上涨情绪` = 0 AND `指数下跌情绪` = 0 THEN 
                        CASE 
                            WHEN kdj_golden_cross > 0 OR (wr_up_trend + cci_up_trend) >= up_threshold THEN
                                CASE 
                                    WHEN strong_buy_signal > 0 THEN CONCAT('上行震荡：建议逢低布局（',strong_buy_signal,'个买点）')
                                    ELSE '上行震荡：建议分批建仓'
                                END
                            WHEN kdj_dead_cross > 0 OR (wr_up_trend + cci_up_trend) <= down_threshold THEN
                                CASE 
                                    WHEN strong_sell_signal > 0 THEN CONCAT('下行震荡：建议反弹减仓（',strong_sell_signal,'个卖点）')
                                    ELSE '下行震荡：建议控制仓位'
                                END
                            ELSE 
                                CASE 
                                    WHEN strong_buy_signal > strong_sell_signal THEN CONCAT('多空平衡：买盘占优（',strong_buy_signal,'次信号）')
                                    WHEN strong_sell_signal > strong_buy_signal THEN CONCAT('多空平衡：卖盘占优（',strong_sell_signal,'次信号）')
                                    ELSE '多空平衡：建议保持观望'
                                END
                        END
                    ELSE '未知状态：暂时无法识别市场状态'
                END AS `操作建议`
            FROM final_data
        ) AS sub_query
        ORDER BY date_int DESC;
        """
        
        with mdb.engine().connect() as conn:
            data = pd.read_sql(text(sql), conn)
        
        if data.empty:
            logging.info("无符合条件的数据")
            return


        print(f'{data}')

        # 插入数据库逻辑（使用优化后的插入方法）
        optimized_data_insert(data)
        
    except Exception as e:
        logging.error(f"处理异常：{e}")
        raise




# 修改表结构（新增字段）
def create_optimized_table():
    table_name = "market_sentiment_a"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `date_int` INT NOT NULL COMMENT '日期YYYYMMDD格式',
        `指数上涨情绪` FLOAT,
        `指数下跌情绪` FLOAT,
        `操作建议` VARCHAR(120),
        `市场情绪` VARCHAR(50),
        UNIQUE KEY `idx_date` (`date_int`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='市场情绪分析表';
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

# 优化后的数据插入函数
def optimized_data_insert(data):
    table_name = "market_sentiment_a"
    try:
        # 自定义插入方法，使用INSERT IGNORE避免重复
        def insert_ignore(table, conn, keys, data_iter):
            from sqlalchemy.dialects.mysql import insert
            data_rows = [dict(zip(keys, row)) for row in data_iter]
            if not data_rows:
                return
            stmt = insert(table.table).values(data_rows).prefix_with('IGNORE')
            conn.execute(stmt)
        
        # 使用自定义方法插入数据
        data.to_sql(
            name=table_name,
            con=mdb.engine(),
            if_exists='append',
            index=False,
            chunksize=500,
            method=insert_ignore
        )
        logging.info("数据插入完成，重复记录已自动忽略")
    except Exception as e:
        logging.error(f"数据插入失败: {e}")
        raise



def main():
    """主入口函数"""
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    try:
        get_market_sentiment_data()
    except Exception as e:
        logging.error(f"程序运行异常: {e}")

if __name__ == '__main__':
    main()