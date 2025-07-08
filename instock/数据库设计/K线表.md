# K线表



### 股票K线表

| 字段名   | 类型        | 说明                 |
| -------- | ----------- | -------------------- |
| id       | INT         | 自增                 |
| date     | DATE        | 日期格式：2025-07-08 |
| date_int | INT         | 日期格式：20250708   |
| code     | VARCHAR(6)  | 股票代码：000001     |
| code_int | INT         | 股票代码：1          |
| name     | VARCHAR(20) | 股票名称             |
| open     | FLOAT       | 开盘价               |
| close    | FLOAT       | 收盘价               |
| high     | FLOAT       | 最高价               |
| low      | FLOAT       | 最低价               |
| volume   | FLOAT       | 成交量（手）         |

