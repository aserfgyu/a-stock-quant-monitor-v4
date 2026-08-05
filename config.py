"""A股量化监控系统 V4 配置 - 多源聚合版"""
import os

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "10000"))
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL", "30"))  # 30秒刷新，更实时

# 数据源优先级（从高到低）
DATA_SOURCES = ["akshare", "sina", "eastmoney"]
DATA_SOURCE_TIMEOUT = 8  # 单个源超时秒数

# 监控指数
INDEX_SYMBOLS = [
    "sh000001",   # 上证指数
    "sz399001",   # 深证成指
    "sz399006",   # 创业板指
    "sh000300",   # 沪深300
    "sh000688",   # 科创50
    "sh000905",   # 中证500
    "sh000016",   # 上证50
    "sz399005",   # 中小板指
    "sh000852",   # 中证1000
]

INDEX_NAMES = {
    "sh000001": "上证指数",
    "sz399001": "深证成指",
    "sz399006": "创业板指",
    "sh000300": "沪深300",
    "sh000688": "科创50",
    "sh000905": "中证500",
    "sh000016": "上证50",
    "sz399005": "中小板指",
    "sh000852": "中证1000",
}

# 全市场扫描参数
MARKET_SCAN_LIMIT = 300
MARKET_SCAN_MIN_CHANGE = 2.0
MARKET_SCAN_MIN_VOL_RATIO = 1.2
MARKET_SCAN_MIN_TURNOVER = 2.5

# 板块监控
SECTOR_TOP_N = 20

# 推荐参数
DAILY_PICK_COUNT = 15
DAILY_SECTOR_PICK = 8

# 功能开关
NORTH_FUND_ENABLED = True
LHB_ENABLED = True
MARKET_FUND_FLOW_ENABLED = True

# K线配置
KLINE_DAYS = 90

CORS_ORIGINS = ["*"]
