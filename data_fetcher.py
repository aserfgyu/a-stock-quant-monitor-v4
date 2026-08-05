"""数据获取 V4 - 多源聚合 (AKShare + 新浪财经 + 东方财富)"""
import os, time, logging, json, asyncio, aiohttp
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

class DataFetcher:
    """多源数据获取器，自动故障转移"""

    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self.ttl = 60
        self._ak = None
        self.data_source_status = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._init_akshare()

    def _init_akshare(self):
        try:
            import akshare as ak
            self._ak = ak
            self.data_source_status['akshare'] = 'ready'
            logger.info("AKShare 初始化成功")
        except ImportError:
            self.data_source_status['akshare'] = 'unavailable'
            logger.warning("AKShare 未安装，将使用HTTP源")

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            )
        return self._session

    def _cache_key(self, key):
        if key in self._cache and time.time() - self._cache_time.get(key, 0) < self.ttl:
            return self._cache[key]
        return None

    def _set_cache(self, key, value):
        self._cache[key] = value
        self._cache_time[key] = time.time()

    async def fetch_index_realtime(self, symbol: str) -> Optional[Dict]:
        cache_key = f"idx_rt_{symbol}"
        cached = self._cache_key(cache_key)
        if cached: return cached

        if self._ak and self.data_source_status.get('akshare') == 'ready':
            try:
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self._fetch_index_akshare, symbol),
                    timeout=8
                )
                if result:
                    self.data_source_status['akshare'] = 'ok'
                    self._set_cache(cache_key, result)
                    return result
            except Exception as e:
                logger.warning(f"AKShare 指数失败 {symbol}: {e}")
                self.data_source_status['akshare'] = 'degraded'

        try:
            result = await self._fetch_index_eastmoney(symbol)
            if result:
                self.data_source_status['eastmoney'] = 'ok'
                self._set_cache(cache_key, result)
                return result
        except Exception as e:
            logger.warning(f"东财指数失败 {symbol}: {e}")

        try:
            result = await self._fetch_index_sina(symbol)
            if result:
                self.data_source_status['sina'] = 'ok'
                self._set_cache(cache_key, result)
                return result
        except Exception as e:
            logger.warning(f"新浪指数失败 {symbol}: {e}")

        return None

    def _fetch_index_akshare(self, symbol: str) -> Optional[Dict]:
        try:
            code = symbol[2:]
            market = "sh" if symbol.startswith("sh") else "sz"
            ak_symbol = f"{market}{code}"
            df = self._ak.stock_zh_index_spot_sina()
            if df is None or df.empty: return None
            row = df[df['代码'] == ak_symbol]
            if row.empty: return None
            r = row.iloc[0]
            return {
                'symbol': symbol, 'name': str(r.get('名称', '')),
                'close': float(r.get('最新价', 0)), 'change': float(r.get('涨跌额', 0)),
                'change_pct': float(r.get('涨跌幅', 0)), 'open': float(r.get('开盘价', 0)),
                'high': float(r.get('最高价', 0)), 'low': float(r.get('最低价', 0)),
                'pre_close': float(r.get('昨收', 0)),
                'volume': int(float(r.get('成交量', 0))),
                'amount': float(r.get('成交额', 0)),
            }
        except Exception as e:
            logger.warning(f"AKShare index error: {e}")
            return None

    async def _fetch_index_sina(self, symbol: str) -> Optional[Dict]:
        code = symbol[2:]
        market = "sh" if symbol.startswith("sh") else "sz"
        url = f"https://hq.sinajs.cn/list=s_{market}{code}"
        session = await self._get_session()
        async with session.get(url) as resp:
            text = await resp.text()
            if '"' not in text: return None
            data = text.split('"')[1]
            parts = data.split(',')
            if len(parts) < 6: return None
            name, today_open, yest_close, current, high, low = parts[:6]
            current_f = float(current) if current else 0
            yest_f = float(yest_close) if yest_close else 0
            change = current_f - yest_f
            change_pct = (change / yest_f * 100) if yest_f else 0
            return {
                'symbol': symbol, 'name': name, 'close': current_f,
                'change': round(change, 2), 'change_pct': round(change_pct, 2),
                'open': float(today_open) if today_open else 0,
                'high': float(high) if high else 0, 'low': float(low) if low else 0,
                'pre_close': yest_f, 'volume': 0, 'amount': 0,
            }

    async def _fetch_index_eastmoney(self, symbol: str) -> Optional[Dict]:
        code = symbol[2:]
        market = "1" if symbol.startswith("sh") else "0"
        url = f"https://push2.eastmoney.com/api/qt/stock/get?secid={market}.{code}&fields=f43,f44,f45,f46,f47,f48,f57,f58,f60,f170"
        session = await self._get_session()
        async with session.get(url) as resp:
            data = await resp.json()
            d = data.get('data', {})
            if not d: return None
            pre_close = float(d.get('f60', 0)) / 100 if d.get('f60') else 0
            close = float(d.get('f43', 0)) / 100 if d.get('f43') else 0
            change_pct = float(d.get('f170', 0)) / 100 if d.get('f170') else 0
            change = round(close - pre_close, 2) if pre_close else 0
            return {
                'symbol': symbol, 'name': str(d.get('f58', '')),
                'close': close, 'change': change, 'change_pct': change_pct,
                'open': float(d.get('f46', 0)) / 100 if d.get('f46') else 0,
                'high': float(d.get('f44', 0)) / 100 if d.get('f44') else 0,
                'low': float(d.get('f45', 0)) / 100 if d.get('f45') else 0,
                'pre_close': pre_close,
                'volume': int(float(d.get('f47', 0))) if d.get('f47') else 0,
                'amount': float(d.get('f48', 0)) / 10000 if d.get('f48') else 0,
            }

    async def fetch_today_all(self) -> pd.DataFrame:
        cache_key = "today_all"
        cached = self._cache_key(cache_key)
        if cached is not None and isinstance(cached, dict) and 'data' in cached:
            return pd.DataFrame(cached['data'])

        if self._ak and self.data_source_status.get('akshare') in ('ready', 'ok'):
            try:
                df = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self._fetch_today_all_akshare),
                    timeout=15
                )
                if df is not None and not df.empty:
                    self.data_source_status['akshare'] = 'ok'
                    self.data_source_status['realtime_data'] = True
                    self._set_cache(cache_key, {'data': df.to_dict('records')})
                    return df
            except Exception as e:
                logger.warning(f"AKShare 全市场失败: {e}")
                self.data_source_status['akshare'] = 'degraded'

        try:
            df = await self._fetch_today_all_eastmoney()
            if df is not None and not df.empty:
                self.data_source_status['eastmoney'] = 'ok'
                self.data_source_status['realtime_data'] = True
                self._set_cache(cache_key, {'data': df.to_dict('records')})
                return df
        except Exception as e:
            logger.warning(f"东财全市场失败: {e}")

        self.data_source_status['realtime_data'] = False
        return self._mock_today_all()

    def _fetch_today_all_akshare(self) -> pd.DataFrame:
        df = self._ak.stock_zh_a_spot_em()
        if df is None or df.empty: return None
        rename_map = {
            '代码': 'code', '名称': 'name', '最新价': 'close',
            '涨跌幅': 'change_pct', '涨跌额': 'change',
            '成交量': 'volume', '成交额': 'amount',
            '振幅': 'amplitude', '最高': 'high', '最低': 'low',
            '今开': 'open', '昨收': 'pre_close',
            '量比': 'vol_ratio', '换手率': 'turnover',
            '市盈率-动态': 'pe', '市净率': 'pb',
            '总市值': 'total_mv', '流通市值': 'float_mv',
            '涨速': 'rise_speed', '5分钟涨跌': 'change_5min',
            '60日涨跌幅': 'change_60d', '年初至今涨跌幅': 'change_ytd',
            '委比': 'wei_bi', '委差': 'wei_cha',
            '内盘': 'inner_vol', '外盘': 'outer_vol',
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        df['symbol'] = df['code'].apply(lambda x: f"{x}.{'SH' if str(x).startswith('6') else 'SZ'}")
        df = df.fillna(0)
        return df

    async def _fetch_today_all_eastmoney(self) -> pd.DataFrame:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            'pn': 1, 'pz': 5000, 'po': 1, 'np': 1, 'fltt': 2, 'invt': 2,
            'fid': 'f20',
            'fs': 'm:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23',
            'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f22,f23,f24,f25,f33,f34,f35',
        }
        session = await self._get_session()
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            diff = data.get('data', {}).get('diff', [])
            if not diff: return pd.DataFrame()
            records = []
            for item in diff:
                code = str(item.get('f12', ''))
                market = item.get('f13', 0)
                symbol = f"{code}.{'SH' if market == 1 else 'SZ'}"
                pre_close = float(item.get('f18', 0)) if item.get('f18') else 0
                close = float(item.get('f2', 0)) if item.get('f2') else 0
                change_pct = float(item.get('f3', 0)) if item.get('f3') else 0
                change = round(close - pre_close, 2) if pre_close else 0
                records.append({
                    'code': code, 'name': str(item.get('f14', '')),
                    'close': close, 'change_pct': change_pct, 'change': change,
                    'volume': int(float(item.get('f5', 0))) if item.get('f5') else 0,
                    'amount': float(item.get('f6', 0)) if item.get('f6') else 0,
                    'amplitude': float(item.get('f7', 0)) if item.get('f7') else 0,
                    'turnover': float(item.get('f8', 0)) if item.get('f8') else 0,
                    'vol_ratio': float(item.get('f10', 0)) if item.get('f10') else 0,
                    'pe': float(item.get('f9', 0)) if item.get('f9') else 0,
                    'pb': float(item.get('f23', 0)) if item.get('f23') else 0,
                    'total_mv': float(item.get('f20', 0)) if item.get('f20') else 0,
                    'float_mv': float(item.get('f21', 0)) if item.get('f21') else 0,
                    'high': float(item.get('f15', 0)) if item.get('f15') else 0,
                    'low': float(item.get('f16', 0)) if item.get('f16') else 0,
                    'open': float(item.get('f17', 0)) if item.get('f17') else 0,
                    'pre_close': pre_close, 'symbol': symbol,
                    'wei_bi': float(item.get('f33', 0)) if item.get('f33') else 0,
                    'outer_vol': float(item.get('f34', 0)) if item.get('f34') else 0,
                    'inner_vol': float(item.get('f35', 0)) if item.get('f35') else 0,
                })
            df = pd.DataFrame(records)
            df = df.fillna(0)
            return df

    async def fetch_sectors(self) -> Dict[str, pd.DataFrame]:
        cache_key = "sectors"
        cached = self._cache_key(cache_key)
        if cached is not None and isinstance(cached, dict):
            return {k: pd.DataFrame(v) for k, v in cached.items()}

        result = {}
        if self._ak and self.data_source_status.get('akshare') in ('ready', 'ok'):
            try:
                industry = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self._ak.stock_board_industry_name_em),
                    timeout=10
                )
                if industry is not None and not industry.empty:
                    result['industry'] = industry.head(60)
                concept = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self._ak.stock_board_concept_name_em),
                    timeout=10
                )
                if concept is not None and not concept.empty:
                    result['concept'] = concept.head(60)
            except Exception as e:
                logger.warning(f"AKShare 板块失败: {e}")

        if not result:
            try:
                result = await self._fetch_sectors_eastmoney()
            except Exception as e:
                logger.warning(f"东财板块失败: {e}")

        if not result:
            result = self._mock_sectors()

        self._set_cache(cache_key, {k: v.to_dict('records') for k, v in result.items()})
        return result

    async def _fetch_sectors_eastmoney(self) -> Dict[str, pd.DataFrame]:
        result = {}
        session = await self._get_session()
        url = "https://push2.eastmoney.com/api/qt/clist/get"

        params = {'pn': 1, 'pz': 60, 'po': 1, 'np': 1, 'fltt': 2, 'invt': 2,
                  'fid': 'f20', 'fs': 'm:90+t:2',
                  'fields': 'f14,f3,f62'}
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            diff = data.get('data', {}).get('diff', [])
            records = [{'板块名称': str(item.get('f14', '')),
                        '涨跌幅': float(item.get('f3', 0)) if item.get('f3') else 0,
                        '主力净流入': round(float(item.get('f62', 0)) / 1e8, 2) if item.get('f62') else 0}
                       for item in diff]
            if records: result['industry'] = pd.DataFrame(records)

        params['fs'] = 'm:90+t:3'
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            diff = data.get('data', {}).get('diff', [])
            records = [{'板块名称': str(item.get('f14', '')),
                        '涨跌幅': float(item.get('f3', 0)) if item.get('f3') else 0,
                        '主力净流入': round(float(item.get('f62', 0)) / 1e8, 2) if item.get('f62') else 0}
                       for item in diff]
            if records: result['concept'] = pd.DataFrame(records)
        return result

    async def fetch_north_fund(self) -> Dict:
        cache_key = "north_fund"
        cached = self._cache_key(cache_key)
        if cached: return cached

        if self._ak and self.data_source_status.get('akshare') in ('ready', 'ok'):
            try:
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self._fetch_north_akshare),
                    timeout=10
                )
                if result:
                    self._set_cache(cache_key, result)
                    return result
            except Exception as e:
                logger.warning(f"AKShare 北向失败: {e}")

        return self._mock_north_fund()

    def _fetch_north_akshare(self) -> Dict:
        df_sh = self._ak.stock_hsgt_hist_em(symbol="沪股通")
        df_sz = self._ak.stock_hsgt_hist_em(symbol="深股通")
        return {
            'sh': self._format_north(df_sh.tail(1)) if df_sh is not None and not df_sh.empty else None,
            'sz': self._format_north(df_sz.tail(1)) if df_sz is not None and not df_sz.empty else None,
        }

    def _format_north(self, df):
        if df is None or df.empty: return None
        row = df.iloc[-1]
        return {'date': str(row.get('日期', '')),
                'inflow': round(float(row.get('当日资金流入', row.get('净流入', 0))), 2),
                'buy': round(float(row.get('当日成交净买额', row.get('净买额', 0))), 2)}

    async def fetch_lhb(self) -> List[Dict]:
        cache_key = "lhb"
        cached = self._cache_key(cache_key)
        if cached: return cached

        if self._ak and self.data_source_status.get('akshare') in ('ready', 'ok'):
            try:
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self._fetch_lhb_akshare),
                    timeout=10
                )
                if result:
                    self._set_cache(cache_key, result)
                    return result
            except Exception as e:
                logger.warning(f"AKShare 龙虎榜失败: {e}")

        return self._mock_lhb()

    def _fetch_lhb_akshare(self) -> List[Dict]:
        df = self._ak.stock_lhb_detail_daily_sina()
        if df is None or df.empty: return self._mock_lhb()
        return [{'code': str(row.get('代码', '')), 'name': str(row.get('名称', '')),
                 'close': round(float(row.get('收盘价', 0)), 2),
                 'change_pct': round(float(row.get('涨跌幅', 0)), 2),
                 'reason': str(row.get('上榜原因', '异动')),
                 'buy_amount': round(float(row.get('龙虎榜成交额', 0)) / 1e8, 2),
                 'net_buy': round(float(row.get('龙虎榜净买额', 0)) / 1e8, 2)}
                for _, row in df.head(20).iterrows()]

    async def fetch_market_fund_flow(self) -> Dict:
        cache_key = "market_fund"
        cached = self._cache_key(cache_key)
        if cached: return cached

        if self._ak and self.data_source_status.get('akshare') in ('ready', 'ok'):
            try:
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self._fetch_market_fund_akshare),
                    timeout=10
                )
                if result:
                    self._set_cache(cache_key, result)
                    return result
            except Exception as e:
                logger.warning(f"AKShare 资金流向失败: {e}")

        return self._mock_market_fund()

    def _fetch_market_fund_akshare(self) -> Dict:
        df = self._ak.stock_market_fund_flow()
        if df is None or df.empty: return self._mock_market_fund()
        row = df.iloc[-1] if len(df) > 0 else df.iloc[0]
        return {'date': str(row.get('日期', datetime.now().strftime('%Y-%m-%d'))),
                'super_in': round(float(row.get('超大单净流入', 0)), 2),
                'big_in': round(float(row.get('大单净流入', 0)), 2),
                'mid_in': round(float(row.get('中单净流入', 0)), 2),
                'small_in': round(float(row.get('小单净流入', 0)), 2),
                'total_in': round(float(row.get('主力净流入', 0)), 2)}

    async def fetch_sector_fund_flow(self) -> Dict[str, List]:
        cache_key = "sector_fund"
        cached = self._cache_key(cache_key)
        if cached: return cached

        result = {'industry': [], 'concept': []}
        if self._ak and self.data_source_status.get('akshare') in ('ready', 'ok'):
            try:
                df_ind = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, lambda: self._ak.stock_sector_fund_flow_rank(indicator="行业板块")
                    ), timeout=10
                )
                if df_ind is not None and not df_ind.empty:
                    result['industry'] = [{'name': str(row.get('名称', '')),
                                           'fund_flow': round(float(row.get('主力净流入', 0)), 2),
                                           'change_pct': round(float(row.get('涨跌幅', 0)), 2)}
                                          for _, row in df_ind.head(20).iterrows()]
                df_con = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, lambda: self._ak.stock_sector_fund_flow_rank(indicator="概念板块")
                    ), timeout=10
                )
                if df_con is not None and not df_con.empty:
                    result['concept'] = [{'name': str(row.get('名称', '')),
                                          'fund_flow': round(float(row.get('主力净流入', 0)), 2),
                                          'change_pct': round(float(row.get('涨跌幅', 0)), 2)}
                                         for _, row in df_con.head(20).iterrows()]
            except Exception as e:
                logger.warning(f"AKShare 板块资金失败: {e}")

        if not result['industry'] and not result['concept']:
            result = self._mock_sector_fund()

        self._set_cache(cache_key, result)
        return result

    async def fetch_news(self) -> List[Dict]:
        cache_key = "news"
        cached = self._cache_key(cache_key)
        if cached: return cached

        if self._ak and self.data_source_status.get('akshare') in ('ready', 'ok'):
            try:
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, self._fetch_news_akshare),
                    timeout=10
                )
                if result:
                    self._set_cache(cache_key, result)
                    return result
            except Exception as e:
                logger.warning(f"AKShare 新闻失败: {e}")

        return self._mock_news()

    def _fetch_news_akshare(self) -> List[Dict]:
        df = self._ak.stock_hot_search_baidu()
        if df is None or df.empty: return self._mock_news()
        return [{'title': str(row.get('keyword', '市场动态')),
                 'time': str(row.get('发布时间', datetime.now().strftime('%H:%M'))),
                 'source': str(row.get('来源', '财经快讯')),
                 'url': str(row.get('链接', '#'))}
                for _, row in df.head(20).iterrows()]

    async def fetch_stock_kline(self, symbol: str, days: int = 90) -> pd.DataFrame:
        cache_key = f"kline_{symbol}_{days}"
        cached = self._cache_key(cache_key)
        if cached is not None and isinstance(cached, dict) and 'data' in cached:
            return pd.DataFrame(cached['data'])

        if self._ak and self.data_source_status.get('akshare') in ('ready', 'ok'):
            try:
                df = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, self._fetch_kline_akshare, symbol, days
                    ), timeout=15
                )
                if df is not None and not df.empty:
                    self._set_cache(cache_key, {'data': df.to_dict('records')})
                    return df
            except Exception as e:
                logger.warning(f"AKShare K线失败 {symbol}: {e}")

        try:
            df = await self._fetch_kline_eastmoney(symbol, days)
            if df is not None and not df.empty:
                self._set_cache(cache_key, {'data': df.to_dict('records')})
                return df
        except Exception as e:
            logger.warning(f"东财K线失败 {symbol}: {e}")

        return self._mock_stock(symbol, days)

    def _fetch_kline_akshare(self, symbol: str, days: int) -> pd.DataFrame:
        code, market = symbol.split('.')
        ak_symbol = f"{market.lower()}{code}"
        df = self._ak.stock_zh_a_daily(symbol=ak_symbol, adjust="qfq")
        if df is None or df.empty: return None
        df = df.rename(columns={'date': 'time'})
        df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y%m%d')
        df['thscode'] = symbol
        df = df.sort_values('time').reset_index(drop=True)
        return df.tail(days)

    async def _fetch_kline_eastmoney(self, symbol: str, days: int) -> pd.DataFrame:
        code, market = symbol.split('.')
        secid = f"{'1' if market == 'SH' else '0'}.{code}"
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {'secid': secid, 'fields1': 'f1,f2,f3,f4,f5,f6',
                  'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
                  'klt': '101', 'fqt': '1', 'end': '20500101', 'lmt': days}
        session = await self._get_session()
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            klines = data.get('data', {}).get('klines', [])
            if not klines: return pd.DataFrame()
            records = []
            for k in klines:
                parts = k.split(',')
                if len(parts) >= 6:
                    records.append({'time': parts[0].replace('-', ''),
                                    'open': float(parts[1]), 'close': float(parts[2]),
                                    'high': float(parts[3]), 'low': float(parts[4]),
                                    'volume': int(float(parts[5])), 'thscode': symbol})
            df = pd.DataFrame(records)
            df = df.sort_values('time').reset_index(drop=True)
            return df

    async def fetch_index_kline(self, symbol: str, days: int = 90) -> pd.DataFrame:
        cache_key = f"idx_kline_{symbol}_{days}"
        cached = self._cache_key(cache_key)
        if cached is not None and isinstance(cached, dict) and 'data' in cached:
            return pd.DataFrame(cached['data'])

        if self._ak and self.data_source_status.get('akshare') in ('ready', 'ok'):
            try:
                df = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, self._fetch_index_kline_akshare, symbol, days
                    ), timeout=15
                )
                if df is not None and not df.empty:
                    self._set_cache(cache_key, {'data': df.to_dict('records')})
                    return df
            except Exception as e:
                logger.warning(f"AKShare 指数K线失败 {symbol}: {e}")

        return self._mock_index(symbol, days)

    def _fetch_index_kline_akshare(self, symbol: str, days: int) -> pd.DataFrame:
        code = symbol[2:]
        market = "sh" if symbol.startswith("sh") else "sz"
        ak_symbol = f"{market}{code}"
        df = self._ak.stock_zh_index_daily(symbol=ak_symbol)
        if df is None or df.empty: return None
        df = df.rename(columns={'date': 'time'})
        df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y%m%d')
        df['thscode'] = symbol
        df = df.sort_values('time').reset_index(drop=True)
        return df.tail(days)

    def _mock_today_all(self):
        np.random.seed(int(datetime.now().timestamp()) % 10000)
        codes = [f"{i:06d}" for i in range(1, 201)]
        names = [f"股票{i}" for i in range(1, 201)]
        df = pd.DataFrame({
            'code': codes, 'name': names,
            'close': np.random.uniform(5, 200, 200),
            'change_pct': np.random.normal(0, 2.8, 200),
            'volume': np.random.uniform(1e6, 5e8, 200),
            'amount': np.random.uniform(1e7, 5e9, 200),
            'vol_ratio': np.random.uniform(0.3, 5, 200),
            'turnover': np.random.uniform(0.5, 25, 200),
            'high': np.random.uniform(5, 210, 200),
            'low': np.random.uniform(4, 190, 200),
            'open': np.random.uniform(5, 200, 200),
            'pre_close': np.random.uniform(5, 200, 200),
            'symbol': [f"{c}.{'SH' if int(c[0])<4 else 'SZ'}" for c in codes],
            'wei_bi': np.random.uniform(-30, 30, 200),
            'outer_vol': np.random.uniform(1e5, 2e8, 200),
            'inner_vol': np.random.uniform(1e5, 2e8, 200),
            'pe': np.random.uniform(5, 200, 200),
            'pb': np.random.uniform(0.5, 15, 200),
            'total_mv': np.random.uniform(1e9, 5e11, 200),
            'float_mv': np.random.uniform(1e9, 3e11, 200),
        })
        return df

    def _mock_sectors(self):
        industry = pd.DataFrame({
            '板块名称': ['半导体','白酒','银行','新能源','医药','AI算力','券商','地产','煤炭','电力','军工','汽车','通信','计算机','传媒'],
            '涨跌幅': np.random.uniform(-2, 6, 15),
            '主力净流入': np.random.uniform(-5, 25, 15),
        })
        concept = pd.DataFrame({
            '板块名称': ['ChatGPT','机器人','充电桩','信创','元宇宙','钙钛矿','一体化压铸','TOPCon电池','数据要素','Chiplet','光刻胶','CPO','存储芯片','卫星互联网','智能驾驶'],
            '涨跌幅': np.random.uniform(-2, 7, 15),
            '主力净流入': np.random.uniform(-3, 20, 15),
        })
        return {'industry': industry, 'concept': concept}

    def _mock_north_fund(self):
        return {
            'sh': {'date': datetime.now().strftime('%Y-%m-%d'), 'inflow': round(np.random.uniform(-30, 50), 2), 'buy': round(np.random.uniform(-20, 40), 2)},
            'sz': {'date': datetime.now().strftime('%Y-%m-%d'), 'inflow': round(np.random.uniform(-30, 50), 2), 'buy': round(np.random.uniform(-20, 40), 2)},
        }

    def _mock_lhb(self):
        return [
            {'code':'000001','name':'平安银行','close':11.2,'change_pct':5.2,'reason':'日涨幅偏离值达7%','buy_amount':2.5,'net_buy':1.2},
            {'code':'300001','name':'特锐德','close':18.5,'change_pct':10.0,'reason':'日收盘价涨幅达20%','buy_amount':3.8,'net_buy':2.1},
            {'code':'600519','name':'贵州茅台','close':1680,'change_pct':3.2,'reason':'日涨幅偏离值达7%','buy_amount':8.5,'net_buy':3.2},
        ]

    def _mock_market_fund(self):
        return {'date': datetime.now().strftime('%Y-%m-%d'),
                'super_in': round(np.random.uniform(-50, 80), 2),
                'big_in': round(np.random.uniform(-80, 50), 2),
                'mid_in': round(np.random.uniform(-60, 40), 2),
                'small_in': round(np.random.uniform(-40, 30), 2),
                'total_in': round(np.random.uniform(-100, 150), 2)}

    def _mock_sector_fund(self):
        return {
            'industry': [
                {'name':'半导体','fund_flow':round(np.random.uniform(-10, 30), 2),'change_pct':round(np.random.uniform(-2, 6), 2)},
                {'name':'AI算力','fund_flow':round(np.random.uniform(-10, 25), 2),'change_pct':round(np.random.uniform(-2, 7), 2)},
                {'name':'新能源','fund_flow':round(np.random.uniform(-10, 20), 2),'change_pct':round(np.random.uniform(-2, 5), 2)},
            ],
            'concept': [
                {'name':'CPO','fund_flow':round(np.random.uniform(-5, 20), 2),'change_pct':round(np.random.uniform(-2, 8), 2)},
                {'name':'ChatGPT','fund_flow':round(np.random.uniform(-5, 18), 2),'change_pct':round(np.random.uniform(-2, 7), 2)},
                {'name':'存储芯片','fund_flow':round(np.random.uniform(-5, 15), 2),'change_pct':round(np.random.uniform(-2, 6), 2)},
            ],
        }

    def _mock_news(self):
        return [
            {'title':'央行降准0.25个百分点，释放流动性约5000亿','time':'09:15','source':'财联社','url':'#'},
            {'title':'北向资金今日净流入超80亿，重点加仓新能源','time':'10:30','source':'东方财富','url':'#'},
            {'title':'半导体板块集体爆发，多只芯片股涨停','time':'11:05','source':'证券时报','url':'#'},
            {'title':'工信部发布新能源汽车产业新规划','time':'13:20','source':'新华社','url':'#'},
            {'title':'白酒龙头二季度业绩超预期，机构上调目标价','time':'14:45','source':'券商中国','url':'#'},
            {'title':'美联储暗示9月可能暂停加息，全球股市反弹','time':'15:30','source':'华尔街见闻','url':'#'},
            {'title':'AI算力需求持续爆发，服务器厂商订单排至年底','time':'16:00','source':'科创板日报','url':'#'},
            {'title':'医药集采政策缓和，创新药板块估值修复','time':'09:45','source':'第一财经','url':'#'},
            {'title':'证监会发布活跃资本市场一揽子政策','time':'08:30','source':'证监会','url':'#'},
            {'title':'人民币汇率企稳回升，外资回流A股','time':'10:00','source':'21世纪经济报道','url':'#'},
        ]

    def _mock_index(self, symbol, days):
        np.random.seed(hash(symbol) % 2**32)
        base = {'sh000001':3200,'sz399001':10400,'sz399006':2100,
                'sh000300':3800,'sh000688':1050,'sh000905':5200,
                'sh000016':2400,'sz399005':6800,'sh000852':5800}.get(symbol, 100)
        dates = pd.date_range(end=datetime.now(), periods=days, freq='B')
        ret = np.random.normal(0.0002, 0.011, len(dates))
        prices = base * np.exp(np.cumsum(ret))
        df = pd.DataFrame({
            'time': [d.strftime('%Y%m%d') for d in dates],
            'open': prices * (1+np.random.normal(0,0.004,len(dates))),
            'high': prices * (1+abs(np.random.normal(0,0.008,len(dates)))),
            'low': prices * (1-abs(np.random.normal(0,0.008,len(dates)))),
            'close': prices,
            'volume': np.random.uniform(2e8,6e8,len(dates)).astype(int),
            'thscode': symbol,
        })
        return df.sort_values('time').reset_index(drop=True)

    def _mock_stock(self, symbol, days):
        np.random.seed(hash(symbol) % 2**32)
        base = {'600519.SH':1680,'000001.SZ':10.8,'300750.SZ':215,
                '600036.SH':32,'000858.SZ':148,'002594.SZ':258,
                '601318.SH':42,'600276.SH':38}.get(symbol, 50)
        dates = pd.date_range(end=datetime.now(), periods=days, freq='B')
        ret = np.random.normal(0.0003, 0.016, len(dates))
        prices = base * np.exp(np.cumsum(ret))
        df = pd.DataFrame({
            'time': [d.strftime('%Y%m%d') for d in dates],
            'open': prices * (1+np.random.normal(0,0.005,len(dates))),
            'high': prices * (1+abs(np.random.normal(0,0.009,len(dates)))),
            'low': prices * (1-abs(np.random.normal(0,0.009,len(dates)))),
            'close': prices,
            'volume': np.random.uniform(5e6,2e8,len(dates)).astype(int),
            'thscode': symbol,
        })
        return df.sort_values('time').reset_index(drop=True)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
