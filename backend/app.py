"""FastAPI主服务 V4 - 多源聚合+实时推送专业版"""
import os, asyncio, json, math
from datetime import datetime
from typing import Dict, Any, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn

from config import (HOST, PORT, INDEX_SYMBOLS, INDEX_NAMES, UPDATE_INTERVAL, CORS_ORIGINS,
                    NORTH_FUND_ENABLED, LHB_ENABLED, MARKET_FUND_FLOW_ENABLED, KLINE_DAYS)
from data_fetcher import DataFetcher
from quant_engine import QuantEngine
from market_scanner import MarketScanner
from recommender import Recommender

app_state = {
    'indices': {}, 'market': {}, 'sectors': {}, 'data_source_status': {},
    'news': [], 'picks': {}, 'chart_data': {},
    'north_fund': {}, 'lhb': [], 'market_fund': {},
    'sector_fund': {}, 'sentiment': {},
    'last_update': None, 'clients': set()
}
fetcher = DataFetcher()

def pd_ok(v):
    if v is None: return False
    try: return not math.isnan(v)
    except: return True

def calc_market_sentiment(market_data: List[Dict], indices: Dict) -> Dict:
    if not market_data:
        return {'index': 50, 'label': '中性', 'color': 'ac'}
    n = len(market_data)
    up = sum(1 for x in market_data if x.get('change_pct', 0) > 0)
    down = n - up
    limit_up = sum(1 for x in market_data if x.get('change_pct', 0) > 9.5)
    limit_down = sum(1 for x in market_data if x.get('change_pct', 0) < -9.5)
    avg_change = sum(x.get('change_pct', 0) for x in market_data) / n
    score_updown = (up / (up + down) * 25) if (up + down) > 0 else 12.5
    score_limit = (limit_up / (limit_up + limit_down) * 25) if (limit_up + limit_down) > 0 else 12.5
    score_avg = min(max((avg_change + 3) / 6 * 25, 0), 25)
    idx_score = 0
    idx_count = 0
    for k, v in indices.items():
        cp = v.get('change_pct', 0)
        idx_score += min(max((cp + 2) / 4 * 25, 0), 25)
        idx_count += 1
    score_idx = idx_score / idx_count if idx_count > 0 else 12.5
    total = score_updown + score_limit + score_avg + score_idx
    if total >= 75: label, color = '极度乐观', 'up'
    elif total >= 60: label, color = '乐观', 'up'
    elif total >= 50: label, color = '偏多', 'ac'
    elif total >= 40: label, color = '中性', 'ac'
    elif total >= 25: label, color = '偏空', 'down'
    else: label, color = '悲观', 'down'
    return {
        'index': round(total, 1), 'label': label, 'color': color,
        'detail': {'up': up, 'down': down, 'limit_up': limit_up, 'limit_down': limit_down,
                   'avg_change': round(avg_change, 2)}
    }

async def process_stock_detail(symbol: str) -> Dict:
    try:
        df = await fetcher.fetch_stock_kline(symbol, days=KLINE_DAYS)
        if df is None or df.empty:
            return None
        processed = QuantEngine.process(df)
        latest = processed.iloc[-1]
        score = QuantEngine.score(processed)
        recent = processed.tail(60)
        return {
            'symbol': symbol, 'close': round(latest['close'], 2),
            'change_pct': round(latest['change_pct'], 2) if pd_ok(latest['change_pct']) else 0,
            'volume': int(latest['volume']), 'total_score': score['total'],
            'signal': score['signal'], 'risk': score['risk'], 'indicators': score['indicators'],
            'kline': {
                'dates': [str(x)[:10] for x in recent['time'].values],
                'open': [round(x, 2) for x in recent['open'].values],
                'high': [round(x, 2) for x in recent['high'].values],
                'low': [round(x, 2) for x in recent['low'].values],
                'close': [round(x, 2) for x in recent['close'].values],
                'volume': [int(x) for x in recent['volume'].values],
                'ma5': [round(x, 2) if pd_ok(x) else None for x in recent['ma5'].values],
                'ma10': [round(x, 2) if pd_ok(x) else None for x in recent['ma10'].values],
                'ma20': [round(x, 2) if pd_ok(x) else None for x in recent['ma20'].values],
                'macd': [round(x, 3) if pd_ok(x) else None for x in recent['macd'].values],
                'macd_signal': [round(x, 3) if pd_ok(x) else None for x in recent['macd_signal'].values],
                'kdj_k': [round(x, 1) if pd_ok(x) else None for x in recent['kdj_k'].values],
                'kdj_d': [round(x, 1) if pd_ok(x) else None for x in recent['kdj_d'].values],
                'kdj_j': [round(x, 1) if pd_ok(x) else None for x in recent['kdj_j'].values],
                'rsi': [round(x, 1) if pd_ok(x) else None for x in recent['rsi'].values],
                'cci': [round(x, 1) if pd_ok(x) else None for x in recent['cci'].values],
                'bb_upper': [round(x, 2) if pd_ok(x) else None for x in recent['bb_upper'].values],
                'bb_lower': [round(x, 2) if pd_ok(x) else None for x in recent['bb_lower'].values],
            }
        }
    except Exception as e:
        print(f"Stock detail error {symbol}: {e}")
        return None

async def update_loop():
    while True:
        try:
            print(f"[{datetime.now()}] 开始全市场扫描 V4...")

            # 1. 指数数据（并行获取）
            print("  获取指数实时行情...")
            index_tasks = [fetcher.fetch_index_realtime(sym) for sym in INDEX_SYMBOLS]
            index_results = await asyncio.gather(*index_tasks, return_exceptions=True)
            indices = {}
            chart_data = {}
            for sym, result in zip(INDEX_SYMBOLS, index_results):
                if isinstance(result, Exception) or result is None:
                    continue
                key = sym.replace('.', '_')
                name = INDEX_NAMES.get(sym, sym)
                indices[key] = {
                    'name': name, 'ticker': sym,
                    'close': result.get('close', 0),
                    'change_pct': result.get('change_pct', 0),
                    'change': result.get('change', 0),
                    'volume': result.get('volume', 0),
                    'amount': result.get('amount', 0),
                    'open': result.get('open', 0),
                    'high': result.get('high', 0),
                    'low': result.get('low', 0),
                    'pre_close': result.get('pre_close', 0),
                }
                try:
                    df_kline = await asyncio.wait_for(
                        fetcher.fetch_index_kline(sym, days=30), timeout=10
                    )
                    if df_kline is not None and not df_kline.empty:
                        p = QuantEngine.process(df_kline)
                        d30 = p.tail(30)
                        chart_data[key] = {
                            'dates': [str(x)[:10] for x in d30['time'].values],
                            'close': [round(x, 2) for x in d30['close'].values],
                            'volume': [int(x) for x in d30['volume'].values],
                            'ma5': [round(x, 2) if pd_ok(x) else None for x in d30['ma5'].values],
                            'ma10': [round(x, 2) if pd_ok(x) else None for x in d30['ma10'].values],
                            'ma20': [round(x, 2) if pd_ok(x) else None for x in d30['ma20'].values],
                        }
                except Exception as e:
                    print(f"Index kline error {sym}: {e}")
            app_state['indices'] = indices
            app_state['chart_data'] = chart_data

            # 2. 全市场扫描
            print("  扫描全市场...")
            today_all = await fetcher.fetch_today_all()
            scanned = MarketScanner.scan(today_all, top_n=300)
            app_state['market'] = MarketScanner.format_scan_result(scanned)

            # 3. 板块数据
            print("  获取板块数据...")
            sectors = await fetcher.fetch_sectors()
            app_state['sectors'] = {}
            for stype, df in sectors.items():
                if df is not None and not df.empty:
                    app_state['sectors'][stype] = []
                    for _, row in df.iterrows():
                        app_state['sectors'][stype].append({
                            'name': str(row.get('板块名称', row.get('name', ''))),
                            'change_pct': round(float(row.get('涨跌幅', row.get('change', 0))), 2),
                            'fund_flow': round(float(row.get('主力净流入', row.get('fund', 0))), 1),
                        })

            # 4. 板块资金流向
            print("  获取板块资金流向...")
            app_state['sector_fund'] = await fetcher.fetch_sector_fund_flow()

            # 5. 北向资金
            if NORTH_FUND_ENABLED:
                print("  获取北向资金...")
                app_state['north_fund'] = await fetcher.fetch_north_fund()

            # 6. 龙虎榜
            if LHB_ENABLED:
                print("  获取龙虎榜...")
                app_state['lhb'] = await fetcher.fetch_lhb()

            # 7. 大盘资金流向
            if MARKET_FUND_FLOW_ENABLED:
                print("  获取大盘资金流向...")
                app_state['market_fund'] = await fetcher.fetch_market_fund_flow()

            # 8. 新闻
            print("  获取新闻...")
            app_state['news'] = await fetcher.fetch_news()

            # 9. 市场情绪
            print("  计算市场情绪...")
            app_state['sentiment'] = calc_market_sentiment(app_state['market'], app_state['indices'])
            app_state['data_source_status'] = fetcher.data_source_status

            # 10. 今日推荐
            print("  生成推荐...")
            app_state['picks'] = Recommender.generate_daily_picks(
                scanned, sectors, app_state['news'],
                app_state.get('north_fund'), app_state.get('lhb')
            )

            app_state['last_update'] = datetime.now().isoformat()
            print(f"[{datetime.now()}] 更新完成")

            await broadcast({
                'type': 'update',
                'indices': app_state['indices'],
                'market': app_state['market'],
                'sectors': app_state['sectors'],
                'sector_fund': app_state['sector_fund'],
                'news': app_state['news'],
                'picks': app_state['picks'],
                'chart_data': app_state['chart_data'],
                'north_fund': app_state.get('north_fund', {}),
                'lhb': app_state.get('lhb', []),
                'market_fund': app_state.get('market_fund', {}),
                'sentiment': app_state.get('sentiment', {}),
                'data_source_status': app_state.get('data_source_status', {}),
                'timestamp': app_state['last_update']
            })
        except Exception as e:
            print(f"Update error: {e}")
            import traceback
            traceback.print_exc()

        await asyncio.sleep(UPDATE_INTERVAL)

async def broadcast(msg):
    dead = set()
    for ws in app_state['clients']:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.add(ws)
    app_state['clients'] -= dead

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(update_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await fetcher.close()

app = FastAPI(title="A股量化监控API V4", version="4.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=CORS_ORIGINS, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def root():
    return HTMLResponse(content=FRONTEND_HTML)

@app.get("/api/data")
def get_data():
    return {
        'indices': app_state.get('indices', {}),
        'market': app_state.get('market', []),
        'sectors': app_state.get('sectors', {}),
        'sector_fund': app_state.get('sector_fund', {}),
        'news': app_state.get('news', []),
        'picks': app_state.get('picks', {}),
        'chart_data': app_state.get('chart_data', {}),
        'north_fund': app_state.get('north_fund', {}),
        'lhb': app_state.get('lhb', []),
        'market_fund': app_state.get('market_fund', {}),
        'sentiment': app_state.get('sentiment', {}),
        'data_source_status': app_state.get('data_source_status', {}),
        'last_update': app_state.get('last_update')
    }

@app.get("/api/market")
def get_market(limit: int = 50, offset: int = 0):
    market = app_state.get('market', [])
    return {'data': market[offset:offset+limit], 'total': len(market)}

@app.get("/api/sectors")
def get_sectors():
    return app_state.get('sectors', {})

@app.get("/api/sector-fund")
def get_sector_fund():
    return app_state.get('sector_fund', {})

@app.get("/api/news")
def get_news():
    return {'data': app_state.get('news', [])}

@app.get("/api/picks")
def get_picks():
    return app_state.get('picks', {})

@app.get("/api/north-fund")
def get_north_fund():
    return app_state.get('north_fund', {})

@app.get("/api/lhb")
def get_lhb():
    return app_state.get('lhb', [])

@app.get("/api/market-fund")
def get_market_fund():
    return app_state.get('market_fund', {})

@app.get("/api/sentiment")
def get_sentiment():
    return app_state.get('sentiment', {})

@app.get("/api/stock/{symbol}/detail")
async def get_stock_detail(symbol: str):
    detail = await process_stock_detail(symbol)
    if detail is None:
        return {'error': '无法获取个股数据', 'symbol': symbol}
    return detail

@app.get("/api/stock/{symbol}/kline")
async def get_stock_kline(symbol: str, days: int = Query(60, ge=5, le=250)):
    try:
        df = await fetcher.fetch_stock_kline(symbol, days=days)
        if df is None or df.empty:
            return {'error': '无法获取K线数据'}
        processed = QuantEngine.process(df)
        recent = processed.tail(days)
        return {
            'symbol': symbol,
            'dates': [str(x)[:10] for x in recent['time'].values],
            'open': [round(x, 2) for x in recent['open'].values],
            'high': [round(x, 2) for x in recent['high'].values],
            'low': [round(x, 2) for x in recent['low'].values],
            'close': [round(x, 2) for x in recent['close'].values],
            'volume': [int(x) for x in recent['volume'].values],
        }
    except Exception as e:
        return {'error': str(e)}

@app.get("/api/rotation")
def get_rotation():
    picks = app_state.get('picks', {})
    return picks.get('rotation', [])

@app.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket.accept()
    app_state['clients'].add(websocket)
    if app_state.get('indices'):
        await websocket.send_json({
            'type': 'init',
            'indices': app_state['indices'],
            'market': app_state.get('market', []),
            'sectors': app_state.get('sectors', {}),
            'sector_fund': app_state.get('sector_fund', {}),
            'news': app_state.get('news', []),
            'picks': app_state.get('picks', {}),
            'chart_data': app_state.get('chart_data', {}),
            'north_fund': app_state.get('north_fund', {}),
            'lhb': app_state.get('lhb', []),
            'market_fund': app_state.get('market_fund', {}),
            'sentiment': app_state.get('sentiment', {}),
            'data_source_status': app_state.get('data_source_status', {}),
            'timestamp': app_state.get('last_update')
        })
    try:
        while True:
            msg = await websocket.receive_text()
            if msg == 'ping':
                await websocket.send_text('pong')
            elif msg.startswith('detail:'):
                symbol = msg[7:]
                detail = await process_stock_detail(symbol)
                if detail:
                    await websocket.send_json({'type': 'stock_detail', 'data': detail})
    except (WebSocketDisconnect, Exception):
        app_state['clients'].discard(websocket)


FRONTEND_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>A股量化全景监控中心 V4</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
:root{--bg:#0a0e1a;--card:#111827;--card2:#1a2332;--border:#1e293b;--up:#ef4444;--down:#22c55e;--ac:#f59e0b;--tt:#94a3b8;--text:#e2e8f0;--text2:#94a3b8;--primary:#3b82f6;--primary2:#2563eb;--shadow:0 4px 20px rgba(0,0,0,.4)}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;line-height:1.5}
.container{max-width:1600px;margin:0 auto;padding:16px}
header{display:flex;justify-content:space-between;align-items:center;padding:16px 0;border-bottom:1px solid var(--border);margin-bottom:20px;flex-wrap:wrap;gap:12px}
header h1{font-size:24px;font-weight:700;background:linear-gradient(90deg,var(--primary),#8b5cf6);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.status-bar{display:flex;gap:16px;align-items:center;font-size:13px;color:var(--text2);flex-wrap:wrap}
.status-bar .badge{padding:4px 10px;border-radius:20px;background:var(--card2);border:1px solid var(--border);font-size:12px}
.status-bar .badge.ok{border-color:#22c55e33;color:#22c55e}
.status-bar .badge.warn{border-color:#f59e0b33;color:#f59e0b}
.status-bar .badge.err{border-color:#ef444433;color:#ef4444}
.grid{display:grid;gap:16px;margin-bottom:20px}
.grid-4{grid-template-columns:repeat(auto-fit,minmax(280px,1fr))}
.grid-3{grid-template-columns:repeat(auto-fit,minmax(350px,1fr))}
.grid-2{grid-template-columns:repeat(auto-fit,minmax(500px,1fr))}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;box-shadow:var(--shadow);transition:transform .2s}
.card:hover{transform:translateY(-2px)}
.card-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.card-title{font-size:15px;font-weight:600;color:var(--text)}
.card-sub{font-size:12px;color:var(--text2)}
.index-card{padding:16px;text-align:center;cursor:pointer;position:relative;overflow:hidden}
.index-card .name{font-size:14px;color:var(--text2);margin-bottom:4px}
.index-card .price{font-size:28px;font-weight:700;margin:4px 0}
.index-card .change{font-size:14px;font-weight:600}
.index-card .extra{display:flex;justify-content:center;gap:16px;margin-top:8px;font-size:12px;color:var(--text2)}
.up{color:var(--up)} .down{color:var(--down)} .ac{color:var(--ac)}
.sentiment-gauge{width:120px;height:120px;margin:0 auto;position:relative}
.sentiment-gauge .value{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);font-size:28px;font-weight:700}
.sentiment-gauge .label{position:absolute;bottom:10px;left:50%;transform:translateX(-50%);font-size:12px;color:var(--text2)}
.table{width:100%;border-collapse:collapse;font-size:13px}
.table th{text-align:left;padding:10px 8px;color:var(--text2);font-weight:500;border-bottom:1px solid var(--border);white-space:nowrap;position:sticky;top:0;background:var(--card);z-index:1}
.table td{padding:10px 8px;border-bottom:1px solid rgba(30,41,59,0.33);vertical-align:middle}
.table tr:hover td{background:rgba(59,130,246,.05)}
.table .sym{font-weight:600;color:var(--text);cursor:pointer}
.table .sym:hover{color:var(--primary)}
.tag{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;margin:1px;background:var(--card2);border:1px solid var(--border)}
.tag.up{background:rgba(239,68,68,.1);border-color:rgba(239,68,68,.3);color:var(--up)}
.tag.down{background:rgba(34,197,94,.1);border-color:rgba(34,197,94,.3);color:var(--down)}
.tag.ac{background:rgba(245,158,11,.1);border-color:rgba(245,158,11,.3);color:var(--ac)}
.news-item{padding:10px 0;border-bottom:1px solid var(--border);display:flex;gap:12px;align-items:flex-start}
.news-item:last-child{border-bottom:none}
.news-time{font-size:12px;color:var(--text2);white-space:nowrap;min-width:50px}
.news-title{font-size:13px;color:var(--text);line-height:1.4}
.news-source{font-size:11px;color:var(--text2);margin-top:2px}
.sector-bar{display:flex;align-items:center;gap:10px;margin:6px 0;font-size:13px}
.sector-bar .name{min-width:80px;font-weight:500}
.sector-bar .bar{flex:1;height:8px;background:var(--card2);border-radius:4px;overflow:hidden;position:relative}
.sector-bar .bar-fill{height:100%;border-radius:4px;transition:width .5s}
.sector-bar .val{min-width:50px;text-align:right;font-size:12px}
.picks-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}
.pick-card{background:var(--card2);border:1px solid var(--border);border-radius:10px;padding:14px;cursor:pointer;transition:all .2s}
.pick-card:hover{border-color:var(--primary);box-shadow:0 0 15px rgba(59,130,246,.15)}
.pick-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.pick-name{font-size:15px;font-weight:600}
.pick-score{font-size:18px;font-weight:700;color:var(--primary)}
.pick-reason{font-size:12px;color:var(--text2);line-height:1.5;margin-top:6px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.modal-overlay{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.7);display:none;justify-content:center;align-items:center;z-index:1000;padding:20px}
.modal-overlay.active{display:flex}
.modal{background:var(--card);border:1px solid var(--border);border-radius:16px;max-width:900px;width:100%;max-height:90vh;overflow-y:auto;box-shadow:var(--shadow)}
.modal-header{padding:20px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;background:var(--card);z-index:10;border-radius:16px 16px 0 0}
.modal-body{padding:20px}
.close-btn{width:32px;height:32px;border-radius:50%;border:1px solid var(--border);background:var(--card2);color:var(--text);cursor:pointer;font-size:18px;display:flex;align-items:center;justify-content:center}
.close-btn:hover{background:var(--border)}
.chart-container{height:320px;margin:16px 0}
.indicators-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin:16px 0}
.indicator-item{background:var(--card2);padding:12px;border-radius:8px;text-align:center}
.indicator-item .label{font-size:11px;color:var(--text2);margin-bottom:4px}
.indicator-item .value{font-size:18px;font-weight:700}
.tabs{display:flex;gap:4px;margin-bottom:16px;border-bottom:1px solid var(--border);padding-bottom:8px}
.tab{padding:8px 16px;border-radius:8px;cursor:pointer;font-size:13px;color:var(--text2);border:none;background:none;transition:all .2s}
.tab:hover{color:var(--text)}
.tab.active{background:var(--primary);color:#fff}
.tab-content{display:none}
.tab-content.active{display:block}
.scroll-box{max-height:400px;overflow-y:auto;padding-right:8px}
.scroll-box::-webkit-scrollbar{width:4px}
.scroll-box::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
.fund-flow{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin:12px 0}
.fund-item{text-align:center;padding:12px;background:var(--card2);border-radius:8px}
.fund-item .label{font-size:11px;color:var(--text2)}
.fund-item .value{font-size:20px;font-weight:700;margin-top:4px}
@media(max-width:768px){.grid-4,.grid-3,.grid-2{grid-template-columns:1fr}.picks-grid{grid-template-columns:1fr}.modal{max-width:100%}}
.load{position:fixed;top:0;left:0;right:0;bottom:0;background:var(--bg);display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:9999}
.spin{width:40px;height:40px;border:3px solid var(--border);border-top-color:var(--primary);border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.fade-in{animation:fadeIn .5s ease}
@keyframes fadeIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
</style>
</head>
<body>
<div class="load" id="loader"><div class="spin"></div><p style="margin-top:16px;color:var(--text2)">系统初始化中...</p></div>
<div class="container" id="app" style="display:none">
<header>
<h1>📊 A股量化全景监控中心 V4</h1>
<div class="status-bar">
<span id="update-time">--:--:--</span>
<span class="badge" id="data-status">数据加载中</span>
<span class="badge" id="ws-status">连接中</span>
<span class="badge" id="source-status">源检测中</span>
</div>
</header>

<div class="grid grid-4" id="indices-grid"></div>

<div class="grid grid-2">
<div class="card">
<div class="card-header"><span class="card-title">📈 市场情绪指数</span><span class="card-sub" id="sentiment-detail"></span></div>
<div style="display:flex;align-items:center;gap:20px">
<div class="sentiment-gauge"><canvas id="sentiment-canvas" width="120" height="120"></canvas><div class="value" id="sentiment-value">--</div><div class="label" id="sentiment-label">--</div></div>
<div style="flex:1" id="sentiment-breakdown"></div>
</div>
</div>
<div class="card">
<div class="card-header"><span class="card-title">💰 大盘资金流向</span><span class="card-sub">单位：亿元</span></div>
<div class="fund-flow" id="fund-flow"></div>
</div>
</div>

<div class="grid grid-2">
<div class="card">
<div class="card-header">
<div class="tabs">
<button class="tab active" onclick="switchTab('industry')">行业板块</button>
<button class="tab" onclick="switchTab('concept')">概念板块</button>
</div>
</div>
<div class="tab-content active" id="tab-industry"><div id="industry-bars"></div></div>
<div class="tab-content" id="tab-concept"><div id="concept-bars"></div></div>
</div>
<div class="card">
<div class="card-header"><span class="card-title">📰 财经快讯</span></div>
<div class="scroll-box" id="news-list"></div>
</div>
</div>

<div class="card">
<div class="card-header"><span class="card-title">🔄 板块轮动分析</span></div>
<div id="rotation-analysis"></div>
</div>

<div class="card">
<div class="card-header"><span class="card-title">⭐ 今日推荐</span><span class="card-sub" id="picks-summary"></span></div>
<div class="picks-grid" id="picks-grid"></div>
</div>

<div class="card">
<div class="card-header"><span class="card-title">🔍 全市场异动扫描</span><span class="card-sub" id="market-count"></span></div>
<div class="scroll-box"><table class="table"><thead><tr><th>代码</th><th>名称</th><th>价格</th><th>涨跌</th><th>量比</th><th>换手</th><th>评分</th><th>信号</th><th>标签</th></tr></thead><tbody id="market-table"></tbody></table></div>
</div>

<div class="card">
<div class="card-header"><span class="card-title">📉 指数走势</span></div>
<div id="index-chart" class="chart-container"></div>
</div>
</div>

<div class="modal-overlay" id="modal-overlay">
<div class="modal" id="modal"></div>
</div>

<script>
const app={data:{},ws:null,reconnectTimer:null,charts:{}};
function fmt(n,d=2){return n===undefined||n===null?'--':(+n).toFixed(d)}
function fmtPct(n){return n===undefined||n===null?'--':(n>0?'+':'')+fmt(n,2)+'%'}
function fmtWan(n){if(!n)return'--';return n>=1e8?(n/1e8).toFixed(1)+'亿':n>=1e4?(n/1e4).toFixed(1)+'万':fmt(n)}
function initWS(){
  const proto=location.protocol==='https:'?'wss:':'ws:';
  app.ws=new WebSocket(proto+'//'+location.host+'/ws');
  app.ws.onopen=()=>{document.getElementById('ws-status').textContent='🟢 实时连接';document.getElementById('ws-status').className='badge ok';};
  app.ws.onmessage=e=>{const msg=JSON.parse(e.data);if(msg.type==='update'||msg.type==='init'){app.data=msg;render();}};
  app.ws.onclose=()=>{document.getElementById('ws-status').textContent='🔴 断开';document.getElementById('ws-status').className='badge err';app.reconnectTimer=setTimeout(initWS,3000);};
  app.ws.onerror=()=>{document.getElementById('ws-status').textContent='⚠️ 错误';document.getElementById('ws-status').className='badge warn';};
}
async function fetchData(){
  try{
    const r=await fetch('/api/data');
    const d=await r.json();
    app.data=d;
    render();
    document.getElementById('loader').style.display='none';
    document.getElementById('app').style.display='block';
  }catch(e){console.error(e);setTimeout(fetchData,2000);}
}
function render(){
  const d=app.data;
  if(!d||!d.indices)return;
  document.getElementById('update-time').textContent=new Date().toLocaleTimeString();
  const st=d.data_source_status||{};
  let srcText='';
  if(st.akshare==='ok')srcText='AKShare';
  else if(st.eastmoney==='ok')srcText='东方财富';
  else if(st.sina==='ok')srcText='新浪财经';
  else srcText='模拟数据';
  const isReal=st.realtime_data===true;
  document.getElementById('data-status').textContent=isReal?'🟢 实时数据':'🟡 模拟数据';
  document.getElementById('data-status').className='badge '+(isReal?'ok':'warn');
  document.getElementById('source-status').textContent='源:'+srcText;
  document.getElementById('source-status').className='badge '+(isReal?'ok':'warn');
  renderIndices(d.indices);
  renderSentiment(d.sentiment);
  renderFundFlow(d.market_fund);
  renderSectors(d.sectors);
  renderNews(d.news);
  renderRotation(d.picks?.rotation);
  renderPicks(d.picks);
  renderMarket(d.market);
  renderIndexChart(d.chart_data);
}
function renderIndices(indices){
  const el=document.getElementById('indices-grid');
  el.innerHTML=Object.entries(indices).map(([k,v])=>{
    const cls=v.change_pct>=0?'up':'down';
    const sign=v.change_pct>=0?'+':'';
    return `<div class="card index-card fade-in" onclick="showIndexDetail('${k}')">
      <div class="name">${v.name}</div>
      <div class="price ${cls}">${fmt(v.close,2)}</div>
      <div class="change ${cls}">${sign}${fmt(v.change,2)} (${sign}${fmt(v.change_pct,2)}%)</div>
      <div class="extra"><span>量:${fmtWan(v.volume)}</span><span>额:${fmtWan(v.amount)}亿</span></div>
    </div>`;
  }).join('');
}
function renderSentiment(s){
  if(!s)return;
  document.getElementById('sentiment-value').textContent=s.index;
  document.getElementById('sentiment-value').className='value '+s.color;
  document.getElementById('sentiment-label').textContent=s.label;
  document.getElementById('sentiment-label').className='label '+s.color;
  document.getElementById('sentiment-detail').textContent=`涨跌比 ${s.detail?.up||0}:${s.detail?.down||0} | 涨停 ${s.detail?.limit_up||0} | 跌停 ${s.detail?.limit_down||0}`;
  drawGauge(s.index,s.color);
}
function drawGauge(val,color){
  const c=document.getElementById('sentiment-canvas');
  const ctx=c.getContext('2d');
  ctx.clearRect(0,0,120,120);
  const cx=60,cy=60,r=50;
  ctx.beginPath();ctx.arc(cx,cy,r,Math.PI*.8,Math.PI*.2);ctx.strokeStyle='#1e293b';ctx.lineWidth=8;ctx.stroke();
  const pct=val/100;
  const end=Math.PI*.8+ pct*(Math.PI*2.4);
  const colors={up:'#ef4444',down:'#22c55e',ac:'#f59e0b',tt:'#94a3b8'};
  ctx.beginPath();ctx.arc(cx,cy,r,Math.PI*.8,end);ctx.strokeStyle=colors[color]||colors.ac;ctx.lineWidth=8;ctx.lineCap='round';ctx.stroke();
}
function renderFundFlow(f){
  if(!f)return;
  const el=document.getElementById('fund-flow');
  const items=[{l:'超大单',v:f.super_in},{l:'大单',v:f.big_in},{l:'中单',v:f.mid_in},{l:'小单',v:f.small_in}];
  el.innerHTML=items.map(i=>`<div class="fund-item"><div class="label">${i.l}</div><div class="value ${i.v>=0?'up':'down'}">${i.v>=0?'+':''}${fmt(i.v,1)}</div></div>`).join('');
}
function renderSectors(sectors){
  if(!sectors)return;
  const renderBar=(data,elId)=>{
    const el=document.getElementById(elId);
    if(!data){el.innerHTML='';return;}
    const maxFund=Math.max(...data.map(x=>Math.abs(x.fund_flow||0)),1);
    el.innerHTML=data.slice(0,15).map(s=>{
      const pct=s.change_pct||0;
      const fund=s.fund_flow||0;
      const cls=pct>=0?'up':'down';
      const barW=Math.min(Math.abs(fund)/maxFund*100,100);
      const barColor=fund>=0?'#ef4444':'#22c55e';
      return `<div class="sector-bar">
        <span class="name">${s.name}</span>
        <div class="bar"><div class="bar-fill" style="width:${barW}%;background:${barColor}"></div></div>
        <span class="val ${cls}">${pct>=0?'+':''}${fmt(pct,2)}%</span>
        <span class="val" style="color:${fund>=0?'#ef4444':'#22c55e'}">${fund>=0?'+':''}${fmt(fund,1)}亿</span>
      </div>`;
    }).join('');
  };
  renderBar(sectors.industry,'industry-bars');
  renderBar(sectors.concept,'concept-bars');
}
function switchTab(tab){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t=>t.classList.remove('active'));
  event.target.classList.add('active');
  document.getElementById('tab-'+tab).classList.add('active');
}
function renderNews(news){
  if(!news)return;
  document.getElementById('news-list').innerHTML=news.slice(0,15).map(n=>`<div class="news-item">
    <span class="news-time">${n.time}</span>
    <div><div class="news-title">${n.title}</div><div class="news-source">${n.source}</div></div>
  </div>`).join('');
}
function renderRotation(rotation){
  if(!rotation){document.getElementById('rotation-analysis').innerHTML='';return;}
  const colors={up:'#ef4444',ac:'#f59e0b',down:'#22c55e'};
  document.getElementById('rotation-analysis').innerHTML=rotation.map(r=>`<div style="display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--border)">
    <span style="padding:4px 10px;border-radius:6px;background:${colors[r.color]}22;color:${colors[r.color]};font-size:12px;font-weight:600;white-space:nowrap">${r.type}</span>
    <span style="font-size:13px;color:var(--text2)">${r.desc}</span>
    <span style="font-size:13px;color:var(--text);flex:1">${r.sectors?.join('、')||''}</span>
  </div>`).join('');
}
function renderPicks(picks){
  if(!picks)return;
  document.getElementById('picks-summary').innerHTML=picks.summary||'';
  document.getElementById('picks-grid').innerHTML=(picks.picks||[]).map(p=>{
    const cls=p.change_pct>=0?'up':'down';
    return `<div class="pick-card" onclick="showStockDetail('${p.symbol}')">
      <div class="pick-header"><span class="pick-name">${p.name} <span style="font-size:12px;color:var(--text2)">${p.code}</span></span><span class="pick-score">${p.total_score}</span></div>
      <div style="display:flex;gap:12px;font-size:13px">
        <span class="${cls}">${p.change_pct>=0?'+':''}${fmt(p.change_pct,2)}%</span>
        <span style="color:var(--text2)">¥${fmt(p.close,2)}</span>
        <span class="tag ${p.signal.includes('买入')?'up':p.signal.includes('卖出')?'down':'ac'}">${p.signal}</span>
      </div>
      <div class="pick-reason">${p.reason}</div>
      <div style="margin-top:6px">${(p.tags||[]).map(t=>`<span class="tag">${t}</span>`).join('')}</div>
    </div>`;
  }).join('');
}
function renderMarket(market){
  if(!market)return;
  document.getElementById('market-count').textContent=`共 ${market.length} 只`;
  document.getElementById('market-table').innerHTML=market.slice(0,50).map(m=>{
    const cls=m.change_pct>=0?'up':'down';
    return `<tr onclick="showStockDetail('${m.symbol}')">
      <td class="sym">${m.code}</td>
      <td>${m.name}</td>
      <td>¥${fmt(m.close,2)}</td>
      <td class="${cls}">${m.change_pct>=0?'+':''}${fmt(m.change_pct,2)}%</td>
      <td>${fmt(m.vol_ratio,2)}</td>
      <td>${fmt(m.turnover,2)}%</td>
      <td style="font-weight:700;color:var(--primary)">${m.total_score}</td>
      <td><span class="tag ${m.signal.includes('买入')?'up':m.signal.includes('卖出')?'down':'ac'}">${m.signal}</span></td>
      <td>${(m.tags||[]).slice(0,3).map(t=>`<span class="tag">${t}</span>`).join('')}</td>
    </tr>`;
  }).join('');
}
function renderIndexChart(chartData){
  if(!chartData||Object.keys(chartData).length===0)return;
  const dom=document.getElementById('index-chart');
  if(!app.charts.index)app.charts.index=echarts.init(dom);
  const firstKey=Object.keys(chartData)[0];
  const cd=chartData[firstKey];
  if(!cd||!cd.dates)return;
  app.charts.index.setOption({
    backgroundColor:'transparent',
    grid:{left:40,right:20,top:20,bottom:40},
    tooltip:{trigger:'axis'},
    xAxis:{type:'category',data:cd.dates,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#94a3b8',fontSize:11}},
    yAxis:{type:'value',axisLine:{lineStyle:{color:'#334155'}},splitLine:{lineStyle:{color:'#1e293b'}},axisLabel:{color:'#94a3b8',fontSize:11}},
    series:[
      {name:'收盘',type:'line',data:cd.close,smooth:true,lineStyle:{color:'#3b82f6',width:2},symbol:'none'},
      {name:'MA5',type:'line',data:cd.ma5,smooth:true,lineStyle:{color:'#f59e0b',width:1},symbol:'none'},
      {name:'MA20',type:'line',data:cd.ma20,smooth:true,lineStyle:{color:'#8b5cf6',width:1},symbol:'none'},
    ]
  });
}
async function showStockDetail(symbol){
  const overlay=document.getElementById('modal-overlay');
  const modal=document.getElementById('modal');
  overlay.classList.add('active');
  modal.innerHTML=`<div class="modal-header"><div><h3 style="font-size:18px">${symbol}</h3><p style="font-size:12px;color:var(--text2)">加载中...</p></div><button class="close-btn" onclick="closeModal()">&times;</button></div><div class="modal-body"><div class="spin" style="margin:40px auto"></div></div>`;
  try{
    const r=await fetch('/api/stock/'+symbol+'/detail');
    const d=await r.json();
    if(d.error){modal.innerHTML=`<div class="modal-header"><h3>错误</h3><button class="close-btn" onclick="closeModal()">&times;</button></div><div class="modal-body">${d.error}</div>`;return;}
    const k=d.kline;
    const cls=d.change_pct>=0?'up':'down';
    modal.innerHTML=`<div class="modal-header">
      <div><h3 style="font-size:18px">${d.symbol}</h3><p style="font-size:12px;color:var(--text2)">评分:${d.total_score} | 信号:${d.signal} | 风险:${d.risk}</p></div>
      <button class="close-btn" onclick="closeModal()">&times;</button>
    </div>
    <div class="modal-body">
      <div style="display:flex;gap:20px;margin-bottom:16px;flex-wrap:wrap">
        <div><span style="font-size:32px;font-weight:700;${cls}">¥${fmt(d.close,2)}</span><span class="${cls}" style="margin-left:12px;font-size:16px">${d.change_pct>=0?'+':''}${fmt(d.change_pct,2)}%</span></div>
        <div style="flex:1"></div>
        <span class="tag ${d.signal.includes('买入')?'up':d.signal.includes('卖出')?'down':'ac'}">${d.signal}</span>
      </div>
      <div class="indicators-grid">
        <div class="indicator-item"><div class="label">RSI</div><div class="value">${d.indicators?.rsi}</div></div>
        <div class="indicator-item"><div class="label">KDJ-J</div><div class="value">${d.indicators?.kdj_j}</div></div>
        <div class="indicator-item"><div class="label">CCI</div><div class="value">${d.indicators?.cci}</div></div>
        <div class="indicator-item"><div class="label">ADX</div><div class="value">${d.indicators?.adx}</div></div>
        <div class="indicator-item"><div class="label">威廉%R</div><div class="value">${d.indicators?.williams_r}</div></div>
        <div class="indicator-item"><div class="label">MFI</div><div class="value">${d.indicators?.mfi}</div></div>
        <div class="indicator-item"><div class="label">BB位置</div><div class="value">${d.indicators?.bb_pos}</div></div>
      </div>
      <div id="stock-kline" style="height:300px"></div>
    </div>`;
    setTimeout(()=>{
      const chartDom=document.getElementById('stock-kline');
      if(!chartDom)return;
      const chart=echarts.init(chartDom);
      const upColor='#ef4444',downColor='#22c55e';
      chart.setOption({
        backgroundColor:'transparent',
        tooltip:{trigger:'axis',axisPointer:{type:'cross'}},
        grid:{left:40,right:60,top:20,bottom:40},
        xAxis:{type:'category',data:k.dates,axisLine:{lineStyle:{color:'#334155'}},axisLabel:{color:'#94a3b8'}},
        yAxis:[{type:'value',axisLine:{lineStyle:{color:'#334155'}},splitLine:{lineStyle:{color:'#1e293b'}},axisLabel:{color:'#94a3b8'}}],
        dataZoom:[{type:'inside',start:50,end:100}],
        series:[
          {name:'K线',type:'candlestick',data:k.dates.map((_,i)=>[k.open[i],k.close[i],k.low[i],k.high[i]]),itemStyle:{color:upColor,color0:downColor,borderColor:upColor,borderColor0:downColor}},
          {name:'MA5',type:'line',data:k.ma5,smooth:true,lineStyle:{color:'#f59e0b',width:1},symbol:'none'},
          {name:'MA10',type:'line',data:k.ma10,smooth:true,lineStyle:{color:'#3b82f6',width:1},symbol:'none'},
          {name:'MA20',type:'line',data:k.ma20,smooth:true,lineStyle:{color:'#8b5cf6',width:1},symbol:'none'},
        ]
      });
    },100);
  }catch(e){modal.innerHTML=`<div class="modal-header"><h3>错误</h3><button class="close-btn" onclick="closeModal()">&times;</button></div><div class="modal-body">加载失败: ${e.message}</div>`;}
}
function closeModal(){document.getElementById('modal-overlay').classList.remove('active');}
document.getElementById('modal-overlay').addEventListener('click',e=>{if(e.target.id==='modal-overlay')closeModal();});
function showIndexDetail(key){}
window.addEventListener('resize',()=>{Object.values(app.charts).forEach(c=>c&&c.resize());});
fetchData();
initWS();
</script>
</body>
</html>"""

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
