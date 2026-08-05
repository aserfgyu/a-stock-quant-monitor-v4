"""全市场扫描器 V4 - 异动检测增强版"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any
from quant_engine import QuantEngine

class MarketScanner:
    @classmethod
    def scan(cls, df: pd.DataFrame, top_n: int = 300) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        num_cols = ['change_pct', 'vol_ratio', 'turnover', 'close', 'amount', 'pe', 'pb']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        mask = (
            (abs(df['change_pct']) > 2.0) |
            (df['vol_ratio'] > 1.2) |
            (df['turnover'] > 2.5) |
            (df['amount'] > 5e8)
        )
        active = df[mask].copy()
        if active.empty:
            active = df.nlargest(top_n, 'amount').copy()
        active['rsi'] = 50 + active['change_pct'] * 2
        active['rsi'] = active['rsi'].clip(0, 100)
        scores = []
        tags_list = []
        for _, row in active.iterrows():
            score = QuantEngine.quick_score(row)
            scores.append(score)
            tags_list.append(cls._generate_tags(row, score))
        active['total_score'] = [s['total'] for s in scores]
        active['signal'] = [s['signal'] for s in scores]
        active['scores'] = scores
        active['tags'] = tags_list
        active = active.sort_values('total_score', ascending=False).head(top_n)
        return active

    @classmethod
    def _generate_tags(cls, row: pd.Series, score: Dict) -> List[str]:
        tags = []
        change = float(row.get('change_pct', 0))
        vr = float(row.get('vol_ratio', 0))
        turnover = float(row.get('turnover', 0))
        amount = float(row.get('amount', 0))
        pe = float(row.get('pe', 0))
        mv = float(row.get('total_mv', 0))
        if score['total'] >= 75: tags.append('强烈推荐')
        elif score['total'] >= 60: tags.append('推荐')
        if change > 9.5: tags.append('涨停')
        elif change > 5: tags.append('强势上涨')
        elif change > 2: tags.append('异动')
        elif change < -5: tags.append('超跌反弹')
        elif change < -2: tags.append('回调')
        if vr > 3: tags.append('巨量')
        elif vr > 2: tags.append('放量')
        elif vr > 1.5: tags.append('活跃')
        if turnover > 15: tags.append('高换手')
        elif turnover > 8: tags.append('换手活跃')
        if amount > 1e9: tags.append('大资金')
        if pe > 0 and pe < 15: tags.append('低估值')
        elif pe > 100: tags.append('高估值')
        if mv > 5e10: tags.append('大盘蓝筹')
        elif mv < 5e9: tags.append('小盘成长')
        elif mv < 2e9: tags.append('微盘股')
        wei_bi = float(row.get('wei_bi', 0))
        outer = float(row.get('outer_vol', 0))
        inner = float(row.get('inner_vol', 0))
        if outer > inner * 1.2: tags.append('外盘主导')
        elif inner > outer * 1.2: tags.append('内盘主导')
        if wei_bi > 20: tags.append('委比强势')
        return tags[:6]

    @classmethod
    def format_scan_result(cls, df: pd.DataFrame) -> List[Dict[str, Any]]:
        results = []
        for _, row in df.iterrows():
            results.append({
                'symbol': str(row.get('symbol', '')),
                'code': str(row.get('code', '')),
                'name': str(row.get('name', '')),
                'close': round(float(row.get('close', 0)), 2),
                'change_pct': round(float(row.get('change_pct', 0)), 2),
                'change': round(float(row.get('change', 0)), 2),
                'volume': int(row.get('volume', 0)),
                'amount': round(float(row.get('amount', 0)) / 1e8, 2),
                'vol_ratio': round(float(row.get('vol_ratio', 0)), 2),
                'turnover': round(float(row.get('turnover', 0)), 2),
                'total_mv': round(float(row.get('total_mv', 0)) / 1e8, 2),
                'pe': round(float(row.get('pe', 0)), 1),
                'pb': round(float(row.get('pb', 0)), 2),
                'total_score': int(row.get('total_score', 0)),
                'signal': str(row.get('signal', '观望')),
                'tags': row.get('tags', []),
                'scores': row.get('scores', {}),
            })
        return results
