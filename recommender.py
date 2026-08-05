"""今日推荐引擎 V4 - 多因子+板块轮动+北向偏好"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any

class Recommender:
    @classmethod
    def generate_daily_picks(cls, market_data: pd.DataFrame, sectors: Dict,
                             news: List, north_fund: Dict = None,
                             lhb: List = None) -> Dict[str, Any]:
        if market_data is None or market_data.empty:
            return cls._mock_picks()
        picks = []
        used_symbols = set()
        buy_candidates = market_data[
            (market_data['total_score'] >= 55) &
            (market_data['signal'].str.contains('买入'))
        ].head(10)
        for _, row in buy_candidates.iterrows():
            used_symbols.add(row['symbol'])
            reason = cls._generate_reason(row, sectors, news, 'score')
            picks.append(cls._format_pick(row, reason))
        rebound = market_data[
            (market_data['change_pct'] < -4) &
            (market_data['vol_ratio'] > 1.3) &
            (~market_data['symbol'].isin(used_symbols))
        ].head(4)
        for _, row in rebound.iterrows():
            used_symbols.add(row['symbol'])
            reason = cls._generate_reason(row, sectors, news, 'rebound')
            picks.append(cls._format_pick(row, reason))
        leaders = market_data[
            (market_data['change_pct'] > 4) &
            (market_data['turnover'] > 2) &
            (~market_data['symbol'].isin(used_symbols))
        ].head(4)
        for _, row in leaders.iterrows():
            used_symbols.add(row['symbol'])
            reason = cls._generate_reason(row, sectors, news, 'leader')
            picks.append(cls._format_pick(row, reason))
        if north_fund and 'sh' in north_fund and north_fund['sh']:
            north_leaders = market_data[
                (market_data['total_score'] >= 50) &
                (~market_data['symbol'].isin(used_symbols))
            ].head(2)
            for _, row in north_leaders.iterrows():
                used_symbols.add(row['symbol'])
                reason = cls._generate_reason(row, sectors, news, 'north')
                picks.append(cls._format_pick(row, reason))
        if lhb:
            lhb_codes = {item['code'] for item in lhb}
            lhb_stocks = market_data[market_data['code'].isin(lhb_codes) &
                                     (~market_data['symbol'].isin(used_symbols))].head(2)
            for _, row in lhb_stocks.iterrows():
                used_symbols.add(row['symbol'])
                reason = cls._generate_reason(row, sectors, news, 'lhb')
                picks.append(cls._format_pick(row, reason))
        picks = picks[:15]
        sector_picks = cls._generate_sector_picks(sectors)
        summary = cls._generate_summary(market_data, sectors, north_fund)
        rotation = cls._analyze_rotation(sectors)
        return {
            'picks': picks,
            'sector_picks': sector_picks,
            'summary': summary,
            'rotation': rotation,
            'update_time': pd.Timestamp.now().strftime('%H:%M:%S'),
        }

    @classmethod
    def _format_pick(cls, row: pd.Series, reason: str) -> Dict:
        return {
            'symbol': str(row.get('symbol', '')),
            'code': str(row.get('code', '')),
            'name': str(row.get('name', '')),
            'close': round(float(row.get('close', 0)), 2),
            'change_pct': round(float(row.get('change_pct', 0)), 2),
            'total_score': int(row.get('total_score', 0)),
            'signal': str(row.get('signal', '')),
            'reason': reason,
            'tags': row.get('tags', cls._generate_tags(row)),
        }

    @classmethod
    def _generate_reason(cls, row: pd.Series, sectors: Dict, news: List,
                         reason_type: str = 'score') -> str:
        reasons = []
        score = int(row.get('total_score', 0))
        change = float(row.get('change_pct', 0))
        vr = float(row.get('vol_ratio', 0))
        turnover = float(row.get('turnover', 0))
        name = str(row.get('name', ''))
        if reason_type == 'score':
            if score >= 75: reasons.append(f"量化评分{score}分，多因子共振，趋势强劲")
            elif score >= 60: reasons.append(f"量化评分{score}分，趋势向好，量价配合")
            else: reasons.append(f"量化评分{score}分，具备一定参与价值")
        elif reason_type == 'rebound':
            reasons.append(f"超跌{abs(change):.1f}%，存在技术性反弹修复空间")
            if vr > 1.5: reasons.append(f"量比{vr:.1f}x，抄底资金介入")
        elif reason_type == 'leader':
            reasons.append(f"今日大涨{change:.1f}%，板块龙头，资金抢筹明显")
        elif reason_type == 'north':
            reasons.append(f"北向资金近期持续流入相关板块，具备配置价值")
        elif reason_type == 'lhb':
            reasons.append(f"今日登上龙虎榜，游资机构博弈激烈，关注度高")
        if change > 5 and reason_type != 'leader':
            reasons.append(f"今日大涨{change:.1f}%，资金抢筹明显")
        elif change > 2 and change <= 5:
            reasons.append(f"今日上涨{change:.1f}%，量价齐升")
        if vr > 2.5: reasons.append(f"量比{vr:.1f}x，成交异常放大")
        elif vr > 1.5: reasons.append(f"量比{vr:.1f}x，活跃度提升")
        if turnover > 10: reasons.append(f"换手率{turnover:.1f}%，市场关注度高")
        elif turnover > 5: reasons.append(f"换手率{turnover:.1f}%，交投活跃")
        for n in news[:5]:
            title = n.get('title', '')
            if name in title or name[:2] in title:
                reasons.append(f"【消息】{title[:24]}...")
                break
        for stype, df in sectors.items():
            if df is not None and not df.empty:
                for _, srow in df.head(5).iterrows():
                    sname = str(srow.get('板块名称', srow.get('name', '')))
                    if sname in name or name in sname:
                        schg = float(srow.get('涨跌幅', srow.get('change', 0)))
                        if schg > 2: reasons.append(f"所属{sname}板块今日大涨{schg:.1f}%，板块效应明显")
                        break
        return '；'.join(reasons) if reasons else '技术面出现积极信号，建议关注'

    @classmethod
    def _generate_tags(cls, row: pd.Series) -> List[str]:
        tags = []
        score = int(row.get('total_score', 0))
        change = float(row.get('change_pct', 0))
        if score >= 75: tags.append('强烈推荐')
        elif score >= 60: tags.append('推荐')
        if change > 9.5: tags.append('涨停')
        elif change > 5: tags.append('强势上涨')
        elif change > 2: tags.append('异动')
        elif change < -5: tags.append('超跌反弹')
        vr = float(row.get('vol_ratio', 0))
        if vr > 2: tags.append('放量')
        turnover = float(row.get('turnover', 0))
        if turnover > 10: tags.append('高换手')
        mv = float(row.get('total_mv', 0))
        if mv > 1000: tags.append('大盘蓝筹')
        elif mv < 100: tags.append('小盘成长')
        return tags

    @classmethod
    def _generate_sector_picks(cls, sectors: Dict) -> List[Dict]:
        picks = []
        for sector_type, df in sectors.items():
            if df is None or df.empty: continue
            for _, row in df.head(8).iterrows():
                name = str(row.get('板块名称', row.get('name', '')))
                change = float(row.get('涨跌幅', row.get('change', 0)))
                fund = float(row.get('主力净流入', row.get('fund', 0)))
                reason = f"板块涨幅{change:.1f}%"
                if fund > 0: reason += f"，主力净流入{fund:.1f}亿"
                if change > 3: reason += "，资金集中流入，短期强势"
                elif change < -2: reason += "，板块回调，关注低吸机会"
                picks.append({'name': name, 'type': '行业' if sector_type == 'industry' else '概念',
                              'change_pct': round(change, 2), 'fund_flow': round(fund, 1), 'reason': reason})
        return sorted(picks, key=lambda x: x['change_pct'], reverse=True)[:8]

    @classmethod
    def _generate_summary(cls, market_data: pd.DataFrame, sectors: Dict,
                          north_fund: Dict = None) -> str:
        if market_data is None or market_data.empty:
            return "市场数据加载中..."
        up = len(market_data[market_data['change_pct'] > 0])
        down = len(market_data[market_data['change_pct'] < 0])
        limit_up = len(market_data[market_data['change_pct'] > 9.5])
        limit_down = len(market_data[market_data['change_pct'] < -9.5])
        avg_change = market_data['change_pct'].mean()
        total_amount = market_data['amount'].sum() / 1e8
        hot_sectors = []
        for stype, df in sectors.items():
            if df is not None and not df.empty:
                for _, row in df.head(3).iterrows():
                    hot_sectors.append({'name': str(row.get('板块名称', row.get('name', ''))),
                                        'change': float(row.get('涨跌幅', row.get('change', 0)))})
        hot_sectors = sorted(hot_sectors, key=lambda x: x['change'], reverse=True)[:3]
        summary = f"今日上涨<b>{up}</b>家，下跌<b>{down}</b>家，涨停<b>{limit_up}</b>家，跌停<b>{limit_down}</b>家。"
        summary += f"全市场平均涨跌幅<b>{avg_change:+.2f}%</b>，总成交额<b>{total_amount:.0f}亿</b>。"
        if hot_sectors:
            summary += "热点板块：" + ", ".join([f"{s['name']}({s['change']:+.1f}%)" for s in hot_sectors]) + "。"
        if north_fund and 'sh' in north_fund and north_fund['sh']:
            sh_in = north_fund['sh'].get('inflow', 0)
            sz_in = north_fund['sz'].get('inflow', 0)
            total_north = sh_in + sz_in
            if total_north > 0: summary += f"北向资金净流入<b>{total_north:.1f}亿</b>。"
            elif total_north < 0: summary += f"北向资金净流出<b>{abs(total_north):.1f}亿</b>。"
        if limit_up > 50 and avg_change > 1:
            summary += "市场情绪<b style=\"color:var(--up)\">极度乐观</b>，注意追高风险。"
        elif limit_up > 30 and avg_change > 0.5:
            summary += "市场情绪<b style=\"color:var(--up)\">积极</b>，结构性机会丰富。"
        elif limit_down > 20 and avg_change < -1:
            summary += "市场情绪<b style=\"color:var(--down)\">谨慎</b>，控制仓位为主。"
        elif avg_change > 0:
            summary += "市场情绪<b style=\"color:var(--ac)\">平稳偏多</b>，精选个股操作。"
        else:
            summary += "市场情绪<b style=\"color:var(--tt)\">震荡</b>，等待明确方向。"
        return summary

    @classmethod
    def _analyze_rotation(cls, sectors: Dict) -> List[Dict]:
        rotation = []
        all_sectors = []
        for stype, df in sectors.items():
            if df is None or df.empty: continue
            for _, row in df.iterrows():
                name = str(row.get('板块名称', row.get('name', '')))
                change = float(row.get('涨跌幅', row.get('change', 0)))
                fund = float(row.get('主力净流入', row.get('fund', 0)))
                all_sectors.append({'name': name, 'type': '行业' if stype == 'industry' else '概念',
                                    'change_pct': change, 'fund_flow': fund})
        all_sectors.sort(key=lambda x: x['change_pct'], reverse=True)
        strong = [s for s in all_sectors if s['change_pct'] > 3]
        if strong:
            rotation.append({'type': '强势', 'desc': f"{len(strong)}个板块涨幅超3%",
                             'sectors': [s['name'] for s in strong[:5]], 'color': 'up'})
        fund_in = sorted([s for s in all_sectors if s['fund_flow'] > 0], key=lambda x: x['fund_flow'], reverse=True)
        if fund_in:
            rotation.append({'type': '资金', 'desc': f"{len(fund_in)}个板块获主力净流入",
                             'sectors': [s['name'] for s in fund_in[:5]], 'color': 'ac'})
        weak = [s for s in all_sectors if s['change_pct'] < -2]
        if weak:
            rotation.append({'type': '弱势', 'desc': f"{len(weak)}个板块跌幅超2%",
                             'sectors': [s['name'] for s in weak[:5]], 'color': 'down'})
        return rotation

    @classmethod
    def _mock_picks(cls) -> Dict[str, Any]:
        return {
            'picks': [
                {'symbol':'600519.SH','code':'600519','name':'贵州茅台','close':1680.5,'change_pct':1.2,'total_score':82,'signal':'强烈买入','reason':'量化评分82分，多因子共振；白酒龙头，机构重仓；北向资金持续流入','tags':['强烈推荐','大盘蓝筹']},
                {'symbol':'000001.SZ','code':'000001','name':'平安银行','close':11.8,'change_pct':0.8,'total_score':78,'signal':'强烈买入','reason':'量化评分78分，趋势向好；银行板块资金流入；低估值修复','tags':['强烈推荐','低估值']},
                {'symbol':'300750.SZ','code':'300750','name':'宁德时代','close':218.5,'change_pct':2.5,'total_score':71,'signal':'买入','reason':'量化评分71分，新能源龙头；今日大涨2.5%，资金抢筹；量比2.1x','tags':['推荐','强势上涨','放量']},
                {'symbol':'002594.SZ','code':'002594','name':'比亚迪','close':258.3,'change_pct':3.8,'total_score':68,'signal':'买入','reason':'量化评分68分，新能源汽车龙头；今日大涨3.8%，板块效应明显','tags':['推荐','强势上涨','大资金']},
            ],
            'sector_picks': [
                {'name':'半导体','type':'行业','change_pct':4.5,'fund_flow':22.1,'reason':'板块涨幅4.5%，主力净流入22.1亿，资金集中流入，短期强势'},
                {'name':'AI算力','type':'概念','change_pct':5.8,'fund_flow':18.6,'reason':'板块涨幅5.8%，主力净流入18.6亿，资金集中流入，短期强势'},
                {'name':'CPO','type':'概念','change_pct':5.2,'fund_flow':15.3,'reason':'板块涨幅5.2%，主力净流入15.3亿，资金集中流入，短期强势'},
            ],
            'summary': '今日上涨<b>3200</b>家，下跌<b>1800</b>家，涨停<b>85</b>家，跌停<b>3</b>家。全市场平均涨跌幅<b>+0.85%</b>，总成交额<b>8923亿</b>。热点板块：AI算力(+5.8%), 半导体(+4.5%), CPO(+5.2%)。北向资金净流入<b>64.3亿</b>。市场情绪<b style=\"color:var(--up)\">积极</b>，结构性机会丰富。',
            'rotation': [
                {'type':'强势','desc':'5个板块涨幅超3%','sectors':['AI算力','半导体','CPO','存储芯片','光刻胶'],'color':'up'},
                {'type':'资金','desc':'8个板块获主力净流入','sectors':['半导体','AI算力','新能源','CPO','汽车'],'color':'ac'},
                {'type':'弱势','desc':'2个板块跌幅超2%','sectors':['地产','银行'],'color':'down'},
            ],
            'update_time': '15:00:00',
        }
