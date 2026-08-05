"""量化计算引擎 V4 - 专业版"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List

class QuantEngine:
    @staticmethod
    def calc_ma(s, n): return s.rolling(n).mean()
    @staticmethod
    def calc_ema(s, n): return s.ewm(span=n, adjust=False).mean()
    @staticmethod
    def calc_rsi(s, n=14):
        d = s.diff()
        g = d.where(d>0,0).rolling(n).mean()
        l = (-d.where(d<0,0)).rolling(n).mean()
        return 100-(100/(1+g/l))
    @staticmethod
    def calc_macd(s, fast=12, slow=26, signal=9):
        ef = s.ewm(span=fast).mean()
        es = s.ewm(span=slow).mean()
        m = ef-es
        sig = m.ewm(span=signal).mean()
        return m, sig, m-sig
    @staticmethod
    def calc_bb(s, n=20, k=2.0):
        m = s.rolling(n).mean()
        std = s.rolling(n).std()
        return m, m+k*std, m-k*std
    @staticmethod
    def calc_atr(df, n=14):
        tr = pd.concat([
            df['high']-df['low'],
            (df['high']-df['close'].shift()).abs(),
            (df['low']-df['close'].shift()).abs()
        ], axis=1).max(axis=1)
        return tr.rolling(n).mean()
    @staticmethod
    def calc_kdj(df, n=9, m1=3, m2=3):
        low_n = df['low'].rolling(n).min()
        high_n = df['high'].rolling(n).max()
        rsv = 100 * (df['close'] - low_n) / (high_n - low_n)
        k = rsv.ewm(com=m1-1, adjust=False).mean()
        d = k.ewm(com=m2-1, adjust=False).mean()
        j = 3 * k - 2 * d
        return k, d, j
    @staticmethod
    def calc_cci(df, n=14):
        tp = (df['high'] + df['low'] + df['close']) / 3
        ma_tp = tp.rolling(n).mean()
        md = tp.rolling(n).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
        return (tp - ma_tp) / (0.015 * md)
    @staticmethod
    def calc_obv(df):
        obv = [0]
        for i in range(1, len(df)):
            if df['close'].iloc[i] > df['close'].iloc[i-1]:
                obv.append(obv[-1] + df['volume'].iloc[i])
            elif df['close'].iloc[i] < df['close'].iloc[i-1]:
                obv.append(obv[-1] - df['volume'].iloc[i])
            else:
                obv.append(obv[-1])
        return pd.Series(obv, index=df.index)
    @staticmethod
    def calc_dmi(df, n=14):
        plus_dm = df['high'].diff()
        minus_dm = df['low'].diff().abs()
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)
        tr = pd.concat([
            df['high']-df['low'],
            (df['high']-df['close'].shift()).abs(),
            (df['low']-df['close'].shift()).abs()
        ], axis=1).max(axis=1)
        atr = tr.rolling(n).mean()
        plus_di = 100 * plus_dm.rolling(n).mean() / atr
        minus_di = 100 * minus_dm.rolling(n).mean() / atr
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(n).mean()
        return plus_di, minus_di, adx
    @staticmethod
    def calc_williams_r(df, n=14):
        high_n = df['high'].rolling(n).max()
        low_n = df['low'].rolling(n).min()
        return -100 * (high_n - df['close']) / (high_n - low_n)
    @staticmethod
    def calc_fund_flow_index(df):
        tp = (df['high'] + df['low'] + df['close']) / 3
        raw_mf = tp * df['volume']
        tp_diff = tp.diff()
        pos_mf = raw_mf.where(tp_diff > 0, 0).rolling(14).sum()
        neg_mf = raw_mf.where(tp_diff < 0, 0).rolling(14).sum()
        mfr = pos_mf / neg_mf
        return 100 - (100 / (1 + mfr))
    @staticmethod
    def calc_vwap(df):
        tp = (df['high'] + df['low'] + df['close']) / 3
        return (tp * df['volume']).cumsum() / df['volume'].cumsum()
    @staticmethod
    def calc_stochastic(df, n=14, m=3):
        low_n = df['low'].rolling(n).min()
        high_n = df['high'].rolling(n).max()
        k_fast = 100 * (df['close'] - low_n) / (high_n - low_n)
        k_slow = k_fast.rolling(m).mean()
        d_slow = k_slow.rolling(m).mean()
        return k_slow, d_slow

    @classmethod
    def process(cls, df):
        df = df.copy().sort_values('time').reset_index(drop=True)
        for n in [5,10,20,60,120]: df[f'ma{n}'] = cls.calc_ma(df['close'], n)
        df['rsi'] = cls.calc_rsi(df['close'])
        df['macd'], df['macd_signal'], df['macd_hist'] = cls.calc_macd(df['close'])
        df['bb_mid'], df['bb_upper'], df['bb_lower'] = cls.calc_bb(df['close'])
        df['atr'] = cls.calc_atr(df)
        df['kdj_k'], df['kdj_d'], df['kdj_j'] = cls.calc_kdj(df)
        df['cci'] = cls.calc_cci(df)
        df['obv'] = cls.calc_obv(df)
        df['plus_di'], df['minus_di'], df['adx'] = cls.calc_dmi(df)
        df['williams_r'] = cls.calc_williams_r(df)
        df['mfi'] = cls.calc_fund_flow_index(df)
        df['vwap'] = cls.calc_vwap(df)
        df['stoch_k'], df['stoch_d'] = cls.calc_stochastic(df)
        df['vol_ma5'] = df['volume'].rolling(5).mean()
        df['vol_ma20'] = df['volume'].rolling(20).mean()
        df['vol_ratio'] = df['volume']/df['vol_ma5']
        df['vol_trend'] = df['vol_ma5']/df['vol_ma20']
        df['change_pct'] = df['close'].pct_change()*100
        df['change_pct_5d'] = (df['close']/df['close'].shift(5)-1)*100
        df['change_pct_20d'] = (df['close']/df['close'].shift(20)-1)*100
        df['change_pct_60d'] = (df['close']/df['close'].shift(60)-1)*100
        df['volatility'] = df['change_pct'].rolling(20).std()
        df['bb_pos'] = (df['close']-df['bb_lower'])/(df['bb_upper']-df['bb_lower'])
        low14 = df['low'].rolling(14).min()
        high14 = df['high'].rolling(14).max()
        df['k'] = 100*(df['close']-low14)/(high14-low14)
        df['d'] = df['k'].rolling(3).mean()
        return df

    @classmethod
    def score(cls, df):
        r = df.iloc[-1]
        s = {}
        trend = 0
        if r['close'] > r['ma5']: trend += 4
        if r['ma5'] > r['ma10']: trend += 4
        if r['ma10'] > r['ma20']: trend += 4
        if r['close'] > r['ma20']: trend += 4
        if r['close'] > r['ma60']: trend += 3
        if r['close'] > r['vwap']: trend += 3
        if r['close'] > r['ma120']: trend += 3
        s['trend'] = min(trend, 25)

        mom = 0
        rsi = r['rsi'] if pd.notna(r['rsi']) else 50
        if 45 <= rsi <= 55: mom += 6
        elif 55 < rsi <= 70: mom += 12
        elif rsi > 70: mom += 16
        elif 30 <= rsi < 45: mom += 8
        else: mom += 4
        if r['macd_hist'] > 0: mom += 4
        if r['macd'] > r['macd_signal']: mom += 4
        kdj_j = r['kdj_j'] if pd.notna(r['kdj_j']) else 50
        if kdj_j > 50: mom += 3
        if kdj_j > 80: mom += 2
        williams = r['williams_r'] if pd.notna(r['williams_r']) else -50
        if williams > -20: mom += 3
        elif williams > -50: mom += 2
        stoch_k = r['stoch_k'] if pd.notna(r['stoch_k']) else 50
        if stoch_k > 50: mom += 3
        s['momentum'] = min(mom, 25)

        vol = 0
        vr = r['vol_ratio'] if pd.notna(r['vol_ratio']) else 1
        vt = r['vol_trend'] if pd.notna(r['vol_trend']) else 1
        if vr > 2.0: vol += 12
        elif vr > 1.5: vol += 10
        elif vr > 1.0: vol += 7
        elif vr > 0.8: vol += 5
        else: vol += 3
        if vt > 1.3: vol += 8
        elif vt > 1.0: vol += 6
        elif vt > 0.9: vol += 4
        else: vol += 2
        mfi = r['mfi'] if pd.notna(r['mfi']) else 50
        if mfi > 60: vol += 3
        elif mfi > 40: vol += 2
        s['volume'] = min(vol, 20)

        pos = 0
        bp = r['bb_pos'] if pd.notna(r['bb_pos']) else 0.5
        if bp < 0.15: pos += 14
        elif bp < 0.3: pos += 11
        elif bp < 0.5: pos += 8
        elif bp < 0.7: pos += 6
        elif bp < 0.85: pos += 4
        else: pos += 2
        vlt = r['volatility'] if pd.notna(r['volatility']) else 2
        if vlt < 1: pos += 3
        elif vlt < 2: pos += 2
        else: pos += 1
        cci = r['cci'] if pd.notna(r['cci']) else 0
        if cci < -200: pos += 3
        elif cci < -100: pos += 2
        s['position'] = min(pos, 15)

        perf = 0
        c5 = r['change_pct_5d'] if pd.notna(r['change_pct_5d']) else 0
        c20 = r['change_pct_20d'] if pd.notna(r['change_pct_20d']) else 0
        c60 = r['change_pct_60d'] if pd.notna(r['change_pct_60d']) else 0
        if c5 > 5: perf += 6
        elif c5 > 0: perf += 4
        elif c5 > -5: perf += 2
        else: perf += 1
        if c20 > 10: perf += 5
        elif c20 > 0: perf += 3
        elif c20 > -10: perf += 2
        else: perf += 1
        if c60 > 20: perf += 4
        elif c60 > 0: perf += 2
        else: perf += 1
        s['performance'] = min(perf, 15)

        adx = r['adx'] if pd.notna(r['adx']) else 20
        plus_di = r['plus_di'] if pd.notna(r['plus_di']) else 20
        minus_di = r['minus_di'] if pd.notna(r['minus_di']) else 20
        dmi_bonus = 0
        if adx > 25 and plus_di > minus_di: dmi_bonus += 3
        elif adx > 20 and plus_di > minus_di: dmi_bonus += 2
        elif adx > 25 and plus_di < minus_di: dmi_bonus -= 2

        s['total'] = s['trend'] + s['momentum'] + s['volume'] + s['position'] + s['performance'] + dmi_bonus
        s['total'] = max(0, min(100, s['total']))

        t = s['total']
        if t >= 80: s['signal'] = '强烈买入'
        elif t >= 65: s['signal'] = '买入'
        elif t >= 50: s['signal'] = '观望偏多'
        elif t >= 40: s['signal'] = '观望'
        elif t >= 30: s['signal'] = '观望偏空'
        elif t >= 20: s['signal'] = '卖出'
        else: s['signal'] = '强烈卖出'

        vlt = r['volatility'] if pd.notna(r['volatility']) else 2
        if vlt > 3.5: s['risk'] = '高风险'
        elif vlt > 2.5: s['risk'] = '中高风险'
        elif vlt > 1.8: s['risk'] = '中等风险'
        elif vlt > 1.2: s['risk'] = '中低风险'
        else: s['risk'] = '低风险'

        s['indicators'] = {
            'rsi': round(r['rsi'], 1) if pd.notna(r['rsi']) else 50,
            'kdj_j': round(r['kdj_j'], 1) if pd.notna(r['kdj_j']) else 50,
            'cci': round(r['cci'], 1) if pd.notna(r['cci']) else 0,
            'adx': round(r['adx'], 1) if pd.notna(r['adx']) else 20,
            'williams_r': round(r['williams_r'], 1) if pd.notna(r['williams_r']) else -50,
            'mfi': round(r['mfi'], 1) if pd.notna(r['mfi']) else 50,
            'bb_pos': round(r['bb_pos'], 3) if pd.notna(r['bb_pos']) else 0.5,
        }
        return s

    @classmethod
    def quick_score(cls, row: pd.Series) -> Dict[str, Any]:
        s = {}
        trend = 0
        if row['close'] > row['open']: trend += 10
        if row['change_pct'] > 0: trend += 5
        if row['change_pct'] > 2: trend += 5
        if row['change_pct'] > 5: trend += 5
        s['trend'] = min(trend, 25)

        mom = 0
        rsi = row.get('rsi', 50)
        if 45 <= rsi <= 55: mom += 10
        elif rsi > 55: mom += 18
        elif rsi > 30: mom += 12
        else: mom += 5
        s['momentum'] = min(mom, 25)

        vol = 0
        vr = row.get('vol_ratio', 1)
        if vr > 2: vol += 15
        elif vr > 1.5: vol += 12
        elif vr > 1.0: vol += 8
        else: vol += 4
        turnover = row.get('turnover', 0)
        if turnover > 10: vol += 5
        elif turnover > 5: vol += 3
        s['volume'] = min(vol, 20)

        pos = 0
        cp = row['change_pct']
        if cp < -5: pos += 14
        elif cp < -3: pos += 11
        elif cp < 0: pos += 7
        elif cp < 3: pos += 5
        else: pos += 3
        s['position'] = min(pos, 15)

        perf = 0
        c5 = row.get('change_pct_5d', 0)
        if c5 > 5: perf += 10
        elif c5 > 0: perf += 6
        elif c5 > -5: perf += 3
        else: perf += 1
        s['performance'] = min(perf, 15)

        s['total'] = s['trend'] + s['momentum'] + s['volume'] + s['position'] + s['performance']
        t = s['total']
        if t >= 80: s['signal'] = '强烈买入'
        elif t >= 65: s['signal'] = '买入'
        elif t >= 50: s['signal'] = '观望偏多'
        elif t >= 40: s['signal'] = '观望'
        elif t >= 30: s['signal'] = '观望偏空'
        elif t >= 20: s['signal'] = '卖出'
        else: s['signal'] = '强烈卖出'
        return s
