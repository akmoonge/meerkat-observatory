# -*- coding: utf-8 -*-
"""V8.0 A48 prototype — A 등급(즉시 사용 가능) 48박스로 1차 시뮬.

원본 (meerkat_observatory.py / season_engine_core.py) 무수정. 임시 파일.

설계 출처:
  - 사용자 92 후보 풀 (2026-04-28 작성)
  - 1차 필터링: 데이터 가용성 — A 등급 (raw_data 즉시 사용)만 채택
  - V8.0 골격: 4계절 × 4축 × 3박스 = 48박스
  - 평가: 축당 점등수 → [0, 1, 2.5, 4.5] 누진 가중
  - 계절 점수 = 4축 합 (max 18). 가산/override 폐기. PA만 유지.

α/δ/ε/ζ 패치 (2026-04-29):
  - α. fpe 영구 폐기 — forward consensus 시계열 부재 (1980-2024). cape + tpe 로 교체.
       trailing_earnings_history.json (1990+ 일별) → tpe 시계열 직접 로딩.
  - δ. A04 디커플링 재설계 — VIX 단독 임계 분리 + HY 1y 평균 대비 박스 신설.
       가지1: hy_3m_chg > 0.3 AND vix < 18
       가지2: hy_pct > hy_1y_avg * 1.1 AND vix < 20
  - ε. S01 봄 채권금리곡선 정상화 + un_3m_chg < 0 게이트.
       recovering 은 침체 후 회복(봄) AND 침체 진입 직전(겨울 초) 양쪽 모두 발생.
       구분 = 실업률 방향. < 0 → 봄, ≥ 0 → 겨울 W01 으로 넘김. (2007-10 가을 1위 잡음)
  - ζ. stale 게이트 전체 적용 + CFNAI 도입.
       (1) 모든 raw 시계열: 마지막 유효 데이터가 offset 시점 기준 90일 이전이면 None.
           backtest (offset>0) 는 trim 으로 자연스레 처리되지만 today (offset=0) 에서
           FRED 갱신 중단 / fetch 실패 시 수년 전 데이터로 평가하는 결함 차단.
       (2) USSLIND 폐기 (Conference Board 가 2020-02 FRED 배포 중단). CFNAI 로 대체.
           여름 R2 = CFNAI 3M avg > 0 (확장).
  - η. [폐기/롤백] 여름 B축 억제 가드 — 의도와 반대 방향 (가을 정확 5→1 폭락).
       backtest 40%+ 시점 발동, n_eval -5.9, 빠진 점수를 가을이 아닌 봄/겨울이 가져감.
       2026-04-29 롤백 결정.
  - θ. 가을 B축 4번째 박스 추가 (A26 LEI/CEI 하락 전환 → CFNAI 둔화 proxy).
       cfnai_3m_avg < 0 단독 (둔화 영역). 6m_avg 비교 폐기 — 너무 좁아 점등 거의 안 됨.
       1984/2006/2018 비밸류 가을 잡기 시도. AXIS_SCORE_TABLE cap 4.5 유지.
  - ι. PA 봄 예외 — pa_active AND 봄 score ≥ 7.0 이면 PA 무시.
       2009-03 같이 시스템이 바닥 회복을 강하게 인식한 시점에 PA 가 봄을 덮는 것 차단.

수정 박스:
  - S01 봄  B1: un_3m_chg < 0 게이트 추가
  - S07 봄  V1: cape ≤ 25 단독 (fpe 조건 삭제)
  - U06 여름 V1: cape ≤ 30 단독
  - U05 여름 R2: CFNAI 3M avg > 0 (lei → cfnai 교체)
  - A08 가을 V1: tpe ≥ 28 OR cape ≥ 32
  - A04 가을 C2: 위 2가지 OR
  - W06 겨울 V1: 그대로 (fpe 미사용)

A 등급 후보 → 4축×3박스 매핑 (최종):

  봄 12:
    B1=S01(3M10Y 정상화)  B2=S05(저점 인하)  B3=S13(달러 급락)
    C1=S03(HY peak 후 진입)  C2=S12(VIX 35+ 후 진정)  C3=S04(HY 6M 개선)
    R1=S08(실업 4%+/급등)  R2=S10(CPI 종결)  R3=S09(DD<-25%)
    V1=S07(fpe≤18 OR cape≤25)  V2=S15(반도체 선행 바닥)  V3=S20(200dma 탈환)
    [제외] S02 중복, S11 합성

  여름 12:
    B1=U01(3M10Y 정상)  B2=U02(2Y10Y 정상)  B3=U04(ff 안정)
    C1=U03(HY<4)  C2=U11(VIX 1M<18)  C3=U12(DXY 안정)
    R1=U07(고용 강세)  R2=LEI 가속  R3=payems_3m>100
    V1=U06(fpe≤22)  V2=U22(cape<30 광기 가드)  V3=U10(시장 폭)
    [제외] U09/U18

  가을 12:
    B1=A01(3M10Y 역전)  B2=A05(고점 인하/긴축)  B3=A13(달러 급등)
    C1=A03(HY 확산)  C2=A04(HY/VIX 디커플링)  C3=A17(VIX 압축)
    R1=A11(메가캡 의존)  R2=A12(WTI 공급충격)  R3=A25(SPX 1Y z>+2σ)
    V1=A08(fpe≥22)  V2=A09(CAPE 극단)  V3=A10(반도체 선행 약세)
    [제외] A02 중복, A23(t10y raw_data 에 없음)

  겨울 12:
    B1=W03(연준 인하 진행)  B2=W01_A(정상화+실업 동반)  B3=실질금리 급락
    C1=W02(HY>5)  C2=W08(VIX>30 지속)  C3=W09(달러 시스템)
    R1=W07(실업 폭증)  R2=W14(NFP 수축)  R3=W11(유가 수요 붕괴)
    V1=W06(하락 둔화)  V2=W19(200dma 하회 60D)  V3=cape 급락
    [제외] W17(t10y raw_data 에 없음)

실행:
  import season_engine_core as M
  import season_engine_v8_helpers as V8A
  M.build_raw_data()
  V8A.simulate_5_points(M.raw_data)
"""
import sys, json
from pathlib import Path
from datetime import date as _date
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from season_engine_core import (
    _safe_iloc_at, _pct_change_at, _abs_change_at, _percentile_at,
    _trim_series_at_offset, _inv_state_at, _inv_recovery_pct,
)


# ═══ tpe 시계열 로더 (α 패치) ═══
_TPE_CACHE = None
def load_tpe_series():
    """trailing_earnings_history.json 의 te 필드 → 일별 tpe 시계열.
    1990-01-01 부터 (Shiller 데이터셋 기반). cape 와 같은 범위.
    """
    global _TPE_CACHE
    if _TPE_CACHE is not None: return _TPE_CACHE
    p = Path.home() / ".meerkat" / "cache" / "trailing_earnings_history.json"
    if not p.exists(): return None
    try:
        with open(p, encoding="utf-8") as f:
            obj = json.load(f)
        recs = obj.get("data", [])
        if not recs: return None
        df = pd.DataFrame(recs)
        df["date"] = pd.to_datetime(df["date"])
        s = df.set_index("date")["te"].dropna()
        _TPE_CACHE = s
        return s
    except Exception:
        return None


# ═══ CFNAI 시계열 로더 (ζ 패치 — USSLIND 대체, retry 추가) ═══
_CFNAI_CACHE = None
def load_cfnai_series(retries=3, retry_delay=2.0):
    """FRED CFNAI (Chicago Fed National Activity Index, 월간, 1967+).
    USSLIND 가 2020-02 FRED 배포 중단되어 대체. 정상 갱신 중."""
    global _CFNAI_CACHE
    if _CFNAI_CACHE is not None: return _CFNAI_CACHE
    import time
    last_err = None
    for attempt in range(retries):
        try:
            cfg_path = Path.home() / ".meerkat" / "config.json"
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            from fredapi import Fred
            fr = Fred(api_key=cfg["fred_api_key"])
            s = fr.get_series("CFNAI", observation_start="1967-01-01").dropna()
            if len(s) == 0:
                last_err = "empty"
                continue
            _CFNAI_CACHE = s
            return s
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(retry_delay)
    print(f"[CFNAI fetch FAIL after {retries} retries] {last_err}", flush=True)
    return None


# ═══ ICSA (Initial Claims, 주간, 1967+) — S_B1/W_B1 분기용 ═══
_ICSA_CACHE = None
def load_iclaims_series(retries=3, retry_delay=2.0):
    """FRED ICSA — Initial Claims, weekly, 1967+. 봄/겨울 분기 (Initial Claims 도함수)."""
    global _ICSA_CACHE
    if _ICSA_CACHE is not None: return _ICSA_CACHE
    import time
    last_err = None
    for attempt in range(retries):
        try:
            cfg_path = Path.home() / ".meerkat" / "config.json"
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            from fredapi import Fred
            fr = Fred(api_key=cfg["fred_api_key"])
            s = fr.get_series("ICSA", observation_start="1967-01-01").dropna()
            if len(s) == 0:
                last_err = "empty"
                continue
            _ICSA_CACHE = s
            return s
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(retry_delay)
    print(f"[ICSA fetch FAIL after {retries} retries] {last_err}", flush=True)
    return None


# ═══ stale 게이트 (ζ 패치) ═══
def _staleness_filtered(raw_dict, offset, max_lag_days=90):
    """모든 시계열에 대해 offset 시점 기준 마지막 유효 데이터 lag 검사.
    lag > max_lag_days 면 None 으로 치환. backtest (offset>0) 는 trim 으로 자연 처리되지만
    today/recent 에서 FRED 갱신 중단/fetch 실패 시 수년 전 데이터로 평가하는 결함 차단."""
    out = dict(raw_dict)
    for k, s in raw_dict.items():
        if s is None: continue
        if not hasattr(s, "index"): continue
        if not isinstance(s.index, pd.DatetimeIndex): continue
        if len(s) == 0:
            out[k] = None
            continue
        # offset 시점 기준 reference date (series 의 last date 에서 offset 일 전)
        ref_date = s.index[-1] - pd.Timedelta(days=offset)
        sub = s[s.index <= ref_date].dropna()
        if len(sub) == 0:
            out[k] = None
            continue
        last_data_date = sub.index[-1]
        lag_days = (ref_date - last_data_date).days
        if lag_days > max_lag_days:
            out[k] = None
    return out

AXIS_SCORE_TABLE = [0.0, 1.0, 2.5, 4.5, 4.5, 4.5]   # n_true 0~5 cap 4.5 (4박스 axis 대응)


# ═══ 박스 평가 헬퍼 ═══
def _box(cond, *deps):
    if any(d is None for d in deps): return None
    return bool(cond)

def _or_box(*branches):
    results = []
    for branch in branches:
        cond, *deps = branch
        if any(d is None for d in deps): results.append(None)
        else: results.append(bool(cond))
    if all(r is None for r in results): return None
    return any(r is True for r in results)

def _max_in_window(s, offset, lookback):
    if s is None: return None
    s_t = _trim_series_at_offset(s, offset)
    if s_t is None or len(s_t) < lookback: return None
    try: return float(s_t.iloc[-lookback:].max())
    except Exception: return None

def _min_in_window(s, offset, lookback):
    if s is None: return None
    s_t = _trim_series_at_offset(s, offset)
    if s_t is None or len(s_t) < lookback: return None
    try: return float(s_t.iloc[-lookback:].min())
    except Exception: return None

def _ma_at_offset(s, offset, window):
    """offset 시점의 window 일 MA."""
    if s is None: return None
    s_t = _trim_series_at_offset(s, offset)
    if s_t is None or len(s_t) < window: return None
    try: return float(s_t.iloc[-window:].mean())
    except Exception: return None

def _spx_dd_3m(spx, offset):
    """SPX 3M drawdown (%, 음수=하락)."""
    if spx is None: return None
    s_t = _trim_series_at_offset(spx, offset)
    if s_t is None or len(s_t) < 63: return None
    peak = float(s_t.iloc[-63:].max()); cur = float(s_t.iloc[-1])
    if peak <= 0: return None
    return (cur / peak - 1) * 100

def _qqq_52w_dd(qqq, offset):
    if qqq is None: return None
    s_t = _trim_series_at_offset(qqq, offset)
    if s_t is None or len(s_t) < 252: return None
    peak = float(s_t.iloc[-252:].max()); cur = float(s_t.iloc[-1])
    if peak <= 0: return None
    return (cur / peak - 1) * 100

def _sox_52w_dd(sox, offset):
    if sox is None: return None
    s_t = _trim_series_at_offset(sox, offset)
    if s_t is None or len(s_t) < 252: return None
    peak = float(s_t.iloc[-252:].max()); cur = float(s_t.iloc[-1])
    if peak <= 0: return None
    return (cur / peak - 1) * 100

def _spx_above_200dma_recovery(spx, offset):
    """S20: SPX > 200dma AND 30일 전 < 200dma."""
    if spx is None: return None
    s_t = _trim_series_at_offset(spx, offset)
    if s_t is None or len(s_t) < 200 + 30: return None
    cur = float(s_t.iloc[-1])
    ma_now = float(s_t.iloc[-200:].mean())
    s_30d_ago = s_t.iloc[:-30]
    if len(s_30d_ago) < 200: return None
    cur_30d_ago = float(s_30d_ago.iloc[-1])
    ma_30d_ago = float(s_30d_ago.iloc[-200:].mean())
    return (cur > ma_now) and (cur_30d_ago < ma_30d_ago)

def _spx_below_200dma_streak(spx, offset, streak_days=60):
    """W19: 60일 연속 SPX < 200dma."""
    if spx is None: return None
    s_t = _trim_series_at_offset(spx, offset)
    if s_t is None or len(s_t) < 200 + streak_days: return None
    rolling_ma = s_t.rolling(200).mean()
    last_n_close = s_t.iloc[-streak_days:]
    last_n_ma = rolling_ma.iloc[-streak_days:]
    pairs = list(zip(last_n_close.values, last_n_ma.values))
    if any(pd.isna(c) or pd.isna(m) for c, m in pairs): return None
    return all(c < m for c, m in pairs)

def _spx_1y_zscore(spx, offset, window_years=20):
    """A25: SPX 1Y 수익률의 z-score (직근 N년 분포 기준)."""
    if spx is None: return None
    s_t = _trim_series_at_offset(spx, offset)
    if s_t is None or len(s_t) < 252: return None
    cur = float(s_t.iloc[-1])
    s_1y_ago = float(s_t.iloc[-252])
    if s_1y_ago <= 0: return None
    ret_1y = (cur / s_1y_ago - 1) * 100
    needed = 252 * window_years
    win = s_t.iloc[-needed:] if len(s_t) >= needed else s_t
    if len(win) < 252 * 5: return None
    rolling_1y = (win / win.shift(252) - 1).dropna() * 100
    if len(rolling_1y) < 100: return None
    mu = float(rolling_1y.mean()); sigma = float(rolling_1y.std())
    if sigma == 0: return None
    return (ret_1y - mu) / sigma

def _qqq_1m_abs_chg(qqq, offset):
    c = _pct_change_at(qqq, offset, 22)
    return abs(c) if c is not None else None


# ═══ 메인 평가 ═══
def evaluate_v8_a48(raw, offset, tpe_series=None, cfnai_series=None,
                    stale_max_lag_days=90):
    """V8.0 A48 평가. raw = season_engine_core.raw_data.
    tpe_series / cfnai_series: 외부 로더로 주입 (None 이면 자동 로딩).
    stale_max_lag_days: ζ stale 게이트 임계 (offset 시점 기준)."""
    # ζ 패치: tpe / cfnai 를 raw 에 합쳐서 stale 게이트 한 번에 처리
    tpe = tpe_series if tpe_series is not None else load_tpe_series()
    cfnai = cfnai_series if cfnai_series is not None else load_cfnai_series()
    raw_ext = dict(raw)
    raw_ext["tpe_s"] = tpe
    raw_ext["cfnai_s"] = cfnai
    raw_ext = _staleness_filtered(raw_ext, offset, max_lag_days=stale_max_lag_days)

    qqq = raw_ext.get("qqq_s"); ff = raw_ext.get("ff_s"); hy = raw_ext.get("hy_s")
    unrate = raw_ext.get("unrate_s")
    inv3m10y = raw_ext.get("t10y3m_s"); inv2y10y = raw_ext.get("t10y2y_s")
    cpi_yoy = raw_ext.get("cpi_yoy_s"); cape = raw_ext.get("cape_s")
    vix = raw_ext.get("vix_s"); sox = raw_ext.get("sox_s"); spx = raw_ext.get("spx_s")
    rsp = raw_ext.get("rsp_s"); spy = raw_ext.get("spy_s")
    payems = raw_ext.get("payems_s")
    dxy = raw_ext.get("dxy_s"); wti = raw_ext.get("wti_s")
    tpe = raw_ext.get("tpe_s"); cfnai = raw_ext.get("cfnai_s")

    # ═══ 시그널 ═══
    inv_state    = _inv_state_at(inv3m10y, offset)
    inv_state_1y = _inv_state_at(inv3m10y, offset + 365)
    inv_state_2y10y = _inv_state_at(inv2y10y, offset)

    ff_now = _safe_iloc_at(ff, offset)
    ff_3m_chg = _abs_change_at(ff, offset, 90)
    ff_6m_chg = _abs_change_at(ff, offset, 180)
    ff_pos_pct = _percentile_at(ff, offset, 252 * 10)

    cpi_now = _safe_iloc_at(cpi_yoy, offset)
    cpi_3m_chg = _abs_change_at(cpi_yoy, offset, 90)
    real_rate_now = (ff_now - cpi_now) if (ff_now is not None and cpi_now is not None) else None
    ff_6m_ago  = _safe_iloc_at(ff, offset + 180) if ff is not None else None
    cpi_6m_ago = _safe_iloc_at(cpi_yoy, offset + 180) if cpi_yoy is not None else None
    real_rate_6m_ago = ((ff_6m_ago - cpi_6m_ago)
                       if (ff_6m_ago is not None and cpi_6m_ago is not None) else None)
    real_rate_chg_6m = ((real_rate_now - real_rate_6m_ago)
                       if (real_rate_now is not None and real_rate_6m_ago is not None) else None)

    # HY (% 단위 정규화)
    hy_now_raw = _safe_iloc_at(hy, offset)
    hy_pct = (hy_now_raw * 100) if (hy_now_raw is not None and hy_now_raw <= 1.0) else hy_now_raw
    def _hy_chg(lookback):
        c = _abs_change_at(hy, offset, lookback)
        if c is None: return None
        return c * 100 if (hy_now_raw is not None and hy_now_raw <= 1.0) else c
    hy_6m_chg = _hy_chg(180); hy_3m_chg = _hy_chg(90)
    hy_max_1y_raw = _max_in_window(hy, offset, 252)
    hy_max_1y = ((hy_max_1y_raw * 100)
                 if (hy_max_1y_raw is not None and hy_max_1y_raw <= 1.0) else hy_max_1y_raw)

    vix_now = _safe_iloc_at(vix, offset)
    vix_t = _trim_series_at_offset(vix, offset) if vix is not None else None
    vix_1m_avg  = (float(vix_t.iloc[-22:].mean())
                   if (vix_t is not None and len(vix_t) >= 22) else None)
    vix_90d_max = (float(vix_t.iloc[-90:].max())
                   if (vix_t is not None and len(vix_t) >= 90) else None)

    un_now = _safe_iloc_at(unrate, offset)
    un_3m_chg = _abs_change_at(unrate, offset, 90)
    payems_t = _trim_series_at_offset(payems, offset) if payems is not None else None
    payems_3m_avg = None
    if payems_t is not None and len(payems_t) >= 4:
        diffs = payems_t.iloc[-3:].diff().dropna()
        if len(diffs) > 0: payems_3m_avg = float(diffs.mean())
    # ζ: USSLIND 폐기 → CFNAI 로 대체
    # θ: cfnai_6m_avg 추가 (가을 B4 LEI/CEI 둔화 proxy)
    cfnai_3m_avg = None
    cfnai_6m_avg = None
    if cfnai is not None:
        cfnai_t = _trim_series_at_offset(cfnai, offset)
        if cfnai_t is not None and len(cfnai_t) >= 3:
            try: cfnai_3m_avg = float(cfnai_t.iloc[-3:].mean())
            except Exception: pass
        if cfnai_t is not None and len(cfnai_t) >= 6:
            try: cfnai_6m_avg = float(cfnai_t.iloc[-6:].mean())
            except Exception: pass
    cfnai_now = _safe_iloc_at(cfnai, offset)

    cape_now = _safe_iloc_at(cape, offset)
    cape_20y_pct = _percentile_at(cape, offset, 252 * 20) if cape is not None else None
    cape_3m_chg_pct = _pct_change_at(cape, offset, 90)
    tpe_now = _safe_iloc_at(tpe, offset)
    # HY 1년 평균 (δ 패치 — 가지 2 분모)
    hy_1y_avg_raw = None
    if hy is not None:
        hy_t = _trim_series_at_offset(hy, offset)
        if hy_t is not None and len(hy_t) >= 252:
            try: hy_1y_avg_raw = float(hy_t.iloc[-252:].mean())
            except Exception: pass
    hy_1y_avg = ((hy_1y_avg_raw * 100)
                 if (hy_1y_avg_raw is not None and hy_1y_avg_raw <= 1.0) else hy_1y_avg_raw)

    sox_3m = _pct_change_at(sox, offset, 63); spx_3m = _pct_change_at(spx, offset, 63)
    sox_1m = _pct_change_at(sox, offset, 22); spx_1m = _pct_change_at(spx, offset, 22)
    spx_6m = _pct_change_at(spx, offset, 126)
    rsp_3m = _pct_change_at(rsp, offset, 63); spy_3m = _pct_change_at(spy, offset, 63)
    rsp_6m = _pct_change_at(rsp, offset, 126); qqq_6m = _pct_change_at(qqq, offset, 126)

    dxy_3m_chg_pct = _pct_change_at(dxy, offset, 90)
    dxy_6m_chg_pct = _pct_change_at(dxy, offset, 180)
    dxy_now = _safe_iloc_at(dxy, offset)

    wti_3m_chg_pct = _pct_change_at(wti, offset, 90)

    qqq_dd = _qqq_52w_dd(qqq, offset)
    sox_dd = _sox_52w_dd(sox, offset)
    spx_dd_3m = _spx_dd_3m(spx, offset)
    qqq_1m_abs = _qqq_1m_abs_chg(qqq, offset)
    spx_above_200dma_recovery = _spx_above_200dma_recovery(spx, offset)
    spx_below_200dma_60d = _spx_below_200dma_streak(spx, offset, streak_days=60)
    spx_1y_z = _spx_1y_zscore(spx, offset)

    # ════════════════════ 박스 정의 ════════════════════
    # ─── 봄 ───
    B_spring = {
        # S01: 채권금리곡선 정상화 중 (ε 패치 — 실업 방향 게이트 추가)
        # recovering 은 두 시점에서 발생: 침체 후 회복(봄) AND 역전 해소 직후 침체 진입 직전(겨울 초).
        # 구분 = un_3m_chg 부호. < 0 → 봄, ≥ 0 → 겨울 W01 으로 넘김.
        "B1_S01_curve_recover":  _box(inv_state == "recovering"
                                      and un_3m_chg is not None and un_3m_chg < 0,
                                      inv_state, un_3m_chg),
        # S05: 연준 저점권 인하
        "B2_S05_low_cut":        _box(ff_pos_pct is not None and ff_pos_pct < 30
                                      and ff_3m_chg is not None and ff_3m_chg < 0,
                                      ff_pos_pct, ff_3m_chg),
        # S13: 달러 급락
        "B3_S13_dxy_drop":       _box(dxy_6m_chg_pct is not None and dxy_6m_chg_pct < -5,
                                      dxy_6m_chg_pct),
    }
    C_spring = {
        # S03: HY peak 후 4% 진입
        "C1_S03_hy_settled":     _box(hy_max_1y is not None and hy_max_1y >= 5
                                      and hy_pct is not None and hy_pct < 4.0,
                                      hy_max_1y, hy_pct),
        # S12: VIX 35+ 후 진정
        "C2_S12_vix_calmed":     _box(vix_90d_max is not None and vix_90d_max >= 35
                                      and vix_now is not None and vix_now < 25,
                                      vix_90d_max, vix_now),
        # S04: HY 6M 개선
        "C3_S04_hy_falling":     _box(hy_6m_chg is not None and hy_6m_chg < -1.0
                                      and hy_max_1y is not None and hy_max_1y >= 5,
                                      hy_6m_chg, hy_max_1y),
    }
    R_spring = {
        # S08: 실업 4%+ 또는 급등
        "R1_S08_un_elevated":    _or_box(
                                    (un_now is not None and un_now >= 4.0, un_now),
                                    (un_3m_chg is not None and un_3m_chg > 0.5, un_3m_chg)),
        # S10: 인플레 종결
        "R2_S10_cpi_ending":     _box(cpi_3m_chg is not None and cpi_3m_chg < 0
                                      and cpi_now is not None and cpi_now < 3,
                                      cpi_3m_chg, cpi_now),
        # S09: QQQ DD < -25% (또는 SPX 3M DD < -15% fallback)
        "R3_S09_dd_bottom":      _or_box(
                                    (qqq_dd is not None and qqq_dd <= -25, qqq_dd),
                                    (spx_dd_3m is not None and spx_dd_3m <= -15, spx_dd_3m)),
    }
    V_spring = {
        # S07: 밸류 재평가 (α 패치 — fpe 폐기, cape 단독)
        "V1_S07_value_cheap":    _box(cape_now is not None and cape_now <= 25, cape_now),
        # S15: 반도체 선행 바닥
        "V2_S15_sox_lead_bottom":_box(sox_1m is not None and spx_1m is not None
                                      and qqq_dd is not None and sox_dd is not None
                                      and sox_1m > spx_1m and qqq_dd < -15 and sox_dd < -20,
                                      sox_1m, spx_1m, qqq_dd, sox_dd),
        # S20: SPX 200dma 탈환
        "V3_S20_200dma_recover": _box(spx_above_200dma_recovery is True
                                      or spx_above_200dma_recovery is False,
                                      spx_above_200dma_recovery)
                                    if spx_above_200dma_recovery is not None else None,
    }

    # ─── 여름 ───
    # η 롤백 (2026-04-29) — 여름 B축 억제 가드가 backtest 40%+ 시점에서 발동되어
    # n_eval -5.9, 여름 빠진 점수를 가을이 아니라 봄/겨울이 가져감. 효과 반대 방향.
    B_summer = {
        # U01: 3M10Y 정상 + 1년 무역전
        "B1_U01_normal_1y":      _box(inv_state == "normal" and inv_state_1y == "normal",
                                      inv_state, inv_state_1y),
        # U02: 2Y10Y 정상
        "B2_U02_2y10y_normal":   _box(inv_state_2y10y == "normal", inv_state_2y10y),
        # U04: 연준 안정
        "B3_U04_fed_stable":     _box(ff_6m_chg is not None and abs(ff_6m_chg) <= 0.5,
                                      ff_6m_chg),
    }
    C_summer = {
        # U03: HY < 4
        "C1_U03_hy_low":         _box(hy_pct is not None and hy_pct < 4.0, hy_pct),
        # U11: VIX 1M_avg < 18
        "C2_U11_vix_calm":       _box(vix_1m_avg is not None and vix_1m_avg < 18, vix_1m_avg),
        # U12: DXY 안정/약세
        "C3_U12_dxy_stable":     _box(dxy_3m_chg_pct is not None and abs(dxy_3m_chg_pct) < 3
                                      and dxy_now is not None and dxy_now < 105,
                                      dxy_3m_chg_pct, dxy_now),
    }
    R_summer = {
        # U07: 고용 강세
        "R1_U07_jobs_strong":    _box(un_3m_chg is not None and un_3m_chg <= 0
                                      and payems_3m_avg is not None and payems_3m_avg > 150,
                                      un_3m_chg, payems_3m_avg),
        # CFNAI 확장 (ζ 패치 — USSLIND 폐기 후 대체)
        # CFNAI 3M avg > 0 = 평균 이상 성장 = 확장 영역
        "R2_CFNAI_expansion":    _box(cfnai_3m_avg is not None and cfnai_3m_avg > 0,
                                      cfnai_3m_avg),
        # 고용 솔리드 (U07 보강)
        "R3_jobs_solid":         _box(payems_3m_avg is not None and payems_3m_avg > 100,
                                      payems_3m_avg),
    }
    V_summer = {
        # U06: 밸류 정당화 (α 패치 — fpe 폐기, cape ≤ 30)
        "V1_U06_value_ok":       _box(cape_now is not None and cape_now <= 30, cape_now),
        # U22: cape < 30 (광기 가드 — 사용자 지시로 유지. U06과 사실상 중복)
        "V2_U22_cape_guard":     _box(cape_now is not None and cape_now < 30, cape_now),
        # U10: 시장 폭 건강
        "V3_U10_breadth":        _box(rsp_6m is not None and rsp_6m > 0
                                      and qqq_6m is not None and qqq_6m > 0,
                                      rsp_6m, qqq_6m),
    }

    # ─── 가을 ───
    B_autumn = {
        # A01: 3M10Y 역전 진입/심화
        "B1_A01_invert":         _box(inv_state in ("entering", "deepening", "deep_stable"),
                                      inv_state),
        # A05: 고점 인하/긴축 (ff_pos≥70)
        "B2_A05_high_zone":      _box(ff_pos_pct is not None and ff_pos_pct >= 70, ff_pos_pct),
        # A13: 달러 급등
        "B3_A13_dxy_surge":      _or_box(
                                    (dxy_6m_chg_pct is not None and dxy_6m_chg_pct > 8,
                                     dxy_6m_chg_pct),
                                    (dxy_now is not None and dxy_now > 110, dxy_now)),
        # A26: LEI/CEI 하락 전환 → CFNAI 둔화 proxy (θ 패치, 임계 완화)
        # cfnai_3m_avg < 0 단독 (둔화 영역). 6m_avg 비교 조건 폐기 — 너무 좁음.
        "B4_A26_cfnai_decel":    _box(cfnai_3m_avg is not None and cfnai_3m_avg < 0,
                                      cfnai_3m_avg),
    }
    C_autumn = {
        # A03: HY 확산
        "C1_A03_hy_widen":       _or_box(
                                    (hy_6m_chg is not None and hy_6m_chg > 0.8, hy_6m_chg),
                                    (hy_3m_chg is not None and hy_3m_chg > 0.5, hy_3m_chg)),
        # A04: HY/VIX 디커플링 (δ 패치 — 2가지 패턴 OR)
        #  가지1: hy_3m_chg > 0.3 AND vix < 18 (방향성형 — 2021 패턴)
        #  가지2: hy_pct > hy_1y_avg * 1.1 AND vix < 20 (수준형 — 1999 패턴)
        "C2_A04_decouple":       _or_box(
                                    (hy_3m_chg is not None and hy_3m_chg > 0.3
                                     and vix_now is not None and vix_now < 18,
                                     hy_3m_chg, vix_now),
                                    (hy_pct is not None and hy_1y_avg is not None
                                     and hy_pct > hy_1y_avg * 1.1
                                     and vix_now is not None and vix_now < 20,
                                     hy_pct, hy_1y_avg, vix_now)),
        # A17: VIX 압축
        "C3_A17_vix_compress":   _box(vix_1m_avg is not None and vix_1m_avg < 14, vix_1m_avg),
    }
    R_autumn = {
        # A11: 메가캡 의존
        "R1_A11_megacap":        _box(spy_3m is not None and rsp_3m is not None
                                      and spy_3m > rsp_3m + 4 and rsp_3m < 0,
                                      spy_3m, rsp_3m),
        # A12: WTI 공급충격
        "R2_A12_wti_shock":      _box(wti_3m_chg_pct is not None and wti_3m_chg_pct > 15
                                      and spx_3m is not None and spx_3m < 0,
                                      wti_3m_chg_pct, spx_3m),
        # A25: SPX 1Y z > +2σ
        "R3_A25_spx_1y_extreme": _box(spx_1y_z is not None and spx_1y_z > 2.0, spx_1y_z),
    }
    V_autumn = {
        # A08: 밸류 극단 (α 패치 — fpe→tpe 교체. tpe ≥ 28 OR cape ≥ 32)
        "V1_A08_value_high":     _or_box(
                                    (tpe_now is not None and tpe_now >= 28, tpe_now),
                                    (cape_now is not None and cape_now >= 32, cape_now)),
        # A09: CAPE 극단
        "V2_A09_cape_extreme":   _or_box(
                                    (cape_now is not None and cape_now >= 35, cape_now),
                                    (cape_now is not None and cape_now >= 32
                                     and cape_20y_pct is not None and cape_20y_pct >= 85,
                                     cape_now, cape_20y_pct)),
        # A10: 반도체 선행 약세
        "V3_A10_sox_lead_weak":  _box(sox_3m is not None and spx_3m is not None
                                      and spx_6m is not None and sox_1m is not None
                                      and sox_3m < spx_3m and spx_6m > 0 and sox_1m < 0,
                                      sox_3m, spx_3m, spx_6m, sox_1m),
    }

    # ─── 겨울 ───
    B_winter = {
        # W03: 연준 진짜 인하 진행
        "B1_W03_real_cut":       _box(ff_3m_chg is not None and ff_3m_chg < -0.5, ff_3m_chg),
        # W01의 A부분: 정상화 + 실업 동반 악화
        "B2_W01A_norm_un":       _box(inv_state == "recovering"
                                      and un_3m_chg is not None and un_3m_chg > 0.3,
                                      inv_state, un_3m_chg),
        # 실질금리 급락 (A등급 합성)
        "B3_real_rate_crash":    _box(real_rate_chg_6m is not None and real_rate_chg_6m < -1.0,
                                      real_rate_chg_6m),
    }
    C_winter = {
        # W02: HY > 5
        "C1_W02_hy_high":        _box(hy_pct is not None and hy_pct > 5.0, hy_pct),
        # W08: VIX > 30 + 1M_avg > 25
        "C2_W08_vix_panic":      _box(vix_now is not None and vix_now > 30
                                      and vix_1m_avg is not None and vix_1m_avg > 25,
                                      vix_now, vix_1m_avg),
        # W09: 달러 시스템 리스크
        "C3_W09_dxy_system":     _box(dxy_now is not None and dxy_now > 110
                                      and dxy_3m_chg_pct is not None and dxy_3m_chg_pct > 5,
                                      dxy_now, dxy_3m_chg_pct),
    }
    R_winter = {
        # W07: 실업률 폭증
        "R1_W07_un_surge":       _box(un_3m_chg is not None and un_3m_chg > 0.5, un_3m_chg),
        # W14: NFP 수축
        "R2_W14_nfp_shrink":     _box(payems_3m_avg is not None and payems_3m_avg < 0,
                                      payems_3m_avg),
        # W11: 유가 수요 붕괴
        "R3_W11_wti_crash":      _box(wti_3m_chg_pct is not None and wti_3m_chg_pct < -20,
                                      wti_3m_chg_pct),
    }
    V_winter = {
        # W06: 하락 둔화 (QQQ DD<-20% AND qqq 1M abs<3%)
        "V1_W06_decline_slow":   _box(qqq_dd is not None and qqq_dd < -20
                                      and qqq_1m_abs is not None and qqq_1m_abs < 3,
                                      qqq_dd, qqq_1m_abs),
        # W19: 200dma 하회 60D
        "V2_W19_below_200dma":   (None if spx_below_200dma_60d is None
                                   else bool(spx_below_200dma_60d)),
        # cape 급락
        "V3_cape_crash":         _box(cape_3m_chg_pct is not None and cape_3m_chg_pct < -10,
                                      cape_3m_chg_pct),
    }

    seasons = {
        "봄":   {"B": B_spring, "C": C_spring, "R": R_spring, "V": V_spring},
        "여름": {"B": B_summer, "C": C_summer, "R": R_summer, "V": V_summer},
        "가을": {"B": B_autumn, "C": C_autumn, "R": R_autumn, "V": V_autumn},
        "겨울": {"B": B_winter, "C": C_winter, "R": R_winter, "V": V_winter},
    }

    # ═══ 점수 ═══
    def _axis_score(axis_dict):
        n_true = sum(1 for v in axis_dict.values() if v is True)
        n_eval = sum(1 for v in axis_dict.values() if v is not None)
        if n_eval == 0: return None, 0, 0
        return AXIS_SCORE_TABLE[min(n_true, len(AXIS_SCORE_TABLE) - 1)], n_true, n_eval

    out = {}
    for season, axes in seasons.items():
        axis_scores = {}; boxes_lit = {}; total = 0.0
        for axis_name, axis_dict in axes.items():
            score, nt, ne = _axis_score(axis_dict)
            axis_scores[axis_name] = {"score": score, "n_true": nt, "n_eval": ne}
            boxes_lit[axis_name] = dict(axis_dict)
            if score is not None: total += score
        out[season] = {"score": round(total, 2), "axes": axis_scores, "boxes": boxes_lit}

    pa_active = (vix_now is not None and vix_now > 40
                 and spx_dd_3m is not None and spx_dd_3m < -10)
    # ι 패치: PA 봄 예외 — 봄 score ≥ 7.0 면 PA 무시 (시스템이 바닥 회복 강하게 인식)
    spring_high_recovery = out["봄"]["score"] >= 7.0
    if pa_active and not spring_high_recovery:
        top_season = "겨울"
    else:
        top_season = max(out.items(), key=lambda kv: kv[1]["score"])[0]

    return {
        "seasons": out,
        "top": top_season,
        "pa_active": bool(pa_active),
    }


# ═══ 디버그/덤프 ═══
def dump(res, label=""):
    print(f"\n=== V8.0 A48 {label} ===")
    print(f"top = {res['top']}   pa_active = {res['pa_active']}")
    for season, info in res["seasons"].items():
        ax_str = "  ".join(
            f"{a}={d['score'] if d['score'] is not None else '-'}"
            f"({d['n_true']}/{d['n_eval']})"
            for a, d in info["axes"].items())
        print(f"  {season:>2s}  total={info['score']:5.2f}   {ax_str}")

def dump_full(res, label=""):
    dump(res, label)
    for season, info in res["seasons"].items():
        print(f"  --- {season} 박스 ---")
        for axis, boxes in info["boxes"].items():
            lit = [k for k, v in boxes.items() if v is True]
            unk = [k for k, v in boxes.items() if v is None]
            print(f"    {axis}: lit={lit}  none={unk}")


# ═══ 5시점 시뮬 ═══
def simulate_5_points(raw, today=None, full=False):
    if today is None:
        today = pd.Timestamp.today().date()
    targets = [
        ("1999-12 광기",  _date(1999, 12, 31)),
        ("2007-10 광기",  _date(2007, 10, 31)),
        ("2021-12 광기",  _date(2021, 12, 31)),
        ("2024-12 광기",  _date(2024, 12, 31)),
        ("today",          today),
    ]
    results = {}
    print("\n" + "=" * 80)
    print("V8.0 A48 prototype — 5시점 시뮬")
    print("=" * 80)
    for label, target in targets:
        offset = max(0, (today - target).days)
        try:
            res = evaluate_v8_a48(raw, offset)
            (dump_full if full else dump)(res, label=f"{label}  (offset={offset})")
            results[label] = res
        except Exception as e:
            print(f"\n=== V8.0 A48 {label} FAIL: {e} ===")
            import traceback; traceback.print_exc()
            results[label] = None
    return results


if __name__ == "__main__":
    import season_engine_core as M
    if M.raw_data is None:
        print("[V8.0 A48] raw_data 미구축 — build_raw_data() 호출 중...")
        M.build_raw_data(verbose=True)
    simulate_5_points(M.raw_data, full=True)
