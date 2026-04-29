# -*- coding: utf-8 -*-
"""V8.0 1층 — 계절 판정 (봄 15 + 여름 12 + 가을 14 + 겨울 12 = 53박스 풀, raw count).
   2026-04-29 보강 명세 적용 — 코퍼스/36박스 출처별로 박스 풀 두텁게.
   2026-04-29 1단계 — 실적 박스(R4) 4개 추가. eg(earnings growth) 일별 시계열 활용.
   2026-04-29 2단계 — 봄 교차 박스(S_CROSS1, S_CROSS2) 2개 추가. Fidelity 선행/후행 교차.
   2026-04-29 도함수 — S_B4 (봄 채권금리곡선 급개선) + A_B4 (가을 채권금리곡선 급악화) 2박스.
   사용자 권고 38박스 채점은 ablation 스크립트의 keep set 으로 결정.

원본 (meerkat_observatory.py / season_engine_core.py / season_classifier_v651.py) 무수정.

설계 (사용자 2026-04-29 명세서):
  - 1층 = 현재 경제 상태. 봄/여름/가을/겨울.
  - 가중치 없음. raw count. 36박스 방식 복귀.
  - 가산 / override / PA 가드 전면 폐기.
  - 밸류 가드 (봄/여름 cape) 만 박스 형태로 유지 — 현재 밸류 상태 측정.
  - fpe 영구 금지 (1980-2024 결측률 99%+).
  - stale 게이트 90일 필수.
  - 동률 시 사이클 후순위 (봄→여름→가을→겨울 중 뒤쪽).

박스 정의: 사용자 명세 그대로 (이름/조건/출처 보존).
"""
import sys, math, json
from pathlib import Path
from datetime import date as _date
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from season_engine_core import (
    _safe_iloc_at, _pct_change_at, _abs_change_at, _percentile_at,
    _trim_series_at_offset, _inv_state_at,
)
from season_engine_v8_helpers import (
    load_tpe_series, load_cfnai_series, load_iclaims_series,
    _staleness_filtered, _box, _or_box,
)


# ═══ Baa proxy (1996 이전 HY OAS 대체, R² 0.85 / 351개월 회귀) ═══
V8_BAA_PROXY_A = 3.1562
V8_BAA_PROXY_B = -2.3761
V8_BAA_PROXY_CUTOFF = pd.Timestamp("1997-01-01")

_BAA_SPREAD_CACHE = None
def load_baa_spread_series():
    """FRED BAA - GS10 스프레드 (월간, 1953+). Backtest 1996 이전 시점 proxy 입력."""
    global _BAA_SPREAD_CACHE
    if _BAA_SPREAD_CACHE is not None: return _BAA_SPREAD_CACHE
    try:
        cfg_path = Path.home() / ".meerkat" / "config.json"
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        from fredapi import Fred
        fr = Fred(api_key=cfg["fred_api_key"])
        baa = fr.get_series("BAA", observation_start="1953-01-01").dropna()
        gs10 = fr.get_series("GS10", observation_start="1953-01-01").dropna()
        common = baa.index.intersection(gs10.index)
        if len(common) == 0: return None
        spread = (baa.loc[common] - gs10.loc[common])
        _BAA_SPREAD_CACHE = spread
        return spread
    except Exception as e:
        print(f"[BAA proxy fetch FAIL] {e}", flush=True)
        return None


def _hy_proxy_pct_at_offset(baa_spread_s, offset):
    """1996 이전 시점이면 Baa proxy 로 hy 값(%p) 반환. 그 외 None.
    hy_s 인자 제거 — stale 게이트로 hy_s 가 None 된 시점에도 작동.
    offset 기준은 baa_spread 의 last date.
    """
    if baa_spread_s is None or len(baa_spread_s) == 0: return None
    try:
        ref_date = baa_spread_s.index[-1] - pd.Timedelta(days=offset)
    except Exception:
        return None
    if ref_date >= V8_BAA_PROXY_CUTOFF:
        return None
    sub = baa_spread_s[baa_spread_s.index <= ref_date].dropna()
    if len(sub) == 0: return None
    baa_val = float(sub.iloc[-1])
    return V8_BAA_PROXY_A * baa_val + V8_BAA_PROXY_B


# ═══ eg (earnings growth) 시계열 로더 (1단계 — 실적 박스용) ═══
_EG_CACHE = None
def load_eg_series():
    """trailing_earnings_history.json 의 eg(YoY %) 일별 시계열. 1990+."""
    global _EG_CACHE
    if _EG_CACHE is not None: return _EG_CACHE
    p = Path.home() / ".meerkat" / "cache" / "trailing_earnings_history.json"
    if not p.exists(): return None
    try:
        with open(p, encoding="utf-8") as f:
            obj = json.load(f)
        recs = obj.get("data", [])
        if not recs: return None
        df = pd.DataFrame(recs)
        df["date"] = pd.to_datetime(df["date"])
        s = df.set_index("date")["eg"].dropna()
        if len(s) == 0: return None
        _EG_CACHE = s
        return s
    except Exception:
        return None

CYCLE = ["봄", "여름", "가을", "겨울"]
SEASON_PREFIX = {"봄": "S_", "여름": "U_", "가을": "A_", "겨울": "W_"}

# ═══ Grid search 임계 (모듈 globals — setattr 로 변경) ═══
GS_A_V1_CAPE   = 32      # A_V1 cape 가지
GS_A_V2_CAPE   = 35      # A_V2 단독 cape 가지
GS_S_V1        = 25      # S_V1 봄 밸류 가드 (cape ≤)
GS_U_V1        = 30      # U_V1 여름 밸류 가드 (cape <)
GS_S_B4        = 0.3     # S_B4 채권금리곡선 도함수 (+)
GS_A_B4        = -0.3    # A_B4 채권금리곡선 도함수 (-)
GS_S_C1        = -1.0    # S_C1 hy_6m_chg <
GS_A_C1        = 0.8     # A_C1 hy_6m_chg >
GS_S_R4        = 1       # S_R4 eg_3m_chg >
GS_A_R4        = -1      # A_R4 eg_3m_chg <
GS_W_R4        = -5      # W_R4 eg_now <
GS_W_B1        = 0.3     # W_B1 un_3m_chg >


# ═══ SPX 보조 헬퍼 ═══
def _spx_dd_52w(spx, offset):
    """SPX 52주 고점 대비 % drawdown (음수 = 하락). 봄 R2 / 겨울 R2 / S_V2 / A보조."""
    if spx is None: return None
    s_t = _trim_series_at_offset(spx, offset)
    if s_t is None or len(s_t) < 252: return None
    peak = float(s_t.iloc[-252:].max()); cur = float(s_t.iloc[-1])
    if peak <= 0: return None
    return (cur / peak - 1) * 100

def _sox_dd_52w(sox, offset):
    if sox is None: return None
    s_t = _trim_series_at_offset(sox, offset)
    if s_t is None or len(s_t) < 252: return None
    peak = float(s_t.iloc[-252:].max()); cur = float(s_t.iloc[-1])
    if peak <= 0: return None
    return (cur / peak - 1) * 100

def _spx_200dma(spx, offset):
    if spx is None: return None
    s_t = _trim_series_at_offset(spx, offset)
    if s_t is None or len(s_t) < 200: return None
    return float(s_t.iloc[-200:].mean())

def _spx_days_below_200dma(spx, offset, lookback=120):
    """직전 lookback 일 동안 SPX < 200dma 인 일수 (연속 아닌 누적)."""
    if spx is None: return None
    s_t = _trim_series_at_offset(spx, offset)
    if s_t is None or len(s_t) < 200 + lookback: return None
    rolling_ma = s_t.rolling(200).mean()
    last_close = s_t.iloc[-lookback:]
    last_ma = rolling_ma.iloc[-lookback:]
    pairs = list(zip(last_close.values, last_ma.values))
    if any(pd.isna(c) or pd.isna(m) for c, m in pairs): return None
    return sum(1 for c, m in pairs if c < m)


# ═══ 메인 평가 ═══
def evaluate_v8_layer1(raw, offset, tpe_series=None, cfnai_series=None,
                        eg_series=None, iclaims_series=None,
                        stale_max_lag_days=90):
    """V8.0 1층 — 49박스 평가 (R4 실적 4개 포함)."""
    tpe = tpe_series if tpe_series is not None else load_tpe_series()
    cfnai = cfnai_series if cfnai_series is not None else load_cfnai_series()
    eg = eg_series if eg_series is not None else load_eg_series()
    iclaims = iclaims_series if iclaims_series is not None else load_iclaims_series()
    raw_ext = dict(raw)
    raw_ext["tpe_s"] = tpe
    raw_ext["cfnai_s"] = cfnai
    raw_ext["eg_s"] = eg
    raw_ext["iclaims_s"] = iclaims
    raw_ext = _staleness_filtered(raw_ext, offset, max_lag_days=stale_max_lag_days)

    ff = raw_ext.get("ff_s"); hy = raw_ext.get("hy_s")
    unrate = raw_ext.get("unrate_s")
    inv3m10y = raw_ext.get("t10y3m_s")
    cape = raw_ext.get("cape_s")
    cpi_yoy = raw_ext.get("cpi_yoy_s")
    vix = raw_ext.get("vix_s"); sox = raw_ext.get("sox_s"); spx = raw_ext.get("spx_s")
    rsp = raw_ext.get("rsp_s"); spy = raw_ext.get("spy_s")
    payems = raw_ext.get("payems_s")
    dxy = raw_ext.get("dxy_s"); wti = raw_ext.get("wti_s")
    tpe = raw_ext.get("tpe_s"); cfnai = raw_ext.get("cfnai_s")
    fpe = raw_ext.get("fpe_s")  # 옵션 C: today 만 가용 (multpl + Top10가중)
    eg = raw_ext.get("eg_s")
    iclaims = raw_ext.get("iclaims_s")

    # ─ 시그널 ─
    inv_state = _inv_state_at(inv3m10y, offset)
    ff_3m_chg = _abs_change_at(ff, offset, 90)
    ff_6m_chg = _abs_change_at(ff, offset, 180)
    ff_pos_pct = _percentile_at(ff, offset, 252 * 10)
    # 도함수: t10y3m 3개월 변화량 (스프레드 속도)
    t10y3m_3m_chg = _abs_change_at(inv3m10y, offset, 90)
    # S_B4 수준 게이트용 — 현재 t10y3m 값
    t10y3m_now = _safe_iloc_at(inv3m10y, offset)

    # HY 정규화 + Baa proxy (1996 이전 backtest 시점 대체, hy_s 가 stale-filtered 되어도 작동)
    hy_now_raw = _safe_iloc_at(hy, offset)
    hy_pct = (hy_now_raw * 100) if (hy_now_raw is not None and hy_now_raw <= 1.0) else hy_now_raw
    _baa_s = load_baa_spread_series() if (hy_pct is None and offset > 0) else None
    if hy_pct is None and _baa_s is not None:
        _proxy = _hy_proxy_pct_at_offset(_baa_s, offset)
        if _proxy is not None:
            hy_pct = _proxy
    def _hy_chg(lookback):
        c = _abs_change_at(hy, offset, lookback)
        if c is not None:
            return c * 100 if (hy_now_raw is not None and hy_now_raw <= 1.0) else c
        # proxy fallback (1996 이전)
        if offset > 0:
            _bs = load_baa_spread_series()
            cur = _hy_proxy_pct_at_offset(_bs, offset)
            past = _hy_proxy_pct_at_offset(_bs, offset + lookback)
            if cur is not None and past is not None:
                return cur - past
        return None
    hy_6m_chg = _hy_chg(180); hy_3m_chg = _hy_chg(90)
    hy_max_1y_raw = None
    if hy is not None:
        hy_t = _trim_series_at_offset(hy, offset)
        if hy_t is not None and len(hy_t) >= 252:
            try: hy_max_1y_raw = float(hy_t.iloc[-252:].max())
            except Exception: pass
    hy_max_1y = ((hy_max_1y_raw * 100)
                 if (hy_max_1y_raw is not None and hy_max_1y_raw <= 1.0) else hy_max_1y_raw)
    # proxy fallback for hy_max_1y (1996 이전, baa 1년 max 변환)
    if hy_max_1y is None and offset > 0:
        _bs = load_baa_spread_series()
        if _bs is not None and len(_bs) > 0:
            try:
                _ref = _bs.index[-1] - pd.Timedelta(days=offset)
                if _ref < V8_BAA_PROXY_CUTOFF:
                    _start = _ref - pd.Timedelta(days=365)
                    _win = _bs[(_bs.index >= _start) & (_bs.index <= _ref)].dropna()
                    if len(_win) > 0:
                        _baa_max = float(_win.max())
                        hy_max_1y = V8_BAA_PROXY_A * _baa_max + V8_BAA_PROXY_B
            except Exception: pass

    vix_now = _safe_iloc_at(vix, offset)
    vix_t = _trim_series_at_offset(vix, offset) if vix is not None else None
    vix_1m_avg = (float(vix_t.iloc[-22:].mean())
                  if (vix_t is not None and len(vix_t) >= 22) else None)
    vix_90d_max = (float(vix_t.iloc[-90:].max())
                   if (vix_t is not None and len(vix_t) >= 90) else None)

    un_now = _safe_iloc_at(unrate, offset)
    un_3m_chg = _abs_change_at(unrate, offset, 90)
    cpi_now = _safe_iloc_at(cpi_yoy, offset)
    cpi_3m_chg = _abs_change_at(cpi_yoy, offset, 90)

    payems_t = _trim_series_at_offset(payems, offset) if payems is not None else None
    payems_3m_avg = None
    if payems_t is not None and len(payems_t) >= 4:
        diffs = payems_t.iloc[-3:].diff().dropna()
        if len(diffs) > 0: payems_3m_avg = float(diffs.mean())

    cape_now = _safe_iloc_at(cape, offset)
    cape_20y_pct = _percentile_at(cape, offset, 252 * 20) if cape is not None else None
    cape_3m_chg_pct = _pct_change_at(cape, offset, 90)
    tpe_now = _safe_iloc_at(tpe, offset)
    tpe_3m_chg = _abs_change_at(tpe, offset, 90)
    fpe_now = _safe_iloc_at(fpe, offset)  # 옵션 C: today 가용 시 A_V1 OR 분기 활성
    # 실적 (B등급, 1단계 추가)
    eg_now = _safe_iloc_at(eg, offset)
    eg_3m_chg = _abs_change_at(eg, offset, 90)

    spx_3m = _pct_change_at(spx, offset, 63);   spx_6m = _pct_change_at(spx, offset, 126)
    spx_1m = _pct_change_at(spx, offset, 22)
    sox_3m = _pct_change_at(sox, offset, 63);   sox_6m = _pct_change_at(sox, offset, 126)
    sox_1m = _pct_change_at(sox, offset, 22)
    spy_3m = _pct_change_at(spy, offset, 63)
    rsp_3m = _pct_change_at(rsp, offset, 63);   rsp_6m = _pct_change_at(rsp, offset, 126)

    spx_now = _safe_iloc_at(spx, offset)
    spx_200 = _spx_200dma(spx, offset)
    spx_below_200_days = _spx_days_below_200dma(spx, offset, lookback=120)
    spx_dd_52 = _spx_dd_52w(spx, offset);   sox_dd_52 = _sox_dd_52w(sox, offset)

    dxy_now = _safe_iloc_at(dxy, offset)
    dxy_3m_chg_pct = _pct_change_at(dxy, offset, 90)
    dxy_6m_chg_pct = _pct_change_at(dxy, offset, 180)
    wti_3m_chg_pct = _pct_change_at(wti, offset, 90)
    # HY 1M chg (W_C3 신용 급확산)
    hy_1m_chg = _hy_chg(30)

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

    # Initial Claims 도함수 (S_B1/W_B1 분기)
    # iclaims_4w_now = 직근 4주 평균. iclaims_4w_3m_max = 직근 13주 동안 4w_avg 의 최댓값.
    # < max * 0.9 = 정점에서 10% 이상 하락 = 회복 진행 (봄). 그 외 = 정점 근처 (겨울).
    iclaims_4w_now = None
    iclaims_4w_3m_max = None
    if iclaims is not None:
        ic_t = _trim_series_at_offset(iclaims, offset)
        if ic_t is not None and len(ic_t) >= 13:
            try:
                ic_4w = ic_t.rolling(4).mean().dropna()
                if len(ic_4w) >= 13:
                    iclaims_4w_now = float(ic_4w.iloc[-1])
                    iclaims_4w_3m_max = float(ic_4w.iloc[-13:].max())
            except Exception:
                pass

    # ════════════════ 박스 정의 ════════════════
    boxes = {}

    # ─ 봄 8박스 ─
    # S_B1: 채권금리곡선 정상화 + Initial Claims 정점 후 하락 (ε 패치 v3)
    # recovering 의 두 얼굴 분기 (Initial Claims 도함수 게이트):
    #   iclaims_4w_now < iclaims_4w_3m_max * 0.9 = 정점에서 10%+ 하락 (회복 진행) → 봄
    #   그 외 = 정점 근처/상승 중 (W_B1 으로 분기)
    # ff_3m_chg 게이트 폐기 — 2023-01 같이 인하 없이도 봄 가능 (Initial Claims 가 더 정밀)
    boxes["S_B1"] = _box(inv_state == "recovering"
                         and iclaims_4w_now is not None and iclaims_4w_3m_max is not None
                         and iclaims_4w_3m_max > 0
                         and iclaims_4w_now < iclaims_4w_3m_max * 0.9,
                         inv_state, iclaims_4w_now, iclaims_4w_3m_max)
    boxes["S_B2"] = _box(ff_3m_chg is not None and ff_3m_chg < 0
                         and ff_pos_pct is not None and ff_pos_pct < 30,
                         ff_3m_chg, ff_pos_pct)
    boxes["S_C1"] = _box(hy_6m_chg is not None and hy_6m_chg < GS_S_C1
                         and hy_max_1y is not None and hy_max_1y >= 5.0,
                         hy_6m_chg, hy_max_1y)
    boxes["S_C2"] = _box(vix_90d_max is not None and vix_90d_max >= 35
                         and vix_now is not None and vix_now < 25,
                         vix_90d_max, vix_now)
    boxes["S_R1"] = _or_box(
        (un_now is not None and un_now >= 4.0, un_now),
        (un_3m_chg is not None and un_3m_chg > 0.5, un_3m_chg))
    boxes["S_R2"] = _box(spx_dd_52 is not None and spx_dd_52 < -20, spx_dd_52)
    boxes["S_V1"] = _box(cape_now is not None and cape_now <= GS_S_V1, cape_now)
    boxes["S_V2"] = _box(sox_1m is not None and spx_1m is not None
                         and spx_dd_52 is not None and sox_dd_52 is not None
                         and sox_1m > spx_1m and spx_dd_52 < -15 and sox_dd_52 < -20,
                         sox_1m, spx_1m, spx_dd_52, sox_dd_52)
    # 봄 보강 (사용자 2026-04-29): S_B3 달러 급락 (코퍼스), S_R3 인플레 종결, S_C3 다중 트리거
    # S_B3 (코퍼스 "달러 떨어지면 숏 잡지 마라" — 22년 9월 롱 전환 직접 근거)
    boxes["S_B3"] = _box(dxy_6m_chg_pct is not None and dxy_6m_chg_pct < -5,
                         dxy_6m_chg_pct)
    # S_R3 (36#8): cpi_yoy_3m_chg < 0 AND cpi_yoy < 3
    boxes["S_R3"] = _box(cpi_3m_chg is not None and cpi_3m_chg < 0
                         and cpi_now is not None and cpi_now < 3,
                         cpi_3m_chg, cpi_now)
    # S_C3 (36#9): un≥4 + vix_90d_max≥35 + spx_dd_52w<-20 중 2+ 점등
    _s_c3_deps = [un_now, vix_90d_max, spx_dd_52]
    if all(d is None for d in _s_c3_deps):
        boxes["S_C3"] = None
    else:
        triggers = sum([
            (un_now is not None and un_now >= 4.0),
            (vix_90d_max is not None and vix_90d_max >= 35),
            (spx_dd_52 is not None and spx_dd_52 < -20),
        ])
        boxes["S_C3"] = triggers >= 2
    # S_R4: 봄 "역실적 + 가속 회복" — 실적 마이너스에서 가속 회복 진입
    boxes["S_R4"] = _box(eg_now is not None and eg_now < 0
                         and eg_3m_chg is not None and eg_3m_chg > GS_S_R4,
                         eg_now, eg_3m_chg)
    # S_CROSS1: 신용 개선 중 + 고용 악화 잔존 (Fidelity 선행/후행 교차)
    #   여름이면 un < 4 라 꺼짐. 겨울이면 hy 악화 중이라 꺼짐. 봄에서만 동시 점등.
    boxes["S_CROSS1"] = _box(hy_3m_chg is not None and hy_3m_chg < -0.3
                             and un_now is not None and un_now >= 4.0,
                             hy_3m_chg, un_now)
    # S_CROSS2: 경기활동 반등 + 실업 아직 상승
    #   여름이면 un_3m_chg ≤ 0 라 꺼짐. 겨울이면 cfnai 아직 하향이라 꺼짐.
    boxes["S_CROSS2"] = _box(cfnai_3m_avg is not None and cfnai_6m_avg is not None
                             and cfnai_3m_avg > cfnai_6m_avg
                             and un_3m_chg is not None and un_3m_chg > 0,
                             cfnai_3m_avg, cfnai_6m_avg, un_3m_chg)
    # S_B4: 봄 채권금리곡선 급속 개선 (도함수 +0.3) — F3 "주가는 속도에 반응"
    # 수준 게이트 적용 (1차 시뮬에서 겨울 GT 5/7 점등 → 명세 트리거):
    #   t10y3m_now > -0.5 일 때만 점등 = 정상/얕은역전에서만 봄. 깊은역전 차단.
    boxes["S_B4"] = _box(t10y3m_3m_chg is not None and t10y3m_3m_chg > GS_S_B4
                         and t10y3m_now is not None and t10y3m_now > -0.5,
                         t10y3m_3m_chg, t10y3m_now)
    # S_C4: 봄 HY 스프레드 급속 축소 (F3 신용 도함수, 비대칭 임계 -0.5)
    boxes["S_C4"] = _box(hy_3m_chg is not None and hy_3m_chg < -0.5, hy_3m_chg)

    # ─ 여름 8박스 ─
    boxes["U_B1"] = _box(inv_state == "normal", inv_state)
    boxes["U_B2"] = _box(ff_6m_chg is not None and abs(ff_6m_chg) < 0.5, ff_6m_chg)
    boxes["U_C1"] = _box(hy_pct is not None and hy_pct < 4.0, hy_pct)
    boxes["U_C2"] = _box(vix_1m_avg is not None and vix_1m_avg < 20, vix_1m_avg)
    boxes["U_R1"] = _box(un_3m_chg is not None and un_3m_chg <= 0
                         and payems_3m_avg is not None and payems_3m_avg > 100,
                         un_3m_chg, payems_3m_avg)
    boxes["U_R2"] = _box(rsp_6m is not None and rsp_6m > 0
                         and spx_6m is not None and spx_6m > 0,
                         rsp_6m, spx_6m)
    boxes["U_V1"] = _box(cape_now is not None and cape_now < GS_U_V1, cape_now)
    boxes["U_V2"] = _box(sox_6m is not None and spx_6m is not None and sox_6m > spx_6m,
                         sox_6m, spx_6m)
    # 여름 보강: U_B3 달러 안정/약세, U_C3 HY 안정, U_R3 CFNAI 확장
    boxes["U_B3"] = _box(dxy_3m_chg_pct is not None and abs(dxy_3m_chg_pct) < 3
                         and dxy_now is not None and dxy_now < 105,
                         dxy_3m_chg_pct, dxy_now)
    boxes["U_C3"] = _box(hy_3m_chg is not None and abs(hy_3m_chg) < 0.3, hy_3m_chg)
    boxes["U_R3"] = _box(cfnai_3m_avg is not None and cfnai_3m_avg > 0, cfnai_3m_avg)
    # U_R4: 여름 "실적 가속" — 양호한 실적 성장 + 가속/안정
    boxes["U_R4"] = _box(eg_now is not None and eg_now > 5
                         and eg_3m_chg is not None and eg_3m_chg >= 0,
                         eg_now, eg_3m_chg)

    # ─ 가을 8박스 ─
    boxes["A_B1"] = _box(inv_state in ("entering", "deepening", "deep_stable"), inv_state)
    boxes["A_B2"] = _or_box(
        (ff_pos_pct is not None and ff_pos_pct >= 70
         and ff_3m_chg is not None and ff_3m_chg < 0,
         ff_pos_pct, ff_3m_chg),
        (ff_6m_chg is not None and ff_6m_chg > 0.5, ff_6m_chg))
    boxes["A_C1"] = _or_box(
        (hy_6m_chg is not None and hy_6m_chg > GS_A_C1, hy_6m_chg),
        (hy_3m_chg is not None and hy_3m_chg > 0.5, hy_3m_chg))
    boxes["A_C2"] = _box(cfnai_3m_avg is not None and cfnai_3m_avg < 0, cfnai_3m_avg)
    boxes["A_R1"] = _box(sox_3m is not None and spx_3m is not None
                         and spx_6m is not None and sox_1m is not None
                         and sox_3m < spx_3m and spx_6m > 0 and sox_1m < 0,
                         sox_3m, spx_3m, spx_6m, sox_1m)
    boxes["A_R2"] = _box(spy_3m is not None and rsp_3m is not None
                         and (spy_3m - rsp_3m) > 4 and rsp_3m < 0,
                         spy_3m, rsp_3m)
    boxes["A_V1"] = _or_box(
        (tpe_now is not None and tpe_now >= 28, tpe_now),
        (cape_now is not None and cape_now >= GS_A_V1_CAPE, cape_now),
        (fpe_now is not None and fpe_now >= 22, fpe_now))   # 옵션 C: today fpe
    boxes["A_V2"] = _or_box(
        (cape_now is not None and cape_now >= GS_A_V2_CAPE, cape_now),
        (cape_now is not None and cape_now >= 32
         and cape_20y_pct is not None and cape_20y_pct >= 85,
         cape_now, cape_20y_pct))
    # 가을 보강: A_C3 신용 디커플링(36#6), A_C4 WTI 공급충격(36#7),
    #            A_B3 달러 급등(코퍼스), A_R3 실적 가속 둔화(F3+F5, B등급)
    boxes["A_C3"] = _box(hy_3m_chg is not None and hy_3m_chg > 0.3
                         and vix_now is not None and vix_now < 18,
                         hy_3m_chg, vix_now)
    boxes["A_C4"] = _box(wti_3m_chg_pct is not None and wti_3m_chg_pct > 15
                         and spx_3m is not None and spx_3m < 0,
                         wti_3m_chg_pct, spx_3m)
    boxes["A_B3"] = _or_box(
        (dxy_6m_chg_pct is not None and dxy_6m_chg_pct > 8, dxy_6m_chg_pct),
        (dxy_now is not None and dxy_now > 108, dxy_now))
    # A_R3: tpe_3m_chg > 0 AND cape_3m_chg > 0 (PER이 튄다 = 주가<<실적 빠르게 빠짐)
    boxes["A_R3"] = _box(tpe_3m_chg is not None and tpe_3m_chg > 0
                         and cape_3m_chg_pct is not None and cape_3m_chg_pct > 0,
                         tpe_3m_chg, cape_3m_chg_pct)
    # A_R4: 가을 "실적 가속 둔화" — eg 양수이나 명확한 감속 (정점 후 둔화)
    boxes["A_R4"] = _box(eg_now is not None and eg_now > 0
                         and eg_3m_chg is not None and eg_3m_chg < GS_A_R4,
                         eg_now, eg_3m_chg)
    # A_B4: 가을 채권금리곡선 급속 악화 (도함수 -0.3) — 1994/2006/2018 패턴
    boxes["A_B4"] = _box(t10y3m_3m_chg is not None and t10y3m_3m_chg < GS_A_B4,
                         t10y3m_3m_chg)
    # A_C5: 가을 HY 스프레드 급속 확산 (F3 신용 도함수)
    # 임계 +0.3 → +0.5 강화 (사용자 2026-04-29: 겨울 false alarm 차단 시도)
    boxes["A_C5"] = _box(hy_3m_chg is not None and hy_3m_chg > 0.5, hy_3m_chg)
    # A_R5: 가을 인플레 재가속 (F2 CPI 방향, 사용자 명세 4순위)
    # cpi_3m_chg > 0 (인플레 가속) + cpi_yoy > 3 (높은 수준) = 인플레 위협 가을
    boxes["A_R5"] = _box(cpi_3m_chg is not None and cpi_3m_chg > 0
                         and cpi_now is not None and cpi_now > 3,
                         cpi_3m_chg, cpi_now)

    # ─ 겨울 8박스 ─
    # W_B1: 채권금리곡선 정상화 + 실업 상승 + Initial Claims 정점 근처 (ε 패치 v3)
    # iclaims_4w_now ≥ iclaims_4w_3m_max * 0.9 = 신규 청구 아직 정점 근처/상승 → 겨울
    # iclaims_4w_now < iclaims_4w_3m_max * 0.9 인 경우는 S_B1 영역 (봄 회복기).
    boxes["W_B1"] = _box(inv_state == "recovering"
                         and un_3m_chg is not None and un_3m_chg > GS_W_B1
                         and iclaims_4w_now is not None and iclaims_4w_3m_max is not None
                         and iclaims_4w_3m_max > 0
                         and iclaims_4w_now >= iclaims_4w_3m_max * 0.9,
                         inv_state, un_3m_chg, iclaims_4w_now, iclaims_4w_3m_max)
    boxes["W_B2"] = _box(ff_3m_chg is not None and ff_3m_chg < 0
                         and ff_pos_pct is not None and ff_pos_pct >= 30,
                         ff_3m_chg, ff_pos_pct)
    boxes["W_C1"] = _box(hy_pct is not None and hy_pct > 5.0, hy_pct)
    boxes["W_C2"] = _box(vix_now is not None and vix_now > 30
                         and vix_1m_avg is not None and vix_1m_avg > 25,
                         vix_now, vix_1m_avg)
    boxes["W_R1"] = _box(un_3m_chg is not None and un_3m_chg > 0.5, un_3m_chg)
    boxes["W_R2"] = _box(spx_dd_52 is not None and spx_dd_52 < -20
                         and spx_1m is not None and abs(spx_1m) < 3,
                         spx_dd_52, spx_1m)
    boxes["W_V1"] = _box(dxy_now is not None and dxy_now > 108
                         and dxy_3m_chg_pct is not None and dxy_3m_chg_pct > 5,
                         dxy_now, dxy_3m_chg_pct)
    boxes["W_V2"] = _box(spx_now is not None and spx_200 is not None
                         and spx_below_200_days is not None
                         and spx_now < spx_200 and spx_below_200_days >= 40,
                         spx_now, spx_200, spx_below_200_days)
    # 겨울 보강: W_R3 유가 수요 붕괴, W_B3 달러 시스템 심화, W_C3 신용 급확산
    boxes["W_R3"] = _box(wti_3m_chg_pct is not None and wti_3m_chg_pct < -20,
                         wti_3m_chg_pct)
    boxes["W_B3"] = _box(dxy_now is not None and dxy_now > 110
                         and dxy_3m_chg_pct is not None and dxy_3m_chg_pct > 8,
                         dxy_now, dxy_3m_chg_pct)
    boxes["W_C3"] = _box(hy_1m_chg is not None and hy_1m_chg > 1.0, hy_1m_chg)
    # W_R4: 겨울 "실적 감소" — eg 명확한 마이너스
    boxes["W_R4"] = _box(eg_now is not None and eg_now < GS_W_R4, eg_now)
    # W_R5: 겨울 인플레 가속 음전 (F2 CPI 방향, 사용자 명세 4순위)
    # cpi_3m_chg < -0.3 = 인플레 급락 = 수요 붕괴 신호
    boxes["W_R5"] = _box(cpi_3m_chg is not None and cpi_3m_chg < -0.3, cpi_3m_chg)

    # ─ 점수 (raw count) ─
    scores = {}; n_evals = {}
    for season, pref in SEASON_PREFIX.items():
        keys = [k for k in boxes if k.startswith(pref)]
        scores[season] = float(sum(1 for k in keys if boxes[k] is True))
        n_evals[season] = sum(1 for k in keys if boxes[k] is not None)

    # 동률 처리 (GT 80 검증 결과 기반, 2026-04-29 개정):
    # 인접 동률 — 사이클 진행 방향 우선이 정답이지만, 봄=겨울만 예외:
    #   봄=여름 → 여름 (다음 진행)
    #   여름=가을 → 가을 (다음 진행)
    #   가을=겨울 → 겨울 (다음 진행)
    #   봄=겨울 → 겨울 (CHANGED) — 침체 한복판에서 회복 시그널 일부 점등 + 위기 진행 중. 위기 우선.
    #     검증: 2008-03/10, 1990-12 — 봄=겨울 동률 3건 모두 GT=겨울 (이전 룰 모두 오답)
    # 비인접 동률 — 회복 vs 정점 충돌:
    #   봄=가을 → 봄 (NEW) — 회복 우세. 검증: 1985-09 GT=봄 1건
    #   여름=겨울 → 겨울 (보수) — 거의 발생 안 함, fallback
    # 3개 이상 동률 — cycle 후순위 fallback (3-way 4건 중 3건 정답)
    max_score = max(scores.values())
    candidates = [s for s in CYCLE if scores[s] == max_score]
    if len(candidates) == 2:
        tiebreak = {
            frozenset(("봄", "겨울")): "겨울",
            frozenset(("여름", "가을")): "가을",
            frozenset(("봄", "여름")): "여름",
            frozenset(("가을", "겨울")): "겨울",
            frozenset(("봄", "가을")): "봄",
            frozenset(("여름", "겨울")): "겨울",
        }
        best = tiebreak.get(frozenset(candidates), candidates[-1])
    else:
        best = candidates[-1]

    return {"boxes": boxes, "scores": scores, "n_evals": n_evals, "best": best}


def season_probabilities(scores, temperature=2.0):
    exp_scores = {s: math.exp(v / temperature) for s, v in scores.items()}
    total = sum(exp_scores.values())
    if total == 0: return {s: 0.25 for s in scores}
    return {s: round(v / total, 3) for s, v in exp_scores.items()}


# ═══ 디버그 ═══
def dump(res, label=""):
    print(f"\n=== V8.0 1층 {label} ===")
    print(f"best = {res['best']}")
    for s in CYCLE:
        print(f"  {s:>2s}  score={int(res['scores'][s])} / n_eval={res['n_evals'][s]}")
    probs = season_probabilities(res['scores'])
    print(f"  probs = " + ", ".join(f"{s}:{probs[s]:.2f}" for s in CYCLE))

def dump_full(res, label=""):
    dump(res, label)
    for season, pref in SEASON_PREFIX.items():
        keys = [k for k in res["boxes"] if k.startswith(pref)]
        lit = [k for k in keys if res["boxes"][k] is True]
        unk = [k for k in keys if res["boxes"][k] is None]
        print(f"  {season} lit={lit}  none={unk}")


if __name__ == "__main__":
    import season_engine_core as M
    if M.raw_data is None:
        print("[V8 1층] raw_data 미구축 — build_raw_data() 호출 중...")
        M.build_raw_data(verbose=True)
    today = pd.Timestamp.today().date()
    targets = [
        ("1999-12 광기", _date(1999, 12, 31)),
        ("2007-10 광기", _date(2007, 10, 31)),
        ("2021-12 광기", _date(2021, 12, 31)),
        ("2024-12",       _date(2024, 12, 31)),
        ("today",         today),
    ]
    print("\n" + "=" * 80)
    print("V8.0 1층 5시점 dump")
    print("=" * 80)
    for label, target in targets:
        offset = max(0, (today - target).days)
        try:
            res = evaluate_v8_layer1(M.raw_data, offset)
            dump_full(res, label=f"{label}  (offset={offset})")
        except Exception as e:
            print(f"\n=== {label} FAIL: {e} ===")
            import traceback; traceback.print_exc()
