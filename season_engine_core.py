# -*- coding: utf-8 -*-
"""미어캣의 관측소 V3.14-prototype — 12박스 (공통 6 + 고유 6) 백필 평가.
원본 meerkat_observatory.py 무수정. 임시 파일 한정.
DXY = yfinance DX-Y.NYB (1971~). S3 = 영구 None (FPE/TPE proxy 거부).
"""
import sys, json
from pathlib import Path
from datetime import date as _date
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from historical_loader import load_cape_history, load_hy_oas_history, load_forward_pe_history

CONFIG = json.loads(Path.home().joinpath(".meerkat/config.json").read_text(encoding="utf-8"))
FRED_KEY = CONFIG["fred_api_key"]

# ═══ helper 함수 ═══
def _series_at_date(s, target_date):
    try:
        if s is None or len(s) == 0: return None
        sub = s[s.index <= target_date]
        if len(sub) == 0: return None
        return float(sub.iloc[-1])
    except Exception: return None

def _safe_iloc_at(s, offset):
    try:
        if s is None or len(s) == 0: return None
        if hasattr(s, "index") and isinstance(s.index, pd.DatetimeIndex):
            target = s.index[-1] - pd.Timedelta(days=offset)
            return _series_at_date(s, target)
        if len(s) <= offset: return None
        return float(s.iloc[-1 - offset])
    except Exception: return None

def _pct_change_at(s, offset, lookback_days):
    try:
        if s is None or len(s) == 0: return None
        if hasattr(s, "index") and isinstance(s.index, pd.DatetimeIndex):
            ref = s.index[-1] - pd.Timedelta(days=offset)
            cur = _series_at_date(s, ref)
            prev = _series_at_date(s, ref - pd.Timedelta(days=lookback_days))
            if cur is None or prev is None or prev == 0: return None
            return (cur / prev - 1) * 100
        return None
    except Exception: return None

def _abs_change_at(s, offset, lookback_days):
    try:
        if s is None or len(s) == 0: return None
        if hasattr(s, "index") and isinstance(s.index, pd.DatetimeIndex):
            ref = s.index[-1] - pd.Timedelta(days=offset)
            cur = _series_at_date(s, ref)
            prev = _series_at_date(s, ref - pd.Timedelta(days=lookback_days))
            if cur is None or prev is None: return None
            return cur - prev
        return None
    except Exception: return None

def _percentile_at(s, offset, lookback_days):
    try:
        if s is None or len(s) == 0: return None
        if hasattr(s, "index") and isinstance(s.index, pd.DatetimeIndex):
            ref = s.index[-1] - pd.Timedelta(days=offset)
            cur = _series_at_date(s, ref)
            if cur is None: return None
            window_start = ref - pd.Timedelta(days=lookback_days)
            window = s[(s.index >= window_start) & (s.index <= ref)].dropna()
            if len(window) < 30: return None
            return float((window <= cur).sum()) / len(window) * 100
        return None
    except Exception: return None

def _trim_series_at_offset(s, offset):
    try:
        if s is None or len(s) == 0: return None
        if hasattr(s, "index") and isinstance(s.index, pd.DatetimeIndex):
            target = s.index[-1] - pd.Timedelta(days=offset)
            return s[s.index <= target]
        return None
    except Exception: return None

def _v(value, *deps):
    """meerkat_observatory.py 의 _v() 헬퍼: deps 중 None 있으면 None, 아니면 bool(value)."""
    if any(d is None for d in deps): return None
    return bool(value)


# ═══ V6.6.x grid search 용 박스 임계 (모듈 globals) ═══
# 시나리오 스크립트에서 이 변수들을 변경 후 _evaluate_12box_at_offset 호출 시 반영
A1_HY_6M_THRESH = 1.2     # V6.6 강화 (V3.15.5 = 0)
A1_HY_3M_THRESH = 0.8     # V6.6 강화 (V3.15.5 = 0.5)
A2_VIX_1M_COMPRESS = 15.0  # V3.15.5 = 15
A2_VIX_3M_CHG_BREAK = 5.0
A2_VIX_NOW_BREAK = 18.0
A3_SOX_DELTA_THRESH = 0.0  # sox_3m < spx_3m - X. V3.15.5 = 0 (즉 sox<spx 만)
A3_SOX_1M_THRESH = 0.0     # V3.15.5 = 0
A4_SPY_RSP_3M_DELTA = 4.0  # V6.6 강화 (V3.15.5 = 3)
A4_RSP_3M_NEG_REQ = True   # V6.6 신규: rsp_3m<0 강제
A5_CAPE_THRESH = 30.0
A5_CAPE_PCT_THRESH = 85.0  # V6.6 신규: cape_20y_pct
A5_FPE_THRESH = 22.0
A5_FPE_PCT_THRESH = 85.0   # V6.6 신규: fpe_20y_pct
A6_SPX_DD_THRESH = 5.0     # V3.15.5 = 5

# V6.6.5 확장: 공통 박스 가을 분기 임계 (D grid)
C1_AUTUMN_OPT = 0          # 0=all 3 (entering+deepening+deep_stable), 1=deepening+deep_stable, 2=deep_stable only
C5_SOX_DELTA_THRESH = 0.0  # sox_3m < spx_3m - X. V3.15.5 = 0
C5_SOX_1M_THRESH = 0.0     # sox_1m < X. V3.15.5 없음 (None 검사만)

# V6.8 미세 조정 lever (오푸스 권고 grid 용)
AUTUMN_BONUS_SCALE = 0.0    # 0.0=폐지(V6.8), 0.5=절반, 1.0=원본(V6.7)
SUMMER_PENALTY_SCALE = 1.0  # summer_penalty 스케일 (1.0=원본)
WINTER_BONUS_SCALE = 1.0    # winter_bonus 스케일 (1.0=원본)
SPRING_RECOVERY_BONUS = 4.5  # spring_recovery_active 시 봄 가산 (4.5=원본, 0=폐지)
BUBBLE_GUARD_CAPE_THRESH = 25  # bubble_guard CAPE 임계 (25=원본, 28/30 = 강화)
BUBBLE_GUARD_FPE_THRESH = 20   # bubble_guard FPE 임계 (20=원본, 22/24 = 강화)
W_OVERRIDE = {}             # 박스 가중치 override (예: {"A5": 1.5, "A1": 1.5})

# ═══ 채권금리곡선 상태머신 (offset 시점 평가) ═══
def _inv_state_at(s, offset, lookback=252):
    """meerkat_observatory.py L1383 _inv_state 의 offset 변형."""
    if s is None or len(s) == 0: return None
    s_t = _trim_series_at_offset(s, offset)
    if s_t is None: return None
    s2 = s_t.dropna()
    if len(s2) < 60: return None
    cur = float(s2.iloc[-1])
    tail = s2.iloc[-min(len(s2), lookback):]
    peak_pp = float(tail.min())
    s_60d = float(s2.iloc[-60]) if len(s2) >= 60 else None
    s_6m  = float(s2.iloc[-126]) if len(s2) >= 126 else None
    if s_60d is not None and s_60d >= 0 and cur < 0:
        return "entering"
    if cur < 0 and s_60d is not None and (cur - s_60d) <= -0.10:
        return "deepening"
    if cur < 0 and s_6m is not None and s_6m < 0 and peak_pp < 0:
        rec_pct_v = (cur - peak_pp) / abs(peak_pp) * 100
        if rec_pct_v < 50:
            return "deep_stable"
    if peak_pp < 0:
        rec_pct_v = (cur - peak_pp) / abs(peak_pp) * 100
        if rec_pct_v >= 50:
            return "recovering"
    if cur > 0 and peak_pp >= 0:
        return "normal"
    return None

def _inv_recovery_pct(s, offset, lookback=252):
    """역전 회복률 (recovery_bp / abs(peak_bp) * 100). peak >= 0 이면 None."""
    if s is None: return None
    s_t = _trim_series_at_offset(s, offset)
    if s_t is None or len(s_t) < lookback: return None
    tail = s_t.iloc[-lookback:]
    peak_pp = float(tail.min()); cur_pp = float(s_t.iloc[-1])
    if peak_pp >= 0: return None
    return (cur_pp - peak_pp) / abs(peak_pp) * 100


# ═══ 12박스 평가 ═══
def _evaluate_12box_at_offset(raw, offset):
    """공통 6 + 고유 6 = 4계절 × 12박스. None-aware."""
    # 시리즈 추출
    qqq = raw.get("qqq_s"); ff = raw.get("ff_s"); hy = raw.get("hy_s")
    unrate = raw.get("unrate_s")
    inv3m10y = raw.get("t10y3m_s"); inv2y10y = raw.get("t10y2y_s")
    cpi_yoy = raw.get("cpi_yoy_s"); cape = raw.get("cape_s"); fpe = raw.get("fpe_s")
    vix = raw.get("vix_s"); sox = raw.get("sox_s"); spx = raw.get("spx_s")
    rsp = raw.get("rsp_s"); spy = raw.get("spy_s"); wti = raw.get("wti_s")
    payems = raw.get("payems_s"); dxy = raw.get("dxy_s")
    t5yifr = raw.get("t5yifr_s"); lei = raw.get("lei_s")

    # ── 공통 시그널 ──
    inv_state_3m10y = _inv_state_at(inv3m10y, offset)
    inv_state_2y10y = _inv_state_at(inv2y10y, offset)
    inv_recovery_3m10y = _inv_recovery_pct(inv3m10y, offset)
    # 1년 전 시점의 채권금리곡선 상태 (S2 normal-after-recovery 분기용)
    inv_state_3m10y_1y_ago = _inv_state_at(inv3m10y, offset + 365)
    # 3개월 전 시점 — fallback "최근 3M 내 normal 진입" 판단용
    inv_state_3m10y_3m_ago = _inv_state_at(inv3m10y, offset + 90)
    normal_recent_entry = (inv_state_3m10y == "normal"
                            and inv_state_3m10y_3m_ago is not None
                            and inv_state_3m10y_3m_ago != "normal")

    hy_now = _safe_iloc_at(hy, offset)
    hy_pct = (hy_now * 100) if (hy_now is not None and hy_now <= 1.0) else hy_now
    hy_6m_chg_raw = _abs_change_at(hy, offset, 180)
    # hy 가 0~1 단위면 hy_6m_chg 도 *100 보정
    hy_6m_chg = (hy_6m_chg_raw * 100) if (hy_now is not None and hy_now <= 1.0 and hy_6m_chg_raw is not None) else hy_6m_chg_raw

    ff_now = _safe_iloc_at(ff, offset)
    ff_3m_chg = _abs_change_at(ff, offset, 90)
    ff_6m_chg = _abs_change_at(ff, offset, 180)
    ff_pos_pct = _percentile_at(ff, offset, 252 * 10)

    sox_3m = _pct_change_at(sox, offset, 63); spx_3m = _pct_change_at(spx, offset, 63)
    sox_6m = _pct_change_at(sox, offset, 126); spx_6m = _pct_change_at(spx, offset, 126)
    sox_1m = _pct_change_at(sox, offset, 22); spx_1m = _pct_change_at(spx, offset, 22)

    fpe_now = _safe_iloc_at(fpe, offset); cape_now = _safe_iloc_at(cape, offset)
    # V6.6: CAPE 직근 20년 백분위 (시대적 컨텍스트 반영)
    # 1980-1995 같은 초기 시점은 백분위 < 20년 윈도우 미달 시 None → A5 절대값 분기 단독 평가 fallback
    cape_20y_pct = _percentile_at(cape, offset, 252 * 20) if cape is not None else None
    fpe_20y_pct = _percentile_at(fpe, offset, 252 * 20) if fpe is not None else None

    # ── 봄 시그널 ──
    un_now = _safe_iloc_at(unrate, offset)
    un_3m_chg = _abs_change_at(unrate, offset, 90)
    cpi_now = _safe_iloc_at(cpi_yoy, offset)
    cpi_3m_chg = _abs_change_at(cpi_yoy, offset, 90)
    lei_now = _safe_iloc_at(lei, offset)
    lei_yoy = _pct_change_at(lei, offset, 365)
    lei_1m_chg = _abs_change_at(lei, offset, 30)

    # ── 직전 충격 흔적 (봄 박스 재정의용) ──
    hy_max_1y = None
    try:
        hy_t = _trim_series_at_offset(hy, offset)
        if hy_t is not None and len(hy_t) >= 252:
            mx = float(hy_t.iloc[-252:].max())
            hy_max_1y = (mx * 100) if mx <= 1.0 else mx
    except Exception: pass
    un_max_2y = None; un_max_1y = None
    try:
        un_t = _trim_series_at_offset(unrate, offset)
        if un_t is not None and len(un_t) >= 24:
            un_max_2y = float(un_t.iloc[-24:].max())
        if un_t is not None and len(un_t) >= 12:
            un_max_1y = float(un_t.iloc[-12:].max())
    except Exception: pass

    # ── 여름 시그널 ──
    payems_now = _safe_iloc_at(payems, offset)
    payems_3m_avg_chg = None
    try:
        if payems is not None:
            ps_t = _trim_series_at_offset(payems, offset)
            if ps_t is not None and len(ps_t) >= 4:
                last3 = ps_t.iloc[-3:].diff().dropna()
                if len(last3) > 0: payems_3m_avg_chg = float(last3.mean())
    except Exception: pass
    payems_3m_avg_thousands = payems_3m_avg_chg  # PAYEMS 는 이미 천명 단위
    t5yifr_now = _safe_iloc_at(t5yifr, offset)

    # ── 가을 시그널 ──
    vix_1m_avg = None; vix_3m_avg = None
    try:
        vix_t = _trim_series_at_offset(vix, offset)
        if vix_t is not None and len(vix_t) >= 22:
            vix_1m_avg = float(vix_t.iloc[-22:].mean())
        if vix_t is not None and len(vix_t) >= 63:
            vix_3m_avg = float(vix_t.iloc[-63:].mean())
    except Exception: pass
    vix_now = _safe_iloc_at(vix, offset)
    vix_3m_chg = _abs_change_at(vix, offset, 90)
    dxy_6m_chg_pct = _pct_change_at(dxy, offset, 180)
    spy_3m = _pct_change_at(spy, offset, 63); rsp_3m = _pct_change_at(rsp, offset, 63)
    spy_1m = _pct_change_at(spy, offset, 22); rsp_1m_late = _pct_change_at(rsp, offset, 22)
    hy_3m_chg = None
    if hy is not None:
        _h3 = _abs_change_at(hy, offset, 90)
        if _h3 is not None and hy_now is not None and hy_now <= 1.0:
            hy_3m_chg = _h3 * 100
        else:
            hy_3m_chg = _h3
    # SPX 3M drawdown: 현재 가격이 직전 63일 max 대비 몇 % 하락했는가 (양수 = 하락폭 %)
    spx_dd_3m_abs = None
    try:
        spx_t = _trim_series_at_offset(spx, offset)
        if spx_t is not None and len(spx_t) >= 63:
            w = spx_t.iloc[-63:]
            cur = float(spx_t.iloc[-1]); peak = float(w.max())
            if peak > 0:
                spx_dd_3m_abs = (1 - cur / peak) * 100  # 양수 = 하락
    except Exception: pass

    rsp_6m = _pct_change_at(rsp, offset, 126); qqq_6m = _pct_change_at(qqq, offset, 126)
    spy_1m = _pct_change_at(spy, offset, 22); rsp_1m = _pct_change_at(rsp, offset, 22)

    # QQQ DD
    qqq_dd = None
    try:
        _qqq_t = _trim_series_at_offset(qqq, offset)
        if _qqq_t is not None and len(_qqq_t) >= 252:
            window = _qqq_t.iloc[-252:]; cur = float(_qqq_t.iloc[-1])
            high_52w = float(window.max())
            if high_52w > 0: qqq_dd = (cur / high_52w - 1) * 100
    except Exception: pass

    # ───────────────────────────── 공통 6박스 ─────────────────────────────
    # ═══ V6.6.5 확장 grid (모듈 globals 임계) ═══
    # C1: 채권금리곡선 3M10Y [C1_AUTUMN_OPT 로 가을 분기 카테고리 선택]
    if C1_AUTUMN_OPT == 0:
        c1_autumn_set = ("entering", "deepening", "deep_stable")
    elif C1_AUTUMN_OPT == 1:
        c1_autumn_set = ("deepening", "deep_stable")
    else:  # 2
        c1_autumn_set = ("deep_stable",)
    C1 = {
        "봄":   _v(inv_state_3m10y == "recovering", inv_state_3m10y),
        "여름": _v(inv_state_3m10y == "normal", inv_state_3m10y),
        "가을": _v(inv_state_3m10y in c1_autumn_set, inv_state_3m10y),
        "겨울": _v(inv_state_3m10y == "recovering", inv_state_3m10y),
    }
    # C3: 2Y10Y 보조 채권금리곡선 (V6.6.1 baseline = V3.15.5 원형)
    C3 = {
        "봄":   _v(inv_state_2y10y == "recovering", inv_state_2y10y),
        "여름": _v(inv_state_2y10y == "normal", inv_state_2y10y),
        "가을": _v(inv_state_2y10y in ("entering", "deepening", "deep_stable"), inv_state_2y10y),
        "겨울": _v(inv_state_2y10y == "recovering", inv_state_2y10y),
    }
    # C4: 연준 액션 (V6.6.1 baseline = V3.15.5 원형)
    C4 = {
        "봄":   _v(ff_pos_pct is not None and ff_pos_pct < 30 and ff_6m_chg is not None and ff_6m_chg < 0, ff_pos_pct, ff_6m_chg),
        "여름": _v(ff_6m_chg is not None and abs(ff_6m_chg) < 0.5, ff_6m_chg),
        "가을": _v(ff_pos_pct is not None and ff_pos_pct >= 70 and ff_6m_chg is not None and (ff_6m_chg < 0 or ff_6m_chg > 0), ff_pos_pct, ff_6m_chg),
        "겨울": _v(ff_3m_chg is not None and ff_3m_chg < 0, ff_3m_chg),
    }
    # C5: 반도체 상대강도 [모듈 임계: C5_SOX_DELTA_THRESH, C5_SOX_1M_THRESH]
    # 가을: sox_3m < spx_3m - delta AND spx_6m > 0 (AND sox_1m < threshold 옵션)
    C5_autumn_main = (sox_3m is not None and spx_3m is not None and spx_6m is not None
                      and sox_3m < spx_3m - C5_SOX_DELTA_THRESH and spx_6m > 0)
    if C5_SOX_1M_THRESH < 0:  # 추가 sox_1m 조건 활성화
        C5_autumn_main = C5_autumn_main and (sox_1m is not None and sox_1m < C5_SOX_1M_THRESH)
        C5_autumn_evaluable = (sox_3m is not None and spx_3m is not None and spx_6m is not None and sox_1m is not None)
    else:
        C5_autumn_evaluable = (sox_3m is not None and spx_3m is not None and spx_6m is not None)
    C5 = {
        "봄":   _v(sox_3m is not None and spx_3m is not None and sox_3m > spx_3m, sox_3m, spx_3m),
        "여름": _v(sox_6m is not None and spx_6m is not None and sox_6m > spx_6m, sox_6m, spx_6m),
        "가을": bool(C5_autumn_main) if C5_autumn_evaluable else None,
        "겨울": _v(sox_1m is not None and spx_1m is not None and qqq_dd is not None and sox_1m > spx_1m and qqq_dd < -15, sox_1m, spx_1m, qqq_dd),
    }
    # C6: 밸류 (V6.6.1 baseline = V3.15.5 원형)
    has_val = (fpe_now is not None or cape_now is not None)
    val_low = ((fpe_now is not None and fpe_now <= 18) or (cape_now is not None and cape_now <= 25)) if has_val else None
    val_mid = (fpe_now is not None and fpe_now < 22) if fpe_now is not None else None
    val_high = ((fpe_now is not None and fpe_now >= 22) or (cape_now is not None and cape_now >= 35)) if has_val else None
    C6 = {
        "봄":   None if not has_val else val_low,
        "여름": val_mid,
        "가을": None if not has_val else val_high,
        "겨울": None if not has_val else val_high,
    }
    common = [C1, C3, C4, C5, C6]

    # ───────────────────────────── 봄 고유 6박스 (재정의: 직전 충격 흔적 필수) ─────────────────────────────
    # HY 데이터 부재 시 공통 fallback:
    #   inv_recovery 100% 완료 AND un_now < un_max_2y - 1.0 AND 최근 3M 내 normal 진입
    #   (3M 진입 조건으로 "정상화 후 1년 지난 시점" 배제)
    hy_eval = (hy_now is not None)
    fallback_shock = (inv_recovery_3m10y is not None and inv_recovery_3m10y >= 100
                       and un_now is not None and un_max_2y is not None
                       and un_now < un_max_2y - 1.0
                       and normal_recent_entry)
    fallback_evaluable = (inv_recovery_3m10y is not None and un_now is not None
                          and un_max_2y is not None
                          and inv_state_3m10y_3m_ago is not None)

    spring = {}
    # S1: hy_6m < -1.0 AND hy_max_1y ≥ 5  (or HY 부재 시 fallback)
    if hy_eval:
        spring["S1"] = _v(hy_6m_chg is not None and hy_6m_chg < -1.0
                           and hy_max_1y is not None and hy_max_1y >= 5,
                           hy_6m_chg, hy_max_1y)
    elif fallback_evaluable:
        spring["S1"] = bool(fallback_shock)
    else:
        spring["S1"] = None

    # S2: (recovering AND inv_recovery≥70 AND HY peak≥5)
    #   OR (normal AND inv_state_1y_ago in {recovering, deepening})
    #   OR (HY 부재 시 fallback: inv_recovery=100% AND un_now < un_max_2y - 1.0)
    s2_branch_a = None
    if hy_eval:
        s2_branch_a = _v(inv_state_3m10y == "recovering"
                          and inv_recovery_3m10y is not None and inv_recovery_3m10y >= 70
                          and hy_max_1y is not None and hy_max_1y >= 5,
                          inv_state_3m10y, inv_recovery_3m10y, hy_max_1y)
    s2_branch_b = _v(inv_state_3m10y == "normal"
                      and inv_state_3m10y_1y_ago in ("recovering", "deepening"),
                      inv_state_3m10y, inv_state_3m10y_1y_ago)
    s2_fallback = bool(fallback_shock) if (not hy_eval and fallback_evaluable) else None
    s2_components = [v for v in (s2_branch_a, s2_branch_b, s2_fallback) if v is not None]
    if not s2_components:
        spring["S2"] = None
    else:
        spring["S2"] = any(s2_components)

    spring["S4"] = _v(cpi_3m_chg is not None and cpi_3m_chg < 0 and cpi_now is not None and cpi_now < 3, cpi_3m_chg, cpi_now)
    # S5: un_now ≥ 4 AND un_3m_chg < 0.2 AND un_max_1y ≥ 5.5  (임계 강화)
    spring["S5"] = _v(un_now is not None and un_now >= 4
                       and un_3m_chg is not None and un_3m_chg < 0.2
                       and un_max_1y is not None and un_max_1y >= 5.5,
                       un_now, un_3m_chg, un_max_1y)
    spring["S6"] = _v(lei_yoy is not None and lei_yoy > -3 and lei_1m_chg is not None and lei_1m_chg > 0, lei_yoy, lei_1m_chg)

    # ───────────────────────────── 여름 고유 6박스 ─────────────────────────────
    summer = {}
    summer["Su1"] = _v(hy_pct is not None and hy_pct < 4, hy_pct)
    summer["Su2"] = _v(un_3m_chg is not None and un_3m_chg <= 0
                       and payems_3m_avg_thousands is not None and payems_3m_avg_thousands > 150,
                       un_3m_chg, payems_3m_avg_thousands)
    summer["Su3"] = _v(t5yifr_now is not None and t5yifr_now < 2.5, t5yifr_now)
    summer["Su4"] = _v(rsp_6m is not None and qqq_6m is not None and rsp_6m > 0 and qqq_6m > 0, rsp_6m, qqq_6m)
    summer["Su5"] = _v(cpi_3m_chg is not None and abs(cpi_3m_chg) < 0.5, cpi_3m_chg)

    # ───────────────────────────── 가을 고유 6박스 V6.6.x (모듈 globals 임계) ─────────────────────────────
    autumn = {}
    # A1. 신용 확대 [모듈 임계: A1_HY_6M_THRESH, A1_HY_3M_THRESH]
    a1_a = (hy_6m_chg is not None and hy_6m_chg > A1_HY_6M_THRESH)
    a1_b = (hy_3m_chg is not None and hy_3m_chg > A1_HY_3M_THRESH)
    a1_evaluable = (hy_6m_chg is not None or hy_3m_chg is not None)
    autumn["A1"] = bool(a1_a or a1_b) if a1_evaluable else None
    # A2. 변동성 체제 전환 [모듈 임계: A2_VIX_1M_COMPRESS, A2_VIX_3M_CHG_BREAK, A2_VIX_NOW_BREAK]
    a2_compress = (vix_1m_avg is not None and vix_1m_avg < A2_VIX_1M_COMPRESS)
    a2_break = (vix_3m_chg is not None and vix_3m_chg > A2_VIX_3M_CHG_BREAK
                 and vix_now is not None and vix_now > A2_VIX_NOW_BREAK)
    a2_evaluable = (vix_1m_avg is not None or (vix_3m_chg is not None and vix_now is not None))
    autumn["A2"] = bool(a2_compress or a2_break) if a2_evaluable else None
    # A3. 반도체 선행 약세 [모듈 임계: A3_SOX_DELTA_THRESH, A3_SOX_1M_THRESH]
    autumn["A3"] = _v(sox_3m is not None and spx_3m is not None and spx_6m is not None and sox_1m is not None
                      and sox_3m < spx_3m - A3_SOX_DELTA_THRESH
                      and spx_6m > 0 and sox_1m < A3_SOX_1M_THRESH,
                      sox_3m, spx_3m, spx_6m, sox_1m)
    # A4. 메가캡 의존 [모듈 임계: A4_SPY_RSP_3M_DELTA, A4_RSP_3M_NEG_REQ]
    if A4_RSP_3M_NEG_REQ:
        a4_main = (spy_3m is not None and rsp_3m is not None
                   and spy_3m > rsp_3m + A4_SPY_RSP_3M_DELTA and rsp_3m < 0)
    else:
        a4_main = (spy_3m is not None and rsp_3m is not None
                   and spy_3m > rsp_3m + A4_SPY_RSP_3M_DELTA)
    a4_evaluable = (spy_3m is not None and rsp_3m is not None)
    autumn["A4"] = bool(a4_main) if a4_evaluable else None
    # A5. PER 극단 [모듈 임계: A5_CAPE_THRESH, A5_CAPE_PCT_THRESH, A5_FPE_THRESH, A5_FPE_PCT_THRESH]
    a5_cape = (cape_now is not None and cape_now >= A5_CAPE_THRESH
               and cape_20y_pct is not None and cape_20y_pct >= A5_CAPE_PCT_THRESH)
    if fpe_now is not None and fpe_now >= A5_FPE_THRESH:
        if fpe_20y_pct is not None:
            a5_fpe = (fpe_20y_pct >= A5_FPE_PCT_THRESH)
        else:
            a5_fpe = True  # 백분위 부재 시 절대값 단독
    else:
        a5_fpe = False
    a5_evaluable = (cape_now is not None or fpe_now is not None)
    autumn["A5"] = bool(a5_cape or a5_fpe) if a5_evaluable else None
    # A6. 충격 진입 [모듈 임계: A6_SPX_DD_THRESH]
    autumn["A6"] = _v(spx_dd_3m_abs is not None and spx_dd_3m_abs > A6_SPX_DD_THRESH
                       and inv_state_3m10y in ("deepening", "deep_stable"),
                       spx_dd_3m_abs, inv_state_3m10y)

    # ───────────────────────────── 겨울 고유 6박스 ─────────────────────────────
    winter = {}
    winter["W1"] = _v(hy_pct is not None and hy_pct > 5, hy_pct)
    winter["W2"] = _v(payems_3m_avg_thousands is not None and payems_3m_avg_thousands < 50, payems_3m_avg_thousands)
    # W3 (롤백): vix_now > 30 AND vix_1m_avg > 25
    winter["W3"] = _v(vix_now is not None and vix_now > 30
                       and vix_1m_avg is not None and vix_1m_avg > 25,
                       vix_now, vix_1m_avg)
    winter["W4"] = _v(un_3m_chg is not None and un_3m_chg > 0.5, un_3m_chg)
    winter["W5"] = _v(spy_1m is not None and rsp_1m is not None and spy_1m > 0 and rsp_1m < 0, spy_1m, rsp_1m)
    winter["W6"] = _v(sox_1m is not None and spx_1m is not None and qqq_dd is not None
                      and sox_1m > spx_1m and qqq_dd < -15,
                      sox_1m, spx_1m, qqq_dd)

    # ── V3.14 prototype V2.0 가중치 (가을 박스 재배치 반영) ──
    # 가을 매핑: A1=신용확대(1.0), A2=변동성체제(1.5), A3=반도체약세(1.5), A4=메가캡(1.5), A5=PER극단(1.0), A6=충격진입(1.0)
    # 가을 ΣW = 7.5(공통) + 7.5(고유) = 15.0 (이전과 동일)
    # V6.9: A2/A3/A4 1.5→1.0 (가을 ΣW 대칭). C2/Su6/S3 박스 영구 삭제.
    W = {
        "C1": 1.5, "C3": 1.5, "C4": 1.0, "C5": 1.5, "C6": 1.0,
        "S1": 1.0, "S2": 1.5, "S4": 0.5, "S5": 0.5, "S6": 1.5,
        "Su1": 1.0, "Su2": 0.5, "Su3": 1.0, "Su4": 1.0, "Su5": 1.0,
        "A1": 1.0, "A2": 1.0, "A3": 1.0, "A4": 1.0, "A5": 1.0, "A6": 1.0,
        "W1": 1.0, "W2": 0.5, "W3": 1.0, "W4": 0.5, "W5": 1.0, "W6": 1.5,
    }
    if W_OVERRIDE: W.update(W_OVERRIDE)  # V6.8 grid lever
    common_keys = ["C1", "C3", "C4", "C5", "C6"]

    # V7.0 광기 가드 (Opus V3 권고): CAPE≥30 AND CAPE_pct≥85 시 봄/여름 고유 박스 점등 차단.
    # CAPE 단독으로는 1980-90s 회복기 false-positive (답지 -5.8%p) → 백분위 동반 가드로 광기만 좁힘.
    _cape_extreme = (cape_now is not None and cape_now >= 30
                     and cape_20y_pct is not None and cape_20y_pct >= 85)
    if _cape_extreme:
        for _k in ["S1", "S2", "S4", "S5", "S6"]:
            if spring.get(_k) is True:
                spring[_k] = None
        for _k in ["Su1", "Su2", "Su3", "Su4", "Su5"]:
            if summer.get(_k) is True:
                summer[_k] = None

    # V6.9 UI: 박스 boolean dict 글로벌 저장 (get_box_states_at_offset 가 읽음)
    global _LAST_BOX_STATES
    _LAST_BOX_STATES = {
        "공통": {ck: dict(cdict) for ck, cdict in zip(common_keys, common)},
        "봄": dict(spring), "여름": dict(summer), "가을": dict(autumn), "겨울": dict(winter),
    }

    seasons_unique = {"봄": spring, "여름": summer, "가을": autumn, "겨울": winter}
    raw_scores = {}; valid_counts = {}
    weighted_scores = {}; sum_valid_w = {}; weighted_ratios = {}

    for sname, unique in seasons_unique.items():
        ws_total = 0.0; svw = 0.0; true_n = 0; valid_n = 0
        # 공통 박스
        for ck, cdict in zip(common_keys, common):
            v = cdict[sname]
            if v is None: continue
            valid_n += 1; svw += W[ck]
            if v: true_n += 1; ws_total += W[ck]
        # 고유 박스
        for uk, uv in unique.items():
            if uv is None: continue
            valid_n += 1; svw += W[uk]
            if uv: true_n += 1; ws_total += W[uk]
        raw_scores[sname] = true_n
        valid_counts[sname] = valid_n
        weighted_scores[sname] = ws_total
        sum_valid_w[sname] = svw
        weighted_ratios[sname] = (ws_total / svw) if svw > 0 else 0.0

    # ═══ V3.15.5 가드 시스템 ═══
    bubble_guard = ((cape_now is not None and cape_now >= BUBBLE_GUARD_CAPE_THRESH)
                    or (fpe_now is not None and fpe_now >= BUBBLE_GUARD_FPE_THRESH))
    tightening_guard = (ff_pos_pct is not None and ff_pos_pct >= 60
                        and ff_6m_chg is not None and ff_6m_chg > 0)
    inversion_guard = (inv_state_3m10y in ("deepening", "deep_stable", "entering"))
    crack_guard_autumn = ((vix_now is not None and vix_3m_chg is not None
                           and vix_now > 22 and vix_3m_chg > 4)
                          or (spx_dd_3m_abs is not None and 5 < spx_dd_3m_abs <= 12))
    crisis_guard_winter = (
        (vix_now is not None and vix_now > 40)
        or (hy_pct is not None and vix_now is not None and hy_pct > 6 and vix_now > 30)
        or (spx_dd_3m_abs is not None and spx_dd_3m_abs > 20)
    )

    # V6.8: 가드 → autumn_bonus / summer_penalty / winter_bonus 모두 module global lever 로 스케일.
    # AUTUMN_BONUS_SCALE 0.0=폐지, 1.0=원본. 다른 lever 도 동일.
    summer_penalty = 0.0; autumn_bonus = 0.0; winter_bonus = 0.0
    if bubble_guard:        summer_penalty += 4.0; autumn_bonus += 3.0
    if tightening_guard:    summer_penalty += 3.0; autumn_bonus += 2.0
    if inversion_guard:     summer_penalty += 3.0; autumn_bonus += 2.0
    if crack_guard_autumn:  summer_penalty += 2.0; autumn_bonus += 2.0
    if crisis_guard_winter: summer_penalty += 2.0; winter_bonus += 5.0; autumn_bonus = max(0, autumn_bonus - 2.0)
    autumn_bonus *= AUTUMN_BONUS_SCALE
    summer_penalty *= SUMMER_PENALTY_SCALE
    winter_bonus *= WINTER_BONUS_SCALE

    # ─── spring_recovery 작동 시 가을/겨울 가산 절반 (회복 시점 보호) ───
    spx_6m_max_dd_local = None
    try:
        spx_t2 = _trim_series_at_offset(spx, offset)
        if spx_t2 is not None and len(spx_t2) >= 126:
            w2 = spx_t2.iloc[-126:]
            running_max2 = w2.expanding().max()
            running_dd2 = (1 - w2 / running_max2) * 100
            spx_6m_max_dd_local = float(running_dd2.max())
    except Exception: pass
    hy_recovering = True
    if hy_pct is not None and hy_6m_chg is not None:
        hy_recovering = (hy_pct < 7 and hy_6m_chg < 1)
    # V7.0 광기 가드: CAPE≥30 AND CAPE_pct≥85 시 봄 회복 가산 차단 (광기 시점 단기 반등은 회복 X)
    _cape_guard_ok = not (cape_now is not None and cape_now >= 30
                          and cape_20y_pct is not None and cape_20y_pct >= 85)
    spring_recovery_active = (spx_3m is not None and spx_3m > 5
                              and spx_6m_max_dd_local is not None and spx_6m_max_dd_local > 10
                              and hy_recovering
                              and _cape_guard_ok)
    if spring_recovery_active:
        autumn_bonus *= 0.2
        winter_bonus *= 0.2
        summer_penalty = max(0, summer_penalty - 2.0)  # 회복 시점 여름 차단도 약화

    weighted_scores["여름"]  = max(0.0, weighted_scores["여름"]  - summer_penalty)
    weighted_scores["가을"] += autumn_bonus
    weighted_scores["겨울"] += winter_bonus

    # ═══ V3.15 봄 회복 박스 — 시장 회복 시그널 ═══
    # spx 3m > +5% AND 직전 6M 내 spx_dd > 10% → 봄 +1.5
    spx_6m_max_dd = None
    try:
        spx_t = _trim_series_at_offset(spx, offset)
        if spx_t is not None and len(spx_t) >= 126:
            w = spx_t.iloc[-126:]
            running_max = w.expanding().max()
            running_dd = (1 - w / running_max) * 100
            spx_6m_max_dd = float(running_dd.max())
    except Exception: pass
    # spring_recovery_active 는 위에서 이미 계산 (가을/겨울 차감용)
    if spring_recovery_active:
        weighted_scores["봄"] += SPRING_RECOVERY_BONUS  # V6.8 lever (default 4.5)

    # 비율 재계산
    for s in ["봄","여름","가을","겨울"]:
        weighted_ratios[s] = (weighted_scores[s] / sum_valid_w[s]) if sum_valid_w[s] > 0 else 0.0

    if all(ws == 0 for ws in weighted_scores.values()):
        return None, raw_scores, valid_counts, weighted_scores, sum_valid_w, weighted_ratios, ""

    # V8.0 (Opus V4): 분자 절대값 기준 최고 계절 선정 (ratio noise 변환 폐기).
    # weighted_ratios 는 박스 1개 점등으로 ratio 폭증하는 분모 noise 문제 발생 (V3 자문).
    # weighted_scores (분자만) 비교 = "박스 많이 켜진 계절이 1위" 정직한 카운트.
    _SO = ["봄", "여름", "가을", "겨울"]
    max_r = max(weighted_scores.values())
    best = next(s for s in reversed(_SO) if abs(weighted_scores[s] - max_r) < 1e-9)

    # 접두사: prev/next weighted_score ≥ 4.0
    bi = _SO.index(best)
    nxt = _SO[(bi + 1) % 4]; prv = _SO[(bi - 1) % 4]
    if weighted_scores[nxt] >= 4.0:
        prefix = "늦"
    elif weighted_scores[prv] >= 4.0:
        prefix = "초"
    else:
        prefix = ""

    return best, raw_scores, valid_counts, weighted_scores, sum_valid_w, weighted_ratios, prefix


# ═══ V6.9 UI 용 박스 boolean 노출 helper ═══
# 47박스 (공통 C1-C6 × 4 + 고유 S/Su/A/W 각 6) boolean dict 반환.
# 각 박스: True (점등) / False (꺼짐) / None (평가 불가). ABLATE_BOXES 박스는 dict 에서 제외.
def get_box_states_at_offset(raw, offset):
    """V6.9 UI 용 박스 boolean dict 반환.
    return = {
        "공통": {"C1": {"봄": bool/None, "여름": ..., "가을": ..., "겨울": ...}, ...},
        "봄": {"S1": bool/None, ..., "S6": ...},
        "여름": {"Su1": ..., ..., "Su6": ...},
        "가을": {"A1": ..., ..., "A6": ...},
        "겨울": {"W1": ..., ..., "W6": ...},
    }
    내부 evaluator 와 동일한 사이드 로직 거치되 결과만 추출.
    """
    # _evaluate_12box_at_offset 내부 로직과 일치해야 함. 가장 안전한 방법:
    # 그 함수를 한 번 호출해서 box dict 생성 후 return.
    # 단 evaluator 가 box dict 를 return 안 하므로, monkeypatch 대신 별도 helper 로 분리.
    # 빠른 구현: evaluator 의 box 생성 부분을 외부에서 다시 한 번 실행 (signal 추출 → box 생성).
    # _evaluate_12box_at_offset 가 매우 길어 중복 위험 → 차선: evaluator 호출 + box 추출 위해
    # evaluator 자체를 수정. 여기서는 evaluator 가 module global 에 마지막 box dict 를 저장하게 한 후 읽기.
    global _LAST_BOX_STATES
    _LAST_BOX_STATES = None
    try:
        _evaluate_12box_at_offset(raw, offset)
    except Exception:
        pass
    return _LAST_BOX_STATES


_LAST_BOX_STATES = None  # evaluator 가 마지막 호출 시 채움


# ═══ 데이터 로딩 — 라이브러리 모드 (build_raw_data() 호출 시에만 실행) ═══
from fredapi import Fred

FRED_TARGETS_DEFAULT = {
    "ff_s": "FEDFUNDS",
    "t10y3m_s": "T10Y3M",
    "t10y2y_s": "T10Y2Y",          # C3
    "wti_s": "DCOILWTICO",
    "cpi_s": "CPIAUCSL",
    "t10y_s": "DGS10",
    "unrate_s": "UNRATE",
    "vix_fred_s": "VIXCLS",
    "payems_s": "PAYEMS",          # W2 / Su2
    "t5yifr_s": "T5YIFR",          # Su3 (5y5y forward inflation, 2003~)
    "lei_s": "USSLIND",            # S6 (St. Louis Fed Smoothed LEI proxy, 1982~)
}
YF_TARGETS_DEFAULT = {
    "spx_s": "^GSPC",
    "qqq_s": "QQQ",
    "sox_s": "SOXX",
    "rsp_s": "RSP",
    "spy_s": "SPY",
    "vix_yf_s": "^VIX",
    "xle_s": "XLE",
    "xlk_s": "XLK",
    "dxy_s": "DX-Y.NYB",  # 1971~ — A3 / Su6
}

# 모듈 globals (build_raw_data() 호출 후 채워짐)
fr = None
fd = {}
yd = {}
cape_s = None
hy_s = None
fpe_s = None
cpi_yoy_s = None
vix_s = None
raw_data = None


def build_raw_data(verbose=True):
    """V3.15.5 코어용 raw_data 구축. FRED + yfinance + historical_loader 통합 페치.
    호출 후 모듈 globals (fr, fd, yd, raw_data 등) 가 채워진다.

    Returns:
        raw_data: dict — _evaluate_12box_at_offset(raw_data, offset) 호출용
    """
    global fr, fd, yd, cape_s, hy_s, fpe_s, cpi_yoy_s, vix_s, raw_data

    if verbose: print("[1/3] FRED 데이터 페치 중...", flush=True)
    fr = Fred(api_key=FRED_KEY)
    fd = {}
    for k, sid in FRED_TARGETS_DEFAULT.items():
        try:
            s = fr.get_series(sid, observation_start="1976-01-01").dropna()
            if len(s) > 0: fd[k] = s
            if verbose:
                print(f"  {k} ({sid}): {len(s)} pts, {s.index[0].date()} ~ {s.index[-1].date()}", flush=True)
        except Exception as e:
            if verbose: print(f"  {k} ({sid}): FAIL {e}", flush=True)

    # CPI YoY 일간 ffill
    _cpi = fd.get("cpi_s"); cpi_yoy_s = None
    if _cpi is not None and len(_cpi) >= 13:
        _yoy = (_cpi / _cpi.shift(12) - 1) * 100
        cpi_yoy_s = _yoy.dropna().resample("D").ffill()

    if verbose: print("\n[2/3] yfinance 데이터 페치 중...", flush=True)
    import yfinance as yf
    yd = {}
    for k, tkr in YF_TARGETS_DEFAULT.items():
        try:
            h = yf.Ticker(tkr).history(period="max", auto_adjust=True)
            if h is None or len(h) == 0: continue
            s = h["Close"].dropna()
            try: s.index = s.index.tz_localize(None)
            except Exception: pass
            yd[k] = s
            if verbose:
                print(f"  {k} ({tkr}): {len(s)} pts, {s.index[0].date()} ~ {s.index[-1].date()}", flush=True)
        except Exception as e:
            if verbose: print(f"  {k} ({tkr}): FAIL {e}", flush=True)

    if verbose: print("\n[3/3] historical 보강...", flush=True)
    cape_s = load_cape_history(); hy_s = load_hy_oas_history(); fpe_s = load_forward_pe_history()
    if verbose:
        for n, ss in [("cape_s", cape_s), ("hy_s", hy_s), ("fpe_s", fpe_s)]:
            if len(ss) > 0:
                print(f"  {n}: {len(ss)} pts, {ss.index[0].date()} ~ {ss.index[-1].date()}", flush=True)

    vix_s = fd.get("vix_fred_s") if fd.get("vix_fred_s") is not None else yd.get("vix_yf_s")
    raw_data = {
        "ff_s":      fd.get("ff_s"),
        "hy_s":      hy_s if len(hy_s) > 0 else None,
        "t10y3m_s":  fd.get("t10y3m_s"),
        "t10y2y_s":  fd.get("t10y2y_s"),
        "wti_s":     fd.get("wti_s"),
        "spx_s":     yd.get("spx_s"),
        "sox_s":     yd.get("sox_s"),
        "qqq_s":     yd.get("qqq_s"),
        "rsp_s":     yd.get("rsp_s"),
        "spy_s":     yd.get("spy_s"),
        "vix_s":     vix_s,
        "cpi_yoy_s": cpi_yoy_s,
        "fpe_s":     fpe_s if len(fpe_s) > 0 else None,
        "cape_s":    cape_s if len(cape_s) > 0 else None,
        "unrate_s":  fd.get("unrate_s"),
        "payems_s":  fd.get("payems_s"),
        "t5yifr_s":  fd.get("t5yifr_s"),
        "lei_s":     fd.get("lei_s"),
        "dxy_s":     yd.get("dxy_s"),
    }
    return raw_data


# ═══ historical date 평가용 ═══
DATES_50 = [
    ("1980-04-30", "Volcker 1차 침체 한복판"),
    ("1981-07-31", "Volcker 2차 긴축 정점"),
    ("1982-08-31", "Volcker 후반, 침체 끝, 직후 대상승"),
    ("1984-06-29", "긴축 후반, 약세장 진입"),
    ("1985-09-30", "플라자 합의, 달러 약세 전환"),
    ("1987-08-31", "블랙먼데이 직전"),
    ("1990-07-31", "걸프전 충격 진입"),
    ("1990-10-31", "걸프전 + 침체 한복판"),
    ("1991-11-29", "걸프전 후 회복 초입"),
    ("1994-04-29", "Greenspan 깜짝 인상"),
    ("1995-07-31", "소프트랜딩 성공기"),
    ("1996-12-31", "그린스펀 비이성적 과열 발언"),
    ("1997-10-31", "아시아 위기 진입"),
    ("1998-09-30", "LTCM 위기 직후"),
    ("1999-04-30", "닷컴 광기 본격화"),
    ("2000-03-31", "닷컴 정점"),
    ("2001-09-28", "닷컴 침체 한복판"),
    ("2002-07-31", "닷컴 침체 후반, 더블딥 우려"),
    ("2003-03-31", "이라크전 직전, 바닥권"),
    ("2004-06-30", "긴축 사이클 시작"),
    ("2005-12-30", "채권금리곡선 역전 진입"),
    ("2006-06-30", "긴축 막바지"),
    ("2007-02-28", "서브프라임 첫 균열"),
    ("2007-10-31", "금융위기 직전 정점"),
    ("2008-03-31", "베어스턴스 직후"),
    ("2008-10-31", "리먼 직후 패닉"),
    ("2009-03-31", "GFC 바닥"),
    ("2010-05-28", "그리스 위기 + 플래시크래시"),
    ("2011-08-31", "미국 신용등급 강등"),
    ("2012-06-29", "유럽 위기 진정"),
    ("2013-05-31", "Taper Tantrum"),
    ("2014-10-31", "Bullard 발언 충격"),
    ("2015-08-31", "차이나 쇼크"),
    ("2016-02-29", "글로벌 디플레 공포 바닥"),
    ("2017-12-29", "감세안 통과, 멜트업"),
    ("2018-12-31", "파월 피벗 직전"),
    ("2019-08-30", "3M10Y 본격 역전"),
    ("2020-03-31", "코로나 패닉"),
    ("2020-06-30", "코로나 회복 초입"),
    ("2020-09-30", "코로나 후 회복 한복판"),
    ("2021-04-30", "재개방 + 인플레 첫 신호"),
    ("2021-11-30", "Powell 인플레 일시적 철회"),
    ("2022-03-31", "긴축 시작 직전"),
    ("2022-06-30", "인플레 정점, 긴축 한복판"),
    ("2022-11-30", "긴축 끝물, 거시 long pivot 시점"),
    ("2023-03-31", "SVB 충격"),
    ("2023-10-31", "10년물 5% 돌파"),
    ("2024-04-30", "인플레 끈적임 복귀"),
    ("2024-07-31", "사힘룰 충격, 일본 캐리 청산"),
    ("2025-04-30", "트럼프 관세 충격"),
]

DATES_19 = [
    ("1928-09-28", "[광기] 대공황 직전 광기 (참고)"),
    ("1972-12-29", "[광기] Nifty Fifty 정점"),
    ("1986-09-30", "[광기] 블랙먼데이 1년 전, 멜트업"),
    ("1987-04-30", "[광기] 블랙먼데이 6개월 전"),
    ("1989-09-29", "[광기] 닛케이 정점 3개월 전"),
    ("1998-12-31", "[광기] LTCM 후 광기 시작"),
    ("1999-12-31", "[광기] 닷컴 광기 절정"),
    ("2000-01-31", "[광기] 닷컴 정점 직전"),
    ("2007-05-31", "[광기] GFC 직전 멜트업"),
    ("2017-12-29", "[광기] 감세안 멜트업"),
    ("2021-01-29", "[광기] 밈주식 광기"),
    ("2021-11-30", "[광기] 성장주/ARKK 정점"),
    ("2003-09-30", "[회복] 이라크전 후 회복"),
    ("2009-09-30", "[회복] GFC 회복 초입"),
    ("2012-12-31", "[회복] 유럽 위기 후 회복"),
    ("2016-08-31", "[회복] 디플레 공포 후 회복"),
    ("2019-01-31", "[회복] 4Q18 폭락 후 반등"),
    ("2020-12-31", "[회복] 백신 발표 후 회복"),
    ("2023-06-30", "[회복] SVB 후 회복"),
]

def _print_set(title, dates_list, raw, today=None):
    """historical date 평가 출력 (테스트용)."""
    if today is None: today = _date.today()
    print(f"\n═══════════════════════════════════════════════════════════════════════════════════════════════")
    print(f"미어캣의 관측소 V3.15.5 코어 — {title}")
    print(f"═══════════════════════════════════════════════════════════════════════════════════════════════")
    print(f"{'날짜':<12} {'계절':<10} {'봄 ws/ΣW (%)':>14} {'여름 ws/ΣW (%)':>15} {'가을 ws/ΣW (%)':>15} {'겨울 ws/ΣW (%)':>15}  사건")
    print("─" * 120)
    for date_str, desc in dates_list:
        target = pd.Timestamp(date_str).date()
        offset = (today - target).days
        res = _evaluate_12box_at_offset(raw, offset)
        season, scores, valid, ws, svw, wr, prefix = res
        if season is None:
            line = (f"{date_str:<12} {'판정불가':<10} "
                    f"{'-':>14} {'-':>15} {'-':>15} {'-':>15}  ({desc})")
        else:
            def _fmt(s, ws=ws, svw=svw, wr=wr):
                return f"{ws[s]:.1f}/{svw[s]:.1f} ({wr[s]*100:.0f}%)"
            label = f"{prefix}{season}"
            line = (f"{date_str:<12} {label:<10} "
                    f"{_fmt('봄'):>14} {_fmt('여름'):>15} {_fmt('가을'):>15} {_fmt('겨울'):>15}  ({desc})")
        print(line)


if __name__ == "__main__":
    # 단독 실행 시: 데이터 로딩 후 50/19 historical date 평가
    raw = build_raw_data()
    today = _date.today()
    print(f"\n오늘 = {today}")
    _print_set("50개 historical date", DATES_50, raw, today)
    _print_set("19 광기/회복 비교", DATES_19, raw, today)
    print("\n※ ws = weighted_score, ΣW = 평가가능 박스 가중치 합, % = ws/ΣW.")
    print("※ 최고 비율 채택. 접두사: 인접 계절(prev/next) weighted_score ≥ 4.0 시 초/늦.")
    print("※ S3 영구 None (trailing eg 부재). 봄 ΣW 항상 -1.0.")
    print("※ 가을 V2.0: A1=신용확대, A2=변동성체제, A3=반도체약세, A4=메가캡, A5=PER극단(cape≥30), A6=충격진입.")
