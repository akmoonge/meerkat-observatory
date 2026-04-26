# -*- coding: utf-8 -*-
"""단기 경보 카드 5종 — 시장 변동성 일반 신호 (보조 지표).

⚠️ 의미 (STEP C-3 검증 후 재정의, 2026-04-27):
  당초 박스 시스템 (월봉, 6M/3M/52w 윈도우) 시간 해상도 한계 보완 목적
  으로 도입했으나 26건 검증 결과 PASS 기준 미달:
    - 한계 4건 중 fire 평균 ≥ 1.5 충족 = 2/4 (목표 3/4 미달)
    - 정상 시기 false fire 평균 = 1.51 (목표 ≤ 1.0 미달)
  → "시간 해상도 한계 보완" 용도 X. "시장 변동성 일반 신호" 로 재정의.

사용 가이드:
  - 메인 박스 = 매크로 사이클 (월봉, 6M~52w 윈도우).
  - 단기 경보 = 1M 윈도우 변동성 신호 (regime 무관 상시 작동).
  - 둘은 독립 신호. 사용자가 종합 판단.

한계 명시:
  - 22-08 같은 매크로 사이클 후반 진입 신호는 단기 경보로 못 잡음.
  - 박스 시스템 자체 한계 (월봉 해상도) 그대로 인정.
  - 단기_역전 / 변동성_클러스터는 영구 역전 / 평소 변동성 시기에도 fire
    → 보조 지표로만 해석.

카드 5종 (임계 default — 미세조정 X):
  1. 변동성 폭발:   VIX 1M max ≥ 30
  2. 급락:          SPX 1M chg ≤ -8%
  3. 신용 급격 확장: HY 1M chg > +0.5
  4. 단기 역전:     3M10Y 1M min < 0
  5. 변동성 클러스터: SPX 1M daily return std ≥ 1.0%

사용:
  alerts = evaluate_short_term_alerts(date, raw)
  # alerts = {"변동성_폭발": True, "급락": False, ...}
  severity = alerts_severity(alerts)
  # ("none","🟢 잠잠")/("warn","🟡 주의")/("alert","🟠 경계")/("shock","🔴 충격")
"""
import pandas as pd

# ─── 임계 (default — STEP C-1 확정) ───
THR_VIX_MAX = 30.0       # VIX 1M max
THR_SPX_CHG = -8.0       # SPX 1M change %
THR_HY_CHG = 0.5         # HY OAS 1M change (%p)
THR_INV_MIN = 0.0        # 3M10Y 1M min
THR_SPX_STD = 1.0        # SPX daily return 1M std %


def _vix_1m_max(target_date, vix_s):
    if vix_s is None or len(vix_s) == 0: return None
    sub = vix_s[(vix_s.index <= target_date)
                & (vix_s.index >= target_date - pd.Timedelta(days=30))].dropna()
    return float(sub.max()) if len(sub) else None


def _spx_1m_pct(target_date, spx_s, lookback_d=22):
    if spx_s is None: return None
    sub = spx_s[spx_s.index <= target_date].dropna()
    if len(sub) < lookback_d + 1: return None
    cur = float(sub.iloc[-1]); prev = float(sub.iloc[-1 - lookback_d])
    return (cur / prev - 1) * 100 if prev > 0 else None


def _hy_1m_chg(target_date, hy_s, lookback_days=30):
    if hy_s is None or len(hy_s) == 0: return None
    n = hy_s[hy_s.index <= target_date]
    if len(n) == 0: return None
    cur = float(n.iloc[-1])
    o = hy_s[hy_s.index <= target_date - pd.Timedelta(days=lookback_days)]
    if len(o) == 0: return None
    return cur - float(o.iloc[-1])


def _inv_1m_min(target_date, inv_s, days_back=30):
    if inv_s is None or len(inv_s) == 0: return None
    sub = inv_s[(inv_s.index <= target_date)
                & (inv_s.index >= target_date - pd.Timedelta(days=days_back))].dropna()
    return float(sub.min()) if len(sub) else None


def _spx_1m_std(target_date, spx_s, window_days=35, min_obs=20):
    if spx_s is None: return None
    sub = spx_s[(spx_s.index <= target_date)
                & (spx_s.index >= target_date - pd.Timedelta(days=window_days))].dropna()
    if len(sub) < min_obs + 1: return None
    rets = sub.pct_change().dropna()
    if len(rets) < min_obs: return None
    return float(rets.std() * 100)  # daily return std (%)


def evaluate_short_term_alerts(target_date, raw):
    """5 카드 boolean 평가. raw = dict {vix_s, spx_s, hy_s, t10y3m_s}.
    반환: dict {카드명: bool}. 각 카드 raw 부재 시 False (평가 불가 = 정상)."""
    if not isinstance(target_date, pd.Timestamp):
        target_date = pd.Timestamp(target_date)
    vix_s = (raw or {}).get("vix_s")
    spx_s = (raw or {}).get("spx_s")
    hy_s = (raw or {}).get("hy_s")
    inv_s = (raw or {}).get("t10y3m_s")
    vmax = _vix_1m_max(target_date, vix_s)
    spx1m = _spx_1m_pct(target_date, spx_s)
    hychg = _hy_1m_chg(target_date, hy_s)
    invmin = _inv_1m_min(target_date, inv_s)
    spxstd = _spx_1m_std(target_date, spx_s)
    return {
        "변동성_폭발":      bool(vmax is not None and vmax >= THR_VIX_MAX),
        "급락":             bool(spx1m is not None and spx1m <= THR_SPX_CHG),
        "신용_급격_확장":   bool(hychg is not None and hychg > THR_HY_CHG),
        "단기_역전":        bool(invmin is not None and invmin < THR_INV_MIN),
        "변동성_클러스터":  bool(spxstd is not None and spxstd >= THR_SPX_STD),
    }


def alerts_raw_dump(target_date, raw):
    """진단용 — alerts + 핵심 raw 값 동시 반환."""
    if not isinstance(target_date, pd.Timestamp):
        target_date = pd.Timestamp(target_date)
    vix_s = (raw or {}).get("vix_s"); spx_s = (raw or {}).get("spx_s")
    hy_s = (raw or {}).get("hy_s"); inv_s = (raw or {}).get("t10y3m_s")
    return {
        "alerts": evaluate_short_term_alerts(target_date, raw),
        "vix_1m_max":  _vix_1m_max(target_date, vix_s),
        "spx_1m_pct":  _spx_1m_pct(target_date, spx_s),
        "hy_1m_chg":   _hy_1m_chg(target_date, hy_s),
        "inv_1m_min":  _inv_1m_min(target_date, inv_s),
        "spx_1m_std":  _spx_1m_std(target_date, spx_s),
    }


def alerts_severity(alerts):
    """단계 결정.
    0 fire = 🟢 잠잠 / 1 fire = 🟡 주의 / 2~3 fire = 🟠 경계 / 4~5 fire = 🔴 충격."""
    if not alerts: return ("none", "🟢 잠잠")
    n = sum(1 for v in alerts.values() if v)
    if n == 0: return ("none", "🟢 잠잠")
    if n == 1: return ("warn", "🟡 주의")
    if n <= 3: return ("alert", "🟠 경계")
    return ("shock", "🔴 충격")


def alerts_label_list(alerts):
    """fire 된 카드 이름 list."""
    if not alerts: return []
    return [k.replace("_", " ") for k, v in alerts.items() if v]


def alerts_avg_in_window(target_date, raw, n_days=15):
    """target ±n_days 영업일 윈도우 카드 fire 평균 (검증용)."""
    if not isinstance(target_date, pd.Timestamp):
        target_date = pd.Timestamp(target_date)
    days = []
    for i in range(-n_days, n_days + 1):
        d = target_date + pd.Timedelta(days=i)
        if d.weekday() < 5: days.append(d)
    counts = []
    for d in days:
        a = evaluate_short_term_alerts(d, raw)
        counts.append(sum(1 for v in a.values() if v))
    return sum(counts) / len(counts) if counts else 0.0
