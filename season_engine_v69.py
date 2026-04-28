# -*- coding: utf-8 -*-
"""미어캣의 관측소 V6.5 — V6.4.1 + W3 보강 + 모순 지수 (Hybrid 병렬 구조).

V6.5 = 박스 시스템 (61.8% 천장) + 모순 지수 (라이브 매매 시그널)

핵심 추가 (V6.4.1 → V6.5):
[1] 코어 W3 보강: VIX>30 AND VIX_1m>25 AND SPX_dd_1m<=-5.0
    → 2010-05 플래시크래시 가짜 겨울 차단
[2] 모순 지수 (Contradiction Index) 병렬 출력 — 박스와 무관, 라이브 매매 시그널
    - G (광기 강도): 지수 함수로 팽창. CAPE/메가캡쏠림/VIX압축 결합
    - R (펀더멘털 견고): 시그모이드 0~1. 채권금리곡선/신용/고용/LEI-CEI 결합
    - Index = G × (1.5 − R) — 광기와 R 붕괴의 상호 증폭
    - tanh 연속 매핑으로 진동 차단
[3] CEI 추가 페치 (FRED USPHCI) → LEI/CEI 비율 계산
"""
import math
import season_engine_core as M
from datetime import date as _date
import pandas as pd

# ═══ V6.5.1 추가 데이터 (CFNAI + BAA-AAA + XLE) — fetch_v651_extras() 호출 시에만 ═══
# USSLIND deprecated (2020-02 종료) → CFNAI 로 대체
# CFNAI: 0 = 추세 성장, +0.7 = 과열 시그널, -0.7 = 침체 진입

# 모듈 globals (fetch_v651_extras() 호출 후 채워짐)
lei_cei_ratio = None  # 실제로는 CFNAI (변수명 호환 유지)
baa_aaa_s = None
xle_s = None


# ═══ V6.6.x grid search 용 MU + BD 임계 (모듈 globals) ═══
MU_CAPE_THRESH = 30.0
MU_CAPE_PCT_THRESH = 85.0
MU_FPE_THRESH = 22.0
MU_FPE_PCT_THRESH = 85.0
MU_SPX_3M_THRESH = -5.0
# V6.6.6: BD 채권금리곡선 조건 (현재 anchor "늦여름" 출력 위해 recovering 도 인정 시도)
BD_INV_STATES = ("normal",)  # default V6.4.1. 옵션: ("normal", "recovering")


def fetch_v651_extras(verbose=True):
    """V6.5.1 평가용 추가 데이터 페치 (CFNAI + BAA + AAA + XLE).
    M.build_raw_data() 가 먼저 호출되어 M.fr, M.yd 가 초기화되어 있어야 한다.
    """
    global lei_cei_ratio, baa_aaa_s, xle_s
    if M.fr is None:
        raise RuntimeError("M.build_raw_data() 를 먼저 호출하라 (M.fr 미초기화)")

    if verbose: print("[V6.5.1] CFNAI 페치 중...", flush=True)
    try:
        cfnai_s = M.fr.get_series("CFNAI", observation_start="1967-01-01").dropna()
        lei_cei_ratio = cfnai_s.resample("D").ffill()
        if verbose:
            print(f"  CFNAI: {len(cfnai_s)} pts, {cfnai_s.index[0].date()} ~ {cfnai_s.index[-1].date()}", flush=True)
    except Exception as e:
        if verbose: print(f"  CFNAI 페치 실패: {e}", flush=True)
        lei_cei_ratio = None

    if verbose: print("[V6.5.1] BAA-AAA 회사채 스프레드 페치 중...", flush=True)
    try:
        baa_raw = M.fr.get_series("BAA", observation_start="1976-01-01").dropna()
        aaa_raw = M.fr.get_series("AAA", observation_start="1976-01-01").dropna()
        baa_aaa_s = (baa_raw - aaa_raw).dropna().resample("D").ffill()
        if verbose:
            print(f"  BAA-AAA: {len(baa_aaa_s)} pts, {baa_aaa_s.index[0].date()} ~ {baa_aaa_s.index[-1].date()}", flush=True)
    except Exception as e:
        if verbose: print(f"  BAA-AAA 페치 실패: {e}", flush=True)
        baa_aaa_s = None

    # XLE 는 build_raw_data 에서 이미 페치됨 (M.yd)
    xle_s = M.yd.get("xle_s") if hasattr(M, "yd") else None
    if verbose and xle_s is not None:
        print(f"  XLE (재사용): {len(xle_s)} pts", flush=True)


# V3.15.5 코어 함수 그대로 재사용
_evaluate_12box_at_offset = M._evaluate_12box_at_offset
_safe_iloc_at = M._safe_iloc_at
_pct_change_at = M._pct_change_at
_abs_change_at = M._abs_change_at
_trim_series_at_offset = M._trim_series_at_offset


# ═══ 오버라이드/가드용 추가 시그널 추출 ═══
def _extract_override_signals(raw, offset):
    """V6.2 오버라이드 + 가드 + 뇌관 평가용 시그널."""
    s = {}
    spx = raw.get("spx_s"); vix = raw.get("vix_s"); hy = raw.get("hy_s")
    cape = raw.get("cape_s"); fpe = raw.get("fpe_s"); ff = raw.get("ff_s")
    unrate = raw.get("unrate_s"); cpi_yoy = raw.get("cpi_yoy_s")
    inv3m10y = raw.get("t10y3m_s")
    dxy = raw.get("dxy_s")
    spy = raw.get("spy_s"); rsp = raw.get("rsp_s")
    # T10Y는 raw_data에 없음 → M.fd 에서 직접 가져옴
    t10y = M.fd.get("t10y_s") if hasattr(M, "fd") else None

    # 채권금리곡선 상태 (HC, BD 가드용)
    s["inv_state"] = M._inv_state_at(inv3m10y, offset)

    # V6.2 뇌관용 추가 시그널
    s["dxy_6m_chg"] = _pct_change_at(dxy, offset, 180)
    s["t10y_3m_chg"] = _abs_change_at(t10y, offset, 90)
    s["cpi_yoy_6m_chg"] = _abs_change_at(cpi_yoy, offset, 180)
    s["ff_now"] = _safe_iloc_at(ff, offset)
    cpi_now_local = _safe_iloc_at(cpi_yoy, offset)
    s["real_rate"] = (s["ff_now"] - cpi_now_local) if (s["ff_now"] is not None and cpi_now_local is not None) else None
    s["spy_3m"] = _pct_change_at(spy, offset, 63)
    s["rsp_3m"] = _pct_change_at(rsp, offset, 63)

    # VIX
    s["vix_now"] = _safe_iloc_at(vix, offset)

    # SPX 1m drawdown (panic check)
    spx_dd_1m = None
    try:
        spx_t = _trim_series_at_offset(spx, offset)
        if spx_t is not None and len(spx_t) >= 22:
            w1 = spx_t.iloc[-22:]
            cur = float(spx_t.iloc[-1]); peak1 = float(w1.max())
            if peak1 > 0: spx_dd_1m = (cur / peak1 - 1) * 100
    except Exception: pass
    s["spx_dd_1m"] = spx_dd_1m

    # SPX 52w drawdown (high cut check)
    spx_dd_52w = None
    try:
        spx_t = _trim_series_at_offset(spx, offset)
        if spx_t is not None and len(spx_t) >= 252:
            w = spx_t.iloc[-252:]
            cur = float(spx_t.iloc[-1]); peak = float(w.max())
            if peak > 0: spx_dd_52w = (cur / peak - 1) * 100
    except Exception: pass
    s["spx_dd_52w"] = spx_dd_52w
    s["spx_at_high"] = (spx_dd_52w is not None and spx_dd_52w > -5)

    # HY
    hy_now = _safe_iloc_at(hy, offset)
    s["hy_pct"] = (hy_now * 100) if (hy_now is not None and hy_now <= 1.0) else hy_now

    # CAPE / FPE (멜트업 + 거품 지연 가드)
    s["cape_now"] = _safe_iloc_at(cape, offset)
    s["fpe_now"] = _safe_iloc_at(fpe, offset)
    # V6.6.1: CAPE 직근 20년 백분위 (시대적 컨텍스트 — MU/A5 일관 적용)
    s["cape_20y_pct"] = M._percentile_at(cape, offset, 252 * 20) if cape is not None else None
    s["fpe_20y_pct"] = M._percentile_at(fpe, offset, 252 * 20) if fpe is not None else None

    # SPX 3m % change (멜트업)
    s["spx_3m"] = _pct_change_at(spx, offset, 63)

    # FF
    s["ff_3m_chg"] = _abs_change_at(ff, offset, 90)

    # UNRATE 3m chg (거품 지연 가드)
    s["un_3m_chg"] = _abs_change_at(unrate, offset, 90)

    # CPI YoY (거품 지연 가드)
    s["cpi_now"] = _safe_iloc_at(cpi_yoy, offset)

    # ═══ V6.5 모순 지수용 시그널 (다른 시그널 정의 후) ═══
    s["vix_1m_avg"] = None
    try:
        vix_t = M._trim_series_at_offset(vix, offset)
        if vix_t is not None and len(vix_t) >= 22:
            s["vix_1m_avg"] = float(vix_t.iloc[-22:].mean())
    except Exception: pass
    s["t10y3m_now"] = _safe_iloc_at(inv3m10y, offset)
    s["hy_oas"] = s["hy_pct"]  # alias for clarity
    s["un_3m_chg"] = _abs_change_at(unrate, offset, 90)
    s["lei_cei_ratio"] = _safe_iloc_at(lei_cei_ratio, offset) if lei_cei_ratio is not None else None

    return s


# ═══ V6.5 모순 지수 (Contradiction Index) ═══
def compute_contradiction_index(sig):
    """G(광기) × (1.5 - R(펀더멘털)) 비선형 결합. 라이브 매매 시그널.

    G: 지수 함수로 팽창 (CAPE 가속, 메가캡 쏠림, VIX 압축)
    R: 시그모이드 0~1 (채권금리곡선, 신용, 고용, LEI/CEI)
    Index: 단방향 거품 크기 벡터 (회복은 0 수렴)
    DCA mult: 1.5 - 1.2 * tanh(Index/2.0) — 연속 매핑 (0.3 ~ 2.7)
    """
    cape = sig.get("cape_now")
    spy_3m = sig.get("spy_3m"); rsp_3m = sig.get("rsp_3m")
    vix_1m = sig.get("vix_1m_avg")

    # G: 광기 강도 (지수 팽창)
    g_val = max(0, math.exp((cape - 25) / 5) - 1) if cape is not None else 0
    g_brd = (math.exp(max(0, spy_3m - rsp_3m - 3) / 2) - 1) if (spy_3m is not None and rsp_3m is not None) else 0
    g_vix = 1 / (1 + math.exp((vix_1m - 18) / 2)) if vix_1m is not None else 0
    G = 0.5 * g_val + 0.3 * g_brd + 0.2 * g_vix

    # R: 펀더멘털 견고 (시그모이드 0~1)
    t10y3m = sig.get("t10y3m_now"); hy = sig.get("hy_oas")
    un_3m = sig.get("un_3m_chg"); lei_cei = sig.get("lei_cei_ratio")

    r_crv = 1 / (1 + math.exp(-(t10y3m * 4))) if t10y3m is not None else 0.5
    r_crd = 1 - (1 / (1 + math.exp((hy - 4.5) * 2))) if hy is not None else 0.5
    r_lbr = 1 - (1 / (1 + math.exp((un_3m - 0.1) * 10))) if un_3m is not None else 0.5
    # V6.5.1: CFNAI 기반 (0 = 균형, +0.7 = 견고, -0.7 = 침체)
    # 시그모이드: CFNAI 0 → 0.5, +0.5 → ~0.92, -0.5 → ~0.08
    r_lei = 1 / (1 + math.exp(-(lei_cei) * 4)) if lei_cei is not None else 0.5
    R = 0.3 * r_crv + 0.3 * r_crd + 0.2 * r_lbr + 0.2 * r_lei

    # Index = G × (1.5 - R)
    Index = G * (1.5 - R)

    # 해석 라벨 (참고용, 매매 자동 연동 X — 사용자 직접 판단)
    if Index > 1.5: label = "광기극단"
    elif Index > 0.5: label = "광기진행"
    else: label = "균형"

    # 연속 DCA mult (참고 출력)
    dca_mult = 1.5 - 1.2 * math.tanh(Index / 2.0)

    return {"G": G, "R": R, "Index": Index, "label": label, "dca_mult": dca_mult,
            "g_val": g_val, "g_brd": g_brd, "g_vix": g_vix,
            "r_crv": r_crv, "r_crd": r_crd, "r_lbr": r_lbr, "r_lei": r_lei}


# ═══ V6.5 메인 평가 함수 ═══
def _evaluate_v60(raw, offset):
    """V6.0: V3.15.5 코어에 V5.0 오버라이드 + V6.0 가드 결합."""
    flags = {"panic": False, "high_cut": False, "meltup": False, "bubble_delay": False}
    sig = _extract_override_signals(raw, offset)

    # ═══ STEP 1: 패닉 오버라이드 (V6.3 보강: 플래시크래시 휩소 차단) ═══
    # VIX 단독 조건 폐기. VIX≥45 + SPX_dd_1m≤-10 동반 시만 발동
    # → 2010-05 같은 단기 발작은 무시, 2008/2020 같은 진짜 패닉만 셧다운
    vn = sig["vix_now"]; dd1m = sig["spx_dd_1m"]; hy = sig["hy_pct"]
    if vn is not None and vn >= 45 and dd1m is not None and dd1m <= -10:
        flags["panic"] = True
        ci = compute_contradiction_index(sig)
        return "겨울", "", {"패닉오버라이드": f"VIX={vn:.1f} SPX_dd_1m={dd1m:.1f}", "contradiction": ci}, flags
    if dd1m is not None and dd1m <= -20 and hy is not None and hy >= 8.0:
        flags["panic"] = True
        ci = compute_contradiction_index(sig)
        return "겨울", "", {"패닉오버라이드": f"SPX_dd_1m={dd1m:.1f} HY={hy:.1f}", "contradiction": ci}, flags

    # ═══ STEP 2: 고점 금리 인하 폭탄 (가을 강제) — V6.3 prefix="늦" 강제 ═══
    # 역전 상태에서의 인하는 가을 끝자락이자 겨울 입구 → 무조건 늦가을
    if (sig["spx_at_high"]
        and sig["ff_3m_chg"] is not None and sig["ff_3m_chg"] <= -0.25
        and sig["inv_state"] in ("entering", "deepening", "deep_stable")):
        flags["high_cut"] = True
        ci = compute_contradiction_index(sig)
        return "가을", "늦", {"고점인하": f"FF_3m={sig['ff_3m_chg']:.2f}", "contradiction": ci}, flags

    # ═══ STEP 3: V3.15.5 코어 평가 ═══
    season, raw_scores, valid_counts, ws, svw, wr, prefix = _evaluate_12box_at_offset(raw, offset)

    if season is None:
        ci = compute_contradiction_index(sig)
        return None, "", {"contradiction": ci}, flags

    # ═══ STEP 3.1: V6.5 W3 보강 후처리 ═══
    # V3.15.5 코어 W3 (vix>30 AND vix_1m>25) 가 단기 VIX spike 로 점등 시
    # SPX_dd_1m > -5.0 이면 W3 효과 차감 (W3 W=1.0)
    # 2010-05 플래시크래시 가짜 겨울 차단
    vn_check = sig["vix_now"]; v1_check = sig["vix_1m_avg"]; dd1m_check = sig["spx_dd_1m"]
    if (vn_check is not None and vn_check > 30
        and v1_check is not None and v1_check > 25
        and dd1m_check is not None and dd1m_check > -5.0):
        # W3 점등됐을 가능성 → 겨울 ws 차감 (W3 weight = 1.0)
        ws["겨울"] = max(0, ws["겨울"] - 1.0)
        # 비율 + 계절 재계산
        for s_name in ["봄","여름","가을","겨울"]:
            wr[s_name] = (ws[s_name] / svw[s_name]) if svw[s_name] > 0 else 0.0
        _SO_w3 = ["봄", "여름", "가을", "겨울"]
        max_r_w3 = max(wr.values())
        season = next(s for s in reversed(_SO_w3) if abs(wr[s] - max_r_w3) < 1e-9)
        flags["w3_filtered"] = True

    # ═══ STEP 3.5: V6.4.1 prefix 3단 구조 (Base Dominance 임계 강화) ═══
    # V6.4 BDg 27건 발동으로 늦가을 정답 시점 prefix 손실 → V6.4.1 임계 강화
    _SO = ["봄", "여름", "가을", "겨울"]
    PREFIX_RATIO_THRESH = 0.25
    BASE_DOMINANCE_RATIO = 0.65       # V6.4: 0.50 → V6.4.1: 0.65
    BASE_DOMINANCE_MULTIPLIER = 3.0   # V6.4: 2.0 → V6.4.1: 3.0

    bi = _SO.index(season)
    nxt = _SO[(bi + 1) % 4]; prv = _SO[(bi - 1) % 4]

    base_ratio = (ws[season] / svw[season]) if svw[season] > 0 else 0.0
    next_ratio = (ws[nxt] / svw[nxt]) if svw[nxt] > 0 else 0.0
    prev_ratio = (ws[prv] / svw[prv]) if svw[prv] > 0 else 0.0

    # 위계 2: Base Dominance Guard
    # base 가 압도적이면 (50%+ AND 다음 계절의 2배+) prefix 차단
    if (base_ratio >= BASE_DOMINANCE_RATIO
        and ws[season] >= BASE_DOMINANCE_MULTIPLIER * ws[nxt]
        and ws[season] >= BASE_DOMINANCE_MULTIPLIER * ws[prv]):
        prefix = ""
        flags["base_dominance"] = True
    # 위계 3: 유기적 전이 (25% 비율)
    elif next_ratio >= PREFIX_RATIO_THRESH:
        prefix = "늦"
    elif prev_ratio >= PREFIX_RATIO_THRESH:
        prefix = "초"
    else:
        prefix = ""

    # ═══ STEP 4 + 5 통합: MU + BD 위계 처리 (위계 1, V6.1.1 핫픽스) ═══
    # 위계: MU+BD 중첩 → 늦여름 (3순위) > MU 단독 → 가을 격상 (4순위)
    cape = sig["cape_now"]; fpe = sig["fpe_now"]; sp3 = sig["spx_3m"]
    uc3 = sig["un_3m_chg"]; hy = sig["hy_pct"]; cpi = sig["cpi_now"]

    # MU 조건 [모듈 임계: MU_CAPE_THRESH, MU_CAPE_PCT_THRESH, MU_FPE_*, MU_SPX_3M_THRESH]
    cape_pct = sig.get("cape_20y_pct"); fpe_pct = sig.get("fpe_20y_pct")
    meltup_active = False
    if cape is not None and cape >= MU_CAPE_THRESH and sp3 is not None and sp3 > MU_SPX_3M_THRESH:
        if cape_pct is not None and cape_pct >= MU_CAPE_PCT_THRESH:
            meltup_active = True
        elif cape_pct is None:
            meltup_active = True  # 1980-90s 초기: 백분위 윈도우 미달 → 절대값 단독
    elif fpe is not None and fpe >= MU_FPE_THRESH and sp3 is not None and sp3 > MU_SPX_3M_THRESH:
        if fpe_pct is not None and fpe_pct >= MU_FPE_PCT_THRESH:
            meltup_active = True
        elif fpe_pct is None:
            meltup_active = True

    # BD 조건 (모든 펀더멘털 토대 견고 + 채권금리곡선 정상)
    cond_value = (cape is not None and cape >= 28) or (fpe is not None and fpe >= 20)
    cond_employ = (uc3 is not None and uc3 <= 0)
    cond_credit = (hy is None or hy < 4.5)
    cond_inflation = (cpi is not None and cpi < 4.0)
    cond_curve_normal = (sig["inv_state"] in BD_INV_STATES)
    bd_active = (cond_value and cond_employ and cond_credit and cond_inflation and cond_curve_normal)

    if meltup_active:
        flags["meltup"] = True
        ws["봄"] = 0.0  # 봄 압살은 항상 적용 (밸류에이션의 몫)

        if bd_active:
            # 3순위: MU + BD 중첩 → 늦여름 강제 (펀더멘털이 가을 진입 유예)
            flags["bubble_delay"] = True
            ci = compute_contradiction_index(sig)
            return "여름", "늦", {"flags_active": flags, "contradiction": ci}, flags
        else:
            # 4순위: MU 단독 → 가을 격상 + prefix="초" 강제 (V6.3)
            # 멜트업은 가을의 시작점이므로 무조건 초가을
            ws["가을"] += 5.0
            for s_name in ["봄","여름","가을","겨울"]:
                wr[s_name] = (ws[s_name] / svw[s_name]) if svw[s_name] > 0 else 0.0
            _SO = ["봄", "여름", "가을", "겨울"]
            max_r = max(wr.values())
            season = next(s for s in reversed(_SO) if abs(wr[s] - max_r) < 1e-9)
            # MU 단독 + 가을 채택 시 prefix="초" 강제
            if season == "가을":
                prefix = "초"
            else:
                # 가을 채택 못했으면 일반 비율 기반 prefix
                bi = _SO.index(season)
                nxt = _SO[(bi + 1) % 4]; prv = _SO[(bi - 1) % 4]
                next_ratio = (ws[nxt] / svw[nxt]) if svw[nxt] > 0 else 0.0
                prev_ratio = (ws[prv] / svw[prv]) if svw[prv] > 0 else 0.0
                if next_ratio >= 0.25: prefix = "늦"
                elif prev_ratio >= 0.25: prefix = "초"
                else: prefix = ""
    elif bd_active:
        # MU 미발동인데 BD만 충족 — 거품 지연 단독 (밸류 28+ 이지만 30 미달)
        flags["bubble_delay"] = True
        ci = compute_contradiction_index(sig)
        return "여름", "늦", {"flags_active": flags, "contradiction": ci}, flags

    ci = compute_contradiction_index(sig)
    return season, prefix, {"flags_active": flags, "contradiction": ci}, flags


# ═══ 답지 50개 ═══
GT = [
    ("1980-04-30","겨울","",   "명확","",          ["F4","F5"]),
    ("1981-07-31","가을","늦", "명확","",          ["F4","F6"]),
    ("1982-08-31","봄",  "초", "모호","봄|겨울",   ["F4","F1","F5"]),
    ("1984-06-29","가을","",   "명확","",          ["F4","F6"]),
    ("1985-09-30","봄",  "",   "보통","",          ["F6","F9"]),
    ("1987-08-31","가을","늦", "명확","",          ["F5","F9"]),
    ("1990-07-31","가을","",   "명확","",          ["F2","F4"]),
    ("1990-10-31","겨울","",   "명확","",          ["F4"]),
    ("1991-11-29","봄",  "",   "보통","봄|겨울",   ["F4","F10"]),
    ("1994-04-29","가을","초", "보통","",          ["F6"]),
    ("1995-07-31","여름","",   "명확","",          ["F4","F6"]),
    ("1996-12-31","여름","늦", "보통","",          ["F5","F8"]),
    ("1997-10-31","가을","",   "보통","",          ["F2","F9"]),
    ("1998-09-30","가을","늦", "명확","",          ["F2","F6"]),
    ("1999-04-30","여름","늦", "명확","",          ["F5","F8"]),
    ("2000-03-31","가을","초", "명확","",          ["F5","F8","F9"]),
    ("2001-09-28","겨울","",   "명확","",          ["F4"]),
    ("2002-07-31","겨울","늦", "보통","겨울|봄",   ["F4","F10"]),
    ("2003-03-31","봄",  "초", "모호","봄|겨울",   ["F4","F10"]),
    ("2004-06-30","여름","",   "명확","",          ["F6"]),
    ("2005-12-30","여름","늦", "보통","",          ["F1","F6"]),
    ("2006-06-30","가을","초", "보통","",          ["F1","F6"]),
    ("2007-02-28","가을","",   "명확","",          ["F1","F2"]),
    ("2007-10-31","가을","늦", "명확","",          ["F5","F9"]),
    ("2008-03-31","겨울","초", "명확","",          ["F4"]),
    ("2008-10-31","겨울","",   "명확","",          ["F4","F2"]),
    ("2009-03-31","봄",  "초", "모호","봄|겨울",   ["F4","F10"]),
    ("2010-05-28","여름","",   "보통","",          ["F2","F9"]),
    ("2011-08-31","가을","",   "보통","",          ["F2","F9"]),
    ("2012-06-29","봄",  "늦", "보통","",          ["F4","F6"]),
    ("2013-05-31","여름","",   "명확","",          ["F6"]),
    ("2014-10-31","여름","늦", "보통","",          ["F2","F9"]),
    ("2015-08-31","가을","",   "보통","",          ["F2","F7"]),
    ("2016-02-29","봄",  "초", "모호","봄|겨울",   ["F4","F7"]),
    ("2017-12-29","여름","늦", "명확","",          ["F5","F8"]),
    ("2018-12-31","가을","늦", "명확","",          ["F6","F2"]),
    ("2019-08-30","가을","",   "명확","",          ["F1","F6"]),
    ("2020-03-31","겨울","",   "명확","",          ["F4","F2"]),
    ("2020-06-30","봄",  "초", "모호","봄|겨울",   ["F4","F6"]),
    ("2020-09-30","여름","",   "보통","",          ["F4","F6"]),
    ("2021-04-30","여름","",   "명확","",          ["F4","F8"]),
    ("2021-11-30","여름","늦", "명확","",          ["F5","F6"]),
    ("2022-03-31","가을","초", "명확","",          ["F4","F6"]),
    ("2022-06-30","가을","",   "명확","",          ["F4","F6"]),
    ("2022-11-30","봄",  "초", "모호","봄|가을",   ["F1","F6","F4"]),
    ("2023-03-31","봄",  "",   "모호","봄|겨울",   ["F2","F6"]),
    ("2023-10-31","가을","",   "보통","",          ["F1","F5"]),
    ("2024-04-30","여름","늦", "보통","",          ["F5","F6"]),
    ("2024-07-31","가을","초", "보통","",          ["F2","F9"]),
    ("2025-04-30","가을","늦", "명확","",          ["F5","F2","F8"]),
]


def score_v60():
    today = _date.today()
    print("\n" + "═" * 145)
    print("미어캣의 관측소 V6.5 — 답지 50개 채점 + 모순 지수 병렬 출력")
    print("(V6.4.1 + W3 보강 + Hybrid: 박스(점수) + 모순지수(라이브 매매 시그널))")
    print("═" * 145)
    print(f"{'날짜':<12} {'정답':<8} {'예측':<8} {'점수':>5}  {'모호도':<5}  G/R/Index/DCAmult        라벨       플래그")
    print("─" * 130)

    cycle = ["봄", "여름", "가을", "겨울"]
    total = 0.0; n = 0
    correct = 0; partial = 0; wrong = 0; miss = 0
    by_ambig = {"명확":[0,0], "보통":[0,0], "모호":[0,0]}
    misclass_records = []
    flag_counts = {"panic": 0, "high_cut": 0, "meltup": 0, "bubble_delay": 0,
                   "dollar_loop": 0, "real_rate_shock": 0, "breadth_collapse": 0,
                   "base_dominance": 0}

    for date_str, gt_base, gt_prefix, ambig, allowed, _f in GT:
        target = pd.Timestamp(date_str).date()
        offset = (today - target).days
        season, prefix, info, flags = _evaluate_v60(M.raw_data, offset)

        for k in flag_counts:
            if flags.get(k): flag_counts[k] += 1

        if season is None:
            line = (f"{date_str:<12} {gt_prefix+gt_base:<8} {'판정불가':<8} "
                    f"{0.0:>5.2f}  {'miss':<22} {ambig:<5}")
            print(line)
            misclass_records.append((date_str, gt_base, gt_prefix, "판정불가", "", 0.0, "miss"))
            n += 1; miss += 1
            continue

        pred_base = season; pred_prefix = prefix or ""
        score = 0.0; detail = ""

        if pred_base == gt_base:
            if pred_prefix == gt_prefix:
                score = 1.0; detail = "정확"
            else:
                score = 0.7; detail = "base일치"
        elif allowed:
            allowed_bases = [a.strip() for a in allowed.split("|")]
            if pred_base in allowed_bases:
                score = 0.8 if pred_prefix == gt_prefix else 0.6
                detail = f"허용({allowed})"
            else:
                if (abs(cycle.index(pred_base) - cycle.index(gt_base)) % 4) in (1, 3):
                    score = 0.3; detail = "인접"
                else:
                    score = 0.0; detail = "오답"
        else:
            diff = abs(cycle.index(pred_base) - cycle.index(gt_base))
            if diff in (1, 3):
                score = 0.3; detail = "인접"
            else:
                score = 0.0; detail = "오답"

        pred_label = (pred_prefix + pred_base) if pred_prefix else pred_base
        gt_label = (gt_prefix + gt_base) if gt_prefix else gt_base
        flag_str = []
        if flags.get("panic"): flag_str.append("PA")
        if flags.get("high_cut"): flag_str.append("HC")
        if flags.get("meltup"): flag_str.append("MU")
        if flags.get("bubble_delay"): flag_str.append("BD")
        if flags.get("dollar_loop"): flag_str.append("DL")
        if flags.get("real_rate_shock"): flag_str.append("RR")
        if flags.get("breadth_collapse"): flag_str.append("BC")
        if flags.get("base_dominance"): flag_str.append("BDg")
        flag_disp = ",".join(flag_str) if flag_str else ""
        ci_info = info.get("contradiction") if isinstance(info, dict) else None
        if ci_info:
            ci_str = f"G={ci_info['G']:.2f}/R={ci_info['R']:.2f}/Idx={ci_info['Index']:+.2f}/m={ci_info['dca_mult']:.2f}"
            ci_label = ci_info["label"]
        else:
            ci_str = "                          "
            ci_label = ""
        line = (f"{date_str:<12} {gt_label:<8} {pred_label:<8} "
                f"{score:>5.2f}  {ambig:<5}  {ci_str}  {ci_label:<6}  {flag_disp}")
        print(line)

        total += score; n += 1
        by_ambig[ambig][0] += score; by_ambig[ambig][1] += 1
        if score == 1.0: correct += 1
        elif score >= 0.5: partial += 1
        elif score == 0: wrong += 1

        if score < 1.0:
            misclass_records.append((date_str, gt_base, gt_prefix, pred_base, pred_prefix, score, detail))

    print("─" * 130)
    print(f"\n총점: {total:.1f} / 50.0  ({total/50*100:.1f}%)")
    print(f"정확: {correct}건, 부분: {partial}건, 오답: {wrong}건, 판정불가: {miss}건")
    print(f"플래그: 패닉 {flag_counts['panic']}, 고점인하 {flag_counts['high_cut']}, 멜트업 {flag_counts['meltup']}, 거품지연 {flag_counts['bubble_delay']}")
    print(f"        Base Dominance {flag_counts['base_dominance']} (prefix 차단)")
    print(f"\n모호도별:")
    for amb in ["명확","보통","모호"]:
        s, c = by_ambig[amb]
        if c > 0:
            print(f"  {amb}: {s:.1f} / {c}.0  ({s/c*100:.1f}%)")

    print(f"\n오분류/부분점수 케이스 ({len(misclass_records)}건):")
    for r in misclass_records:
        ds, gb, gp, pb, pp, sc, dt = r
        gl = (gp or "") + gb
        pl = (pp or "") + pb if pb != "판정불가" else pb
        print(f"  {ds}  정답:{gl:<8} 예측:{pl:<8} 점수:{sc:.2f}  ({dt})")

    return total, n


if __name__ == "__main__":
    # 단독 실행: 데이터 로딩 후 50개 답지 채점
    M.build_raw_data()
    fetch_v651_extras()
    print(f"\n오늘 = {_date.today()}")
    print("V6.5.1 명세 (V6.4.1 + W3 보강 + 모순 지수):")
    print("- 박스 시스템 (V6.4.1 그대로): 답지 점수 천장 61.8%")
    print("- W3 보강: VIX 패닉 + SPX_dd_1m>-5 시 W3 차감 (2010-05 차단)")
    print("- 모순 지수 (병렬 출력): G × (1.5 - R) — 라이브 매매 시그널")
    print("  G = 0.5×exp((CAPE-25)/5) + 0.3×exp(쏠림) + 0.2×sigmoid(VIX 압축)")
    print("  R = 0.3×채권금리곡선 + 0.3×신용 + 0.2×고용 + 0.2×CFNAI")
    print("  DCA mult = 1.5 - 1.2×tanh(Index/2.0) — 연속 매핑\n")
    total, n = score_v60()
    print(f"\n※ V3.15.5: 61.8% / V4.0: 59.6% / V4.2: 57.0% / V5.0: 50.0%")
    delta_v315 = total/50*100 - 61.8
    print(f"※ V6.0: {total/50*100:.1f}%  (vs V3.15.5 {'+' if delta_v315>0 else ''}{delta_v315:.1f}%p)")
