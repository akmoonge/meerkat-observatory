# -*- coding: utf-8 -*-
"""STEP 5 historical 데이터 적재 loader 모듈.
메인 앱 본체와 분리. STEP 5-1: Shiller CAPE.

사용법:
    from historical_loader import load_cape_history, get_cape_at
    s = load_cape_history()  # pandas Series, DatetimeIndex daily
    v = get_cape_at("2025-04-30")  # 32.63
"""
import json
from pathlib import Path
from datetime import datetime
from functools import lru_cache

SD = Path.home() / ".meerkat"
CAPE_HISTORY = SD / "cache" / "cape_history.json"
FWD_EPS_HISTORY = SD / "cache" / "forward_eps_history.json"  # STEP 5-2 (placeholder)
TRAILING_EARN_HISTORY = SD / "cache" / "trailing_earnings_history.json"  # STEP 5-3 적재됨
HY_OAS_HISTORY = SD / "cache" / "hy_oas_history.json"  # STEP A-2 적재됨 (Wayback + FRED 합본)
FORWARD_PE = SD / "cache" / "forward_pe.json"  # STEP 5-8 라이브 누적 적재 (앱 실행마다 1 entry)
FORWARD_PE_STALE_DAYS = 14  # 14일 초과 시 stale → trailing fallback

# Repo-bundled fallback (신규 사용자 / ~/.meerkat 캐시 부재 시)
_BUNDLED_DIR = Path(__file__).parent / "data"
_BUNDLED_FALLBACK = {
    CAPE_HISTORY: _BUNDLED_DIR / "cape_history.json",
    HY_OAS_HISTORY: _BUNDLED_DIR / "hy_oas_history.json",
    TRAILING_EARN_HISTORY: _BUNDLED_DIR / "trailing_earnings_history.json",
}

def _resolve_path(user_path):
    """user 캐시 우선. 부재 시 repo 번들 폴백."""
    if user_path.exists(): return user_path
    fb = _BUNDLED_FALLBACK.get(user_path)
    if fb is not None and fb.exists(): return fb
    return user_path  # 둘 다 없으면 user_path 반환 (caller에서 .exists() False 분기)


@lru_cache(maxsize=1)
def load_cape_history():
    """CAPE 일별 시계열 (1990-01-01 ~ 현재 forward-filled).
    반환: pandas.Series (DatetimeIndex), 또는 빈 Series."""
    import pandas as pd
    _path = _resolve_path(CAPE_HISTORY)
    if not _path.exists(): return pd.Series(dtype=float)
    try:
        with open(_path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        records = obj.get("data") or []
        if not records: return pd.Series(dtype=float)
        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        return pd.Series(df["cape"].values, index=df["date"], name="cape")
    except Exception: return pd.Series(dtype=float)


def get_cape_at(target_date):
    """target_date (str 'YYYY-MM-DD' 또는 Timestamp/date) 시점 CAPE 값.
    target_date 이하 가장 최근 값. 부재 시 None."""
    import pandas as pd
    s = load_cape_history()
    if s.empty: return None
    try:
        td = pd.Timestamp(target_date)
        sub = s[s.index <= td]
        if len(sub) == 0: return None
        return float(sub.iloc[-1])
    except Exception: return None


def cape_meta():
    """CAPE JSON 메타데이터."""
    _path = _resolve_path(CAPE_HISTORY)
    if not _path.exists(): return {}
    try:
        with open(_path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return {k: v for k, v in obj.items() if k != "data"}
    except Exception: return {}


# ═══ HY OAS (BAMLH0A0HYM2) — Wayback + FRED 합본 ═══
@lru_cache(maxsize=1)
def load_hy_oas_history():
    """HY OAS 일별 시계열 (1996-12-31 ~ 현재).
    소스: Wayback fredgraph.csv (정책 변경 직전 archive) + FRED 현재 fetch.
    반환: pandas.Series (DatetimeIndex), 단위 = % (BAMLH0A0HYM2 와 동일)."""
    import pandas as pd
    _path = _resolve_path(HY_OAS_HISTORY)
    if not _path.exists(): return pd.Series(dtype=float)
    try:
        with open(_path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        records = obj.get("data") or []
        if not records: return pd.Series(dtype=float)
        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        return pd.Series(df["hy_oas"].values, index=df["date"], name="hy_oas")
    except Exception: return pd.Series(dtype=float)


def get_hy_oas_at(target_date):
    """target_date 시점 HY OAS 값 (단위 %). 이하 가장 최근. 부재 시 None."""
    import pandas as pd
    s = load_hy_oas_history()
    if s.empty: return None
    try:
        td = pd.Timestamp(target_date)
        sub = s[s.index <= td]
        if len(sub) == 0: return None
        return float(sub.iloc[-1])
    except Exception: return None


def get_hy_oas_6m_chg(target_date):
    """target_date 시점 HY OAS 6개월 변화 (= now - 180일전). 부재 시 None."""
    import pandas as pd
    n = get_hy_oas_at(target_date)
    if n is None: return None
    td = pd.Timestamp(target_date) - pd.Timedelta(days=180)
    o = get_hy_oas_at(td)
    return (n - o) if o is not None else None


def get_hy_oas_1m_chg(target_date):
    """단기 경보 카드 후보 (STEP C). 30일 변화."""
    import pandas as pd
    n = get_hy_oas_at(target_date)
    if n is None: return None
    td = pd.Timestamp(target_date) - pd.Timedelta(days=30)
    o = get_hy_oas_at(td)
    return (n - o) if o is not None else None


def hy_oas_meta():
    """HY OAS JSON 메타데이터."""
    _path = _resolve_path(HY_OAS_HISTORY)
    if not _path.exists(): return {}
    try:
        with open(_path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return {k: v for k, v in obj.items() if k != "data"}
    except Exception: return {}


# ═══ STEP 5-8 Forward PE — 라이브 누적 적재 ═══
@lru_cache(maxsize=1)
def load_forward_pe_history():
    """forward_pe.json 의 fpe 시계열 (라이브 누적 entries).
    반환: pandas.Series (DatetimeIndex), updated_at 기준 stale 체크는 별도."""
    import pandas as pd
    if not FORWARD_PE.exists(): return pd.Series(dtype=float)
    try:
        with open(FORWARD_PE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        records = obj.get("data") or []
        if not records: return pd.Series(dtype=float)
        # date + fpe 기반 시계열
        dates = []; vals = []
        for r in records:
            if r.get("fpe") is not None and r.get("date"):
                dates.append(pd.to_datetime(r["date"]))
                vals.append(float(r["fpe"]))
        if not dates: return pd.Series(dtype=float)
        return pd.Series(vals, index=pd.DatetimeIndex(dates), name="fpe").sort_index()
    except Exception: return pd.Series(dtype=float)


def forward_pe_meta():
    """forward_pe.json 메타 (data 제외) + 마지막 entry updated_at."""
    if not FORWARD_PE.exists(): return {}
    try:
        with open(FORWARD_PE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        meta = {k: v for k, v in obj.items() if k != "data"}
        records = obj.get("data") or []
        meta["n_entries"] = len(records)
        if records:
            last = max(records, key=lambda r: r.get("date", ""))
            meta["last_entry"] = {"date": last.get("date"), "fpe": last.get("fpe"),
                                   "source": last.get("source"), "updated_at": last.get("updated_at")}
        return meta
    except Exception: return {}


def is_forward_pe_stale(target_date=None):
    """target_date (또는 today) 시점에서 forward PE 가 stale 인지 (마지막 entry 가 14일 초과 전).
    True = stale, fpe 사용 X, trailing fallback. False = 신선, fpe 사용."""
    import pandas as pd
    if not FORWARD_PE.exists(): return True
    s = load_forward_pe_history()
    if s.empty: return True
    td = pd.Timestamp(target_date) if target_date is not None else pd.Timestamp.now().normalize()
    last = s.index[-1]
    return (td - last).days > FORWARD_PE_STALE_DAYS


def get_fpe_at(target_date):
    """target_date 시점 forward PE. stale or 부재 시 None.
    None 반환 시 호출자는 trailing fallback 사용해야."""
    import pandas as pd
    s = load_forward_pe_history()
    if s.empty: return None
    try:
        td = pd.Timestamp(target_date)
        sub = s[s.index <= td]
        if len(sub) == 0: return None
        last_date = sub.index[-1]
        # stale 체크 — entry 가 target_date 기준으로 14일 초과 전이면 None
        if (td - last_date).days > FORWARD_PE_STALE_DAYS:
            return None
        return float(sub.iloc[-1])
    except Exception: return None


def append_forward_pe_entry(date_str, fpe, source="manual", feps=None, spx=None):
    """forward_pe.json 에 entry 추가. atomic write (tmp → rename).
    fpe is None → False 반환 (적재 거부). 기존 같은 date 덮어쓰기. 성공 시 entry dict 반환."""
    import os, tempfile
    if fpe is None: return False
    try:
        fpe = float(fpe)
        if fpe <= 0 or fpe != fpe:  # NaN 거부
            return False
    except Exception: return False
    # date 정규화
    try:
        if hasattr(date_str, "strftime"): date_str = date_str.strftime("%Y-%m-%d")
        else: date_str = str(date_str)[:10]
    except Exception: return False
    FORWARD_PE.parent.mkdir(parents=True, exist_ok=True)
    if FORWARD_PE.exists():
        try:
            with open(FORWARD_PE, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            obj = {"schema_version":"1.0","data":[]}
    else:
        obj = {"schema_version":"1.0","data":[]}
    obj.setdefault("data", [])
    new_entry = {
        "date": date_str,
        "fpe": round(fpe, 4),
        "source": str(source),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if feps is not None:
        try: new_entry["feps"] = round(float(feps), 4)
        except: pass
    if spx is not None:
        try: new_entry["spx"] = round(float(spx), 4)
        except: pass
    # dedup + sort
    obj["data"] = [r for r in obj["data"] if r.get("date") != date_str] + [new_entry]
    obj["data"].sort(key=lambda r: r.get("date", ""))
    # atomic write
    try:
        tmp = FORWARD_PE.with_suffix(FORWARD_PE.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, FORWARD_PE)
    except Exception:
        return False
    load_forward_pe_history.cache_clear()
    return new_entry


# 기존 placeholder 호환
def load_forward_eps_history():
    """STEP 5-8 forward PE 로 대체. 호환성 alias."""
    return load_forward_pe_history()


def load_trailing_earnings_history():
    """STEP 5-3 후 활성. 현재 placeholder."""
    import pandas as pd
    return pd.DataFrame()


def load_historical_fundamentals(target_date):
    """STEP 5-5 통합 로더 (단일 진입점). 박스 평가 fpe 우선, trailing fallback."""
    import pandas as pd
    fpe = get_fpe_at(target_date)
    fpe_source = "forward" if fpe is not None else "trailing_fallback"
    # te / eg from trailing_earnings_history
    te = eg = te_3m = None
    _te_path = _resolve_path(TRAILING_EARN_HISTORY)
    if _te_path.exists():
        try:
            with open(_te_path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            df = pd.DataFrame(obj.get("data") or [])
            if len(df):
                df["date"] = pd.to_datetime(df["date"])
                td = pd.Timestamp(target_date)
                sub = df[df["date"] <= td]
                if len(sub):
                    te = sub.iloc[-1].get("te"); eg = sub.iloc[-1].get("eg")
                    sub_3m = df[df["date"] <= td - pd.Timedelta(days=90)]
                    if len(sub_3m): te_3m = (te - sub_3m.iloc[-1].get("te")) if te is not None else None
        except Exception: pass
    return {
        "cape": get_cape_at(target_date),
        "fpe": fpe,
        "fpe_source": fpe_source,
        "te": te,
        "eg": eg,
        "te_3m_chg": te_3m,
        "rg": None,  # multpl 미제공
        "hy_oas": get_hy_oas_at(target_date),
    }


if __name__ == "__main__":
    import sys
    print(f"CAPE meta: {cape_meta()}")
    s = load_cape_history()
    print(f"CAPE 시계열: {len(s)} entries, {s.index[0].date()} ~ {s.index[-1].date()}")
    test_dates = ["2025-04-30", "2024-08-05", "2022-11-30", "2020-03-23",
                   "2008-09-15", "2007-07-15", "2026-04-26"]
    for d in test_dates:
        v = get_cape_at(d)
        print(f"  {d}: CAPE = {v}")
    print(f"\nload_historical_fundamentals('2025-04-30') = {load_historical_fundamentals('2025-04-30')}")
