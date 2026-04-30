"""
미어캣의 관측소 (Meerkat's Observatory)
=========================================

매크로 환경 관측 도구. 47개 차원으로 시장의 현재 상태를 기록한다.

미어캣 무리에서 관측소는 외부 환경을 지속 감시하는 곳이다.
관측은 행동하지 않는다. 행동은 프로토콜의 일이다.

원작: akmoonge
공개: 2026-04
GitHub: https://github.com/akmoonge/meerkat-observatory

본 도구의 매크로 분석 프레임워크는 2022~2025년 한국에서 활동한
한 익명 거시 분석가의 글에서 영감을 받아 구축되었습니다.
우라가미 쿠니오의 사계절론, 조지 소로스의 반사성 이론, 그리고
위 익명 분석가의 통합적 적용 방식이 본 시스템의 분석 철학입니다.

원전 분석가는 본인의 익명성을 일관되게 유지해온 분이므로,
본 도구는 그분의 이름이나 식별자를 직접 명기하지 않습니다.
다만 본 도구가 존재할 수 있게 한 그분의 지적 작업에 깊은
감사를 표합니다.

분석 프레임워크 출처: 익명 한국 거시 분석가 (2022-2025)
- 우라가미 쿠니오 사계절론 적용
- 조지 소로스 반사성 이론 적용
- 다중 차원 매크로 통합 진단 방식

License: CC BY-NC-SA 4.0
"""
__author__ = "akmoonge"
__version__ = "3.10.5"
__license__ = "CC BY-NC-SA 4.0"
__source__ = "https://github.com/akmoonge/meerkat-observatory"
__created__ = "2026-04"

import streamlit as st, pandas as pd, numpy as np, plotly.graph_objects as go, json, warnings
import plotly.io as pio
from datetime import datetime, timedelta, date as _date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
warnings.filterwarnings("ignore")

# ═══ V8.0 1층 (40박스) + 2층 (ANFCI + CAPE_pct) — 2026-04-29 머지 ═══
import season_engine_v69 as V651
import season_engine_v8 as V8L1
import season_engine_vulnerability as V8L2

# V8 박스 ID → 한국어 라벨 (auto_season checks dict + _SEASON_BOX_HELP 키와 1:1 일치)
V8_BOX_LABELS = {
    # 봄 11
    "S_B1": "채권: 역전이 풀리는 중인데 고용도 살아난다",
    "S_B2": "연준: 저점에서 인하한다",
    "S_B3": "달러: 빠르게 떨어진다",
    "S_B4": "채권금리곡선: 빠르게 개선 중이다",
    "S_C1": "신용: 공포가 물러나는 중이다",
    "S_C2": "공포: 극단을 찍고 진정됐다",
    "S_R1": "실업률: 4%를 넘었거나 급등했다",
    "S_R2": "바닥: 고점에서 20% 이상 빠졌다",
    "S_R4": "실적: 나쁘지만 바닥은 지났다",
    "S_V1": "밸류: 거품이 다 빠졌다",
    "S_V2": "반도체: 가장 먼저 저점을 본다",
    # 여름 9
    "U_B1": "채권: 형이 안심하고 있다",
    "U_B2": "연준: 건드리지 않고 있다",
    "U_C1": "신용: 아무도 걱정하지 않는다",
    "U_C2": "공포: 없다",
    "U_R1": "고용: 견고하다",
    "U_R2": "시장 폭: 전 업종 동반 강세",
    "U_R4": "실적: 좋고 더 좋아지고 있다",
    "U_V1": "밸류: 정당화 가능하다",
    "U_V2": "반도체: 시장을 끌고 간다",
    # 가을 11
    "A_B1": "채권: 역전이 시작됐거나 깊어지고 있다",
    "A_B2": "연준: 고점에서 내리거나 올리고 있다",
    "A_B3": "달러: 비정상이다",
    "A_B4": "채권금리곡선: 빠르게 악화 중이다",
    "A_C1": "신용: 슬슬 벌어진다",
    "A_C2": "경기활동: 둔화 중이다",
    "A_R1": "반도체: 먼저 꺾였다",
    "A_R2": "시장 폭: 메가캡만 끌고 간다",
    "A_R4": "실적: 좋지만 느려지고 있다",
    "A_V1": "밸류: 어떤 잣대로 봐도 비싸다",
    "A_V2": "CAPE: 역사가 말한다",
    # 겨울 9
    "W_B1": "채권: 역전이 풀리는데 경제가 무너진다",
    "W_B2": "연준: 급하게 내리고 있다",
    "W_C1": "신용: 이미 깨졌다",
    "W_C2": "공포: 한 달 내내 지속된다",
    "W_R1": "실업률: 빠르게 오르고 있다",
    "W_R2": "하락: 멈췄지만 바닥이다",
    "W_R4": "실적: 확실히 무너졌다",
    "W_V1": "달러: 시스템이 위험하다",
    "W_V2": "추세: 200일선 아래 두 달째",
}

V8_SEASON_BOXES = {
    "봄":   ["S_B1", "S_B2", "S_B3", "S_B4", "S_C1", "S_C2",
             "S_R1", "S_R2", "S_R4", "S_V1", "S_V2"],
    "여름": ["U_B1", "U_B2", "U_C1", "U_C2",
             "U_R1", "U_R2", "U_R4", "U_V1", "U_V2"],
    "가을": ["A_B1", "A_B2", "A_B3", "A_B4", "A_C1", "A_C2",
             "A_R1", "A_R2", "A_R4", "A_V1", "A_V2"],
    "겨울": ["W_B1", "W_B2", "W_C1", "W_C2",
             "W_R1", "W_R2", "W_R4", "W_V1", "W_V2"],
}

VERSION = "8.0"
# 3.6 변경점: 거시 스코어 공식 개편 (10Y-3M 스프레드 tm 신규 추가, t 가중치 10→7)
#            + F5 가속도 모니터 윈도우 3M/6M → 1M/3M + threshold 차별화/clip
# 공식이 바뀌었으므로 3.5 히스토리와 시계열을 한 줄에 섞으면 Δ/ΔΔ 계산이 오염된다.
# 따라서 score_version 필드 + velocity/delta_delta 버전 가드 + 히스토리 파일 분리.
VERSION_STARTED = "2026-04-23"  # 3.6 적용 시작일 (적응기 배지 계산 기준, 60일간 Δ/ΔΔ 제한 표시)
SD = Path.home() / ".meerkat"
SF = SD / "state.json"; CF = SD / "config.json"; OD = SD / "observations"
BKD = SD / "backups"  # 일 1회 자동 백업 폴더 (14일 유지)

# ═══ V8.0 raw_data 디스크 캐시 (TTL 24h) + 히스테리시스 + prefix ═══
_V69_CACHE_KEY = "v8.0.0"
_RAW_PICKLE = SD / "cache" / "raw_data_v8.pickle"
_RAW_PICKLE_TTL_HOURS = 24
_HYSTERESIS_MARGIN = 1.5
_V8_PREFIX_RATIO = 0.33


def _load_pickled_raw_if_fresh():
    import time, pickle
    if not _RAW_PICKLE.exists(): return None
    try:
        age_hours = (time.time() - _RAW_PICKLE.stat().st_mtime) / 3600
        if age_hours > _RAW_PICKLE_TTL_HOURS: return None
        with open(_RAW_PICKLE, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def _save_pickled_raw(raw_data):
    import pickle
    try:
        _RAW_PICKLE.parent.mkdir(parents=True, exist_ok=True)
        with open(_RAW_PICKLE, "wb") as f:
            pickle.dump(raw_data, f)
    except Exception:
        pass


@st.cache_resource(show_spinner="V8.0 데이터 로딩 중 (FRED + yfinance + historical) ...")
def _v651_init(_cache_key=_V69_CACHE_KEY):
    """raw_data 디스크 캐시 (TTL 24h) + V8 1층 임계 적용."""
    cached = _load_pickled_raw_if_fresh()
    if cached is not None:
        V651.M.raw_data = cached
    else:
        V651.M.build_raw_data(verbose=False)
        _save_pickled_raw(V651.M.raw_data)
    V8L1.GS_S_V1 = 22
    return True


def _v8_prefix(season, v8_scores):
    """V8 분모별 prefix (초/늦) — 인접 계절 비율 ≥ 33% 시 부착."""
    cycle = ["봄", "여름", "가을", "겨울"]
    if season not in cycle: return ""
    bi = cycle.index(season)
    nxt = cycle[(bi + 1) % 4]; prv = cycle[(bi - 1) % 4]
    nxt_total = len(V8_SEASON_BOXES.get(nxt, []))
    prv_total = len(V8_SEASON_BOXES.get(prv, []))
    if nxt_total == 0 or prv_total == 0: return ""
    nxt_ratio = v8_scores.get(nxt, 0) / nxt_total
    prv_ratio = v8_scores.get(prv, 0) / prv_total
    if nxt_ratio >= _V8_PREFIX_RATIO: return "늦"
    if prv_ratio >= _V8_PREFIX_RATIO: return "초"
    return ""


def _v8_resolve_best(scores):
    """V8_SEASON_BOXES 카운트 scores → best season.
    GT 80 검증 기반 동률 룰 (2026-04-29):
      인접: 봄=여름→여름, 여름=가을→가을, 가을=겨울→겨울, 봄=겨울→겨울 (예외, 침체 진행 우선)
      비인접: 봄=가을→봄, 여름=겨울→겨울
      3개 이상: cycle 후순위 fallback
    """
    _SO = ["봄", "여름", "가을", "겨울"]
    _max = max(scores.values())
    cands = [s for s in _SO if scores[s] == _max]
    if len(cands) == 2:
        tiebreak = {
            frozenset(("봄", "겨울")): "겨울",
            frozenset(("여름", "가을")): "가을",
            frozenset(("봄", "여름")): "여름",
            frozenset(("가을", "겨울")): "겨울",
            frozenset(("봄", "가을")): "봄",
            frozenset(("여름", "겨울")): "겨울",
        }
        return tiebreak.get(frozenset(cands), cands[-1])
    return cands[-1]


def _v8_eval_at(offset):
    _v8_eval = V8L1.evaluate_v8_layer1(V651.M.raw_data, offset)
    _boxes = _v8_eval.get("boxes", {})
    _scores = {
        _sn: sum(1 for _bid in V8_SEASON_BOXES[_sn] if _boxes.get(_bid) is True)
        for _sn in ("봄", "여름", "가을", "겨울")
    }
    _best = _v8_resolve_best(_scores)
    return _best, _scores, _boxes


def _v8_apply_transition(curr_best, prev_30d_best, cape_20y_pct):
    """방안 5 룰 B + CAPE 게이트 (2026-04-29 GT 80 검증):
      박스 시스템에 '기억' 부재 → 직전 30일 best 전파해 사이클 순서 강제.
      가을/겨울 → 여름 사이클 역행 시 봄 삽입 (얕은 침체 봄 detection).
      CAPE 20y %ile < 50 게이트로 V자 반등 false 봄 차단.
    """
    if curr_best != "여름": return curr_best
    if prev_30d_best not in ("겨울", "가을"): return curr_best
    if cape_20y_pct is None: return curr_best
    if cape_20y_pct >= 50: return curr_best
    return "봄"


def evaluate_v651_today(offset=0, hysteresis=True, hysteresis_margin=None):
    """V8.0 1층 (40박스) + 2층 (ANFCI + CAPE_pct) 평가 + 히스테리시스."""
    if hysteresis_margin is None:
        hysteresis_margin = _HYSTERESIS_MARGIN
    _v651_init()
    # 자가 복구: cache_resource 가 실패 결과 캐시한 경우 V651.M.raw_data 가 None
    if V651.M.raw_data is None:
        try:
            cached = _load_pickled_raw_if_fresh()
            if cached is not None:
                V651.M.raw_data = cached
            else:
                V651.M.build_raw_data(verbose=False)
                _save_pickled_raw(V651.M.raw_data)
        except Exception as _e:
            print(f"[V8 self-recover FAIL] {type(_e).__name__}: {_e}")
            return None
    try:
        raw_best, _v8_scores, _v8_boxes = _v8_eval_at(offset)
    except Exception as _e:
        import traceback
        print(f"[V8 evaluate FAIL offset={offset}] {type(_e).__name__}: {_e}")
        traceback.print_exc()
        return None
    season = raw_best
    hyst_held = False
    if hysteresis and offset == 0:
        try:
            yest_best, yest_scores, _ = _v8_eval_at(offset + 1)
            if raw_best != yest_best:
                margin = _v8_scores[raw_best] - _v8_scores[yest_best]
                if margin < hysteresis_margin:
                    season = yest_best
                    hyst_held = True
        except Exception:
            pass
    try:
        _v8_layer2 = V8L2.compute_layer2(V651.M.raw_data, offset)
    except Exception:
        _v8_layer2 = {}
    # 방안 5 룰 B + CAPE 게이트: 사이클 역행 (가을/겨울 → 여름) 봄 삽입
    try:
        _prev_30d_best, _, _ = _v8_eval_at(offset + 30)
        _cape_pct_now = _v8_layer2.get("cape_pct") if _v8_layer2 else None
        season = _v8_apply_transition(season, _prev_30d_best, _cape_pct_now)
    except Exception:
        pass
    prefix = _v8_prefix(season, _v8_scores)
    # V8 confidence — best ≤ 4 → "판정 불가" (명세) / 그 외 비율 기반
    _top_total = len(V8_SEASON_BOXES.get(season, [])) or 1
    _abs_b = _v8_scores.get(season, 0)
    _ratio = _abs_b / _top_total
    if   _abs_b <= 4:    conf = "판정 불가"
    elif _ratio >= 0.6:  conf = "매우 높음"
    elif _ratio >= 0.45: conf = "높음"
    elif _ratio >= 0.3:  conf = "보통"
    else:                conf = "낮음"
    return {
        "base": season, "prefix": prefix, "label": prefix + season,
        "confidence": conf,
        "raw_season": raw_best,
        "hysteresis_held": hyst_held,
        "v8_boxes": _v8_boxes,
        "v8_scores": _v8_scores,
        "v8_layer2": _v8_layer2,
    }


# ═══ 자동 백업 ═══
# 앱 실행 시 하루 1회 핵심 파일을 ~/.meerkat/backups/YYYY-MM-DD/ 에 복사.
# 14일 지난 폴더는 자동 삭제. silent wipe(_dc_clear, 파싱 실패 덮어쓰기) 대비 최후의 안전망.
_AUTO_BACKUP_RAN = False  # 세션당 1회만
def _auto_backup():
    global _AUTO_BACKUP_RAN
    if _AUTO_BACKUP_RAN: return
    _AUTO_BACKUP_RAN = True
    try:
        BKD.mkdir(parents=True, exist_ok=True)
        today = _date.today().isoformat()
        today_dir = BKD / today
        if not today_dir.exists():
            today_dir.mkdir(parents=True, exist_ok=True)
            import shutil
            # 백업 대상: 누적 히스토리 + 상태 + 관측 로그
            _targets = [
                SD / "state.json", SD / "config.json", SD / "presets.json",
                SD / "cache" / "forward_eps_history.json",
                SD / "cache" / "mac_score_history.json",
                SD / "history" / "observations.jsonl",
                SD / "history" / "backfill_done.json",
            ]
            for src in _targets:
                try:
                    if src.exists() and src.stat().st_size > 0:
                        shutil.copy2(src, today_dir / src.name)
                except Exception: pass
            # observations/ 폴더 전체도 복사 (수동 저장분)
            try:
                if OD.exists():
                    obs_bk = today_dir / "observations"
                    obs_bk.mkdir(exist_ok=True)
                    for f in OD.glob("obs_*.json"):
                        try: shutil.copy2(f, obs_bk / f.name)
                        except Exception: pass
            except Exception: pass
        # 14일 초과 백업은 삭제
        try:
            from datetime import timedelta as _td
            cutoff = _date.today() - _td(days=14)
            import shutil as _sh
            for d in BKD.iterdir():
                if not d.is_dir(): continue
                try:
                    dd = _date.fromisoformat(d.name)
                    if dd < cutoff:
                        _sh.rmtree(d, ignore_errors=True)
                except Exception: pass
        except Exception: pass
    except Exception: pass

def lcfg():
    if CF.exists():
        try: return json.loads(CF.read_text("utf-8"))
        except: pass
    return {}
def scfg(c):
    SD.mkdir(parents=True, exist_ok=True); ex=lcfg(); ex.update(c); CF.write_text(json.dumps(ex,ensure_ascii=False,indent=2),"utf-8")
def sstate(d):
    """state.json 갱신. 기존 파일이 손상되면 백업 보존 후 새로 시작 (silent wipe 방지)."""
    SD.mkdir(parents=True, exist_ok=True); s = {}
    if SF.exists():
        try:
            s = json.loads(SF.read_text("utf-8"))
            if not isinstance(s, dict): raise ValueError("not a dict")
        except Exception:
            try:
                _stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                SF.rename(SF.with_name(f"{SF.stem}.corrupt-{_stamp}.json"))
            except Exception: pass
            s = {}
    s["meerkat_observatory"] = d
    SF.write_text(json.dumps(s, ensure_ascii=False, indent=2), "utf-8")
def lmk():
    if SF.exists():
        try: return json.loads(SF.read_text("utf-8")).get("meerkat",{})
        except: pass
    return {}
def sobs(t, ds):
    """관측 파일 저장. 실패 시 예외 상위로 올려 UI가 알도록."""
    OD.mkdir(parents=True, exist_ok=True)
    (OD / f"obs_{ds}.json").write_text(t, "utf-8")
def lobs():
    if not OD.exists(): return []
    return sorted(OD.glob("obs_*.json"),reverse=True)

REPO_RAW_BASE = "https://raw.githubusercontent.com/akmoonge/meerkat-observatory/main"

@st.cache_data(ttl=300, show_spinner=False)
def _check_update_available():
    """GitHub raw 의 meerkat_observatory.py 와 로컬을 md5 비교 (CRLF 정규화).
    5분 1회 체크. True = 업데이트 있음.
    """
    import urllib.request, hashlib
    try:
        req = urllib.request.Request(f"{REPO_RAW_BASE}/meerkat_observatory.py",
                                     headers={"User-Agent": "meerkat-observatory-checker"})
        remote_bytes = urllib.request.urlopen(req, timeout=10).read()
        local_path = Path(__file__)
        if not local_path.exists(): return False
        local_lf  = local_path.read_bytes().replace(b'\r\n', b'\n')
        remote_lf = remote_bytes.replace(b'\r\n', b'\n')
        return hashlib.md5(local_lf).hexdigest() != hashlib.md5(remote_lf).hexdigest()
    except Exception:
        return False

def _self_update():
    """GitHub raw 에서 최신 파일을 직접 가져와 덮어쓴다 (git 불필요).
    md5 비교 후 변경된 파일만 저장. (updated_list, failed_list) 반환.
    """
    import urllib.request, hashlib
    APP_DIR = Path(__file__).parent
    targets = ["meerkat_observatory.py", "meerkat_observatory.bat", "install.bat"]
    updated, failed = [], []
    for fn in targets:
        try:
            req = urllib.request.Request(f"{REPO_RAW_BASE}/{fn}", headers={"User-Agent": "meerkat-observatory-updater"})
            new_bytes = urllib.request.urlopen(req, timeout=15).read()
            local = APP_DIR / fn
            if local.exists():
                if hashlib.md5(local.read_bytes()).hexdigest() == hashlib.md5(new_bytes).hexdigest():
                    continue
            local.write_bytes(new_bytes)
            updated.append(fn)
        except Exception as e:
            failed.append(f"{fn}: {type(e).__name__}: {str(e)[:80]}")
    return updated, failed

# 테마 호환 색상: --mac-* CSS 변수로 라이트/다크 모드 자동 대응
C=dict(green="#1D9E75",red="#A32D2D",orange="#D85A30",blue="#3266ad",purple="#534AB7",
       gold="#EF9F27",teal="#2BA8A4",muted="var(--mac-muted, #8b949e)",
       bg="var(--mac-bg)",
       card="var(--mac-card)",
       border="var(--mac-border)",
       text="var(--mac-text)",
       bright="var(--mac-bright)")
SC={"봄":"#1D9E75","여름":"#EF9F27","가을":"#D85A30","겨울":"#3266ad"}
SL={"봄":"금융장세","여름":"실적장세","가을":"역금융장세","겨울":"역실적장세"}
SL_EASY={"봄":"금융장세 — 실적은 나쁘지만 돈이 풀림. 주가 상승",
         "여름":"실적장세 — 실적이 따라오면서 주가도 오름",
         "가을":"역금융장세 — 실적은 좋은데 긴축 시작. 주가 하락",
         "겨울":"역실적장세 — 실적도 떨어짐. 근데 하락 둔화. 여기서 사면 됨"}
SL_BS={"봄":"금융장세 (돈풀기장. 실적 좆망인데 주가 오름 ㅋ)",
       "여름":"실적장세 (실적 따라감장. 정상영업)",
       "가을":"역금융장세 (돈잠금장. 연준 긴축 ㅂㅂ)",
       "겨울":"역실적장세 (다 ㅈ됨장. 근데 여기서 줍줍)"}
def season_label(s,m="일반"):
    _base = s.lstrip("초늦")  # 초/늦 접두사는 SL 키에 없으므로 제거
    if m=="병신": return f"{s} ({SL_BS.get(_base,'')})"
    if m=="쉬운": return f"{s} ({SL_EASY.get(_base,'')})"
    return f"{s} ({SL.get(_base,'')})"

BS={"📡 대시보드":"📡 나스닥 꼬라지 (장 개박살났냐)","🚀 쌍발 엔진":"🚀 쌍발엔진 꼬라지 (QQQ·SOXX 둘 다 보자)","📈 채권/금리":"📈 채권행님 뷰 (채권이 형이다 눈치 챙겨라)",
    "💰 밸류에이션":"💰 씹거품 설거지각이냐","🏭 반도체":"🏭 킹도체 성님들 무빙",
    "🌡️ 계절 판단":"🌡️ 좆스닥 사계절 (지금 겨울이노)","📋 관찰 기록":"📋 매매 똥글 (기도메타 기록)",
    "역전 심화":"역전 심화 ㅆㅂ (나락 카운트다운)","역전":"장단기 역전ㅋㅋ (침체 드가자)",
    "정상":"정상 (아직 꿀통 안 터짐)",
    "극단 강세":"킹달러 애미터짐 (환율 좆창남)","공포 극대":"빅스 풀발기 (다 뒤졌다 빤스런 쳐라)",
    "극도의 낙관":"대불장 광기 (호구들 낭낭하게 입장중)",
    "자만":"빚투충 자만 오지네 ㅋㅋ (리스크 무시)","신용경색":"하이일드 터짐ㅋ (좀비기업 한강 정모)",
    "미어캣의 관측소":"미어캣의 관측소","심안":"심안 (deep mode)",
    "거시 스코어":"미장 대가리 발열 지수","미어캣 스코어":"내 시드 녹아내리는 온도"}
EASY={"10Y-2Y 스프레드":"10Y-2Y 스프레드 — 2년-10년 금리차. 마이너스면 형이 걱정하는 거다.",
      "10Y-3M 스프레드":"10Y-3M 스프레드 — 3개월-10년. 이게 역전되고 침체 안 온 적 없다.",
      "2Y-3M 스프레드":"2Y-3M 스프레드 — 3개월-2년. 연준이 너무 올렸는지 보는 지표.",
      "DXY":"DXY 달러인덱스 — 달러의 힘. 높으면 공포가 달러를 올리고 있다.",
      "원/달러":"원/달러 환율 — 환전 타이밍. 높으면 달러 팔 때, 낮으면 살 때.",
      "연방기금금리":"연방기금금리 — 연준이 정한 기준금리. 올리면 긴축, 내리면 완화. 방향이 전부다.",
      "FF 6M 변화":"FF 6M 변화 — 6개월 전 대비 기준금리 변화. 마이너스면 인하 중, 플러스면 인상 중.",
      "VIX":"VIX — 35 넘으면 사라. 20 밑이면 경계하라. 역발상 지표다.",
      "Fear & Greed":"Fear & Greed — 시장 심리 지수. 0이면 극도의 공포, 100이면 극도의 탐욕. 공포에 사라.",
      "Forward PE":"Forward PE — 예상이익 기준 주가배수. 22 넘으면 비싸다.",
      "Trailing PE":"Trailing PE — 지난 12개월 실적 기준 주가배수. 28 넘으면 역사적으로 터지지 않은 적 없다.",
      "Shiller CAPE":"Shiller CAPE — 10년 평균이익 기준. 35 넘으면 2000년밖에 없다.",
      "하이일드 스프레드":"하이일드 스프레드 — 우량-정크 금리차. 벌어지면 세상이 무서운 거다.",
      "SOX/SPX":"SOX/SPX — 반도체÷시장. 반도체가 지면 겨울이 온다.",
      "실질금리":"실질금리 — 명목금리-인플레기대. 파월이 진짜 보는 숫자.",  # 연준의장 바뀌면 여기 갱신
      "CPI YoY":"CPI YoY — 소비자물가 전년비. 방향만 봐라.",
      "CPI 코어 YoY":"CPI 코어 YoY — 식품·에너지 뺀 물가. 끈적한 인플레를 본다.",
      "PCE YoY":"PCE YoY — 개인소비지출 물가. CPI보다 범위가 넓다. 방향만 봐라.",
      "PCE 코어 YoY":"PCE 코어 YoY — 연준이 제일 중시한다. 2%가 목표.",
      "JOLTS":"JOLTS 구인건수 — 꺾이면 6개월 뒤 실업률이 올라간다.",
      "NFP":"NFP 비농업고용 — 매달 새 일자리 수. 마이너스면 침체다.",
      "GDP 성장률":"GDP 성장률 — 경제 성장 속도. 마이너스면 침체다. 분기에 한 번 나온다.",
      "실업률":"실업률 — 일자리 없는 사람 비율. 올라가기 시작하면 침체가 오고 있다.",
      "실업률 3M 변화":"실업률 3M 변화 — 3개월 새 실업률이 얼마나 올랐는지. 0.5%p 넘으면 삼의 법칙 발동.",
      "소비자신뢰":"소비자신뢰 — 사람들이 경제를 어떻게 느끼는지. 떨어지면 소비가 줄고 실적이 꺾인다.",
      "카드 연체율":"카드 연체율 — 미국인들이 카드값을 못 막고 있는 비율. 올라가면 소비가 죽는다.",
      "국채/GDP":"국채/GDP — 나라빚÷경제규모. 높으면 위기 때 정부가 돈 풀 여력이 없다.",
      "WTI 유가":"WTI 유가 — 유가가 인플레를 결정하고 인플레가 연준을 결정한다.",
      "Gold":"Gold — 금값은 달러에 대한 불신의 가격이다. 달러와 같이 오르면 뭔가 진짜 터지고 있는 거다.",
      "BEI 5Y":"BEI 5Y — 시장이 보는 5년 뒤 인플레. 흔들리면 연준이 움직인다.",
      "버핏 지표":"버핏 지표 — 시총÷GDP. 불균형은 균형으로 간다.",
      "배당수익률":"배당수익률 — 낮을수록 비싸다. 지금보다 낮았던 건 2000년뿐이다.",
      "XLE-SPY 3M":"XLE-SPY 3M — 에너지 vs 시장 3개월 상대수익. 에너지가 이기면 인플레 압력.",
      "XLK-SPY 3M":"XLK-SPY 3M — 기술 vs 시장 3개월 상대수익. 기술이 이기면 성장 기대.",
      "섹터 로테이션":"섹터 로테이션 — 에너지와 기술의 힘겨루기. 어느 쪽이 이기냐가 계절을 말한다.",
      "2Y10Y 역전 해소":"2Y10Y 역전 해소 — 형이 걱정을 풀고 있는 정도. 100%면 형이 마음을 놨다.",
      "3M10Y 역전 해소":"3M10Y 역전 해소 — 침체 신호 해소율. 풀리는 속도가 침체 도착 시점을 말한다.",
      "FF금리 위치":"FF금리 위치 — 10년 중 어디쯤 있냐. 같은 인하라도 출발점이 다르면 의미가 정반대다.",
      "인하 사이클":"인하 사이클 — 첫 인하 이후 지금이 어느 단계냐. 초입엔 환영, 후반엔 의심해야 한다.",
      "Forward EPS 추세":"Forward EPS 추세 — 분석가들이 1년 뒤 EPS 추정치를 올리냐 내리냐. 가격이 따라간다.",
      "가속도 모니터":"가속도 모니터 — 지표가 빨라지고 있냐 느려지고 있냐. 느려지기 시작하면 방향이 바뀐다.",
      "2차 도함수 매트릭스":"2차 도함수 매트릭스 — 지표가 빨라지고 있냐 느려지고 있냐. 방향보다 가속도가 전환점을 말한다.",
      "10Y-2Y ΔΔ":"10Y-2Y ΔΔ — 장단기금리차의 변화 속도가 빨라지는지 느려지는지.",
      "VIX ΔΔ":"VIX ΔΔ — 공포가 커지는 속도가 빨라지면 진짜 위기. 느려지면 소화 중.",
      "HY ΔΔ":"HY ΔΔ — 신용 긴장이 빨라지는지. 벌어지는 속도가 핵심이다.",
      "실업률 ΔΔ":"실업률 ΔΔ — 실업률이 오르는 속도가 빨라지면 침체 확정. 미세 가속이 선행 신호.",
      "CPI ΔΔ":"CPI ΔΔ — 물가가 내려가는 속도가 느려지면 인플레 재점화 경고.",
      "CFNAI MA3":"CFNAI MA3 — 미국 경제 활동 85개 지표를 합친 숫자다. 0 위면 경기 괜찮고, 0 밑이면 나빠지고 있다. -0.7 밑이면 침체다."}

def bsl(s,m="일반"):
    if m=="병신": return BS.get(s,s)
    if m=="쉬운": return EASY.get(s,s)
    return s

DQ=["관찰은 나침반이다. 나침반은 방향을 알려주지 걸어주지 않는다.",
    "보이는 것은 잠깐이요 보이지 않는 것은 영원함이라. — 고린도후서 4:18",
    "채권시장의 붕괴는 세상물정 모르는 나스닥 동생을 보고 느낀 형의 걱정이다.",
    "주가는 위치가 아닌 속도에 반응하는 방정식이다.",
    "항룡유회(亢龍有悔). 극에 이른 것은 반드시 뉘우친다.",
    "연극은 끝이 있고 거품은 꺼지며 불균형은 균형으로 간다.",
    "꽃이 지기로소니 바람을 탓하랴. — 조지훈",
    "반도체로 돈을 벌려면 기술을 공부하지 말고 밤하늘의 달을 보라.",
    "22년 12월엔 안 보이던 혁명이 왜 24년에 보이냐. 가격이 올랐기 때문이다.",
    "연준의 목표가 물가 안정이라 하는 사람은 연준을 모르는 사람이다. 걔네 기본적으로 금융인이다.",
    "3개월물과 10년물이 금리역전되고 침체 안 온 적 있나? 없다.",
    "모두가 모두가 병들었는데 아무도 아프지 않았다. — 이성복, '그날'",
    "나는 급류가 흐르는 강가의 난간이다. 잡을 수 있으면 나를 잡아라. 그러나 나는 너희들의 지팡이는 아니다. — 니체",
    "실업률이 올라갈 때부터 주식을 사기 시작하면 된다.",
    "뉴스 듣고 팔고 사는 건 조건반사다. 금수나 하는 짓이다.",
    "비싸게 팔 수 있는 걸 가지고 있으면 안 된다.",
    "물극필반(物極必反). 악재가 극에 이르면 반드시 호재가 나타난다.",
    "이 정도 고평가된 PER에서 숏을 잡아 실패한 사례가 없다. 140년간 예외 없었다.",
    "조롱에 사고 자랑에 팔아라.",
    "보름달이 뜨면 그믐을 보고, 얼어붙은 대지에서 봄을 볼 수 있는 눈."]
SQ={"과열":["집 팔아서라도 숏 잡아야 하는 시기다.","이 거품이 너희들 눈에는 안 보이냐.",
            "욕심이 잉태한즉 죄를 낳고 죄가 장성한즉 사망을 낳느니라. — 야고보서",
            "안전한 주식은 없다. 미국 주식 사면 안 된다.",
            "지금 롱 잡은 사람들은 자본주의 시스템과 함께 장엄하게 전사하는 거다."],
    "가을":["채권시장의 붕괴는 세상물정 모르는 나스닥 동생을 보고 느낀 형의 걱정이다.",
            "고점에서의 금리인하는 주가 하락을 이끈다.",
            "연극은 끝이 있고 거품은 꺼지며 불균형은 균형으로 간다.",
            "주가를 떨어뜨릴 힘은 지금보다 계속 작아질 수밖에 없다.",
            "호재가 작동 안 하는 게 하락장이고 악재가 작동 안 하는 게 반등장이다."],
    "겨울":["실업률이 올라갈 때부터 주식을 사기 시작하면 된다.",
            "역실적장세에선 매수가 정석이다.","이게 지옥이다. 주식은 원래 지옥이다. 네가 그토록 기다리던 지옥이 열렸다.",
            "떨어지면 사도 된다는 뜻이다.",
            "물극필반(物極必反). 악재가 극에 이르면 반드시 호재가 나타난다."],
    "바닥":["밤이 깊을수록 새벽은 가깝다. 견뎌라.",
            "보름달이 뜨면 그믐을 보고, 얼어붙은 대지에서 봄을 볼 수 있는 눈.",
            "반도체로 돈을 벌려면 기술을 공부하지 말고 밤하늘의 달을 보라.",
            "틀려도 맞는 법. 어느 시나리오든 이 가격대는 무조건 회복하는 가격대다.",
            "나는 항상 남들보다 먼저 들어간다. 그리고 꾸준히 산다. 마지막으로 견딘다."]}
def dq(): return DQ[datetime.now().toordinal()%len(DQ)]
def sq(gs):
    if gs is None: return "—","가을"
    b="바닥" if gs>=75 else "겨울" if gs>=50 else "가을" if gs>=25 else "과열"
    p=SQ[b]; return p[datetime.now().toordinal()%len(p)],b

# 미어캣 스코어 한줄평: 매집 환경 온도계
MQ={"평시":["지금은 네 차례가 아니다. DCA나 해라.",
            "총알을 아껴라. 쏠 때가 아니다.",
            "기다리는 것도 포지션이다.",
            "현금이 네 무기다. 지금은 장전하는 시간이다.",
            "네 시스템이나 지켜라. 규칙 없이 사는 놈들은 고점에서 사고 바닥에서 판다."],
    "경계":["채권이 먼저 움직인다. 주식은 아직 모른다. 형을 봐라.",
            "프로토콜 트리거를 확인해라. 아직 안 왔다.",
            "현금 보유고 잔액을 점검해라.",
            "냄새가 난다. 근데 냄새만으로 사지 마라.",
            "준비하되 서두르지 마라. 시장은 네 일정에 맞춰주지 않는다."],
    "작동":["트리거가 울리면 산다. 안 울리면 안 산다.",
            "프로토콜대로 해라. 감으로 하지 마라.",
            "떨어질 때 사는 게 쉬워 보이냐. 안 쉽다. 그래서 시스템이 있다.",
            "지금 사는 게 무섭다면 정상이다. 무서울 때 사는 거다.",
            "견뎌라. 이게 네가 기다린 구간이다."],
    "매집":["여기서 산 놈이 3년 뒤에 웃는다.",
            "공포에 사라. 욕심으로 사지 마라.",
            "현금 보유고를 쏟아부어라. 이게 그 총알의 용도다.",
            "쫄리면 디지시든가. 안 쫄리면 더 사라.",
            "실업률이 올라가고 있다. 여기서부터다."],
    "바닥":["모두가 죽었다고 할 때 산다.",
            "네 인생에서 이런 기회는 두세 번 온다.",
            "반도체 끝났다고? 끝난 적 없다.",
            "이 가격은 무조건 회복하는 가격이다. 시간문제일 뿐.",
            "나는 항상 남들보다 먼저 들어간다. 그리고 꾸준히 산다. 마지막으로 견딘다."]}
def mq(ms):
    if ms is None: return "—","평시"
    b="바닥" if ms>=80 else "매집" if ms>=60 else "작동" if ms>=40 else "경계" if ms>=20 else "평시"
    p=MQ[b]; return p[datetime.now().toordinal()%len(p)],b

# ── 툴팁 ──
TIP_CSS = """<style>
.gtp{position:relative;display:inline-block;cursor:help;font-size:var(--mac-fs-md);color:#888;margin-left:6px;vertical-align:super;outline:none}
.gtp .gtxt{visibility:hidden;opacity:0;position:absolute;z-index:9999;width:360px;max-width:90vw;padding:14px 16px;
background:#1a1a2e;border:1px solid #555;border-radius:10px;font-size:var(--mac-fs-sm);line-height:1.7;color:#ccc;
white-space:pre-line;left:0;top:100%;margin-top:8px;
max-height:400px;overflow-y:auto;box-shadow:0 4px 20px rgba(0,0,0,0.5);text-align:left;font-weight:400;font-style:normal}
.gtp-r .gtxt{left:auto;right:0}
.gtp:hover .gtxt,.gtp:focus .gtxt,.gtp:focus-within .gtxt{visibility:visible;opacity:1}
.gtp:focus,.gtp:focus-within{color:#e8e8e8}
.gtxt b{color:#e8e8e8}
.gtxt .tsep{border-top:1px solid #444;margin:10px 0}
[data-testid="stHorizontalBlock"]{overflow:visible !important}
[data-testid="column"]>div{overflow:visible !important}
</style>"""
def _tip(t):
    return f'<span class="gtp" tabindex="0">ⓘ<span class="gtxt">{t}</span></span>'

_TG_I = ("시장의 체온이다. 채권, 금리, 밸류에이션, 반도체, 고용을 "
"전부 합산해서 숫자 하나로 말한다. "
"높을수록 시장이 싸고, 무섭고, 빠져있다.\n"
"거시는 나침반이고 미어캣은 방아쇠다.")
_TG_R = {
0: "<b>\u25b8 지금 구간: 0~20</b>\n시장 과열. 모두가 돈을 벌고 있고 아무도 위험을 안 본다. "
"VIX는 바닥이고 하이일드는 역사적 최저다. "
'"이번엔 다르다"가 나오는 구간이다. 다르지 않다. '
"가지치기가 작동하고 현금이 쌓이는 시기.",
20: "<b>\u25b8 지금 구간: 20~40</b>\n식어가고 있다. 채권이 먼저 걱정하기 시작했다. "
"실적은 아직 좋은데 밸류에이션이 부담스럽다. "
"아직 매집 구간은 아니다. 근데 겨울 냄새가 난다. "
"현금을 점검하고 환전 타이밍을 재라. 준비하는 구간이다.",
40: "<b>\u25b8 지금 구간: 40~60</b>\n균열이 보인다. 실적이 꺾이기 시작하거나 "
"연준이 방향을 바꾸려 한다. 프로토콜 트리거가 접근 중이다. "
'"고점에서의 금리인하는 주가 하락을 이끈다." '
"여기서부터 거시와 미어캣 스코어를 같이 봐라.",
60: "<b>\u25b8 지금 구간: 60~80</b>\n본격 하락. 실업률이 올라가고 연준이 금리를 내리고 있다. "
'사람들이 "이번엔 끝났다"를 말한다. '
"프로토콜 트리거가 작동하는 구간이다. "
'"실업률이 올라갈 때부터 주식을 사기 시작하면 된다."',
80: "<b>\u25b8 지금 구간: 80~100</b>\n극도의 공포. VIX 40 이상. 하이일드 700bp 이상. "
"모두가 죽었다고 한다. 역사적으로 여기서 산 사람은 전부 이겼다. "
"22년 10월, 20년 3월, 09년 3월이 이 구간이었다. "
"네 인생에서 두세 번 온다."}
def _tip_mac(gs):
    b = 80 if gs>=80 else 60 if gs>=60 else 40 if gs>=40 else 20 if gs>=20 else 0
    return _TG_I + '<div class="tsep"></div>' + _TG_R[b]

_TM_I = ("네 계좌의 전투 준비 상태다. 시장이 빠졌는지, 실탄이 있는지, "
"트리거가 가까운지를 본다. 높을수록 매집 환경이 가깝다.\n"
"근데 이 숫자로 사지 마라. 매매는 프로토콜 트리거가 결정한다. "
"스코어는 트리거가 올 환경인지를 미리 알려줄 뿐이다.")
_TM_R = {
"평시": "<b>\u25b8 지금 구간: 0~20 평시</b>\n네 차례가 아니다. 시장은 고점 근처고 "
"트리거는 저 멀리 있다. DCA나 해라. 총알을 아껴라.",
"경계": "<b>\u25b8 지금 구간: 20~40 경계</b>\n바람이 바뀌기 시작한다. QQQ가 슬슬 빠지고 있다. "
"아직 경계장 진입 전이다. 현금 보유고 잔액을 확인해라. "
"환전할 거 있으면 지금 해라. 준비하되 서두르지 마라.",
"작동": "<b>\u25b8 지금 구간: 40~60 작동</b>\n프로토콜 트리거가 작동하기 시작하는 구간이다. "
"경계장에 진입했거나 조정장으로 가고 있다. "
"매수 배율이 올라가고 현금 보유고가 투입되기 시작한다. "
"지금 사는 게 무서우면 정상이다. 무서울 때 사는 거다.",
"매집": "<b>\u25b8 지금 구간: 60~80 매집</b>\n폭락장이다. QQQ가 -25% 이상 갔다. "
"SOXX도 -25%~-35% 구간이다. 매수 배율 3배. "
"현금 보유고 24%/주 투입. 여기서 산 놈이 3년 뒤에 웃는다. "
"프로토콜대로 해라. 감으로 하지 마라.",
"바닥": "<b>\u25b8 지금 구간: 80~100 바닥</b>\n극도의 공포에 실탄까지 충분하다. "
"네 인생에서 이런 조합은 두세 번 온다. "
"22년 10월, 20년 3월이 여기였다. "
"모든 재원을 프로토콜대로 투입하는 구간이다."}
def _tip_mk(ms):
    b = "바닥" if ms>=80 else "매집" if ms>=60 else "작동" if ms>=40 else "경계" if ms>=20 else "평시"
    return _TM_I + '<div class="tsep"></div>' + _TM_R[b]

_TX = {
"gg": "<b>\u25b8 거시\u2191 미어캣\u2191</b>\n세상도 싸고 나도 준비됐다. "
"프로토콜 트리거가 작동 중이고 거시 환경이 뒷받침한다. "
"매집 확신 최대. 프로토콜대로 실행하는 구간이다.",
"gl": "<b>\u25b8 거시\u2191 미어캣\u2193</b>\n거시는 겨울인데 가격이 아직 안 빠졌다. "
"채권은 경고하는데 나스닥은 아직 버티고 있다. "
"곧 온다. 현금 들고 기다려라. "
"트럼프의 한마디, 연준의 한마디에 하루 만에 뒤집힐 수 있다.",
"lg": "<b>\u25b8 거시\u2193 미어캣\u2191</b>\n가격은 빠졌는데 거시가 아직 과열이다. "
"기술적 조정이지 계절이 바뀐 게 아닐 수 있다. "
"속지 마라. 채권이 안 따라오는 주식 하락은 반등하면 원래 자리로 돌아간다.",
"ll": "<b>\u25b8 거시\u2193 미어캣\u2193</b>\n평시다. 시장도 비싸고 내 계좌도 평온하다. "
"할 일이 없다. DCA만 하고 프로토콜을 지켜라. "
"기다리는 게 지금은 최적 행동이다."}
def _tip_mx(g, m):
    k = ("g" if g is not None and g>=50 else "l") + ("g" if m is not None and m>=50 else "l")
    return _TX[k]

_TS = {
"봄": "<b>봄 \u2014 금융장세</b>\n실적은 바닥인데 연준이 돈을 푼다. "
"주가가 실적 없이 오른다. 유동성이 근거다. 20년 코로나 직후가 교과서적 봄이다.",
"여름": "<b>여름 \u2014 실적장세</b>\n풀린 돈이 경제에 스며들어 기업 이익이 올라오기 시작한다. "
"주가 상승이 정당화된다. 21년, 25년이 여름이었다. "
"가장 편안한 계절이다. 근데 편안할 때가 가장 위험하다.",
"가을": "<b>가을 \u2014 역금융장세</b>\n실적은 아직 좋다. 근데 긴축이 시작됐다. "
"금리가 올라가거나 외부 충격이 긴축 효과를 낸다. "
"주가가 실적 대비 비싸다는 걸 시장이 인식하기 시작한다. 22년 상반기가 가을이었다.",
"겨울": "<b>겨울 \u2014 역실적장세</b>\n실적이 빠진다. 근데 주가 하락은 둔화된다. "
"실업률이 올라가고 연준이 금리를 내린다. "
"\"역실적장세에선 매수가 정석이다.\" 22년 하반기가 겨울이었다."}
_TS_CYCLE = "\n\n순서: 봄\u2192여름\u2192가을\u2192겨울\u2192봄. 건너뛰지 않는다. 계절의 길이만 다를 뿐이다."
def _tip_season(sa):
    b = sa.lstrip("초늦")
    return _TS.get(b, _TS["가을"]) + _TS_CYCLE

# Δ 주요 지표 방향성 — 행 툴팁
_TD_ROW = {
"VIX": "<b>VIX</b>\n시장의 공포 체온계. 15 밑이면 시장이 방심하고 있다 — 과열 신호일 수 있다. "
"35 넘으면 시장이 비명을 지르고 있다 — 역사적으로 매수 구간에 가깝다.\n"
"1W와 1M이 같은 방향이면 추세다. 반대면 단기 노이즈일 가능성이 높다. "
"숫자 자체보다 방향의 가속도를 봐라. \"주가는 위치가 아닌 속도에 반응한다.\"",
"HY_bp": "<b>HY (하이일드 스프레드)</b>\n정크본드와 국채의 금리차. 채권시장은 주식시장보다 똑똑하고 빠르다. "
"스프레드가 벌어지기 시작하면 채권시장이 \"위험하다\"고 말하는 거다 — 주식은 아직 모르고 있어도. "
"좁으면 시장이 위험을 무시하고 있다. 좁은 상태가 오래 지속되면 그 자체가 경고다.\n"
"\"채권시장의 붕괴는 세상물정 모르는 나스닥 동생을 보고 느낀 걱정의 결과다.\"",
"2Y10Y_bp": "<b>10Y-2Y</b>\n장단기 금리차. 마이너스면 역전 — 채권시장이 침체를 예고한다. "
"역전 자체보다 역전이 풀리는 과정이 더 위험하다. 풀린다는 건 단기 금리가 내려간다는 뜻이고, "
"단기 금리가 내려가는 건 연준이 겁먹었다는 뜻이다.\n"
"숫자보다 부호 전환 시점을 봐라. \"장단기금리역전의 고점은 인플레의 고점 근처이고 주가의 저점 근처다.\"",
"DXY": "<b>DXY</b>\n달러의 힘. 오르면 두 가지 중 하나다 — 긴축이 강하거나, 공포에 달러로 몰리고 있거나. "
"내리면 역시 두 가지 — 완화 기대이거나, 달러 시스템 자체에 대한 신뢰가 흔들리거나.\n"
"전쟁·위기 때 단기 급등하고 종료되면 급락한다. 장기 방향은 미국 재정과 기축통화 지위가 결정한다.",
"SOX_SPX": "<b>SOX/SPX</b>\n반도체의 시장 대비 상대강도. 반도체가 시장을 이기고 있으면 사이클 초입이거나 AI 과열 구간이다. "
"반도체가 시장에 지기 시작하면 하락이 온다 — 반도체는 시장에 선행한다.\n"
"이 비율이 고점에서 꺾이는 시점이 계절 전환 신호다. \"반도체 산업의 주가는 시장에 선행한다.\"",
"KRW": "<b>원/달러</b>\n높으면 달러가 비싸다 — 원화를 달러로 바꾸기 불리하다. 낮으면 달러가 싸다 — 매집 재원을 환전하기 유리하다.\n"
"전쟁 프리미엄, 금리차, 수출 실적이 동시에 영향을 준다. 환전 타이밍은 달러 장기 방향(DXY)과 함께 봐야 한다."}

# 클러스터 괴리도 툴팁
_TIP_DIVERGENCE = ("<b>클러스터 괴리도</b>\n거시 스코어를 구성하는 5개 클러스터(채권/금리, 밸류에이션, 스트레스, 실물, 반도체) 간 "
"최대 점수 차이. 숫자가 작으면 지표들이 같은 말을 하고 있다 — 신호가 선명하다. "
"숫자가 크면 지표끼리 엇갈리고 있다 — 어떤 영역은 \"사라\" 하고 다른 영역은 \"아직이다\" 한다.\n"
"채권/금리가 먼저 움직이고 반도체가 나중에 따라오는 게 역사적 패턴이다. "
"괴리가 클수록 시장이 아직 소화하지 못한 정보가 많다는 뜻이고, 다음 움직임이 크다.")

# 미어캣의 관측소 (5클러스터 레이더) 툴팁
_TIP_MAC_EYE = ("<b>미어캣의 관측소</b>\n시장의 매수환경을 다섯 감각으로 분해한 레이더 차트. "
"12개 지표를 각각 0~10으로 매핑한 뒤 클러스터별 가중평균을 내고 100점으로 환산한다. "
"높을수록 매수환경이 좋다 — 공포가 크고, 금리가 꺾이고, 밸류에이션이 싸고, 실물이 바닥을 다지고, 반도체가 먼저 돌아서는 구간이다. "
"모양이 크고 고르면 모든 영역이 \"사라\"고 말하고 있다. "
"작고 찌그러져 있으면 엇갈리고 있다 — 누군가는 \"사라\" 하고 누군가는 \"아직이다\" 한다. "
"형(채권/금리)이 먼저 뾰족해지고 동생(반도체)이 마지막에 따라오는 게 역사적 패턴이다. "
"다섯 축이 동시에 커질 때까지 기다려라. \"욕심으로 사지 말고 두려움으로 사라.\"")

_TIP_CL_BOND = ("<b>채권/금리</b>\n10Y-2Y 스프레드 + 10Y-3M 스프레드 + FF금리 6M 변화 + 실질금리. "
"채권이 형이다. 형 말을 먼저 들어라. "
"10Y-3M 역전은 거시가 가장 신뢰하는 침체 예측 지표다 — 이게 역전되고 침체 안 온 적이 없다. "
"점수가 높으면 금리 인하가 진행 중이거나 임박해 있고 실질금리가 긴축적이라 완화 여지가 크다는 뜻이다. "
"이 축이 혼자 뾰족한데 나머지가 납작하면 — 형이 소리치고 있는데 동생은 아직 놀고 있다. 동생은 곧 울게 된다.")

_TIP_CL_VAL = ("<b>밸류에이션</b>\nForward P/E, Shiller CAPE. 주가가 이익 대비 얼마나 비싼지. "
"점수가 낮으면 비싸다. 높으면 싸다. 비싸면 방향은 확실하되 시기가 불확실하다. "
"\"역사상 이 정도 고평가된 PER에서 숏을 잡아 실패한 사례가 없다. 140년간 예외 없었다.\" "
"이 축이 올라와야 진짜 바닥이다.")

_TIP_CL_STRESS = ("<b>스트레스</b>\nHY 스프레드, VIX, 공포탐욕지수. 시장이 얼마나 겁먹었는지. "
"점수가 높으면 공포가 극단적이다. 극단적 공포는 매수의 조건이다. "
"\"vix 35에 지수 사서 실패한 사람 본 적 있니? 없어.\" "
"이 축이 어중간하면 아직 때가 아니다. 극단으로 갈 때까지 기다려라.")

_TIP_CL_REAL = ("<b>실물</b>\n실업률 3개월 변화, GDP 성장률. 경제가 실제로 꺾이고 있는지. 후행 영역이다. "
"여기가 높아지면 침체가 이미 왔거나 오고 있다는 뜻이다. "
"\"실업률 올라갈 때부터 주식을 사기 시작하면 된다.\" 역실적장세에선 매수가 정석이다. "
"다만 이 축이 높아졌을 때는 다른 축들도 이미 높아져 있어야 한다. "
"실물만 혼자 높으면 아직 하락이 덜 끝난 거다.")

_TIP_CL_SEMI = ("<b>반도체</b>\nSOX 고점 대비 낙폭, SOX 대 S&P 상대수익률. "
"\"반도체 산업의 주가는 시장에 선행한다.\" 먼저 빠지고 먼저 돌아선다. "
"이 축이 높아지기 시작하면 바닥이 가깝다. "
"가장 마지막에 올라오는 축이지만 가장 먼저 확인해야 하는 축이다. "
"\"예로부터 반도체 산업으로 돈을 벌려면 기술을 공부하지 말고 밤하늘의 달을 보라 했다.\"")

_TIP_CLUSTER_MAP = {"채권/금리": _TIP_CL_BOND, "밸류에이션": _TIP_CL_VAL,
                    "스트레스": _TIP_CL_STRESS, "실물": _TIP_CL_REAL, "반도체": _TIP_CL_SEMI}

# ═══ 채권 탭 심화 카드 (DGS20/DGS30/T10YIE/MOVE) — 단독 관찰용, 스코어 미편입 ═══
_DESC_DGS20  = "20년 만기 미국 국채 금리. 장기 자본비용의 기준점."
_DESC_DGS30  = "30년 만기 미국 국채 금리. 재정 건전성에 가장 민감한 구간."
_DESC_T10YIE = "10년 기대 인플레이션. 명목금리에서 TIPS 금리 뺀 값."
_DESC_MOVE   = "미국 국채 변동성 지수. 채권판 VIX다."

_TIP_DGS20_LONG = ("<b>DGS20 (20년 국채금리)</b>\n"
"20년물은 진짜 장기 균형금리에 가깝다. 10년물이 단기 시장 평가면 이건 장기 할인율이다. "
"5% 넘어가면 부동산, 장기 성장주, 인프라 — 다 재평가된다. 사면 그 금리가 확정 수익이다.")
_TIP_DGS30_LONG = ("<b>DGS30 (30년 국채금리)</b>\n"
"30년물은 연준 정책보다 재정 건전성에 반응한다. 이게 장기 추세선을 뚫으면 시장이 미국 부채 지속가능성을 "
"의심하기 시작했다는 뜻이다. 2023년 10월이 그랬다.")
_TIP_T10YIE_LONG = ("<b>T10YIE (10년 기대 인플레이션)</b>\n"
"연준이 실질금리 계산할 때 역산에 쓰는 숫자다. 2.0% 밑이면 디플레 공포, 2.5% 근처면 타겟 정렬, "
"2.8% 넘으면 기대가 닻을 잃은 거다. 그때 연준은 긴축으로 회귀한다.")
_TIP_MOVE_LONG = ("<b>MOVE (채권 변동성 지수)</b>\n"
"주식 VIX보다 선행한다. 2023년 3월 SVB 터지기 전에 MOVE가 먼저 튀었다. "
"금융시스템 스트레스 조기경보다. \"채권이 형이다.\"")

# 레인지 판정 → (라벨, 색, 센티넬 키). 코멘트는 _BOND_SENTINEL_COMMENTS 에서 룩업.
# 🟢 = 네가 유리한 구간 원칙 통일.
def j_dgs_abs(v):
    if v is None:     return ("—",          C["muted"],  None)
    if v < 4.0:       return ("너무 늦음",   C["red"],    "too_late")    # 🔴
    if v < 4.5:       return ("이미 비쌈",   C["orange"], "expensive")   # 🔴 (밝은 톤)
    if v < 5.0:       return ("관망",        C["gold"],   "watch")       # 🟡
    return             ("매수 기회",   C["green"],  "buy")         # 🟢

def j_t10yie(v):
    if v is None:     return ("—",                  C["muted"], None)
    if v < 2.0:       return ("디플레 공포",        C["red"],   "deflation")    # 🔴
    if v < 2.5:       return ("안정",               C["green"], "stable")       # 🟢
    if v < 2.8:       return ("이탈 조짐",          C["gold"],  "drift")        # 🟡
    return             ("연준 긴축 회귀",    C["red"],   "fed_return")   # 🔴

def j_move(v):
    if v is None:     return ("—",         C["muted"],  None)
    if v < 80:        return ("평온",      C["green"],  "calm")        # 🟢
    if v < 100:       return ("주의",      C["gold"],   "caution")     # 🟡
    if v < 120:       return ("스트레스",  C["orange"], "stress")      # 🔴
    return             ("패닉",      C["red"],    "panic")       # 🔴

# 센티넬 → 거시식 코멘트. 사용자가 직접 채울 슬롯.
# 공란이면 UI에 코멘트 블록 자체를 표시하지 않는다.
_BOND_SENTINEL_COMMENTS = {
    # DGS20 (20년 국채금리) — 장기 자본비용
    ("dgs20", "too_late"):   "20년물 4% 밑이다. 살 게 없다. 이미 다 올라간 자리다. 기다려라.",                             # <4.0    🔴 너무 늦음
    ("dgs20", "expensive"):  "20년물 4~4.5. 사면 안 되는 건 아닌데 그릇이 비싸다. 급할 거 없다.",                           # 4.0~4.5 🔴 이미 비쌈
    ("dgs20", "watch"):      "20년물 4.5~5 구간. 나쁘지 않다. 급하면 사도 되고 5 뚫는 거 보고 사도 된다.",                  # 4.5~5.0 🟡 관망
    ("dgs20", "buy"):        "20년물 5 넘었다. 사면 확정 수익 5다. 떨어지면 자본이득이 붙는다. 무한물타기 구간.",              # ≥5.0    🟢 매수 기회
    # DGS30 (30년 국채금리) — 재정 건전성
    ("dgs30", "too_late"):   "30년물 4% 밑이다. 금리 사이클 이미 돈 자리다. 뒷북은 내 방식이 아니다.",                      # <4.0    🔴 너무 늦음
    ("dgs30", "expensive"):  "30년물 4~4.5. 구조적 매수구간은 아니다. 더 떨어질 때 사라.",                                  # 4.0~4.5 🔴 이미 비쌈
    ("dgs30", "watch"):      "30년물 4.5~5. 분할매수 시작해도 되는 자리다. 5 뚫으면 확신 가지고 실어라.",                    # 4.5~5.0 🟡 관망
    ("dgs30", "buy"):        "30년물 5 넘었다. 시장이 미국 부채 의심하고 있다는 뜻이다. 그래서 싸다. 받아내라.",                # ≥5.0    🟢 매수 기회
    # T10YIE (10년 기대 인플레) — 디플레↔긴축회귀
    ("t10yie", "deflation"):  "기대 인플레 2% 밑. 시장이 디플레를 걱정하기 시작했다. 경기침체 전조다.",                       # <2.0    🔴 디플레 공포
    ("t10yie", "stable"):     "기대 인플레 안정적이다. 연준이 일을 하고 있다.",                                              # 2.0~2.5 🟢 안정
    ("t10yie", "drift"):      "기대가 닻에서 조금 벗어나는 중이다. 연준이 눈치를 본다.",                                      # 2.5~2.8 🟡 이탈 조짐
    ("t10yie", "fed_return"): "기대 인플레 2.8 넘었다. 닻이 풀렸다. 연준은 인하 못 한다. 긴축으로 돌아간다.",                   # ≥2.8    🔴 연준 긴축 회귀
    # MOVE (채권 변동성) — 채권판 VIX
    ("move",   "calm"):       "채권시장 평온하다. 장기채 편입하기 좋은 구간이다. 소란 없을 때 사는 게 정석.",                    # <80     🟢 평온
    ("move",   "caution"):    "채권 변동성이 올라오고 있다. 긴장하되 아직 사건은 아니다.",                                    # 80~100  🟡 주의
    ("move",   "stress"):     "채권시장에 스트레스가 쌓이는 중이다. 주식 VIX보다 먼저 튀는 게 이 지표다.",                      # 100~120 🔴 스트레스
    ("move",   "panic"):      "채권판 VIX가 패닉이다. 2023년 3월 SVB 직전에 튄 게 이 모양이었다. 금융시스템이 아프다.",           # ≥120    🔴 패닉
}

# 카드별 구간 기준 (icard 우측 상단 ⓘ 툴팁)
# 디버깅 + 임계값 한눈에. 코드의 if/elif와 어긋나면 그 자체가 버그 신호.
_CARD_RANGES = {
    "10Y-2Y 스프레드":  "<b>10Y-2Y 스프레드</b>\n🔴 역전 심화   <-40bp\n🔴 역전        -40~0\n🟡 해소 중     0~30\n🟢 정상        ≥30",
    "10Y-3M 스프레드":  "<b>10Y-3M 스프레드</b>\n🔴 역전 심화   <-40bp\n🔴 역전        -40~0\n🟡 해소 중     0~30\n🟢 정상        ≥30",
    "2Y-3M 스프레드":   "<b>2Y-3M 스프레드</b> (장기-단기 부호 통일, 음수 = 역전)\n🔴 역전 심화   <-40bp\n🔴 역전        -40~0\n🟡 해소 중     0~30\n🟢 정상        ≥30",
    "DXY":             "<b>DXY (달러 인덱스)</b>\n🟢 약세        <95\n🟡 중립        95~105\n🔴 강세        105~110\n🔴 극단 강세   ≥110",
    "원/달러":          "<b>원/달러 환율</b>\n🟢 강세        <1100\n🟢 정상        1100~1300\n🟡 약세        1300~1450\n🔴 극단 약세   ≥1450",
    "연방기금금리":     "<b>연방기금금리</b>\n🟢 완화        <1%\n🟡 중립        1~3\n🟡 제한적      3~5\n🔴 긴축        ≥5",
"실질금리": "<b>실질금리 (명목 - BEI)</b>\n🟢 완화        <0%\n🟡 중립        0~1.0\n🟡 긴축적      1.0~2.0\n🔴 강한 긴축   2.0~3.0\n🔴 극단        ≥3.0",
    "FF 6M 변화":       "<b>FF금리 6M 변화</b>\n🟢 인하 진행   <-0.5pp\n🟢 인하 시작   -0.5~-0.05\n🟡 동결        -0.05~0.05\n🔴 인상        0.05~0.5\n🔴 인상 진행   ≥0.5",
    "Forward PE":      "<b>Forward PE (S&P 500)</b>\n🟢 정상        <22\n🔴 극단        ≥22\n\n<b>이중 레인지</b>\n100년: 🟢<14 ⚪14~18 🟡18~22 🔴>22\n20년: ⚪<17 ⚪17~20 🟡20~24 🔴>24",
    "Shiller CAPE":    "<b>Shiller CAPE</b>\n⚪ 정상        <25\n⚪ 비싸다      25~35\n🔴 극단        ≥35\n(역사 평균 ≈17)\n\n<b>이중 레인지</b>\n100년: ⚪<15 ⚪15~25 🟡25~30 🔴>30\n20년: ⚪<22 ⚪22~30 🟡30~35 🔴>35",
    "Trailing PE":     "<b>Trailing PE (S&P 500)</b>\n⚪ 싸다        <20\n⚪ 비싸다      20~28\n🔴 극단        ≥28\n\n<b>이중 레인지</b>\n100년: 🟢<15 ⚪15~20 🟡20~25 🔴>25\n20년: ⚪<18 ⚪18~22 🟡22~28 🔴>28",
    "배당수익률":       "<b>S&P 500 배당수익률</b>\n🔴 경고        <1.5%\n⚪ 낮음        1.5~2.0\n⚪ 정상        ≥2.0",
    "버핏 지표":        "<b>버핏 지표 (시총/GDP %)</b>\n🟢 저평가       <160%   역사적 매수 구간\n🟡 보통         160~190%\n🔴 고평가       190~220%\n🔴 극단 고평가  ≥220%   거품 정점 영역\nFRED Z.1 비금융기업 주식 부채(NCBEILQ027S) 기준.\nWilshire5000 단종으로 후속 시리즈 사용. 분기 갱신.\nGDP는 미국 내 생산만 잡는다. 글로벌 기업이 커지면서\n숫자가 올라갔다. 숫자 자체보다 방향을 봐라.",
    "2Y10Y 역전 해소":  "<b>역전 해소 폭 (52주 최심점 대비)</b>\n🟢 해소 완료    회복 100% — 역전 종료. 봄이거나 이미 봄 지남.\n🟡 해소 진행 중  회복 50~99% — 겨울 후반. 봄 냄새.\n🟡 해소 초기    회복 10~49% — 아직 겨울. 근데 방향은 맞다.\n🔴 역전 지속    회복 <10% — 깊은 역전. 형이 아직 걱정 중.\n52주 내 역전 없었으면 표시 안 함.",
    "3M10Y 역전 해소":  "<b>역전 해소 폭 (52주 최심점 대비)</b>\n🟢 해소 완료    회복 100% — 역전 종료. 봄이거나 이미 봄 지남.\n🟡 해소 진행 중  회복 50~99% — 겨울 후반. 봄 냄새.\n🟡 해소 초기    회복 10~49% — 아직 겨울. 근데 방향은 맞다.\n🔴 역전 지속    회복 <10% — 깊은 역전. 형이 아직 걱정 중.\n52주 내 역전 없었으면 표시 안 함.",
    "FF금리 위치":      "<b>FF금리 수준 (10년 분위수 기준)</b>\n🔴 고점권  상위 30% 이상 — 여기서 인하하면 가을이다. 연준이 겁먹은 거다.\n⚪ 중립권  30~70% — 판단 보류. 다른 지표와 교차 확인.\n🟢 저점권  하위 30% — 여기서 인하하면 봄이다. 유동성이 풀리는 토양.",
    "인하 사이클":      "<b>인하 사이클 경과 (첫 인하 이후)</b>\n🟢 초입 0~3M — 첫 인하 직후. 시장은 반긴다. 근데 왜 내렸는지를 봐라.\n🟢 중반 3~9M — 유동성 전환 진행 중. 채권이 먼저 반응하고 주식이 따라온다.\n🟡 후반 9~15M — 인하가 길어지고 있다. 경기가 안 살아나면 이건 좋은 신호가 아니다.\n🔴 장기화 15M+ — 약발이 안 먹힌다. 제로금리까지 갈 수 있다. 09년, 20년이 여기였다.\nFF금리 위치(고점권/저점권)와 교차 확인 필수. 고점에서 시작된 인하는 가을이다.",
    "Forward EPS 추세": "<b>Forward EPS 컨센서스 변화 (30일)</b>\n🟢 급등 +1.5%↑ — 분석가들이 실적 추정을 크게 올리고 있다. 실적이 따라오는 거면 여름. 가격이 끌어올린 거면 거품.\n🟢 상향 0~+1.5% — 완만한 상향. 정상적 실적장세.\n⚪ 정체 -1.5~0% — 움직임 없다. 추세 전환 전 고요일 수 있다.\n🔴 하향 -1.5%↓ — 실적 추정이 꺾이고 있다. 속도의 방정식이다. 실적이 나빠지는 게 아니라 나빠지는 속도가 빨라지는 게 문제다.\n※ 최초 30일은 캐시 누적 기간. 데이터 부족 시 판정 보류.\n※ 가격 방향과 컨센서스 방향이 같은 기간이 6개월 이상 지속되면 반사성 경고.",
    "가속도 모니터":    "<b>가속도 모니터</b>\n핵심 5개 지표(VIX·HY·2Y10Y·DXY·SOX/SPX)의 속도가 빨라지는지 느려지는지 본다. 1M 변화가 3M 평균 한 달치 속도보다 크면 가속. 작으면 감속.\n속도는 방향이 아니다. 속도의 속도다. 추세가 꺾이는 순간을 잡는 것이 이 카드의 목적이다.\nVIX와 HY는 오르면 시장에 나쁘다. 그래서 이 둘의 가속은 내부적으로 뒤집어 집계한다. 5개 지표 전부 \"시장에 좋은 방향\"으로 정렬한 뒤 판정한다.\n🟢 가속 우세 — 3개 이상이 우호적 방향으로 가속. 추세가 아직 살아있다. 상승장 진행 중이라는 확인이지 매수 신호가 아니다. 꺾이기 전 과열 국면일 수 있다. 위치를 봐라. 이미 많이 올랐으면 가속 우세는 고점 근처의 풍경이다.\n🔴 감속 우세 — 3개 이상이 감속. 추세가 꺾이고 있다. 이게 이 카드의 본래 용도다. 전환점은 소리없이 온다. 이 신호가 뜨면 주시해라. 다만 감속 우세 한 번으로 방향 확정짓지 마라. 두 주 이어지면 그건 전환이다.\n⚪ 혼재 — 방향 불일치. 노이즈다. 다음 주 다시 봐라. 시장이 숨 고르는 중이다. 이 상태에서 포지션 바꾸지 마라.\n※ 2026-04 윈도우 변경 (3M/6M → 1M/3M). 사후 확인용에서 전환점 포착용으로 해상도를 올렸다.",
    "VIX":             "<b>VIX (역발상 지표)</b>\n🔴 극도의 낙관 <20\n⚪ 중립        20~25\n🟡 공포        25~35\n🟢 공포 극대   ≥35\n※ 역발상 지표. 다른 카드와 색상이 반대다. 초록 = 공포 = 매수기회. 빨강 = 낙관 = 과열.",
    "하이일드 스프레드":"<b>HY 스프레드</b>\n🟡 자만        ≤300bp\n🟡 중립        300~400\n🔴 경고        400~500\n🔴 신용경색    ≥500",
    "Fear & Greed":    "<b>Fear & Greed Index</b>\n🟢 Extreme Fear  <25\n🟡 중립           25~75\n🔴 Extreme Greed >75\n※ 역발상 지표. 다른 카드와 색상이 반대다. 초록 = 공포 = 매수기회. 빨강 = 탐욕 = 과열.",
    "실업률":          "<b>실업률 (U-3)</b>\n🟢 안정        <4%\n🟡 주의        4~5\n🔴 경고        ≥5",
    "실업률 3M 변화":   "<b>실업률 3M 변화</b>\n🟢 안정        ≤0pp\n🟡 주의        0~0.3\n🔴 경고        0.3~0.5\n🔴 Sahm 발동   >0.5",
    "CFNAI MA3":       "<b>CFNAI MA3 (시카고 연준 활동지수, 3개월 평균)</b>\n🟢 확장 강세  ≥+0.30\n🟢 정상       0~+0.30\n🟡 주의       -0.30~0\n🔴 경고       -0.70~-0.30\n🔴 침체       <-0.70 (시카고 룰)\n시카고 연준이 매달 발표하는 경제활동지수다. 고용·생산·소비·주문·재고 85개 월간 지표의 가중평균. 0이 장기 추세 성장이다. 3개월 평균이 -0.70 밑으로 가면 침체가 시작됐다는 뜻이다. 1967년 이후 이 룰이 틀린 적 없다. GDP나 실업률보다 빠르다. 후행지표 2개만 보던 실물 클러스터에 선행성을 준다.",
    "JOLTS":           "<b>JOLTS (구인건수)</b>\n🔴 냉각        <4000K (말랐음)\n🟡 냉각 시작   4000~5000\n🟢 정상        5000~8000\n🔴 과열        ≥8000",
    "NFP":             "<b>NFP (비농업고용 MoM)</b>\n🔴 수축        <0K\n🟡 약화        0~100\n🟡 둔화        100~200\n🟢 호조        ≥200",
    "GDP 성장률":      "<b>GDP 성장률 (분기 QoQ 연율)</b>\n🔴 침체        <0%\n🟡 실속        0~2\n🟢 정상        2~3\n🟢 호조        ≥3",
    "소비자신뢰":      "<b>UMich 소비자신뢰</b>\n🔴 비관        <60\n🟡 보통        60~80\n🟢 낙관        ≥80",
    "BEI 5Y":          "<b>BEI 5Y (기대 인플레)</b>\n🔴 디플레 경고 <2.0%\n🟢 안정        2.0~2.8\n🔴 인플레 경고 ≥2.8",
    "CPI YoY":         "<b>CPI YoY</b>\n수준:  🟢 ≤2  🟢 2~2.5  🟡 2.5~3  🟡 3~4  🔴 >4\n방향:  ↑ = 3M YoY 상승,  ↓ = 하락\n조합 6단계 라벨 (과열/고착/반등 ↔ 피크아웃/둔화/목표근접)",
    "CPI 코어 YoY":    "<b>CPI 코어 YoY</b>\n수준:  🟢 ≤2  🟢 2~2.5  🟡 2.5~3  🟡 3~4  🔴 >4\n방향:  ↑ = 3M YoY 상승,  ↓ = 하락\n조합 6단계 라벨 (과열/고착/반등 ↔ 피크아웃/둔화/목표근접)",
    "PCE YoY":         "<b>PCE YoY</b>\n수준:  🟢 ≤2  🟢 2~2.5  🟡 2.5~3  🟡 3~4  🔴 >4\n방향:  ↑ = 3M YoY 상승,  ↓ = 하락\n조합 6단계 라벨 (과열/고착/반등 ↔ 피크아웃/둔화/목표근접)",
    "PCE 코어 YoY":    "<b>PCE 코어 YoY</b>\n수준:  🟢 ≤2  🟢 2~2.5  🟡 2.5~3  🟡 3~4  🔴 >4\n방향:  ↑ = 3M YoY 상승,  ↓ = 하락\n조합 6단계 라벨 (과열/고착/반등 ↔ 피크아웃/둔화/목표근접)",
    "카드 연체율":      "<b>카드 연체율</b>\n🟢 안정        <3%\n🟡 주의        3~5\n🔴 경고        >5",
    "국채/GDP":         "<b>미국 국가부채/GDP</b>\n⚪ 여력        <100%\n⚪ 부담        100~120\n🔴 경고        >120",
    "WTI 유가":         "<b>WTI 유가</b>\n🔴 수요 부진   <$50\n🟢 정상        50~80\n🟡 부담        80~100\n🔴 위험        ≥100",
    "Gold":            "<b>Gold (YoY)</b>\n🔴 하락        <0%\n🟢 안정        0~10\n🟡 상승        10~25\n🔴 과열        >25",
    "SOX/SPX":         "<b>SOX/SPX 3M 상대수익률</b>\n🟢 아웃퍼폼    >0%\n🟡 언더퍼폼    ≤0",
    "XLE-SPY 3M":      "<b>XLE-SPY 3M</b>\n🔴 강한 아웃퍼폼  >+5%p\n🟡 아웃퍼폼      +2~+5\n⚪ 균형          -2~+2\n🟡 언더퍼폼      -3~-2\n🟢 강한 언더퍼폼  <-3%p",
    "XLK-SPY 3M":      "<b>XLK-SPY 3M</b>\n🟢 강한 아웃퍼폼  >+5%p\n🟡 아웃퍼폼      +2~+5\n⚪ 균형          -2~+2\n🟡 언더퍼폼      -3~-2\n🔴 강한 언더퍼폼  <-3%p",
    "섹터 로테이션":    "<b>섹터 로테이션 4사분면</b>\n🔴 인플레 회귀  XLE>+5, XLK<-3\n🟢 성장 회귀    XLK>+5, XLE<0\n🔴 동반 약세    둘 다 <-2\n🟡 균형 강세    둘 다 >+2 (거품 후기)\n⚪ 균형/노이즈  나머지",
    "2차 도함수 매트릭스": (
        "<b>2차 도함수 매트릭스 (ΔΔ)</b>\n"
        "5클러스터 + 거시 종합의 (값, Δ, ΔΔ) 테이블.\n"
        "ΔΔ 임계: ±1.5 등속 / ±5.0 급변\n"
        "⏫ 급가속 >+5.0\n↗ 가속 +1.5~+5.0\n➡ 등속 ±1.5\n↘ 감속 -1.5~-5.0\n⏬ 급감속 <-5.0\n"
        "Δ↑ + ΔΔ↘ = Angstblüte 경고. 겉은 개선인데 속은 꺾이고 있다."),
    "10Y-2Y ΔΔ": (
        "<b>10Y-2Y 스프레드 가속도</b>\n등속: ±5bp\n급변: ±15bp\n"
        "역전 해소가 가속 중이면 봄이 다가온다.\n역전 심화가 가속 중이면 형이 더 걱정하고 있다."),
    "VIX ΔΔ": (
        "<b>VIX 가속도</b>\n등속: ±1.0pt\n급변: ±3.0pt\n"
        "VIX가 가속 상승하면 공포가 공포를 먹는다.\n감속하면 패닉이 소화되고 있다."),
    "HY ΔΔ": (
        "<b>하이일드 스프레드 가속도</b>\n등속: ±8bp\n급변: ±25bp\n"
        "스프레드 벌어지는 속도가 빨라지면 신용경색 진입.\n느려지면 채권시장이 소화 중."),
    "실업률 ΔΔ": (
        "<b>실업률 가속도</b>\n등속: ±0.03%p\n급변: ±0.08%p\n"
        "실업률은 갑자기 오른다. 미세 가속이 가장 중요한 선행 신호."),
    "CPI ΔΔ": (
        "<b>CPI YoY 가속도</b>\n등속: ±0.05%p\n급변: ±0.15%p\n"
        "디스인플레 감속 = 인플레 재점화 경고.\n가속 = 연준이 편해진다."),
}

# ─── 채권 탭 장단기금리차 그래프 위 ⓘ 툴팁 (스프레드 3종 미어캣 톤) ───
# ─── 금리곡선 6상태 (A=10Y-2Y / B=10Y-3M / C=2Y-3M 역전 부호 조합) ───
# 종속성: A = B + C → 8 조합 중 2개 수학적 불가능.
_CURVE_STATE = {
    (False, False, False): (
        "정상 우상향", "3M < 2Y < 10Y", "green",
        "여름이다. 긴축도 완화도 아닌 순항 구간. 기간 프리미엄이 정상 작동, 은행은 단기로 빌려 장기로 빌려주며 마진을 챙긴다.",
        ("여름이다. 긴축도 완화도 아닌 순항 구간.<br><br>"
         "기간 프리미엄이 정상 작동하고 있다. 은행은 단기로 빌려 장기로 빌려주며 마진을 챙기고, "
         "대출은 원활하고, 신용은 돈다. 경기가 탄탄하거나 완만한 확장기다.<br><br>"
         "이 상태에서 걱정할 건 없다. 다만 이 상태가 ‘영원하다’고 생각하는 순간이 가을의 시작이다.<br><br>"
         "역사적 출현: 1995-96, 2004-06, 2013-16, 2017"),
    ),
    (True, False, False): (
        "10Y-2Y 단독 역전", "3M < 10Y < 2Y", "gold",
        "가장 흔한 초기 경고. 가을 진입 신호. 시장은 ‘지금은 금리가 높지만 2년 뒤엔 내려야 한다’고 말한다. 아직 3개월물까진 안 갔다 — 연준이 명백 실수는 아직.",
        ("가장 흔한 초기 경고다. 가을 진입 신호.<br><br>"
         "시장은 ‘지금은 금리가 높지만 2년 뒤엔 내려야 한다’고 말하고 있다. "
         "근데 아직 3개월물까지 역전되진 않았다 — 연준이 당장 실수를 저지른 건 아니라는 뜻이다.<br><br>"
         "이 상태가 오래 지속될수록 위험하다. 은행 마진이 줄고 대출이 위축되기 시작한다. "
         "장단기금리 역전은 예측이 아니라 원인이다. 고3이 스스로 재수를 예상하면 보통 재수한다.<br><br>"
         "22년 4~10월이 이 상태였다. 10Y-2Y가 먼저 역전되고 한참 뒤에 10Y-3M가 따라갔다.<br><br>"
         "역사적 출현: 2006 중반, 2019 초, 2022 4~10월"),
    ),
    (False, False, True): (
        "2Y-3M 단독 역전", "2Y < 3M < 10Y", "gold",
        "연준이 긴축 막바지에서 오버슈팅 중. 시장은 ‘연준이 곧 내린다’고 확신하지만 10년물은 아직 정상 — 장기 구조는 건전.",
        ("연준이 긴축 막바지에서 오버슈팅하고 있다.<br><br>"
         "3개월물은 기준금리에 직결된다. 2년물은 시장이 보는 향후 2년 평균. "
         "3M > 2Y면 시장은 ‘연준이 곧 내린다’고 확신하는 거다. "
         "근데 10년물은 아직 정상 — 장기 구조는 건전하다는 뜻이다.<br><br>"
         "파월이 12월에 50bp를 할지 25bp를 할지 — 이 스프레드가 먼저 말해준다. "
         "C가 역전된 상태에서 파월이 더 올리면 B도 역전된다. 그게 단기 전면 역전이다.<br><br>"
         "이 상태는 오래 안 간다. 연준이 인하하면 정상으로 돌아가고, 안 내리면 A까지 역전되며 더 악화된다.<br><br>"
         "역사적 출현: 2006 말 (짧게), 2019 7~8월"),
    ),
    (False, True, True): (
        "단기 전면 역전", "2Y < 10Y < 3M", "orange",
        "드문 상태. 3개월물이 10년물까지 넘었다 = 연준 명백 오버슈팅. 10Y > 2Y는 장기 기간 프리미엄 살아있다는 뜻 — 시스템 붕괴는 아직.",
        ("드문 상태다. 근데 나타나면 위험하다.<br><br>"
         "3개월물이 10년물까지 넘었다는 건 연준이 명백하게 오버슈팅한 거다. "
         "시장은 ‘경기가 망가질 것’이라고 확신한다. "
         "근데 10Y가 2Y보다 높다는 건 장기 기간 프리미엄이 아직 살아 있다는 뜻이다 — 시스템 붕괴는 아니다. 아직은.<br><br>"
         "이 상태에서 연준이 움직이지 않으면 A까지 역전돼서 완전 역전으로 간다.<br><br>"
         "역사적 출현: 2000 2~3월 (닷컴 직전, 짧게), 2023 하반기 일부 구간"),
    ),
    (True, True, False): (
        "장기 전면 역전", "10Y < 3M < 2Y", "orange",
        "채권시장 경고등 빨간불. 10년물이 단·중기 모두 아래. 장기 채권시장이 ‘미래는 지옥’이라 말한다. C 정상 = 연준 인상 끝났거나 끝물.",
        ("채권시장의 경고등이 빨간불이다.<br><br>"
         "10년물이 2년물과 3개월물 모두 아래로 갔다. 장기 채권시장이 ‘미래는 지옥’이라고 말하는 거다. "
         "근데 C가 정상이면 시장은 ‘연준이 당장은 더 올리지 않을 것’이라 본다. 금리 인상은 끝났거나 끝나간다.<br><br>"
         "이게 82년 11월 상황이다. 10Y-2Y 역전 67bp, 40년 만의 최대. 근데 그게 인플레 고점 근처였고 주가 저점 근처였다.<br><br>"
         "역전이 극에 이르면 사야 한다. 근데 그게 지금인지 아닌지는 속도를 봐야 한다. "
         "역전이 더 벌어지는 중이면 아직이다. 벌어지다가 멈추거나 좁혀지기 시작하면 그때다.<br><br>"
         "역사적 출현: 1982 2~8월, 2022 11월~2023 초"),
    ),
    (True, True, True): (
        "완전 역전", "10Y < 2Y < 3M", "red",
        "채권시장 만장일치. 침체가 온다. 단기부터 장기까지 전부 역전. 은행 마진 사라지고 대출 수축, 신용 경색. 예측이 아니라 원인이 작동 중.",
        ("채권시장 만장일치. 침체가 온다.<br><br>"
         "3M이 2Y보다 높고, 2Y가 10Y보다 높다. 단기부터 장기까지 전부 역전됐다. "
         "은행 마진이 완전히 사라졌다. 대출은 수축하고 신용은 경색된다. 예측이 아니라 원인이 작동하고 있다.<br><br>"
         "3개월물과 10년물이 금리역전되고 침체 안 온 적 있나? 없다.<br><br>"
         "22년 11월~23년 중반이 이 상태였다. ‘거의 모든 종류의 채권에서 역전이 나타난다’고 내가 썼다. "
         "그리고 이 상태에서 주가는 올랐다. 왜? 악재가 극에 이르면 내성이 생기고, 내성이 생기면 주가는 오른다.<br><br>"
         "이 상태에서 봐야 할 건 ‘해소 방향’이다. 단기금리가 내려와서 해소되면 봄이 온다. "
         "장기금리가 올라가서 해소되면 그건 채권시장 붕괴다. 같은 해소라도 방향이 정반대다.<br><br>"
         "역사적 출현: 1980-81, 2000 초, 2006-07, 2022 11월~2023"),
    ),
}


def _curve_state(t10y2y_v, t10y3m_v, t3m2y_v):
    """3 스프레드 입력 (모두 '장기-단기' 부호) → _CURVE_STATE 매칭.
    부호 통일: 음수 = 역전 (3종 모두). 양수 = 정상."""
    if t10y2y_v is None or t10y3m_v is None or t3m2y_v is None:
        return None
    a_inv = t10y2y_v < 0   # 10Y-2Y < 0 = 2Y > 10Y
    b_inv = t10y3m_v < 0   # 10Y-3M < 0 = 3M > 10Y
    c_inv = t3m2y_v < 0    # 2Y-3M < 0 = 3M > 2Y (부호 통일)
    return _CURVE_STATE.get((a_inv, b_inv, c_inv))


_SPREAD_HELP = {
    "10Y-2Y": (
        "가장 많이 인용되는 스프레드다. 근데 이것만 보면 안 된다.<br><br>"
        "역전되면 경기침체가 온다. 예측이 아니라 원인이다. 장기금리가 단기금리보다 낮아지면 "
        "은행 마진이 줄고, 대출이 줄고, 신용이 경색된다. 예측의 자기실현이다.<br><br>"
        "봐야 할 것: 역전의 깊이보다 해소 속도가 중요하다. 역전이 깊어지는 건 긴축이 진행 중이라는 뜻이고, "
        "해소가 시작되면 긴축의 끝이 보인다는 뜻이다.<br><br>"
        "82년 11월 역전 67bp — 40년 만의 최대. 그게 인플레 고점 근처였고 주가 저점 근처였다. "
        "역전이 극에 이르면 오히려 사야 한다.<br><br>"
        "언제 보냐: 항상 본다. 가장 기본이다. 근데 이게 역전됐다고 바로 팔면 안 된다. "
        "역전은 12~18개월 선행한다. 성급하면 돈 잃는다."
    ),
    "10Y-3M": (
        "내가 가장 신뢰하는 침체 예측 지표다. 3개월물과 10년물이 금리역전되고 침체 안 온 적 있나? 없다.<br><br>"
        "10Y-2Y보다 이걸 더 봐야 하는 이유: 3개월물은 연준 기준금리에 거의 직결된다. "
        "시장 기대가 아니라 연준의 현재 행동이 반영된다. 10년물은 시장이 보는 미래다. "
        "이 둘의 차이는 ‘연준의 현재 행동과 시장이 보는 미래의 괴리’다. "
        "2년물은 시장 기대가 섞여서 노이즈가 있다.<br><br>"
        "봐야 할 것: 역전 진입 시점과 해소 시점. 역전이 시작되면 시계가 돌아가기 시작한 거다. "
        "해소가 시작되면 두 가지 중 하나다. 연준이 인하했거나(봄 또는 겨울), 장기금리가 올라갔거나(채권시장 붕괴).<br><br>"
        "3개월 변화(도함수)를 봐라. 3M에 +30bp 이상 개선이면 사이클 전환 신호다. "
        "-30bp 이상 악화면 역전 가속이다. 수준이 아니라 속도에 반응하는 방정식이다.<br><br>"
        "언제 보냐: 사이클 전환기에 가장 중요하다. 긴축 끝물, 인하 초입. "
        "이때 이 스프레드의 방향이 계절을 결정한다."
    ),
    "2Y-3M": (
        "이건 연준이 얼마나 오버슈팅했는지 보는 지표다.<br><br>"
        "3개월물은 현재 기준금리. 2년물은 시장이 예상하는 향후 2년간 평균 금리. "
        "정상이면 2년물이 높다 — 금리가 더 오를 거라는 뜻이니까. "
        "역전되면? 시장이 ‘연준이 너무 올렸다, 앞으로 내린다’고 말하는 거다.<br><br>"
        "22년 11월 내가 봤을 때 3개월물과 2년물의 차이가 벌어지고 있었다. 이건 채권시장 참여자들이 "
        "금리는 4.75 이상 올라가고 인플레는 2년 안엔 안 잡힌다고 말하는 거였다.<br><br>"
        "봐야 할 것: 이 스프레드가 역전에서 정상으로 돌아오는 순간. "
        "시장이 ‘연준의 다음 움직임은 인하’라고 확신하기 시작했다는 뜻이다. "
        "12월 FOMC에서 파월이 50bp를 할지 25bp를 할지 — 이 스프레드가 먼저 말해준다.<br><br>"
        "10Y-2Y, 10Y-3M와 같이 봐야 한다. 세 스프레드가 동시에 역전이면 채권시장 전체가 침체를 확신하는 거다. "
        "하나만 역전이면 아직 의견이 갈리는 거다.<br><br>"
        "언제 보냐: FOMC 전후. 연준의 다음 행동을 채권시장이 어떻게 가격 매기는지 이게 가장 빨리 보여준다."
    ),
}

# 1W 컬럼 헤더 툴팁
_TIP_1W = ("<b>1W</b>\n지난 1주 변화량. 노이즈가 많다. 뉴스 하나, 트럼프 트윗 하나에 흔들린다. "
"이건 날씨다. 날씨를 보고 계절을 판단하면 안 된다.\n"
"1M과 방향이 같은지 확인하는 용도로만 써라. 1W만 보고 \"추세가 바뀌었다\"고 판단하는 건 금수나 하는 짓이다.\n"
"금리류는 bp(1bp = 0.01%), 가격류는 %.")

# 1M 컬럼 헤더 툴팁
_TIP_1M = ("<b>1M</b>\n지난 1개월 변화량. 여기서부터 계절이 보이기 시작한다. "
"1W와 같은 방향이면 추세가 살아 있다. 반대면 두 가지 중 하나다 — 추세가 전환 중이거나, 1W가 일시적으로 이탈한 거다.\n"
"구분법은 간단하다. 다음 주에 1W가 1M 방향으로 돌아오면 노이즈였던 거고, 그 다음 주에도 안 돌아오면 전환이다.\n"
"방향 자체보다 가속도를 봐라. 같은 방향이어도 1M 변화폭이 줄고 있으면 힘이 빠지고 있는 거다. "
"\"주가는 위치가 아닌 속도에 반응한다.\" 속도가 줄고 있으면 방향이 바뀔 준비를 하고 있는 거다.")


# ═══ DISK CACHE ═══
CACHE_DIR = SD / "cache"

def _dc_path(key):
    import hashlib
    return CACHE_DIR / f"{hashlib.md5(key.encode()).hexdigest()[:16]}.json"

def _dc_get(key, ttl):
    """디스크 캐시 읽기. TTL 초과 → None."""
    import time
    p = _dc_path(key)
    if not p.exists(): return None
    try:
        d = json.loads(p.read_text("utf-8"))
        if time.time() - d.get("ts", 0) > ttl: return None
        return d.get("data")
    except: return None

_LAST_FETCH = SD / "last_fetch"
def _dc_set(key, data):
    """디스크 캐시 쓰기."""
    import time
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try: _dc_path(key).write_text(json.dumps({"ts": time.time(), "data": data}, ensure_ascii=False, default=str), "utf-8")
    except: pass
    try: _LAST_FETCH.write_text(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "utf-8")
    except: pass

def _dc_clear():
    """디스크 캐시 전체 삭제.
    누적 히스토리 파일(forward_eps_history.json, mac_score_history.json)은 보존한다.
    해시 16자 파일명 캐시만 지우도록 제한."""
    if CACHE_DIR.exists():
        import re
        _hex16 = re.compile(r"^[0-9a-f]{16}\.json$")
        _keep = {"forward_eps_history.json", "mac_score_history.json"}
        for f in CACHE_DIR.glob("*.json"):
            if f.name in _keep: continue
            if not _hex16.match(f.name): continue
            try: f.unlink()
            except: pass

def _s2d(s):
    """pd.Series → JSON dict."""
    if s is None or len(s) == 0: return None
    return {str(k): float(v) for k, v in zip(s.index, s.values)}

def _d2s(d):
    """JSON dict → pd.Series."""
    if not d: return None
    try:
        s = pd.Series(d, dtype=float); s.index = pd.to_datetime(s.index); return s.sort_index()
    except: return None

# ═══ FETCHERS ═══

# TTL 상수 — 데이터 갱신 주기별
TTL_DAILY   = 7200    # 2시간: 일간 갱신 지표
TTL_WEEKLY  = 43200   # 12시간: 주간 갱신 지표
TTL_MONTHLY = 86400   # 24시간: 월간 갱신 지표
TTL_QUARTER = 259200  # 72시간: 분기 갱신 지표

# FRED 시리즈별 TTL 매핑
FRED_TTL = {
    "DGS2": TTL_DAILY, "DGS10": TTL_DAILY, "DGS20": TTL_DAILY, "DGS30": TTL_DAILY, "DTB3": TTL_DAILY,
    "T10Y2Y": TTL_DAILY, "T10Y3M": TTL_DAILY, "VIXCLS": TTL_DAILY,
    "DCOILWTICO": TTL_DAILY, "T10YIE": TTL_DAILY, "T5YIE": TTL_DAILY,
    "DEXKOUS": TTL_DAILY,  # 원/달러 환율
    "BAMLH0A0HYM2": TTL_WEEKLY,
    "UNRATE": TTL_MONTHLY, "FEDFUNDS": TTL_MONTHLY, "PAYEMS": TTL_MONTHLY,
    "CPIAUCSL": TTL_MONTHLY, "CPILFESL": TTL_MONTHLY,
    "PCEPI": TTL_MONTHLY, "PCEPILFE": TTL_MONTHLY,
    "JTSJOL": TTL_MONTHLY, "UMCSENT": TTL_MONTHLY,
    "A191RL1Q225SBEA": TTL_QUARTER, "GDP": TTL_QUARTER, "DRCCLACBS": TTL_QUARTER, "GFDEGDQ188S": TTL_QUARTER,
    "NCBEILQ027S": TTL_QUARTER,  # F4 버핏 지표 — Z.1 비금융기업 주식 부채 (Wilshire 단종 후속)
    "CFNAI": TTL_MONTHLY,        # V3.3 CFNAI MA3 — 실물 클러스터 선행 슬롯
}

@st.cache_data(ttl=TTL_DAILY, show_spinner=False)
def ffred_daily(sid, key, start="2019-01-01"):
    dc = _d2s(_dc_get(f"fred_{sid}_{start}", TTL_DAILY))
    if dc is not None: return dc
    try:
        from fredapi import Fred; s = Fred(api_key=key).get_series(sid, observation_start=start).dropna()
        if len(s) > 0: _dc_set(f"fred_{sid}_{start}", _s2d(s))
        return s
    except: return pd.Series(dtype=float)

@st.cache_data(ttl=TTL_WEEKLY, show_spinner=False)
def ffred_weekly(sid, key, start="2019-01-01"):
    dc = _d2s(_dc_get(f"fred_{sid}_{start}", TTL_WEEKLY))
    if dc is not None: return dc
    try:
        from fredapi import Fred; s = Fred(api_key=key).get_series(sid, observation_start=start).dropna()
        if len(s) > 0: _dc_set(f"fred_{sid}_{start}", _s2d(s))
        return s
    except: return pd.Series(dtype=float)

@st.cache_data(ttl=TTL_MONTHLY, show_spinner=False)
def ffred_monthly(sid, key, start="2019-01-01"):
    dc = _d2s(_dc_get(f"fred_{sid}_{start}", TTL_MONTHLY))
    if dc is not None: return dc
    try:
        from fredapi import Fred; s = Fred(api_key=key).get_series(sid, observation_start=start).dropna()
        if len(s) > 0: _dc_set(f"fred_{sid}_{start}", _s2d(s))
        return s
    except: return pd.Series(dtype=float)

@st.cache_data(ttl=TTL_QUARTER, show_spinner=False)
def ffred_quarter(sid, key, start="2019-01-01"):
    dc = _d2s(_dc_get(f"fred_{sid}_{start}", TTL_QUARTER))
    if dc is not None: return dc
    try:
        from fredapi import Fred; s = Fred(api_key=key).get_series(sid, observation_start=start).dropna()
        if len(s) > 0: _dc_set(f"fred_{sid}_{start}", _s2d(s))
        return s
    except: return pd.Series(dtype=float)

def ffred(sid, key, start="2019-01-01"):
    """TTL 자동 분기 래퍼."""
    ttl = FRED_TTL.get(sid, TTL_DAILY)
    if ttl == TTL_QUARTER: return ffred_quarter(sid, key, start)
    if ttl == TTL_MONTHLY: return ffred_monthly(sid, key, start)
    if ttl == TTL_WEEKLY:  return ffred_weekly(sid, key, start)
    return ffred_daily(sid, key, start)

def ffred_parallel(sd, key, start="2019-01-01"):
    """병렬 FRED 로드. 각 시리즈는 개별 TTL 캐시 적용."""
    out = {}
    def _f(n, s): return n, ffred(s, key, start)
    with ThreadPoolExecutor(max_workers=8) as ex:
        fs = {ex.submit(_f, n, s): n for n, s in sd.items()}
        for f in as_completed(fs):
            try: n, s = f.result(); out[n] = s
            except: out[fs[f]] = pd.Series(dtype=float)
    return out

@st.cache_data(ttl=TTL_DAILY, show_spinner=False)
def fyf_batch(tickers, per="5y"):
    """yfinance 배치 다운로드. 단일 호출로 데이터 혼선 방지."""
    _ck = f"yf_{'_'.join(sorted(tickers))}_{per}"
    dc = _dc_get(_ck, TTL_DAILY)
    if dc is not None:
        return {k: _d2s(v) for k, v in dc.items() if _d2s(v) is not None}
    import yfinance as yf
    try:
        df = yf.download(tickers, period=per, progress=False, auto_adjust=True, group_by="ticker")
        if df.empty: return {}
        out = {}
        for tk in tickers:
            try:
                if len(tickers) == 1:
                    c = df["Close"]
                else:
                    c = df[tk]["Close"]
                if isinstance(c, pd.DataFrame): c = c.iloc[:, 0]
                s = c.dropna()
                if len(s) > 0: out[tk] = s
            except: pass
        # V3.6-hotfix: stale 검사 + 단일 ticker retry
        # yf.download(group_by='ticker') 배치 모드가 ^VIX/^GSPC 같은 인덱스 티커의
        # 마지막 1~2일을 가끔 빠뜨림. 누락된 거 + 마지막 날짜가 4영업일 이상 옛날인
        # 시리즈를 단일 ticker history() 로 재페치 후 union/대체.
        try:
            import datetime as _dt
            _today = pd.Timestamp(_dt.datetime.now(_dt.timezone.utc).date())
            _stale_thr = pd.Timedelta(days=4)
            _to_retry = []
            for tk in tickers:
                s = out.get(tk)
                if s is None or len(s) == 0:
                    _to_retry.append(tk)
                else:
                    _last = pd.Timestamp(s.index[-1]).tz_localize(None) if s.index[-1].tzinfo else pd.Timestamp(s.index[-1])
                    if _today - _last > _stale_thr:
                        _to_retry.append(tk)
            for tk in _to_retry:
                try:
                    _t = yf.Ticker(tk)
                    _h = _t.history(period=per, auto_adjust=True)
                    if _h is not None and not _h.empty:
                        _c = _h["Close"].dropna()
                        # tz-naive 통일 (배치 결과와 인덱스 타입 일치)
                        if _c.index.tz is not None:
                            _c.index = _c.index.tz_localize(None)
                        if len(_c) > 0:
                            _old = out.get(tk)
                            if _old is not None and len(_old) > 0:
                                if _old.index.tz is not None:
                                    _old.index = _old.index.tz_localize(None)
                                _merged = pd.concat([_old, _c])
                                _merged = _merged[~_merged.index.duplicated(keep="last")].sort_index()
                                out[tk] = _merged
                            else:
                                out[tk] = _c
                except: pass
        except: pass
        if out: _dc_set(_ck, {k: _s2d(v) for k, v in out.items()})
        return out
    except: return {}

def fyf_load(td, per="5y"):
    """이름→티커 딕셔너리로 yfinance 배치 로드."""
    tickers = list(td.values())
    raw = fyf_batch(tuple(sorted(tickers)), per)
    out = {}
    for name, tk in td.items():
        out[name] = raw.get(tk, pd.Series(dtype=float))
    return out

@st.cache_data(ttl=300, show_spinner=False)
def fyf_live_price(ticker: str):
    """V3.6: 프리/애프터마켓 포함 최신 체결가.
    1m × 1d, prepost=True 의 마지막 bar Close → 정규장 종가가 아닌
    현재 호가 (프리장/정규/애프터 어디든 마지막 거래가). TTL 5분.
    실패 시 None → 호출부가 일간 종가로 폴백."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        df = t.history(period="1d", interval="1m", prepost=True, auto_adjust=False)
        if df is None or df.empty:
            return None
        return float(df["Close"].dropna().iloc[-1])
    except: return None

@st.cache_data(ttl=TTL_DAILY, show_spinner=False)
def fetch_krx_etf_close(ticker_code: str):
    """KRX ETF 직전 거래일 종가(원). 실패/빈 결과 시 None → 호출부가 기존 셀 값 유지.

    pykrx 는 KRX 웹 스크래핑 기반이라 간헐적 타임아웃/Rate limit 있음.
    - V4.5 시점 TIGER 381180 는 OK, K-QLD 278420 는 empty 반환 (상장 폐지 가능성).
    - get_etf_ohlcv_by_date 는 내부 ISIN 조회로 KRX 로그인 요구 → 실패 가능.
      get_market_ohlcv_by_date 가 더 안정적 (ETF 도 동일하게 조회됨)."""
    try:
        from pykrx import stock
        today = datetime.now().strftime("%Y%m%d")
        from_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
        df = stock.get_market_ohlcv_by_date(from_date, today, ticker_code)
        if df is None or df.empty:
            return None
        return float(df["종가"].iloc[-1])
    except Exception:
        return None

@st.cache_data(ttl=TTL_DAILY, show_spinner=False)
def ffg():
    """Fear & Greed: CNN 공식 지수만 사용. 실패 시 None."""
    dc = _dc_get("fg", TTL_DAILY)
    if dc is not None: return dc
    # CNN은 단순 UA만 보내면 418("I'm a teapot. You're a bot.")로 차단한다.
    # edition.cnn.com에서 실제로 호출할 때의 헤더 세트를 그대로 흉내낸다.
    _CNN_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://edition.cnn.com/",
        "Origin": "https://edition.cnn.com",
    }
    try:
        import urllib.request, json as j
        req = urllib.request.Request("https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                                     headers=_CNN_HEADERS)
        with urllib.request.urlopen(req, timeout=10) as r: d = j.loads(r.read())
        sc = d.get("fear_and_greed", {}).get("score")
        rt = d.get("fear_and_greed", {}).get("rating")
        if sc is not None:
            r = {"score": sc, "rating": rt, "source": "CNN"}; _dc_set("fg", r); return r
    except: pass
    try:
        import urllib.request, json as j
        req = urllib.request.Request("https://production.dataviz.cnn.io/index/fearandgreed/current",
                                     headers=_CNN_HEADERS)
        with urllib.request.urlopen(req, timeout=10) as r: d = j.loads(r.read())
        sc = d.get("score"); rt = d.get("rating")
        if sc is not None:
            r = {"score": sc, "rating": rt, "source": "CNN"}; _dc_set("fg", r); return r
    except: pass
    return {"score": None, "rating": None, "source": None}

@st.cache_data(ttl=TTL_MONTHLY, show_spinner=False)
def fval_av(av_key):
    """Alpha Vantage OVERVIEW → PE, ForwardPE, DividendYield."""
    R = {"trailing_pe": None, "forward_pe": None, "div_yield": None, "cape": None}
    if not av_key: return R
    dc = _dc_get("val_av", TTL_MONTHLY)
    if dc is not None: return dc
    try:
        import urllib.request, json as j
        url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol=SPY&apikey={av_key}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r: d = j.loads(r.read())
        pe = d.get("PERatio"); fpe = d.get("ForwardPE"); dy = d.get("DividendYield")
        if pe and pe not in ("None", "-", "0"): R["trailing_pe"] = round(float(pe), 1)
        if fpe and fpe not in ("None", "-", "0"): R["forward_pe"] = round(float(fpe), 1)
        if dy and dy not in ("None", "-", "0"): R["div_yield"] = round(float(dy) * 100, 2)
    except: pass
    if any(v is not None for v in R.values()): _dc_set("val_av", R)
    return R

@st.cache_data(ttl=TTL_MONTHLY, show_spinner=False)
def fval(av_key=""):
    R = {"trailing_pe": None, "cape": None, "div_yield": None, "forward_pe": None, "source": "없음"}
    dc = _dc_get("val", TTL_MONTHLY)
    if dc is not None: return dc
    # 1) multpl.com — meta description에서 추출 (2026~ 구조 대응)
    try:
        import urllib.request, re
        H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        for url, k, pat in [
            ("https://www.multpl.com/s-p-500-pe-ratio", "trailing_pe", r"Current S&P 500 PE Ratio is ([\d.]+)"),
            ("https://www.multpl.com/shiller-pe", "cape", r"Current Shiller PE Ratio is ([\d.]+)"),
            ("https://www.multpl.com/s-p-500-dividend-yield", "div_yield", r"Current S&P 500 Dividend Yield is ([\d.]+)%"),
        ]:
            req = urllib.request.Request(url, headers=H)
            with urllib.request.urlopen(req, timeout=10) as r: html = r.read().decode("utf-8")
            m = re.search(pat, html)
            if m: R[k] = float(m.group(1))
        if R["trailing_pe"]: R["source"] = "multpl.com"; _dc_set("val", R); return R
    except: pass
    # 2) Alpha Vantage
    if av_key:
        av = fval_av(av_key)
        filled = False
        for k in ("trailing_pe", "forward_pe", "div_yield"):
            if av.get(k) is not None: R[k] = av[k]; filled = True
        if filled: R["source"] = "Alpha Vantage"; _dc_set("val", R); return R
    # 3) yfinance — SPY + VOO 복수 시도
    for tk in ["SPY", "VOO"]:
        try:
            import yfinance as yf; info = yf.Ticker(tk).info or {}
            pe = info.get("trailingPE"); fpe = info.get("forwardPE"); dy = info.get("trailingAnnualDividendYield")
            if pe and pe > 0: R["trailing_pe"] = round(float(pe), 1)
            if fpe and fpe > 0: R["forward_pe"] = round(float(fpe), 1)
            if dy and dy > 0: R["div_yield"] = round(float(dy) * 100, 2)
            if R["trailing_pe"]:
                R["source"] = f"yfinance ({tk})"
                break
        except: continue
    if R["trailing_pe"]: _dc_set("val", R)
    return R

@st.cache_data(ttl=TTL_DAILY, show_spinner=False)
def fspy_info():
    """SPY earnings data for auto season detection."""
    R = {"earningsGrowth": None, "revenueGrowth": None, "trailingEps": None, "forwardEps": None}
    dc = _dc_get("spy_info", TTL_DAILY)
    if dc is not None: return dc
    try:
        import yfinance as yf; info = yf.Ticker("SPY").info or {}
        for k in R:
            v = info.get(k)
            if v is not None: R[k] = float(v)
    except: pass
    if any(v is not None for v in R.values()): _dc_set("spy_info", R)
    return R

@st.cache_data(ttl=TTL_MONTHLY, show_spinner=False)
def fest_fwd_pe():
    """Top 10 시총가중 Forward PE. Top10이 S&P500의 ~35-40% 시총이므로 대표성 충분."""
    dc = _dc_get("fwd_pe_top10", TTL_MONTHLY)
    if dc is not None: return dc
    import yfinance as yf
    TOP = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "BRK-B", "UNH", "JPM", "V"]
    wfpe = 0; wmcap = 0
    for tk in TOP:
        try:
            info = yf.Ticker(tk).info or {}
            fp = info.get("forwardPE"); mc = info.get("marketCap")
            if fp and fp > 0 and mc and mc > 0:
                wfpe += float(fp) * float(mc); wmcap += float(mc)
        except: continue
    if wmcap == 0: return None
    r = round(wfpe / wmcap, 1)
    _dc_set("fwd_pe_top10", r)
    return r

def auto_season(fd, yd, ff, unemp, fpe, tpe, cape, wti, spy_info,
                fpe_z=None, fpe_3m_chg=None):
    """V8.0 1층 40박스 + 히스테리시스 + 2층 (ANFCI/CAPE) 평가.
    evaluate_v651_today() 호출 — 대시보드/계절판단 탭 일치 보장.
    """
    res = evaluate_v651_today(offset=0)
    if res is None:
        return "—", "판정 불가", {"봄":[],"여름":[],"가을":[],"겨울":[]}, {"봄":0,"여름":0,"가을":0,"겨울":0}
    best_label = res["label"]
    v8_boxes = res.get("v8_boxes", {})
    v8_scores = res.get("v8_scores", {})
    checks = {}
    for season in ("봄", "여름", "가을", "겨울"):
        items = []
        for box_id in V8_SEASON_BOXES[season]:
            v = v8_boxes.get(box_id)
            items.append((V8_BOX_LABELS[box_id], v is True))
        checks[season] = items
    scores = {s: float(v8_scores.get(s, 0)) for s in ("봄", "여름", "가을", "겨울")}
    conf = res.get("confidence", "보통")
    return best_label, conf, checks, scores


def _legacy_auto_season_v3_UNREACHABLE(fd, yd, ff, unemp, fpe, tpe, cape, wti, spy_info,
                fpe_z=None, fpe_3m_chg=None):
    """[unreachable] V3.12.1 36박스 평가 — V8 머지 후 폐기. 본문은 호출 안 됨."""
    from datetime import timedelta as td
    def _ago(s, days):
        """시계열에서 N일 전 값. 월간/일간 데이터 모두 대응."""
        if s is None or len(s) < 2: return None
        target = s.index[-1] - td(days=days)
        older = s[s.index <= target]
        return float(older.iloc[-1]) if len(older) > 0 else None

    eg = spy_info.get("earningsGrowth"); rg = spy_info.get("revenueGrowth")
    te = spy_info.get("trailingEps"); fe = spy_info.get("forwardEps")
    # Earnings 판정: 1차 earningsGrowth → 2차 EPS비교 → 3차 Forward/Trailing PE 비율
    earnings_2q_decline = False  # V3.8 신규 (5종째)
    if eg is not None:
        earnings_bad = eg < 0
        earnings_good = eg > 0
        earnings_accel = rg is not None and eg > rg and eg > 0.05
        earnings_declining = eg < 0 and fe is not None and te is not None and fe < te
        # 2분기 연속 감소: earningsGrowth + revenueGrowth 둘 다 음수
        earnings_2q_decline = (eg < 0) and (rg is not None and rg < 0)
    elif te is not None and fe is not None and te > 0 and fe > 0:
        earnings_bad = fe < te
        earnings_good = fe > te
        earnings_accel = fe > te * 1.1
        earnings_declining = fe < te * 0.9
        # fe < te × 0.85 → 강한 하락 (2q decline 대용)
        earnings_2q_decline = fe < te * 0.85
    elif fpe is not None and tpe is not None and fpe > 0 and tpe > 0:
        # fpe < tpe → forward earnings > trailing → 실적 성장
        earnings_bad = fpe > tpe * 1.05
        earnings_good = fpe < tpe * 0.95
        earnings_accel = fpe < tpe * 0.85
        earnings_declining = fpe > tpe * 1.1
        # PE 급등 = 실적 급락
        earnings_2q_decline = fpe > tpe * 1.15
    else:
        earnings_bad = False; earnings_good = False
        earnings_accel = False; earnings_declining = False
        earnings_2q_decline = False

    # FEDFUNDS 변화 — 날짜 기반 (월간 데이터 대응)
    ff_s = fd.get("FEDFUNDS"); dgs2_s = fd.get("DGS2")
    ff_now = float(ff_s.iloc[-1]) if ff_s is not None and len(ff_s) > 0 else None
    ff_6m_ago = _ago(ff_s, 180); ff_3m_ago = _ago(ff_s, 90)
    ff_6m_chg = (ff_now - ff_6m_ago) if (ff_now is not None and ff_6m_ago is not None) else None
    ff_3m_decline = (ff_now < ff_3m_ago) if (ff_now is not None and ff_3m_ago is not None) else False
    dgs2_below_ff = False
    if dgs2_s is not None and len(dgs2_s) > 0 and ff is not None:
        dgs2_below_ff = float(dgs2_s.iloc[-1]) < ff - 0.5

    # UNRATE 변화 — 날짜 기반
    un_s = fd.get("UNRATE")
    un_now = float(un_s.iloc[-1]) if un_s is not None and len(un_s) > 0 else None
    un_3m_ago = _ago(un_s, 90)
    un_3m = (un_now - un_3m_ago) if (un_now is not None and un_3m_ago is not None) else None

    # NFP (PAYEMS) 월간 변화
    payems_s = fd.get("PAYEMS"); payems_chg = None
    if payems_s is not None and len(payems_s) > 1:
        try:
            _gap = (payems_s.index[-1] - payems_s.index[-2]).days
            if _gap <= 45: payems_chg = float(payems_s.iloc[-1]) - float(payems_s.iloc[-2])
        except Exception: pass

    # QQQ 52주
    qqq_s = yd.get("QQQ"); qqq_bounce = False; qqq_dd_deep = False; qqq_flat = False
    if qqq_s is not None and len(qqq_s) > 252:
        hi = float(qqq_s.iloc[-252:].max()); lo = float(qqq_s.iloc[-252:].min()); cur = float(qqq_s.iloc[-1])
        from_hi = (cur / hi - 1) * 100; from_lo = (cur / lo - 1) * 100
        qqq_bounce = from_lo > 10 and from_hi < -20
        qqq_dd_deep = from_hi < -20
    if qqq_s is not None and len(qqq_s) > 21:
        m1_chg = (float(qqq_s.iloc[-1]) / float(qqq_s.iloc[-22]) - 1) * 100
        qqq_flat = -3 <= m1_chg <= 3

    # WTI 3M 변화 — FRED DCOILWTICO
    wti_s = fd.get("WTI"); wti_3m_surge = False
    if wti_s is not None and len(wti_s) > 63:
        wti_3m_surge = (float(wti_s.iloc[-1]) / float(wti_s.iloc[-64]) - 1) * 100 > 30

    # QQQ 6M 수익률
    qqq_6m_pos = False
    if qqq_s is not None and len(qqq_s) > 126:
        qqq_6m_pos = float(qqq_s.iloc[-1]) > float(qqq_s.iloc[-127])

    # 섹터 디커플링: XLE/XLK vs SPX 3M 상대수익률
    xle_s = yd.get("XLE"); xlk_s = yd.get("XLK"); spx_s_as = yd.get("SPX")
    def _r3m(s):
        if s is None or len(s) < 64: return None
        return (float(s.iloc[-1]) / float(s.iloc[-64]) - 1) * 100
    _xle3 = _r3m(xle_s); _xlk3 = _r3m(xlk_s); _spx3 = _r3m(spx_s_as)
    _xle_spy = (_xle3 - _spx3) if (_xle3 is not None and _spx3 is not None) else None
    _xlk_spy = (_xlk3 - _spx3) if (_xlk3 is not None and _spx3 is not None) else None
    inflation_rotation = _xle_spy is not None and _xlk_spy is not None and _xle_spy > 5 and _xlk_spy < -3
    growth_rotation    = _xle_spy is not None and _xlk_spy is not None and _xlk_spy > 5 and _xle_spy < 0
    narrow_market      = _xle_spy is not None and _xlk_spy is not None and _xle_spy < -2 and _xlk_spy < -2

    # HY 정점 후 축소: 6개월 최대값 대비 80% 미만 + 정점이 의미 있게 높았어야(>4%)
    hy_s = fd.get("HY"); hy_peak_drop = False
    if hy_s is not None and len(hy_s) > 1:
        _tail = hy_s.iloc[-126:] if len(hy_s) >= 126 else hy_s
        hy_peak = float(_tail.max()); hy_now = float(hy_s.iloc[-1])
        hy_peak_drop = (hy_now < hy_peak * 0.80) and (hy_peak > 0.04)

    # 소비 버팀: UMICH 소비자신뢰 또는 카드 연체율
    um_s = fd.get("UMCSENT"); cd_s = fd.get("DRCCLACBS")
    um_v = float(um_s.iloc[-1]) if um_s is not None and len(um_s) > 0 else None
    cd_v = float(cd_s.iloc[-1]) if cd_s is not None and len(cd_s) > 0 else None
    consumer_ok = (um_v is None and cd_v is None) or (um_v is not None and um_v > 70) or (cd_v is not None and cd_v < 4)

    # ── F3 FF금리 historical 위치: 인하 사이클 단계 분기 ──
    # 같은 인하라도 고점에서 내리면 가을(위기 대응), 저점에서 내리면 봄(구제)
    _ff_pos_in = _ff_position(ff_s, lookback_years=10)
    ff_high_zone = _ff_pos_in is not None and _ff_pos_in >= 70  # 고점권
    ff_low_zone  = _ff_pos_in is not None and _ff_pos_in <  30  # 저점권
    ff_cutting = ff_6m_chg is not None and ff_6m_chg < 0
    # 봄 인하 = 저점/중립 + 인하. 가을 인하 = 고점 + 인하.
    spring_cut = ff_cutting and not ff_high_zone   # 봄 신호로 카운트
    autumn_cut = ff_cutting and ff_high_zone       # 가을 신호로 카운트

    # ── V3.8 신규 헬퍼 호출 ──
    _3m10y_s = fd.get("T10Y3M"); _2y10y_s = fd.get("T10Y2Y")
    sox_s = yd.get("SOXX"); rsp_s = yd.get("RSP")
    spy_s = yd["SPY"] if ("SPY" in yd and yd["SPY"] is not None) else yd.get("SPX")
    spx_s = yd.get("SPX"); vix_s = fd.get("VIXCLS")
    _inv_3m10y = _inv_state(_3m10y_s)
    _inv_2y10y = _inv_state(_2y10y_s)
    _hy_decoup = _hy_decoupling(fd.get("HY"), vix_s)
    _wti_shock = _wti_inflation_shock(fd.get("WTI"), spx_s)
    _breadth_n = _breadth_narrow(spy_s, rsp_s)
    # ff_3m_chg (양/음 부호) — 기존 ff_3m_decline 옆에 추가 산출
    ff_3m_chg = (ff_now - ff_3m_ago) if (ff_now is not None and ff_3m_ago is not None) else None
    # HY 절대수준 + 6M 변화량
    _hy_s_in = fd.get("HY")
    _hy_now = float(_hy_s_in.iloc[-1]) if (_hy_s_in is not None and len(_hy_s_in) > 0) else None
    _hy_6m_ago = _ago(_hy_s_in, 180)
    if _hy_now is not None and _hy_now <= 1.0:
        _hy_now_pct = _hy_now * 100
        _hy_6m_pct  = _hy_6m_ago * 100 if _hy_6m_ago is not None else None
    else:
        _hy_now_pct = _hy_now
        _hy_6m_pct  = _hy_6m_ago
    _hy_6m_chg = (_hy_now_pct - _hy_6m_pct) if (_hy_now_pct is not None and _hy_6m_pct is not None) else None

    # QQQ 52주 from_lo/from_hi 재계산 (봄 #6 임계 완화용 변수 노출)
    _qqq_from_lo = None; _qqq_from_hi = None
    if qqq_s is not None and len(qqq_s) > 252:
        _hi252 = float(qqq_s.iloc[-252:].max()); _lo252 = float(qqq_s.iloc[-252:].min()); _cur252 = float(qqq_s.iloc[-1])
        _qqq_from_hi = (_cur252 / _hi252 - 1) * 100
        _qqq_from_lo = (_cur252 / _lo252 - 1) * 100

    # VIX 정점 통과 (봄 #9): 90일 max ≥ 30 + 현재 < 25
    _vix_peak_passed = False
    if vix_s is not None:
        _vix_d = vix_s.dropna()
        if len(_vix_d) >= 90:
            _vix_90d_max = float(_vix_d.iloc[-90:].max()); _vix_now_v = float(_vix_d.iloc[-1])
            _vix_peak_passed = (_vix_90d_max >= 30) and (_vix_now_v < 25)

    # RSP 6M 양수 (여름 #9 시장 폭 건강)
    _rsp_6m_pos = False
    if rsp_s is not None:
        _rsp_d = rsp_s.dropna()
        if len(_rsp_d) >= 127:
            _rsp_127 = float(_rsp_d.iloc[-127])
            if _rsp_127 != 0:
                _rsp_6m_pos = (float(_rsp_d.iloc[-1]) / _rsp_127 - 1) > 0

    # ── V3.8.1 봄 박스 보수 강화용 보조 변수 ──
    # CPI YoY 추세 — 인플레 명시적 종결 판정용 (deep 모드 OFF 시 fd 에 키 없음 → None)
    _cpi_s = fd.get("CPIAUCSL")
    cpi_yoy_now = None; cpi_yoy_3m_chg = None; cpi_yoy_below_3 = False
    if _cpi_s is not None and len(_cpi_s) >= 16:
        try:
            _cpi_now_v = float(_cpi_s.iloc[-1])
            _cpi_12m_ago = float(_cpi_s.iloc[-13])
            cpi_yoy_now = (_cpi_now_v / _cpi_12m_ago - 1) * 100
            _cpi_3m_ago = float(_cpi_s.iloc[-4])
            _cpi_15m_ago = float(_cpi_s.iloc[-16])
            cpi_yoy_3m_ago = (_cpi_3m_ago / _cpi_15m_ago - 1) * 100
            cpi_yoy_3m_chg = cpi_yoy_now - cpi_yoy_3m_ago
            cpi_yoy_below_3 = cpi_yoy_now < 3.0
        except Exception:
            pass

    # SOX 1M 변화율 — (현재는 _sox_lead 안에서만 쓰지만 후속 패치 대비 노출)
    sox_1m_chg = None
    if sox_s is not None and len(sox_s) >= 22:
        try:
            sox_1m_chg = (float(sox_s.iloc[-1]) / float(sox_s.iloc[-22]) - 1) * 100
        except Exception:
            pass

    # VIX 90D 최대값 — _vix_peak_passed 계산 시 이미 _vix_90d_max 산출했으나 if 분기 밖 노출
    vix_90d_max = None; vix_now_val = None
    if vix_s is not None:
        _vix_d2 = vix_s.dropna()
        if len(_vix_d2) >= 90:
            try:
                vix_90d_max = float(_vix_d2.iloc[-90:].max())
                vix_now_val = float(_vix_d2.iloc[-1])
            except Exception:
                pass

    # 봄 다중 트리거 카운트 (실업률 4%+ / VIX 90D max 35+ / DD -25%-)
    _spring_multi_count = sum([
        bool(un_now is not None and un_now >= 4.0),
        bool(vix_90d_max is not None and vix_90d_max >= 35),
        bool(_qqq_from_hi is not None and _qqq_from_hi < -25),
    ])

    # 9박스 체크리스트 (공통 5 + 고유 4)
    checks = {
        "봄": [
            # 공통 5박스 — V3.8.1 강화
            ("채권: 역전 정상화 진행 중",
                _inv_3m10y == "recovering"),
            ("신용: HY 정점 후 4% 진입",
                hy_peak_drop and (_hy_now_pct is not None and _hy_now_pct < 4.0)),
            ("연준: 저점권 인하 진행",
                spring_cut and ff_low_zone),
            ("실적: 나쁘다 (역실적)",
                earnings_bad),
            ("밸류: 재평가 완료",
                (fpe is not None and fpe <= 18) or (cape is not None and cape <= 25)),
            # 고유 4박스 — V3.8.1 강화
            ("실업률 4% 돌파 또는 0.5%p 진입",
                (un_now is not None and un_now >= 4.0)
                or (un_3m is not None and un_3m > 0.5)),
            ("바닥권 도달 (DD < -25%)",
                _qqq_from_hi is not None and _qqq_from_hi < -25),
            ("인플레 명시적 종결",
                cpi_yoy_3m_chg is not None
                and cpi_yoy_3m_chg < 0
                and cpi_yoy_below_3),
            ("매크로 동시 트리거 (실업/공포/조정 중 2개+)",
                _spring_multi_count >= 2),
        ],
        "여름": [
            # 공통 5
            ("채권: 3M10Y 정상",         _inv_3m10y == "normal"),
            ("신용: HY < 4%",            _hy_now_pct is not None and _hy_now_pct < 4.0),
            ("연준: 안정 유지",          ff_6m_chg is not None and abs(ff_6m_chg) < 0.5),
            ("실적: 좋고 가속",          earnings_good and earnings_accel),
            ("밸류: 정당화 가능",        (fpe_z is not None and fpe_z <= 0) or (fpe is not None and fpe < 22)),
            # 고유 4
            ("고용 강세",                 (un_3m is not None and un_3m <= 0) and (payems_chg is not None and payems_chg > 150)),
            ("소비 버팀",                 consumer_ok),
            ("반도체 동행",               _sox_lead(sox_s, spx_s, "summer_lead")),
            ("시장 폭 건강",              _rsp_6m_pos and qqq_6m_pos),
        ],
        "가을": [
            # 공통 5
            ("채권: 3M10Y 역전 진입/심화", _inv_3m10y in ("entering", "deepening", "deep_stable")),
            ("신용: HY 6M 변화 > 0",       _hy_6m_chg is not None and _hy_6m_chg > 0),
            ("연준: 고점 인하/긴축",        autumn_cut or (ff_6m_chg is not None and ff_6m_chg > 0)),
            ("실적: 아직 좋다",            earnings_good),
            ("밸류: 극단 (OR)",            (fpe is not None and fpe >= 22) or (tpe is not None and tpe >= 28) or (cape is not None and cape >= 35)),
            # 고유 4
            ("신용 디커플링",              _hy_decoup),
            ("인플레 디커플링 (WTI 충격)", _wti_shock),
            ("반도체 선행 약세",           _sox_lead(sox_s, spx_s, "early_weakness")),
            ("PER 역사적 극단",            (cape is not None and cape >= 35)),
        ],
        "겨울": [
            # 공통 5
            ("채권: 역전 정상화 진행",    _inv_3m10y == "recovering"),
            ("신용: HY > 5% (벌어진 상태)", _hy_now_pct is not None and _hy_now_pct > 5.0),
            ("연준: 진짜 인하 진행 중",   ff_3m_chg is not None and ff_3m_chg < 0),
            ("실적: 2분기 연속 감소",     earnings_2q_decline),
            ("밸류: PER 튐 (주가<실적)",  fpe_3m_chg is not None and fpe_3m_chg > 0 and earnings_declining),
            # 고유 4
            ("하락 둔화 (DD<-20% & 1M ±3%)", qqq_dd_deep and qqq_flat),
            ("실업률 폭증",                un_3m is not None and un_3m > 0.5),
            ("메가캡 의존 (RSP 약세)",     _breadth_n),
            ("반도체 선행 바닥 신호",      _sox_lead(sox_s, spx_s, "bottom_signal", qqq_s=qqq_s)),
        ],
    }
    scores = {s: sum(1 for _, v in items if v) for s, items in checks.items()}
    _SO = ["봄", "여름", "가을", "겨울"]
    # 타이브레이커: 동점 시 사이클 후순위(시장은 앞으로 간다). reversed 순회로 마지막 동점을 잡는다.
    _max_sc = max(scores.values())
    best = next(s for s in reversed(_SO) if scores[s] == _max_sc)
    bi = _SO.index(best)

    nxt_s, prv_s = _SO[(bi + 1) % 4], _SO[(bi - 1) % 4]
    # 확신도 9박스 재매핑
    _sorted = sorted(scores.values(), reverse=True)
    _diff = _sorted[0] - _sorted[1]
    _b = scores[best]
    if   _b >= 8 and _diff >= 3: conf = "매우 높음"
    elif _b >= 8:                conf = "높음"
    elif _b >= 7 and _diff >= 2: conf = "높음"
    elif _b == 7:                conf = "보통"
    elif _b == 6:                conf = "보통"
    elif _b == 5:                conf = "낮음"
    else:                        conf = "판정 불가"
    # 초/늦 접두사: 9박스 임계 ≥4
    if   scores[nxt_s] >= 4: prefix = "늦"
    elif scores[prv_s] >= 4: prefix = "초"
    else:                     prefix = ""
    return prefix + best, conf, checks, scores

# ═══ TRENDS ═══
RIB = {"10Y-2Y": False, "10Y-3M": False, "2Y-3M": True, "DXY": True, "KRW": True, "VIX": True, "HY": True,
       "SPX": False, "SOXX": False, "GOLD": True, "WTI": True, "UNRATE": True, "FEDFUNDS": True,
       "CPI": True, "PCE": True, "JOLTS": False, "NFP": False, "GDP": False, "UMCSENT": False,
       "CARD": True, "DEBT": True, "BEI": True, "WILSHIRE": True}
def _lb_offset(k):
    # "1W" → DateOffset(weeks=1), "3M"/"6M"/"1Y"/"2Q"/"5D" 등
    n = int("".join(c for c in k if c.isdigit()) or "0")
    u = "".join(c for c in k if c.isalpha()).upper()
    if n == 0: return None
    if u == "D": return pd.DateOffset(days=n)
    if u == "W": return pd.DateOffset(weeks=n)
    if u == "M": return pd.DateOffset(months=n)
    if u == "Q": return pd.DateOffset(months=3 * n)
    if u == "Y": return pd.DateOffset(years=n)
    return None
def ctrends(s, mode="pct", P=None):
    if P is None: P = {"1W": 5, "2W": 10, "1M": 21, "3M": 63, "6M": 126, "1Y": 252}
    out = {k: (None, "—") for k in P}
    if s is None or len(s) < 2: return out
    s = s.dropna()
    if len(s) < 2: return out
    cur = float(s.iloc[-1])
    last_dt = s.index[-1]
    first_dt = s.index[0]
    for lb in P:
        off = _lb_offset(lb)
        if off is None: continue
        target = last_dt - off
        if target < first_dt: continue  # 데이터 부족
        old_v = s.asof(target)
        if old_v is None or pd.isna(old_v): continue
        old = float(old_v)
        if mode == "pct":
            if old == 0: continue
            ch = (cur / old - 1) * 100
            out[lb] = (ch, f"{ch:+.1f}% {'↑' if ch > 0.1 else '↓' if ch < -0.1 else '→'}")
        elif mode == "abs":
            ch = cur - old
            out[lb] = (ch, f"{ch:+.2f} {'↑' if ch > 0.01 else '↓' if ch < -0.01 else '→'}")
        elif mode == "abs_bp":
            ch = (cur - old) * 100
            out[lb] = (ch, f"{ch:+.0f}bp {'↑' if ch > 1 else '↓' if ch < -1 else '→'}")
        elif mode == "abs_pp":
            ch = cur - old
            out[lb] = (ch, f"{ch:+.2f}pp {'↑' if ch > 0.01 else '↓' if ch < -0.01 else '→'}")
    return out

# 월간/분기 데이터용 기간
PM = {"1M": 1, "3M": 3, "6M": 6, "1Y": 12}
PQ = {"1Q": 1, "2Q": 2, "1Y": 4}
def yoy_s(s):
    """월간 시리즈 → YoY% 시리즈."""
    if s is None or len(s) < 13: return None
    return ((s / s.shift(12) - 1) * 100).dropna()
def diff_s(s):
    """레벨 시리즈 → 차분 시리즈 (NFP MoM 변화량용)."""
    if s is None or len(s) < 2: return None
    return s.diff().dropna()
def tcol(k, ch):
    if ch is None or abs(ch) < 0.01: return C["muted"]
    bad = RIB.get(k, True); return C["red"] if (ch > 0 and bad) or (ch < 0 and not bad) else C["green"]
def tbar(tr, k=None):
    if not tr: return ""
    return "<div style='margin-top:4px;line-height:1.8'>" + "".join(
        f"<span style='color:{tcol(k,ch) if k and ch is not None else C['muted']};font-size:var(--mac-fs-xs);margin-right:10px'>{p} {t}</span>"
        for p, (ch, t) in tr.items()) + "</div>"

# ═══ F1 역전 해소 변곡점 ═══
def _inv_recovery(s):
    """52주 내 peak(최저점) 대비 현재 회복도.
    return: dict {peak_bp, cur_bp, recovery_bp, recovery_pct} or None.
    peak < 0 (역전 있었음): 역전 해소 진행도. 100% = 정상화 완료, >100% = 정상화 후 추가 스프레드 형성.
    peak >= 0 (역전 없었음): 1Y 최저 스프레드 대비 현재 변화율. 카드를 비우지 않고 추세 가시화용.
    """
    if s is None or len(s) < 252: return None
    tail = s.iloc[-252:]
    peak_pp = float(tail.min())  # %p (FRED 시리즈는 %p 단위)
    cur_pp = float(s.iloc[-1])
    peak_bp = peak_pp * 100
    cur_bp = cur_pp * 100
    rec_bp = cur_bp - peak_bp
    # peak 부호 무관하게 일관 식 (peak=0 일 때 division-by-zero 방지)
    if abs(peak_bp) < 1e-9: return None
    rec_pct = (rec_bp / abs(peak_bp)) * 100
    return {"peak": round(peak_bp, 0), "cur": round(cur_bp, 0),
            "recovery_bp": round(rec_bp, 0), "recovery_pct": round(rec_pct, 0)}

# ═══ F3 FF금리 historical 위치 ═══
def _ff_position(ff_s, lookback_years=10):
    """FF금리의 N년 내 percentile (0~100). 높을수록 고점권."""
    if ff_s is None or len(ff_s) < 24: return None
    tail = ff_s.iloc[-min(len(ff_s), lookback_years * 12):]
    cur = float(ff_s.iloc[-1])
    pct = (tail < cur).sum() / len(tail) * 100
    return round(pct, 0)

# ═══ V3.8 4계절 9박스 확장 헬퍼 ═══
def _inv_state(s, lookback=252):
    """역전 상태 분류 — 52주 윈도. %p 단위, 음수 = 역전.
    Returns: 'entering' / 'deepening' / 'deep_stable' / 'recovering' / 'normal' / None."""
    if s is None: return None
    s2 = s.dropna()
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
        rec_pct = (cur - peak_pp) / abs(peak_pp) * 100
        if rec_pct < 50:
            return "deep_stable"
    if peak_pp < 0:
        rec_pct = (cur - peak_pp) / abs(peak_pp) * 100
        if rec_pct >= 50:
            return "recovering"
    if cur > 0 and peak_pp >= 0:
        return "normal"
    return None

def _hy_decoupling(hy_s, vix_s, floor_pct=3.5):
    """가을 신용 디커플링: HY 6M 벌어지는데 VIX 잠잠. 절대수준 floor 게이트."""
    if hy_s is None or vix_s is None: return False
    hy = hy_s.dropna(); vix = vix_s.dropna()
    if len(hy) < 127 or len(vix) < 1: return False
    hy_now = float(hy.iloc[-1]); hy_6m = float(hy.iloc[-127])
    vix_now = float(vix.iloc[-1])
    if hy_now <= 1.0:
        hy_now_pct = hy_now * 100; hy_6m_pct = hy_6m * 100
    else:
        hy_now_pct = hy_now; hy_6m_pct = hy_6m
    return (hy_now_pct - hy_6m_pct > 0) and (hy_now_pct > floor_pct) and (vix_now < 20)

def _wti_inflation_shock(wti_s, spx_s, wti_thresh=15.0):
    """가을 공급충격: WTI 3M > thresh%, SPX 3M < 0."""
    if wti_s is None or spx_s is None: return False
    w = wti_s.dropna(); p = spx_s.dropna()
    if len(w) < 64 or len(p) < 64: return False
    w0 = float(w.iloc[-64]); p0 = float(p.iloc[-64])
    if w0 == 0 or p0 == 0: return False
    w_chg = (float(w.iloc[-1]) / w0 - 1) * 100
    p_chg = (float(p.iloc[-1]) / p0 - 1) * 100
    return (w_chg > wti_thresh) and (p_chg < 0)

def _breadth_narrow(spy_s, rsp_s, window_days=22):
    """겨울 메가캡 의존: SPY 양 / RSP 음. RSP 부재 시 False."""
    if spy_s is None or rsp_s is None: return False
    sp = spy_s.dropna(); rs = rsp_s.dropna()
    if len(sp) <= window_days or len(rs) <= window_days: return False
    sp0 = float(sp.iloc[-window_days-1]); rs0 = float(rs.iloc[-window_days-1])
    if sp0 == 0 or rs0 == 0: return False
    sp_chg = (float(sp.iloc[-1]) / sp0 - 1) * 100
    rs_chg = (float(rs.iloc[-1]) / rs0 - 1) * 100
    return (sp_chg > 0) and (rs_chg < 0)

def _sox_lead(sox_s, spx_s, mode, qqq_s=None):
    """반도체 선행 신호.
    bottom_lead    : SOX 1M > 0 AND SOX 6M > SPX 6M
    early_weakness : SOX 3M < SPX 3M AND SPX 6M > 0
    bottom_signal  : SOX 1M > SPX 1M AND QQQ from_hi < -15 AND SOX from_hi < -20
                     (V3.8.1: 단순 아웃퍼폼 거부 — 진짜 조정장 + SOX 깊은 DD 동시 요구)
    summer_lead    : SOX 6M > SPX 6M
    """
    if sox_s is None or spx_s is None: return False
    sx = sox_s.dropna(); sp = spx_s.dropna()
    if len(sx) < 130 or len(sp) < 130: return False
    def _pct(s, n):
        if len(s) <= n: return None
        d0 = float(s.iloc[-n-1])
        if d0 == 0: return None
        return (float(s.iloc[-1]) / d0 - 1) * 100
    sx_1m, sp_1m = _pct(sx, 22),  _pct(sp, 22)
    sx_3m, sp_3m = _pct(sx, 63),  _pct(sp, 63)
    sx_6m, sp_6m = _pct(sx, 126), _pct(sp, 126)
    if mode == "bottom_lead":
        if None in (sx_1m, sx_6m, sp_6m): return False
        return (sx_1m > 0) and (sx_6m > sp_6m)
    if mode == "early_weakness":
        if None in (sx_3m, sp_3m, sp_6m): return False
        return (sx_3m < sp_3m) and (sp_6m > 0)
    if mode == "bottom_signal":
        # V3.8.1 강화: SOX 아웃퍼폼만으로는 부족 — 진짜 조정장 + SOX 깊은 DD 동시 요구
        if None in (sx_1m, sp_1m): return False
        if not (sx_1m > sp_1m): return False
        if qqq_s is None: return False  # QQQ 시리즈 미전달 시 게이트 미충족
        qq = qqq_s.dropna()
        if len(qq) < 252 or len(sx) < 252: return False
        qq_hi = float(qq.iloc[-252:].max()); qq_cur = float(qq.iloc[-1])
        sx_hi = float(sx.iloc[-252:].max()); sx_cur = float(sx.iloc[-1])
        if qq_hi == 0 or sx_hi == 0: return False
        qq_from_hi = (qq_cur / qq_hi - 1) * 100
        sx_from_hi = (sx_cur / sx_hi - 1) * 100
        return (qq_from_hi < -15) and (sx_from_hi < -20)
    if mode == "summer_lead":
        if None in (sx_6m, sp_6m): return False
        return sx_6m > sp_6m
    return False

def _fpe_zscore(fpe_now, fpe_history_list, lookback_years=10):
    """V3.8: fpe 시계열 z-score. 24개월(≈504스냅샷) 미만이면 None."""
    if fpe_now is None or not fpe_history_list: return None
    vals = []
    for h in fpe_history_list:
        v = h.get("fpe") if isinstance(h, dict) else None
        if v is not None:
            try: vals.append(float(v))
            except: pass
    if len(vals) < 504: return None
    cutoff = lookback_years * 252
    if len(vals) > cutoff: vals = vals[-cutoff:]
    try:
        import statistics
        mu = statistics.mean(vals); sd = statistics.pstdev(vals)
        if sd == 0: return None
        return (fpe_now - mu) / sd
    except Exception:
        return None

def _fpe_3m_change(fpe_history_list):
    """V3.8: 90 스냅샷 전 fpe 대비 변화량 (PE point)."""
    if not fpe_history_list: return None
    vals = [h.get("fpe") for h in fpe_history_list if isinstance(h, dict) and h.get("fpe") is not None]
    if len(vals) < 90: return None
    try:
        return float(vals[-1]) - float(vals[-90])
    except Exception:
        return None

# ═══ F6 반사성 프록시 (Forward EPS 시계열 캐시) ═══
FWD_HIST_FILE = SD / "cache" / "forward_eps_history.json"

def _load_fwd_hist():
    """히스토리 로드. 파싱 실패 시 ValueError 상위로 올려 호출자가 wipe 여부 결정."""
    if not FWD_HIST_FILE.exists(): return []
    raw = FWD_HIST_FILE.read_text("utf-8")
    return json.loads(raw) if raw.strip() else []

def _save_fwd_snapshot(date_str, fpe, feps, spx):
    """오늘 fpe/fEPS/spx를 캐시에 누적. 같은 날짜는 갱신.
    파싱 실패 시 원본을 .corrupt-<타임스탬프> 로 보존하고 새 히스토리 시작 — silent wipe 방지."""
    if feps is None and fpe and spx and fpe > 0:
        feps = round(spx / fpe, 4)
    if fpe is None and feps is None: return
    try:
        FWD_HIST_FILE.parent.mkdir(parents=True, exist_ok=True)
        try:
            hist = _load_fwd_hist()
            if not isinstance(hist, list): raise ValueError("not a list")
        except Exception as _e:
            # 손상된 파일을 백업으로 보존 → 새 히스토리로 덮어쓰기 전에 증거 남김
            try:
                _stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                _bak = FWD_HIST_FILE.with_name(f"{FWD_HIST_FILE.stem}.corrupt-{_stamp}.json")
                if FWD_HIST_FILE.exists(): FWD_HIST_FILE.rename(_bak)
            except Exception: pass
            hist = []
        rec = {"date": date_str, "fpe": fpe, "feps": feps, "spx": spx}
        idx = next((i for i, h in enumerate(hist) if isinstance(h, dict) and h.get("date") == date_str), None)
        if idx is not None: hist[idx] = rec
        else: hist.append(rec)
        hist = hist[-365:]  # 최근 1년만 유지
        FWD_HIST_FILE.write_text(json.dumps(hist, ensure_ascii=False, indent=2), "utf-8")
    except Exception as _err:
        try: st.sidebar.caption(f"⚠️ Forward EPS 누적 실패: {type(_err).__name__}")
        except: pass

def _reflexivity(hist, days=30):
    """N일(또는 max available) 전 fEPS 대비 변화율 + spx 변화율 + 동조도."""
    valid = [h for h in hist if h.get("feps") is not None]
    if len(valid) < 5: return None
    cur = valid[-1].get("feps")
    if cur is None: return None
    target_idx = max(0, len(valid) - days - 1)
    old = valid[target_idx].get("feps")
    if old is None or old == 0: return None
    n_days = len(valid) - target_idx
    eps_chg = (cur / old - 1) * 100
    # SPX 변화율 (있으면)
    spx_chg = None
    cur_spx = valid[-1].get("spx"); old_spx = valid[target_idx].get("spx")
    if cur_spx and old_spx and old_spx > 0:
        spx_chg = (cur_spx / old_spx - 1) * 100
    return {"cur": cur, "old": old, "n": n_days,
            "eps_chg": round(eps_chg, 2),
            "spx_chg": round(spx_chg, 2) if spx_chg is not None else None}

# ═══ F2 금리 인하 사이클 단계 분해 ═══
def _cut_cycle(ff_s):
    """현재 활성 인하 사이클 감지 + 단계 분류.
    알고리즘:
      - 최근 36개월 내 max를 사이클 peak로 간주
      - peak - cur ≥ 25bp 면 활성 사이클
      - 단계는 경과 개월 수로 분류
    Returns: dict 또는 None.
    """
    if ff_s is None or len(ff_s) < 12: return None
    s = ff_s.dropna()
    if len(s) < 12: return None
    cur = float(s.iloc[-1])
    lookback = min(len(s), 36)
    tail = s.iloc[-lookback:]
    peak_val = float(tail.max())
    peak_idx = tail.idxmax()
    cum_cut_bp = (peak_val - cur) * 100  # FF금리는 %
    if cum_cut_bp < 25:
        return {"active": False, "stage": "비활성", "months": 0, "cum_cut_bp": 0,
                "peak": peak_val, "cur": cur, "start_date": None}
    # 경과 개월 (peak → 현재)
    months = (s.index[-1].year - peak_idx.year) * 12 + (s.index[-1].month - peak_idx.month)
    if months <= 3:    stage = "초입"
    elif months <= 9:  stage = "중반"
    elif months <= 15: stage = "후반"
    else:              stage = "장기화"
    return {"active": True, "stage": stage, "months": months,
            "cum_cut_bp": round(cum_cut_bp, 0), "peak": round(peak_val, 2),
            "cur": round(cur, 2), "start_date": str(peak_idx)[:10]}

# ═══ F5 가속도 (1차 미분의 변화: 1M chg vs 3M chg pace) ═══
# 윈도우 변경: 2026-04, 전환점 해상도 상향 목적
# 3M/6M(63/126거래일)은 심리/가격 고주파 데이터에서 한 달 이상 신호 지연 → 1M/3M(21/63거래일)로 단축

# 지표별 pace~0 threshold (2026-04 산출: 최근 3년 1M 절대 변화량 중앙값 기반 지표별 튜닝)
# 백테스트 기반 trade-off 최적화:
# - median 대비 균일한 비율이 아니라 지표 특성별로 차등 적용
# - VIX/DXY (고변동): 높은 threshold 로 극단 ratio clip 억제
# - T10Y2Y (저변동): 낮은 threshold 로 판정 불능 회복 (단 c3=0 구조적 한계 존재)
#
# 검증 (2026-04, 최근 180 거래일):
#   - T10Y2Y 판정 불능: 19.4% → 5.0% (남은 9일은 c3=0 flat 구간, 구조적 한계)
#   - VIX clip 발동: 30일 → 8일
#   - 종합 판정 전환점 리드: median 14일 유지
# 재튜닝 시 별도 테스트 스크립트로 median 재산출 + 윈도우 검증.
PACE_THRESHOLDS = {
    "VIX":     0.30,     # median 2.375 × 12.6% — clip 발동 30→8일 (목표 <10 ✓)
    "HY":      0.01,     # median 0.20 × 5%
    "T10Y2Y":  0.0001,   # median 0.08 × 0.13% — 판정 불능 19.4%→5.0%
    "DXY":     0.10,     # median 1.39% × 7.2%
    "SOX/SPX": 0.15,     # median 3.83% × 3.9%
}
PACE_THRESHOLD_DEFAULT = 0.01  # 지표명 매칭 실패 시 폴백
RATIO_CLIP = 10.0              # |ratio| > 10 은 ±10 으로 클립, label 에 '*' + tooltip

def _accel(s, mode="pct", indicator=None):
    """추세의 가속/감속. ratio = (1M 변화) / (3M 변화의 1/3).
    분모 /3: "최근 3M 평균에서 기대되는 1M 속도" vs "실제 1M 속도" 비교.
    ratio > 1.3 = 가속, 0.7~1.3 = 유지, < 0.7 = 감속.
    |raw_ratio| > 10 은 ±10 으로 클립 + label 에 '*' 마킹 (분모 불안정 시각화).
    return: dict {c1, c3, ratio, raw_ratio, clipped, label} or None.
    """
    if s is None or len(s) < 70: return None
    s2 = s.dropna()
    if len(s2) < 2: return None
    n = float(s2.iloc[-1])
    def _ago(months):
        target = s2.index[-1] - pd.DateOffset(months=months)
        if target < s2.index[0]: return None
        v = s2.asof(target)
        return float(v) if v is not None and not pd.isna(v) else None
    o1 = _ago(1); o3 = _ago(3)
    if o1 is None or o3 is None: return None
    if mode == "pct":
        if o1 == 0 or o3 == 0: return None
        c1 = (n / o1 - 1) * 100
        c3 = (n / o3 - 1) * 100
    else:
        c1 = n - o1
        c3 = n - o3
    pace = c3 / 3
    pace_th = PACE_THRESHOLDS.get(indicator, PACE_THRESHOLD_DEFAULT)
    if abs(pace) < pace_th:
        return {"c1": round(c1, 2), "c3": round(c3, 2), "ratio": None,
                "raw_ratio": None, "clipped": False, "label": "→"}
    raw_ratio = c1 / pace
    if abs(raw_ratio) > RATIO_CLIP:
        display_ratio = RATIO_CLIP if raw_ratio > 0 else -RATIO_CLIP
        clipped = True
    else:
        display_ratio = raw_ratio
        clipped = False
    if display_ratio > 1.3:   lbl = "↑↑"   # 가속
    elif display_ratio > 0.7: lbl = "→"    # 유지
    elif display_ratio > -0.3: lbl = "↓"   # 감속
    else: lbl = "↓↓"                        # 강한 감속/반전
    if clipped: lbl = lbl + "*"
    return {
        "c1": round(c1, 2),
        "c3": round(c3, 2),
        "ratio": round(display_ratio, 2),
        "raw_ratio": round(raw_ratio, 2),
        "clipped": clipped,
        "label": lbl,
    }


# ═══ ΔΔ (2차 도함수) ═══

_DD_STEADY  = 1.5
_DD_RAPID   = 5.0

_DD_RAW_THRESHOLDS = {
    "T10Y2Y": {"steady": 5.0,   "rapid": 15.0,  "mode": "abs_bp"},
    "VIX":    {"steady": 1.0,   "rapid": 3.0,   "mode": "abs"},
    "HY":     {"steady": 8.0,   "rapid": 25.0,  "mode": "abs_bp"},
    "UNEMP":  {"steady": 0.03,  "rapid": 0.08,  "mode": "abs"},
    "CPI":    {"steady": 0.05,  "rapid": 0.15,  "mode": "abs"},
}

def _dd_label_1st(delta):
    if delta > 5:  return "빠른 개선 ↑↑"
    if delta > 0:  return "개선 중 ↑"
    if delta > -5: return "악화 중 ↓"
    return "빠른 악화 ↓↓"

def _dd_label_2nd(dd, steady=_DD_STEADY, rapid=_DD_RAPID):
    if dd > rapid:    return "급가속 ⏫"
    if dd > steady:   return "가속 ↗"
    if dd >= -steady: return "등속 ➡"
    if dd >= -rapid:  return "감속 ↘"
    return "급감속 ⏬"

def _dd_label_2nd_raw(dd, thresholds):
    """원본 지표용 ΔΔ 라벨 (개별 임계값)."""
    return _dd_label_2nd(dd, steady=thresholds["steady"], rapid=thresholds["rapid"])

def compute_delta_delta(series, window=7, lag=30):
    """Δ(1차 도함수)와 ΔΔ(2차 도함수). lag*2 일치 필요."""
    # pd.Series 입력
    if isinstance(series, pd.Series):
        s = series.dropna()
        if len(s) < lag * 2: return None
        today = s.index[-1]
        def _mavg(anchor, w):
            sl = s[(s.index >= anchor - pd.Timedelta(days=w)) & (s.index <= anchor)]
            return float(sl.mean()) if len(sl) >= 1 else None
        r0 = _mavg(today, window)
        r1 = _mavg(today - pd.Timedelta(days=lag), window)
        r2 = _mavg(today - pd.Timedelta(days=lag * 2), window)
        if r0 is None or r1 is None or r2 is None: return None
        delta = r0 - r1; dd = delta - (r1 - r2)
        return {"delta": round(delta, 4), "delta_delta": round(dd, 4),
                "delta_label": _dd_label_1st(delta),
                "delta_delta_label": _dd_label_2nd(dd)}
    # list 입력 (observations/mac_history)
    if isinstance(series, list):
        if series and isinstance(series[0], dict):
            # V3.6 버전 가드: 마지막 lag*2 = 60일 윈도우에 score_version 2개 이상 섞이면 None
            # (공식 개편 구간에서 Δ/ΔΔ 는 오염됨)
            recent = series[-lag * 2:] if len(series) >= lag * 2 else series
            _versions = {h.get("score_version") for h in recent if h.get("score_version")}
            if len(_versions) >= 2: return None
            vals = []
            for h in series:
                v = None
                for k in ("score", "value", "val"):
                    if k in h and h[k] is not None: v = h[k]; break
                vals.append(v)
        else:
            vals = list(series)
        vals = [v for v in vals if v is not None]
        if len(vals) < lag * 2: return None
        def _avg(arr, end, w):
            start = max(0, end - w + 1)
            sl = arr[start:end + 1]
            return sum(sl) / len(sl) if sl else None
        n = len(vals)
        r0 = _avg(vals, n - 1, window)
        r1 = _avg(vals, n - 1 - lag, window)
        r2 = _avg(vals, n - 1 - lag * 2, window)
        if r0 is None or r1 is None or r2 is None: return None
        delta = r0 - r1; dd = delta - (r1 - r2)
        return {"delta": round(delta, 4), "delta_delta": round(dd, 4),
                "delta_label": _dd_label_1st(delta),
                "delta_delta_label": _dd_label_2nd(dd)}
    return None


# ── ΔΔ 코멘트 dict 5개 (빈 값 — 텍스트는 별도 세션에서 작성) ──

_DD_MATRIX_COMMENT = {
    "all_accel":   "다섯 영역이 전부 가속하고 있다. 계절이 바뀌는 중이다. 이 방향에 올라타라.",
    "all_decel":   "다섯 영역이 전부 감속하고 있다. 추세가 끝나가고 있다. 다음 방향을 준비해라.",
    "angstblute":  "겉은 개선이다. 속은 꺾이고 있다. Angstblüte — 공포의 꽃. 가장 아름다울 때가 가장 위험하다.",
    "broad_accel": "대부분이 가속 중이다. 추세가 힘을 받고 있다. 아직 전환점이 아니다.",
    "broad_decel": "대부분이 감속 중이다. 추세의 끝자락이다. 방향이 바뀌기 직전일 수 있다.",
    "mixed":       "영역마다 속도가 다르다. 전환기이거나 노이즈다. 다음 주에 다시 봐라.",
}

_DD_CLUSTER_COMMENT = {
    # 채권/금리
    "bond_rapid_accel": "채권이 빠르게 좋아지고 있다. 형이 달리기 시작했다. 동생은 아직 모른다.",
    "bond_accel":       "금리 환경 개선이 빨라지고 있다. 연준이 방향을 틀었거나 틀 준비를 하고 있다.",
    "bond_steady":      "채권 쪽은 등속이다. 방향은 유지되고 있으나 가속도는 없다. 다른 클러스터를 봐라.",
    "bond_decel":       "금리 환경 개선이 느려지고 있다. 형이 멈칫하고 있다. 왜 멈칫하는지를 봐라.",
    "bond_rapid_decel": "채권이 빠르게 나빠지고 있다. 금리가 다시 올라가거나 스프레드가 벌어지고 있다. 형이 걱정하기 시작했다.",
    # 밸류에이션
    "val_rapid_accel": "밸류에이션이 빠르게 싸지고 있다. 주가가 빠지고 있다는 뜻이다. 기회가 오고 있다.",
    "val_accel":       "밸류에이션이 싸지는 방향으로 가속 중이다. 아직 충분히 싸진 않을 수 있다. 기다려라.",
    "val_steady":      "밸류에이션은 등속이다. 비싸면 비싼 채로, 싸면 싼 채로 유지 중이다.",
    "val_decel":       "밸류에이션 개선이 느려지고 있다. 주가 하락이 둔화되고 있거나 다시 비싸지기 시작했다.",
    "val_rapid_decel": "밸류에이션이 빠르게 비싸지고 있다. 주가가 실적 없이 올라가고 있다. 반사성이다.",
    # 스트레스
    "stress_rapid_accel": "공포가 빠르게 커지고 있다. VIX가 뛰고 HY가 벌어지고 있다. 역사적으로 여기서 산 사람이 이겼다.",
    "stress_accel":       "스트레스가 가속 중이다. 시장이 현실을 인식하기 시작했다.",
    "stress_steady":      "스트레스는 등속이다. 공포가 있으면 있는 대로, 없으면 없는 대로 유지 중이다.",
    "stress_decel":       "공포가 줄어드는 속도가 느려지고 있다. 아직 불안이 남아 있다.",
    "stress_rapid_decel": "공포가 빠르게 빠지고 있다. 시장이 안심하고 있다. 안심할 때가 위험하다.",
    # 실물
    "real_rapid_accel": "실물 지표가 빠르게 나빠지고 있다. 침체 진입 속도가 빠르다. 역실적장세에선 매수가 정석이다.",
    "real_accel":       "실물이 나빠지는 방향으로 가속 중이다. 실업률과 GDP를 같이 봐라.",
    "real_steady":      "실물은 등속이다. 좋으면 좋은 대로, 나쁘면 나쁜 대로 관성이 작동하고 있다.",
    "real_decel":       "실물 악화가 둔화되고 있다. 바닥을 다지고 있을 수 있다.",
    "real_rapid_decel": "실물이 빠르게 좋아지고 있다. 고용이 살아나고 있다. 여름이 오고 있다.",
    # 반도체
    "semi_rapid_accel": "반도체가 빠르게 좋아지고 있다. 선행지표가 달리기 시작했다. 봄이 왔다.",
    "semi_accel":       "반도체 환경이 가속 개선 중이다. 사이클 바닥을 지났을 가능성이 높다.",
    "semi_steady":      "반도체는 등속이다. 방향은 유지되나 힘이 추가로 붙지는 않고 있다.",
    "semi_decel":       "반도체 개선이 느려지고 있다. 사이클 고점이 가까울 수 있다. 달을 봐라.",
    "semi_rapid_decel": "반도체가 빠르게 나빠지고 있다. 반도체가 시장에 선행한다. 겨울이 오고 있다.",
}

_DD_SCORE_COMMENT = {
    "rapid_accel": "환경이 빠르게 좋아지고 있다. 속도가 속도를 만들고 있다. 계절 전환 구간이다.",
    "accel":       "매수 환경이 가속 개선 중이다. 방향이 맞고 힘이 붙고 있다.",
    "steady":      "변화의 변화가 없다. 현 추세가 관성으로 가고 있다. 기다려라.",
    "decel":       "개선 또는 악화의 속도가 줄고 있다. 추세가 꺾이기 직전일 수 있다. 속도가 줄면 방향이 바뀐다.",
    "rapid_decel": "환경이 빠르게 나빠지고 있다. 속도가 빠르면 바닥도 빨리 온다.",
}

_DD_RAW_COMMENT = {
    # 10Y-2Y 스프레드
    "spread_rapid_accel": "역전 해소가 가속 중이다. 형이 안심하기 시작했다. 봄이 가까워진다.",
    "spread_accel":       "금리차가 벌어지는 속도가 빨라지고 있다. 정상화가 진행 중이다.",
    "spread_steady":      "금리차 변화에 가속도가 없다. 현 상태가 관성으로 유지 중이다.",
    "spread_decel":       "금리차 변화가 느려지고 있다. 역전 해소가 주춤하거나 역전 심화가 둔화 중이다.",
    "spread_rapid_decel": "역전이 빠르게 심화되고 있다. 형이 심하게 걱정하고 있다. 침체 신호가 강해지고 있다.",
    # VIX
    "vix_rapid_accel": "공포가 공포를 먹고 있다. VIX가 가속 상승 중이다. 패닉이다.",
    "vix_accel":       "공포가 빨라지고 있다. 아직 패닉은 아닌데 방향이 나쁘다.",
    "vix_steady":      "VIX 변화에 가속도가 없다. 조용하다. 폭풍 전의 고요일 수 있다.",
    "vix_decel":       "공포가 소화되고 있다. VIX 상승이 둔화 중이거나 하락이 느려지고 있다.",
    "vix_rapid_decel": "VIX가 빠르게 빠지고 있다. 시장이 안심하고 있다. 낙관이 극에 달하면 경계해라.",
    # 하이일드 스프레드
    "hy_rapid_accel": "하이일드가 빠르게 벌어지고 있다. 신용경색 진입이다. 잠수함에서 물이 터지고 있다.",
    "hy_accel":       "스프레드 확대가 빨라지고 있다. 채권시장이 위험을 인식하기 시작했다.",
    "hy_steady":      "스프레드 변화에 가속도가 없다. 좁으면 좁은 대로 자만이고, 넓으면 넓은 대로 긴장이다.",
    "hy_decel":       "스프레드 변화가 느려지고 있다. 확대가 둔화 중이면 최악은 지났을 수 있다.",
    "hy_rapid_decel": "스프레드가 빠르게 좁아지고 있다. 신용시장이 안심하고 있다. 좁을 때가 위험하다는 걸 잊지 마라.",
    # 실업률
    "unemp_rapid_accel": "실업률이 가속 상승 중이다. 삼의 법칙 발동 근처다. 침체가 시작됐을 수 있다.",
    "unemp_accel":       "실업률 상승이 빨라지고 있다. 미세 가속이 선행 신호다. 여기서부터 사기 시작하면 된다.",
    "unemp_steady":      "실업률 변화에 가속도가 없다. 올라가고 있으면 올라가는 대로, 안정이면 안정 대로.",
    "unemp_decel":       "실업률 상승이 둔화 중이다. 고용 시장이 바닥을 잡고 있을 수 있다.",
    "unemp_rapid_decel": "실업률이 빠르게 안정되고 있다. 고용이 살아나고 있다. 여름의 신호다.",
    # CPI YoY
    "cpi_rapid_accel": "인플레가 가속 둔화 중이다. 연준이 편해지고 있다. 봄이 가까워진다.",
    "cpi_accel":       "디스인플레가 빨라지고 있다. 방향이 맞다. 연준이 인하할 명분이 쌓이고 있다.",
    "cpi_steady":      "물가 변화에 가속도가 없다. 내려가고 있으면 내려가는 속도 그대로다.",
    "cpi_decel":       "디스인플레가 느려지고 있다. 인플레 재점화 경고다. 연준이 멈출 수 있다.",
    "cpi_rapid_decel": "디스인플레가 멈추거나 반전 중이다. 물가가 다시 올라가고 있다. 연준이 못 내린다.",
}

_VAL_DUAL_COMMENT = {
    # ── Shiller CAPE ──
    "cape_red_red":       "100년으로 봐도 비싸고 20년으로 봐도 비싸다. 중력을 거스르고 있다. 140년간 예외 없었다.",
    "cape_red_neutral":   "100년 중력은 작동 중이다. 근데 직근 20년 구조가 바뀌었다. 빅테크 마진율, 자사주 매입. 타이밍에 여유가 있을 수 있다.",
    "cape_red_green":     "100년 기준으로는 비싸다. 20년 기준으로는 싸다. 구조적 변화를 믿느냐 역사적 중력을 믿느냐의 문제다.",
    "cape_neutral_red":   "100년으로는 보통인데 20년으로는 비싸다. 최근 평균 대비 고평가다. 직근 사이클에서 거품이 끼어 있다.",
    "cape_neutral_neutral": "양쪽 다 중립이다. 밸류에이션이 매매 근거가 안 되는 구간이다. 다른 지표를 봐라.",
    "cape_neutral_green": "100년으로 보통, 20년으로 싸다. 최근 사이클 대비 할인 구간에 진입 중이다.",
    "cape_green_red":     "100년 역사로는 싸다. 근데 20년으로는 비싸다. 이 조합은 드물다. 데이터를 다시 확인해라.",
    "cape_green_neutral": "100년으로 싸다. 20년으로 보통이다. 장기 투자자에게는 기회다.",
    "cape_green_green":   "양쪽 다 싸다. 역사적 매수 구간이다. 여기서 산 사람은 전부 이겼다.",
    # ── Forward PE ──
    "fpe_red_red":        "예상 실적 기준으로도 비싸다. 양쪽 레인지 모두. 실적이 기대를 못 채우면 이 숫자가 폭탄이 된다.",
    "fpe_red_neutral":    "100년 기준 비싸다. 20년으로는 보통이다. 직근 사이클의 고 마진 구조가 반영된 거다. 지속 가능한지를 봐라.",
    "fpe_red_green":      "100년으로 비싸지만 20년으로 싸다. 실적 성장이 가격을 정당화하고 있을 수 있다. 성장 속도가 유지되는지를 봐라.",
    "fpe_neutral_red":    "100년 보통인데 20년 비싸다. 최근 사이클 대비 고평가다.",
    "fpe_neutral_neutral": "양쪽 다 중립이다. Forward PE가 매매 근거가 안 되는 구간이다.",
    "fpe_neutral_green":  "100년 보통, 20년 싸다. 실적 대비 주가가 적정하거나 낮다.",
    "fpe_green_red":      "100년으로 싸다. 20년으로 비싸다. 드문 조합이다. 실적 추정치가 급변한 건 아닌지 확인해라.",
    "fpe_green_neutral":  "100년으로 싸고 20년으로 보통이다. 합리적 가격 영역이다.",
    "fpe_green_green":    "양쪽 다 싸다. 실적 대비 주가가 역사적 저점에 가깝다. 사라.",
    # ── Trailing PE ──
    "tpe_red_red":        "과거 실적 기준으로 양쪽 다 비싸다. 터지기 전 PER은 28이었다. 60, 70은 터진 뒤의 PER이다.",
    "tpe_red_neutral":    "100년으로 비싸다. 20년으로 보통이다. 빅테크 시대의 구조적 PER 상향을 반영한 거다. 구조가 유지되면 버틴다. 안 되면 무너진다.",
    "tpe_red_green":      "100년 비싸고 20년 싸다. 실적이 빠르게 올라왔다는 뜻이다. 속도가 유지되는지를 봐라. 속도가 꺾이면 이 숫자가 뒤집힌다.",
    "tpe_neutral_red":    "100년 보통인데 20년 비싸다. 직근 사이클 대비 고평가다. 실적이 꺾이면 여기서부터 빠진다.",
    "tpe_neutral_neutral": "양쪽 다 중립이다. Trailing PE가 매매 근거가 안 되는 구간이다.",
    "tpe_neutral_green":  "100년 보통, 20년 싸다. 실적 대비 주가가 낮다. 역실적장세 초입이거나 공포 할인이다.",
    "tpe_green_red":      "100년으로 싸다. 20년으로 비싸다. 이 조합은 거의 안 나온다. 데이터 확인해라.",
    "tpe_green_neutral":  "100년으로 싸다. 20년으로 보통이다. 장기 매수 구간에 가깝다.",
    "tpe_green_green":    "양쪽 다 싸다. 역사적으로 여기서 사면 10년 뒤에 웃는다.",
}

# ── 밸류에이션 이중 레인지 경계 ──
VAL_RANGES = {
    "shiller_cape": {
        "full":   {"low": 15, "mid": 25, "high": 30},
        "recent": {"low": 22, "mid": 30, "high": 35},
    },
    "forward_per": {
        "full":   {"low": 14, "mid": 18, "high": 22},
        "recent": {"low": 17, "mid": 20, "high": 24},
    },
    "trailing_per": {
        "full":   {"low": 15, "mid": 20, "high": 25},
        "recent": {"low": 18, "mid": 22, "high": 28},
    },
}

def _val_dual_level(value, rng):
    """value → 'green' / 'neutral' / 'red'"""
    if value is None: return None
    if value < rng["mid"]: return "green"
    if value < rng["high"]: return "neutral"
    return "red"

_TIP_DD_MATRIX = (
    "<b>2차 도함수 매트릭스</b>\n"
    "속도의 속도다. Δ가 방향이면 ΔΔ는 그 방향의 힘이 빨라지는지 느려지는지를 본다.\n"
    "\"주가는 위치가 아닌 속도에 반응한다.\" 그 속도가 가속하는지 감속하는지가 전환점이다.\n\n"
    "<b>읽는 법</b>\n"
    "• ΔΔ 전부 같은 방향 → 계절이 확정된 거다. 고민 필요 없다.\n"
    "• ΔΔ 엇갈림 → 전환기다. 돈 벌 때이기도 하고 잃을 때이기도 하다.\n"
    "• Δ↑ 인데 ΔΔ↘ 인 클러스터 ≥3 → Angstblüte. 겉은 좋아지는데 속은 꺾이고 있다. 가장 위험한 국면.\n\n"
    "<b>임계값</b>\n"
    "클러스터/스코어: ±1.5 등속 / ±5.0 급변\n"
    "원본 지표: 개별 단위별 (카드 ⓘ 참조)")


# ═══ SCORE ═══
def _nm(v, bp):
    if v is None or np.isnan(v): return None
    if v <= bp[0][0]: return bp[0][1]
    if v >= bp[-1][0]: return bp[-1][1]
    for i in range(len(bp) - 1):
        v0, s0 = bp[i]; v1, s1 = bp[i + 1]
        if v0 <= v <= v1: return s0 + (v - v0) / (v1 - v0) * (s1 - s0)
    return bp[-1][1]
def mac_sc(d):
    sc = {}
    sc["t"] = _nm(d.get("t10y2y", 0) * 100 if d.get("t10y2y") is not None else None, [(-80, 10), (-40, 8), (0, 5), (100, 2), (200, 0)])
    # V3.6 신규: 10Y-3M 스프레드 (거시가 가장 신뢰하는 침체 예측). t와 동일 패턴 (역전 심화 = 고득점).
    sc["tm"] = _nm(d.get("t10y3m", 0) * 100 if d.get("t10y3m") is not None else None, [(-80, 10), (-40, 8), (0, 5), (100, 2), (200, 0)])
    sc["f"] = _nm(d.get("ff6m"), [(-1.5, 10), (-0.5, 8), (0, 4), (0.5, 2), (1.5, 0)]) if d.get("ff6m") is not None else None
    sc["r"] = _nm(d.get("rr"), [(-1, 0), (0, 2), (1, 5), (2, 8), (2.5, 10)])
    sc["p"] = _nm(d.get("fpe"), [(14, 10), (17, 7), (20, 4), (23, 2), (25, 0)])
    sc["c"] = _nm(d.get("cape"), [(15, 8), (20, 6), (28, 3), (35, 1), (38, 0)])
    hy = d.get("hy"); sc["h"] = _nm(hy * 100 if hy is not None else None, [(300, 0), (400, 2), (500, 4), (600, 5), (700, 7)])
    sc["sd"] = _nm(d.get("sdd"), [(-40, 10), (-25, 7), (-15, 4), (-5, 1), (0, 0)])
    sc["sr"] = _nm(d.get("sr3m"), [(-15, 5), (-5, 3), (0, 2), (5, 1), (10, 0)])
    sc["v"] = _nm(d.get("vix"), [(12, 0), (18, 2), (25, 4), (35, 7), (45, 8)])
    sc["g"] = _nm(d.get("fg"), [(10, 7), (25, 5), (50, 3), (75, 1), (90, 0)])
    sc["u"] = _nm(d.get("u3m"), [(-0.3, 0), (0, 3), (0.2, 5), (0.5, 7), (1.0, 8)])
    sc["gd"] = _nm(d.get("gdp"), [(-2, 7), (0, 5), (1, 4), (2.5, 2), (4, 0)])
    # NCBEILQ027S 기준 캘리 (Wilshire 대비 체계적으로 ~30% 높음)
    sc["bf"] = _nm(d.get("buffett"), [(130, 10), (160, 7), (190, 4), (220, 2), (250, 0)])
    # V3.3 CFNAI MA3 — 시카고 연준 -0.70 침체룰 직결. 0 = 추세 성장.
    sc["cf"] = _nm(d.get("cf"), [(-1.5, 0), (-0.7, 2), (-0.3, 4), (0, 7), (0.3, 10)])
    # V3.6 가중치: 채권/금리 클러스터 내 t+tm 분리 (10Y-2Y 기본, 10Y-3M 침체 예측 핵심)
    W = {
        "t":   7,   # F1 10Y-2Y 스프레드 — 장단기역전 기본
        "tm":  5,   # F1 10Y-3M 스프레드 — "거시가 가장 신뢰하는 침체 예측" (t에서 3 이관)
        "f":  10,   # F6 FF금리 6M 변화
        "r":  10,   # F6 실질금리 (파월 선호 프레이밍)
        "p":  10,   # F4 Forward PE
        "c":   8,   # F4 Shiller CAPE
        "h":   7,   # 하이일드 스프레드
        "sd": 10,   # 반도체 52주 DD
        "sr":  5,   # 반도체 3M 상대강도
        "v":   8,   # VIX
        "g":   7,   # Fear & Greed
        "u":   8,   # 실업률 3M 변화
        "gd":  7,   # GDP
        "bf":  7,   # 버핏 지표
        "cf":  9,   # CFNAI MA3
    }
    tw = 0; ts = 0
    for k, w in W.items():
        s = sc.get(k)
        if s is not None: ts += s * w / 10; tw += w
    total = round(ts / tw * 100, 1) if tw > 0 else None
    detail = {}
    for k, w in W.items():
        s = sc.get(k)
        if s is not None:
            detail[k] = {"raw": round(s, 2), "weight": w, "contrib": round(s * w / 10, 2)}
    return total, detail

# 5클러스터 서브스코어
_CLUSTERS = {
    "채권/금리": ["t", "tm", "f", "r"],   # V3.6: tm (10Y-3M) 추가
    "밸류에이션": ["p", "c", "bf"],
    "스트레스": ["h", "v", "g"],
    "실물": ["u", "gd", "cf"],
    "반도체": ["sd", "sr"],
}
def mac_clusters(detail):
    """gs_detail → {클러스터명: {score: 0~100, drivers: {지표: detail}}}"""
    if not detail: return {}
    # V3.6: mac_sc()의 W와 동기화 (t: 7, tm: 5 신규). 변경 시 양쪽 같이 수정.
    W = {"t": 7, "tm": 5, "f": 10, "r": 10, "p": 10, "c": 8, "h": 7, "sd": 10, "sr": 5, "v": 8, "g": 7, "u": 8, "gd": 7, "bf": 7, "cf": 9}
    _LABELS = {"t":"2Y10Y","tm":"3M10Y","f":"FF금리6M변화","r":"실질금리","p":"Forward PE","c":"CAPE",
               "h":"HY스프레드","sd":"SOX고점대비","sr":"SOX상대3M","v":"VIX","g":"Fear&Greed","u":"실업률3M변화","gd":"GDP","bf":"버핏지표","cf":"CFNAI MA3"}
    out = {}
    for cname, keys in _CLUSTERS.items():
        tw = 0; ts = 0; drivers = {}
        for k in keys:
            d = detail.get(k)
            if d is not None:
                ts += d["raw"] * W[k] / 10; tw += W[k]
                drivers[_LABELS.get(k, k)] = d
        out[cname] = {"score": round(ts / tw * 100, 1) if tw > 0 else None, "drivers": drivers}
    return out

# ═══ V3.4 클러스터 디커플링 자동 코멘트 ═══
# 5클러스터 점수 차이가 ≥30이면 top/bot 조합으로 룰 매칭. <30이면 일관 코멘트.
# 텍스트는 V3.4 지시문 그대로. 절대 수정 금지.
_CL_DECOUPLE_NORMAL = {
    ("채권/금리","밸류에이션"): "채권은 봄을 말하는데 주가는 아직 가을 가격이다.\n형이 먼저 걱정을 풀었다. 동생이 따라올 때까지 기다려라.",
    ("채권/금리","반도체"):     "형이 먼저 움직이고 있다. 동생은 아직이다.\n역사적으로 채권이 먼저 뾰족해지고 반도체가 마지막에 따라온다.\n기다려라.",
    ("채권/금리","스트레스"):   "금리는 완화인데 시장은 아직 무섭다.\n채권시장이 맞다. 공포가 풀리는 건 시간문제다.",
    ("채권/금리","실물"):       "금리가 돌고 있는데 실물이 안 따라온다.\n금리인하가 실물에 스며들려면 6개월은 걸린다. 기다려라.",
    ("스트레스","밸류에이션"):  "공포는 커졌는데 주가는 안 빠졌다.\n둘 중 하나가 틀렸다. 주가가 틀렸을 확률이 높다.",
    ("스트레스","반도체"):      "시장은 무서워하는데 반도체는 안 빠졌다.\n아직 바닥이 아니다. 반도체가 박살나야 진짜 바닥이다.",
    ("스트레스","채권/금리"):   "VIX가 뛰는데 채권이 안 움직인다.\n공포가 과한 거다. 채권이 형이다. 형이 태평하면 괜찮다.",
    ("실물","밸류에이션"):      "경기는 바닥을 다지는데 주가는 아직 비싸다.\n빠져야 살 수 있다. 인내가 필요하다.",
    ("실물","반도체"):          "실물이 돌기 시작했는데 반도체가 안 따라온다.\n이례적이다. 반도체 실적을 확인해라.",
    ("반도체","실물"):          "반도체가 먼저 돈다. 실물은 아직이다.\n반도체 산업의 주가는 시장에 선행한다. 이게 정상이다.",
    ("반도체","밸류에이션"):    "반도체는 싸졌는데 시장 전체는 아직 비싸다.\n반도체부터 사는 구간이다.",
    ("반도체","채권/금리"):     "반도체가 살아나는데 금리가 아직 높다.\n금리가 꺾이면 반도체가 폭발한다. 아직 참아라.",
    ("밸류에이션","채권/금리"): "주가가 싸 보이는데 채권이 경고한다.\n채권이 형이다. 형 말 들어라. 형이 안심할 때까지 사지 마라.",
    ("밸류에이션","반도체"):    "시장은 싸졌는데 반도체는 아직이다.\n반도체가 바닥 찍으면 진짜 바닥이다. 기다려라.",
    ("밸류에이션","스트레스"):  "싸졌는데 무섭지도 않다.\n바닥은 공포 속에서 온다. 아직 공포가 부족하다.",
    ("밸류에이션","실물"):      "싸졌는데 경기가 아직 안 좋다.\n역실적장세 초입일 수 있다. 여기서부터 천천히 사라.",
}
_CL_DECOUPLE_EASY = {
    ("채권/금리","밸류에이션"): "금리 쪽은 좋아지는데 주가는 아직 비싸다.",
    ("채권/금리","반도체"):     "금리가 먼저 움직이고 반도체는 아직이다. 순서대로 온다.",
    ("채권/금리","스트레스"):   "금리는 괜찮은데 시장이 아직 무서워한다.",
    ("채권/금리","실물"):       "금리가 좋아지는데 경기는 아직 안 따라왔다.",
    ("스트레스","밸류에이션"):  "시장은 무서워하는데 주가가 안 빠졌다. 이상하다.",
    ("스트레스","반도체"):      "무서운데 반도체가 안 빠졌다. 아직 바닥이 아니다.",
    ("스트레스","채권/금리"):   "무서워하는데 금리는 괜찮다. 공포가 과한 거다.",
    ("실물","밸류에이션"):      "경기는 돌아오는데 아직 비싸다.",
    ("실물","반도체"):          "경기는 도는데 반도체가 안 따라온다.",
    ("반도체","실물"):          "반도체가 먼저 좋아지고 있다. 경기는 나중에 따라온다.",
    ("반도체","밸류에이션"):    "반도체는 싸졌는데 전체 시장은 아직 비싸다.",
    ("반도체","채권/금리"):     "반도체는 좋은데 금리가 아직 높다.",
    ("밸류에이션","채권/금리"): "싸 보이는데 금리가 경고한다. 조심해라.",
    ("밸류에이션","반도체"):    "시장은 싸졌는데 반도체는 아직이다.",
    ("밸류에이션","스트레스"):  "싸졌는데 공포가 없다. 진짜 바닥은 더 무서울 때 온다.",
    ("밸류에이션","실물"):      "싸졌는데 경기가 아직 나쁘다.",
}
def cluster_decouple_comment(clusters, divergence, mode="일반"):
    """5클러스터 점수 → 디커플링 코멘트. UI 카드용."""
    if not clusters or divergence is None:
        return ""
    if mode == "쉬운":
        if divergence < 30: return "다섯 영역이 비슷한 신호다. 읽기 쉬운 장이다."
        items = [(n, v["score"]) for n, v in clusters.items() if v.get("score") is not None]
        if len(items) < 2: return ""
        top = max(items, key=lambda x: x[1])[0]
        bot = min(items, key=lambda x: x[1])[0]
        return _CL_DECOUPLE_EASY.get((top, bot), "영역마다 신호가 다르다. 지켜보는 게 좋다.")
    # 일반/병신
    if divergence < 30: return "5개 영역이 같은 말을 하고 있다. 방향이 명확하다."
    items = [(n, v["score"]) for n, v in clusters.items() if v.get("score") is not None]
    if len(items) < 2: return ""
    top = max(items, key=lambda x: x[1])[0]
    bot = min(items, key=lambda x: x[1])[0]
    return _CL_DECOUPLE_NORMAL.get((top, bot), "클러스터 간 괴리가 크다.\n시장이 소화 못한 정보가 많다. 성급하게 판단하지 마라.")

# ═══ V3.4 거시 스코어 속도 (Δ30D) ═══
MAC_HIST_FILE = SD / "cache" / "mac_score_history.json"
MAC_HIST_V35_FILE = SD / "cache" / "mac_score_history.v35.json"  # 3.6 개편 전 3.5 시절 기록 보존용
_MIGRATION_RAN = False
def _migrate_v35_history_once():
    """3.6 공식 개편 대비 1회성 마이그레이션.
    mac_score_history.json 이 존재하고 v35 파일이 아직 없으면, 기존을 v35 로 rename.
    새 파일은 3.6 부터 빈 상태로 시작 → score_version 오염 방지."""
    global _MIGRATION_RAN
    if _MIGRATION_RAN: return
    _MIGRATION_RAN = True
    try:
        if MAC_HIST_FILE.exists() and not MAC_HIST_V35_FILE.exists():
            # 기존 파일의 엔트리가 모두 score_version 없거나 3.5 인 경우만 rename
            try:
                _raw = MAC_HIST_FILE.read_text("utf-8")
                _parsed = json.loads(_raw) if _raw.strip() else []
                if isinstance(_parsed, list):
                    _has_v36 = any(
                        isinstance(h, dict) and h.get("score_version") == "3.6"
                        for h in _parsed
                    )
                    if not _has_v36:
                        # 안전: rename (copy 가 아니라 move — 오염 방지)
                        MAC_HIST_FILE.rename(MAC_HIST_V35_FILE)
            except Exception:
                pass
    except Exception:
        pass

def mac_score_history_append(gs, mk=None, clusters=None, divergence=None, season=None):
    """오늘 스냅샷을 ~/.meerkat/cache/mac_score_history.json에 append (같은 날짜는 덮어쓰기).
    V3.5: gs 외에 mk(미어캣), 5클러스터 점수, 괴리도, 사계절도 같이 저장.
    V3.6: score_version 필드 추가 (공식 개편으로 인한 버전 섞임 방지).
    옛 엔트리({date, score}만 있는 것)는 그대로 호환."""
    if gs is None: return
    _migrate_v35_history_once()  # 3.5 데이터 분리 (1회성)
    try:
        MAC_HIST_FILE.parent.mkdir(parents=True, exist_ok=True)
        hist = []
        if MAC_HIST_FILE.exists():
            _raw = MAC_HIST_FILE.read_text("utf-8")
            try:
                _parsed = json.loads(_raw) if _raw.strip() else []
                if isinstance(_parsed, list): hist = _parsed
                else: raise ValueError("not a list")
            except Exception:
                # 손상 파일은 덮어쓰지 않고 백업으로 보존 → silent wipe 방지
                try:
                    _stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    _bak = MAC_HIST_FILE.with_name(f"{MAC_HIST_FILE.stem}.corrupt-{_stamp}.json")
                    MAC_HIST_FILE.rename(_bak)
                except Exception: pass
                hist = []
        today = _date.today().isoformat()
        hist = [h for h in hist if h.get("date") != today]
        _row = {"date": today, "score": float(gs), "score_version": VERSION}
        if mk is not None:
            try: _row["mk"] = float(mk)
            except: pass
        if clusters:
            _cl = {}
            for k, v in clusters.items():
                _s = v.get("score") if isinstance(v, dict) else None
                if _s is not None:
                    try: _cl[k] = float(_s)
                    except: pass
            if _cl: _row["clusters"] = _cl
        if divergence is not None:
            try: _row["divergence"] = float(divergence)
            except: pass
        if season:
            _row["season"] = str(season)
        hist.append(_row)
        hist.sort(key=lambda h: h["date"])
        MAC_HIST_FILE.write_text(json.dumps(hist, ensure_ascii=False), "utf-8")
    except: pass

def mac_score_velocity():
    """최근 7일 평균 - 30일 전 7일 평균. 30일치 미만이면 None.
    V3.6: 윈도우(지난 37일) 내 score_version 이 2개 이상 섞여있으면 None.
    공식이 바뀐 구간에 baseline/현재 비교는 오염된 속도값이 나옴 → 표시 차단."""
    try:
        if not MAC_HIST_FILE.exists(): return None
        hist = json.loads(MAC_HIST_FILE.read_text("utf-8"))
        if len(hist) < 30: return None
        hist.sort(key=lambda h: h["date"])
        today = _date.today()
        window_rows = [h for h in hist if (today - _date.fromisoformat(h["date"])).days <= 37]
        # 버전 가드: 윈도우 내 score_version 고유값이 2개 이상이면 오염
        _versions = {h.get("score_version") for h in window_rows if h.get("score_version")}
        if len(_versions) >= 2: return None
        recent = [h["score"] for h in hist if (today - _date.fromisoformat(h["date"])).days <= 7]
        older  = [h["score"] for h in hist if 23 <= (today - _date.fromisoformat(h["date"])).days <= 37]
        if not recent or not older: return None
        return round(sum(recent)/len(recent) - sum(older)/len(older), 2)
    except: return None

def mac_history_load():
    """mac_score_history.json을 정렬된 list[dict]로 로드. 옛 엔트리도 그대로 통과."""
    try:
        if not MAC_HIST_FILE.exists(): return []
        hist = json.loads(MAC_HIST_FILE.read_text("utf-8"))
        if not isinstance(hist, list): return []
        hist = [h for h in hist if isinstance(h, dict) and h.get("date")]
        hist.sort(key=lambda h: h["date"])
        return hist
    except: return []

def mac_history_v35_load():
    """V3.5 시절 기록 (공식 개편 전). 시계열 탭에서 회색 점선으로 병기."""
    try:
        if not MAC_HIST_V35_FILE.exists(): return []
        hist = json.loads(MAC_HIST_V35_FILE.read_text("utf-8"))
        if not isinstance(hist, list): return []
        hist = [h for h in hist if isinstance(h, dict) and h.get("date")]
        hist.sort(key=lambda h: h["date"])
        return hist
    except: return []

# ═══ V3.7 관측 히스토리 (raw backfill + observation log) ═══
# raw.jsonl         : FRED + yfinance 가격 시리즈 max history (wide JSONL, 시리즈 dense)
# observations.jsonl: 가공 지표 + 백필 불가 원본의 앱 방문 스냅샷 (append sparse)
# backfill_done.json: 시리즈별 1회 백필 완료 마커
HIST_DIR = SD / "history"
RAW_JSONL = HIST_DIR / "raw.jsonl"
OBS_JSONL = HIST_DIR / "observations.jsonl"
HIST_MARKER = HIST_DIR / "backfill_done.json"

# 백필 가능 FRED 시리즈 (FRED series ID 기준)
RAW_FRED_IDS = [
    "DGS2", "DGS10", "T10Y3M", "T10Y2Y", "VIXCLS", "FEDFUNDS", "UNRATE",
    "PAYEMS", "JTSJOL", "UNEMPLOY", "CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE",
    "DCOILWTICO", "DEXKOUS", "DRCCLACBS", "GDP", "UMCSENT", "T5YIE",
    "NCBEILQ027S", "GFDEGDQ188S", "CFNAI", "BAMLH0A0HYM2",
]
# 백필 가능 yfinance 티커
RAW_YF_TICKERS = [
    "DX-Y.NYB", "SOXX", "^GSPC", "GC=F", "QQQ", "XLE", "XLK",
    "^VIX", "CL=F", "KRW=X", "TQQQ", "SOXL", "VOO", "SGOV",
    "SPY",  # V3.10.5: 배당수익률 시계열 합성용 (SPY dividends + price)
]
# fd 내부 키 → raw 저장 키 (FRED series ID)
FD_TO_RAW = {
    "DGS2":"DGS2","DGS10":"DGS10","T10Y2Y":"T10Y2Y","T10Y3M":"T10Y3M",
    "VIXCLS":"VIXCLS","FEDFUNDS":"FEDFUNDS","UNRATE":"UNRATE","PAYEMS":"PAYEMS",
    "JTSJOL":"JTSJOL","UNEMPLOY":"UNEMPLOY","CPIAUCSL":"CPIAUCSL","CPILFESL":"CPILFESL","PCEPI":"PCEPI",
    "PCEPILFE":"PCEPILFE","WTI":"DCOILWTICO","KRW":"DEXKOUS","DRCCLACBS":"DRCCLACBS",
    "GDP_NOMINAL":"GDP","UMCSENT":"UMCSENT","T5YIE":"T5YIE",
    "WILSHIRE":"NCBEILQ027S","GDP":"GDP","CFNAI":"CFNAI","HY":"BAMLH0A0HYM2",
}
# yd 내부 키 → raw 저장 키 (yfinance 티커)
YD_TO_RAW = {
    "DXY":"DX-Y.NYB","SOXX":"SOXX","SPX":"^GSPC","GOLD":"GC=F","QQQ":"QQQ",
    "XLE":"XLE","XLK":"XLK","VIX_YF":"^VIX","WTI_YF":"CL=F","KRW_YF":"KRW=X",
    "TQQQ":"TQQQ","SOXL":"SOXL","VOO":"VOO","SGOV":"SGOV",
}

def _hist_dir():
    try: HIST_DIR.mkdir(parents=True, exist_ok=True)
    except: pass

def _hist_load_marker():
    if not HIST_MARKER.exists(): return {"fred": {}, "yf": {}}
    try:
        m = json.loads(HIST_MARKER.read_text("utf-8"))
        if not isinstance(m, dict): return {"fred": {}, "yf": {}}
        m.setdefault("fred", {}); m.setdefault("yf", {})
        return m
    except: return {"fred": {}, "yf": {}}

def _hist_save_marker(m):
    _hist_dir()
    try: HIST_MARKER.write_text(json.dumps(m, ensure_ascii=False, indent=2), "utf-8")
    except: pass

def _hist_load_raw_wide():
    """raw.jsonl → {date_str: {series: value}}"""
    if not RAW_JSONL.exists(): return {}
    out = {}
    try:
        with RAW_JSONL.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    row = json.loads(line)
                    d = row.get("date")
                    if not d: continue
                    vals = {k: v for k, v in row.items() if k != "date" and v is not None}
                    if d in out: out[d].update(vals)
                    else: out[d] = vals
                except: continue
    except: pass
    return out

def _hist_save_raw_wide(wide):
    """{date_str: {series: value}} → raw.jsonl (전체 덮어쓰기, 날짜 정렬)"""
    _hist_dir()
    try:
        tmp = RAW_JSONL.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for d in sorted(wide.keys()):
                row = {"date": d}
                row.update(wide[d])
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp.replace(RAW_JSONL)  # atomic
    except: pass

def _hist_merge_series(wide, series_key, pd_series, tail=None):
    """pd.Series → wide 에 merge. tail=None 이면 전체, 숫자면 최근 N개."""
    if pd_series is None or len(pd_series) == 0: return
    try:
        s = pd_series.tail(tail) if tail else pd_series
        for idx, val in s.items():
            try:
                d = str(idx.date()) if hasattr(idx, "date") else str(idx)[:10]
                vf = float(val)
                if vf != vf: continue  # NaN 거르기
            except: continue
            wide.setdefault(d, {})[series_key] = vf
    except: pass

def _hist_backfill_once(api_key, force_fred=None, force_yf=None):
    """첫 실행 시 1회 백필. force_fred/force_yf = None 이면 마커 미완료만, list 면 해당만 강제.
    반환: (fred_ok, fred_fail, yf_ok, yf_fail, elapsed_sec)."""
    import time as _tm
    _t0 = _tm.perf_counter()
    _hist_dir()
    marker = _hist_load_marker()
    wide = _hist_load_raw_wide()
    f_ok = f_fail = y_ok = y_fail = 0
    # FRED
    if api_key:
        fred_todo = force_fred if force_fred is not None else [s for s in RAW_FRED_IDS if not marker["fred"].get(s)]
        for sid in fred_todo:
            try:
                ser = ffred(sid, api_key, start="1950-01-01")
                if ser is not None and len(ser) > 0:
                    _hist_merge_series(wide, sid, ser, tail=None)
                    marker["fred"][sid] = True
                    f_ok += 1
                else:
                    f_fail += 1
            except Exception:
                f_fail += 1
    # yfinance
    yf_todo = force_yf if force_yf is not None else [t for t in RAW_YF_TICKERS if not marker["yf"].get(t)]
    if yf_todo:
        try:
            import yfinance as yf
            for tk in yf_todo:
                try:
                    h = yf.Ticker(tk).history(period="max", auto_adjust=True)
                    if h is None or h.empty: y_fail += 1; continue
                    ser = h["Close"].dropna()
                    if ser.index.tz is not None:
                        ser.index = ser.index.tz_localize(None)
                    _hist_merge_series(wide, tk, ser, tail=None)
                    marker["yf"][tk] = True
                    y_ok += 1
                except Exception:
                    y_fail += 1
        except Exception:
            y_fail += len(yf_todo)
    # V3.10.5: SPY 배당수익률 시계열 합성 (SPY price + SPY dividends → trailing 12M yield).
    # API 추가 키 0. 1993~ (SPY 출시) 일별 시계열 약 8000+ 포인트.
    try:
        import yfinance as _yf_div
        _spy_t = _yf_div.Ticker("SPY")
        _divs = _spy_t.dividends
        if _divs is not None and len(_divs) > 0:
            if _divs.index.tz is not None:
                _divs.index = _divs.index.tz_localize(None)
            _spy_prc = wide.get("SPY")
            if _spy_prc is None or len(_spy_prc) == 0:
                _h = _spy_t.history(period="max", auto_adjust=True)
                if _h is not None and not _h.empty:
                    _spy_prc = _h["Close"].dropna()
                    if _spy_prc.index.tz is not None:
                        _spy_prc.index = _spy_prc.index.tz_localize(None)
            if _spy_prc is not None and len(_spy_prc) > 0:
                _ttm = _divs.rolling(window="365D").sum()
                _ttm_d = _ttm.reindex(_spy_prc.index, method="ffill")
                _div_yld = (_ttm_d / _spy_prc) * 100
                _div_yld = _div_yld.dropna()
                if len(_div_yld) > 0:
                    _hist_merge_series(wide, "DIVIDEND_YIELD", _div_yld, tail=None)
                    y_ok += 1
    except Exception:
        pass
    _hist_save_raw_wide(wide)
    _hist_save_marker(marker)
    return (f_ok, f_fail, y_ok, y_fail, _tm.perf_counter() - _t0)

def _hist_update_raw_latest(fd, yd, tail_days=90):
    """매 방문 시 fd/yd 의 최근 N일을 raw.jsonl 에 merge. 백필 없이도 점진 축적 가능."""
    _hist_dir()
    wide = _hist_load_raw_wide()
    for fd_k, raw_k in FD_TO_RAW.items():
        _hist_merge_series(wide, raw_k, fd.get(fd_k), tail=tail_days)
    for yd_k, raw_k in YD_TO_RAW.items():
        _hist_merge_series(wide, raw_k, yd.get(yd_k), tail=tail_days)
    _hist_save_raw_wide(wide)

@st.cache_data(ttl=120, show_spinner=False)
def _hist_load_raw_df():
    """raw.jsonl → pandas DataFrame (index=datetime, columns=시리즈)."""
    wide = _hist_load_raw_wide()
    if not wide: return None
    try:
        df = pd.DataFrame.from_dict(wide, orient="index")
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[df.index.notna()].sort_index()
        return df
    except: return None

def _hist_append_observation(row):
    """한 행 append. 앱 방문 1회 = 1 행. 실패는 상위로 전파 → 호출자가 UI에 노출."""
    if not row: return
    _hist_dir()
    with OBS_JSONL.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

@st.cache_data(ttl=60, show_spinner=False)
def _hist_load_obs_df():
    """observations.jsonl → pandas DataFrame (ts 인덱스).
    하루 여러 번 실행으로 같은 date에 여러 행이 있을 수 있음 → date 기준 최신 ts만 남김."""
    if not OBS_JSONL.exists(): return None
    try:
        rows = []
        with OBS_JSONL.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try: rows.append(json.loads(line))
                except: continue
        if not rows: return None
        df = pd.DataFrame(rows)
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
            df = df[df["ts"].notna()].sort_values("ts").reset_index(drop=True)
        # 같은 date 중복 제거 — 최신 ts만 유지 (ΔΔ 계산 안정성)
        if "date" in df.columns:
            df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        return df
    except: return None


def mac_velocity_label(v, mode="일반"):
    """velocity → (label, color, comment)"""
    if v is None:
        if mode == "쉬운": return ("축적 중", "muted", "아직 데이터가 부족하다.")
        return ("축적 중", "muted", "30일치 데이터가 쌓여야 속도를 말할 수 있다.")
    if mode == "쉬운":
        if v >  5: return ("빠른 개선 ↑↑", "green", "점수가 빠르게 올라가는 중.")
        if v >  0: return ("개선 중 ↑",   "green", "점수가 올라가고 있다.")
        if v > -5: return ("악화 중 ↓",   "gold",  "점수가 내려가고 있다.")
        return ("빠른 악화 ↓↓", "red", "점수가 빠르게 내려가는 중.")
    if v >  5: return ("빠른 개선 ↑↑", "green", "환경이 빠르게 좋아지고 있다.")
    if v >  0: return ("개선 중 ↑",   "green", "환경이 나아지고 있다. 방향이 맞다.")
    if v > -5: return ("악화 중 ↓",   "gold",  "환경이 나빠지고 있다. 아직 급하진 않다.")
    return ("빠른 악화 ↓↓", "red", "환경이 빠르게 나빠지고 있다. 속도가 빠르면 바닥도 빨리 온다.")

# ═══ V3.9.1 역사 매칭 — ERA_LIBRARY 거리 매칭 (1929~2018, 32개) ═══
# 10차원 enum 상태 → 32개 era 와 가중 일치도 계산. THRESHOLD 미달 시 fallthrough.
# 코퍼스 직접 인용 22개 + 매크로 사이클 보강 10개. 2020년대 era 전부 제외.

def _build_current_state(season, ff_pos, val_score, semi_dir,
                         wti_3m, hy_now, hy_6m_chg, inv_state,
                         dxy_now, cpi_yoy_now, cpi_yoy_3m_chg,
                         ff_3m_chg, ff_6m_chg):
    """현재 매크로 상태 → 10차원 enum dict."""
    ffp = None
    if ff_pos == "저점권":   ffp = "low"
    elif ff_pos == "중립권": ffp = "mid"
    elif ff_pos == "고점권": ffp = "high"

    if ff_3m_chg is None:    ffa = None
    elif ff_3m_chg < -0.25:  ffa = "cutting"
    elif ff_3m_chg > 0.25:   ffa = "hiking"
    else:                     ffa = "hold"

    if cpi_yoy_now is None or cpi_yoy_3m_chg is None:
        infl = "unknown"
    elif cpi_yoy_3m_chg > 0.3:
        infl = "accelerating"
    elif cpi_yoy_now >= 4.0 and cpi_yoy_3m_chg <= 0:
        infl = "peaking"
    elif cpi_yoy_3m_chg < -0.2:
        infl = "cooling"
    elif abs(cpi_yoy_3m_chg) <= 0.2 and cpi_yoy_now < 3.5:
        infl = "stable"
    else:
        infl = "unknown"

    if val_score is None: val = None
    elif val_score < 20:  val = "extreme"
    elif val_score < 40:  val = "high"
    elif val_score < 70:  val = "normal"
    else:                  val = "low"

    _hy_pct = None
    if hy_now is not None:
        _hy_pct = hy_now * 100 if hy_now <= 1.0 else hy_now
    if _hy_pct is None:
        cred = "normal"
    elif _hy_pct < 3.5:
        cred = "tight"
    elif _hy_pct < 5.0 and (hy_6m_chg is None or hy_6m_chg <= 0):
        cred = "normal"
    elif hy_6m_chg is not None and hy_6m_chg > 0 and _hy_pct < 7:
        cred = "widening"
    elif _hy_pct >= 7:
        cred = "panic"
    else:
        cred = "normal"

    if inv_state in ("entering", "deepening", "deep_stable"):
        yc = "inverted"
    elif inv_state == "recovering":
        yc = "recovering"
    elif inv_state == "normal":
        yc = "normal"
    else:
        yc = None

    semi = semi_dir if semi_dir in ("up", "down", "flat") else None

    if dxy_now is None:    doll = "neutral"
    elif dxy_now < 95:     doll = "weak"
    elif dxy_now < 102:    doll = "neutral"
    else:                   doll = "strong"

    if wti_3m is not None and wti_3m > 50:
        sh = "geopolitical"
    elif wti_3m is not None and wti_3m > 30:
        sh = "oil"
    elif hy_6m_chg is not None and hy_6m_chg > 1.5:
        sh = "financial"
    else:
        sh = "none"

    return {
        "season":           season,
        "ff_pos":           ffp,
        "ff_action":        ffa,
        "inflation_trend":  infl,
        "valuation":        val,
        "credit":           cred,
        "yield_curve":      yc,
        "semiconductor":    semi,
        "dollar":           doll,
        "external_shock":   sh,
    }


# ── ERA_LIBRARY 32개 (1929~2018, 코퍼스 22 + 사이클 보강 10) ──
ERA_LIBRARY = [
    # ===== 1920~30년대 =====
    {
        "id": "1929_광기",
        "label": "1929 대공황 직전 광기",
        "season": "여름", "ff_pos": "low", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "extreme",
        "credit": "tight", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "strong", "external_shock": "none",
        "comment": "거품의 마지막 여름. 빚으로 산 마지막 광기.\n실적이 따라온다는 건 거품 마지막 단계에서도 사실이다.",
        "quote": "거품 정점에선 매매를 멈춰라. 욕심이 잉태한즉 죄를 낳는다.",
        "aftermath": "1929~32 다우 -89%. 회복에 25년 걸렸다.",
        "era_type": "period",
        "historical_duration_days": 365,
        "hist_start": "1928-06-01",
        "hist_end": "1929-09-03",
        "hist_days": 459,
    },
    {
        "id": "1932_바닥",
        "label": "1932 대공황 종결",
        "season": "봄", "ff_pos": "low", "ff_action": "hold",
        "inflation_trend": "cooling", "valuation": "low",
        "credit": "panic", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "weak", "external_shock": "none",
        "comment": "다우 -89% 후 바닥. 모두가 절망했을 때.\n역실적장세에선 매수가 정석이다.",
        "quote": "악재가 다 나왔다. 그러니 지금부턴 사고 호재를 기다리자.",
        "aftermath": "1932~37 다우 4배. 5년 강세장 시작.",
        "era_type": "moment",
        "historical_duration_days": 14,
        "hist_start": "1932-06-01",
        "hist_end": "1932-09-30",
        "hist_days": 122,
    },
    {
        "id": "1937_더블딥",
        "label": "1937 정책실수 더블딥",
        "season": "봄", "ff_pos": "mid", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "normal",
        "credit": "widening", "yield_curve": "normal",
        "semiconductor": "flat", "dollar": "strong", "external_shock": "financial",
        "comment": "32년 바닥 후 회복하던 경제가 37년 재차 폭락.\n연준의 너무 이른 긴축이 만든 두 번째 침체다.",
        "quote": "긴축은 너희들이 주식을 사고 싶지 않을 때까지 계속하는 거다. 멈추면 안 된다.",
        "aftermath": "1937~38 다우 -49%. 침체 18개월.",
        "era_type": "period",
        "historical_duration_days": 240,
        "hist_start": "1937-03-01",
        "hist_end": "1938-04-30",
        "hist_days": 425,
    },
    # ===== 1970년대 =====
    {
        "id": "1971_인플레_출발",
        "label": "1971 스태그플레이션 진입",
        "season": "가을", "ff_pos": "mid", "ff_action": "hiking",
        "inflation_trend": "accelerating", "valuation": "high",
        "credit": "normal", "yield_curve": "normal",
        "semiconductor": "flat", "dollar": "weak", "external_shock": "none",
        "comment": "닉슨 쇼크 직후. 달러 신뢰가 무너지고 인플레가 출발.\n70년대 스태그플레이션의 시작.",
        "quote": "환전해서 원화 챙겨라. 통화 신뢰가 흔들릴 땐 미국 주식 사지마라.",
        "aftermath": "73~74년 1차 오일쇼크로 진짜 침체 진입.",
        "era_type": "period",
        "historical_duration_days": 720,
        "hist_start": "1971-08-15",
        "hist_end": "1973-01-11",
        "hist_days": 515,
    },
    {
        "id": "1973_오일쇼크",
        "label": "1973 1차 오일쇼크",
        "season": "가을", "ff_pos": "high", "ff_action": "hiking",
        "inflation_trend": "accelerating", "valuation": "normal",
        "credit": "widening", "yield_curve": "inverted",
        "semiconductor": "down", "dollar": "weak", "external_shock": "geopolitical",
        "comment": "공급충격 가을. 욤키푸르 전쟁과 OPEC 금수조치.\n유가가 인플레를 밀고 인플레가 시장을 민다.",
        "quote": "유가→인플레→연준→시장. 8차선 도로를 무단횡단하는 것과 같다.",
        "aftermath": "73~74년 다우 -45%. 21개월 베어마켓.",
        "era_type": "event",
        "historical_duration_days": 90,
        "hist_start": "1973-10-17",
        "hist_end": "1974-12-06",
        "hist_days": 415,
    },
    {
        "id": "1974_10월_바닥",
        "label": "1974 10월 오일쇼크 종결",
        "season": "겨울", "ff_pos": "high", "ff_action": "cutting",
        "inflation_trend": "peaking", "valuation": "low",
        "credit": "widening", "yield_curve": "recovering",
        "semiconductor": "down", "dollar": "weak", "external_shock": "geopolitical",
        "comment": "오일쇼크 + 닉슨 사임 + 워터게이트. 다우 -45% 후 바닥.\n인플레 정점과 주가 저점은 같이 온다.",
        "quote": "장단기금리역전의 고점은 인플레의 고점 근처이고 주가의 저점 근처다.",
        "aftermath": "74~76년 다우 75% 회복. 그러나 70년대 박스권은 끝나지 않았다.",
        "era_type": "moment",
        "historical_duration_days": 14,
        "hist_start": "1974-10-01",
        "hist_end": "1975-04-30",
        "hist_days": 211,
    },
    {
        "id": "1979_이란혁명",
        "label": "1979 이란혁명 + 볼커 임명",
        "season": "가을", "ff_pos": "high", "ff_action": "hiking",
        "inflation_trend": "accelerating", "valuation": "normal",
        "credit": "widening", "yield_curve": "inverted",
        "semiconductor": "flat", "dollar": "weak", "external_shock": "geopolitical",
        "comment": "이란 혁명 + 볼커 등판. 인플레 잡기 위한 진짜 긴축이 시작.\n주가가 더 빠지진 않으나 80년 시스템 위기가 진짜 충격이었다.",
        "quote": "인플레는 초반에 잡아야 한다. 늦으면 더 큰 대가를 치른다.",
        "aftermath": "80~82년 볼커 긴축 + 깊은 침체로 이어졌다.",
        "era_type": "event",
        "historical_duration_days": 180,
        "hist_start": "1979-01-01",
        "hist_end": "1980-01-31",
        "hist_days": 395,
    },
    # ===== 1980년대 =====
    {
        "id": "1980_볼커1차",
        "label": "1980 볼커 1차 (특수 국면)",
        "season": "가을", "ff_pos": "high", "ff_action": "hiking",
        "inflation_trend": "peaking", "valuation": "low",
        "credit": "normal", "yield_curve": "inverted",
        "semiconductor": "flat", "dollar": "strong", "external_shock": "none",
        "comment": "1980년은 일반적인 가을 아니다. 브레튼우즈 붕괴 후\n달러 신뢰 회복이 핵심 미션. 다우는 그 기간 올랐다.",
        "quote": "1980년 볼커는 6개월간 금리를 20퍼센트까지 올렸지만 다우와 나스닥은 그 기간동안 올랐다.",
        "aftermath": "81년 멕시코 외환위기 터지면서 그 이후 주가 폭락했다.",
        "era_type": "event",
        "historical_duration_days": 120,
        "hist_start": "1980-01-01",
        "hist_end": "1980-09-30",
        "hist_days": 273,
    },
    {
        "id": "1981_82_침체",
        "label": "1981~82 의도된 깊은 침체",
        "season": "겨울", "ff_pos": "high", "ff_action": "hold",
        "inflation_trend": "cooling", "valuation": "low",
        "credit": "widening", "yield_curve": "inverted",
        "semiconductor": "down", "dollar": "strong", "external_shock": "none",
        "comment": "볼커가 인플레 잡기 위해 만든 의도된 침체. 실업률 11퍼센트 넘었다.\n그러나 그 끝에 82년 봄이 있었다.",
        "quote": "인위적인 디플레를 만들어 해결하지 않으면 경제는 사망에 이르게 된다.",
        "aftermath": "82년 8월 봄 진입. 10개월간 나스닥 2배 됐다.",
        "era_type": "period",
        "historical_duration_days": 390,
        "hist_start": "1981-07-01",
        "hist_end": "1982-08-12",
        "hist_days": 407,
    },
    {
        "id": "1982_봄",
        "label": "1982 8월 ~ 1983 6월 봄",
        "season": "봄", "ff_pos": "high", "ff_action": "cutting",
        "inflation_trend": "cooling", "valuation": "low",
        "credit": "widening", "yield_curve": "recovering",
        "semiconductor": "up", "dollar": "strong", "external_shock": "none",
        "comment": "82년 8월부터 83년 6월까지 10개월간 나스닥 2배.\n장단기금리역전의 고점은 인플레 고점 근처이고 주가의 저점 근처다.",
        "quote": "떨어지면 사도 된다는 뜻이다.",
        "aftermath": "82~87년 5년 강세장. 다우 3배 됐다.",
        "era_type": "period",
        "historical_duration_days": 1825,
        "hist_start": "1982-08-12",
        "hist_end": "1983-06-30",
        "hist_days": 322,
    },
    {
        "id": "1985_플라자",
        "label": "1985 플라자 합의",
        "season": "여름", "ff_pos": "mid", "ff_action": "hold",
        "inflation_trend": "stable", "valuation": "normal",
        "credit": "normal", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "strong", "external_shock": "financial",
        "comment": "강달러 종료의 정치적 결정. 환율 정책이 주가를 만든다.\n달러 약세 전환되면 미국 외 자산이 강해진다.",
        "quote": "환율의 비정상이 정상으로 돌아갈 때 자본은 움직인다.",
        "aftermath": "86~87년 약달러 + 신흥국 자산 강세 이어졌다.",
        "era_type": "event",
        "historical_duration_days": 180,
        "hist_start": "1985-09-22",
        "hist_end": "1987-08-25",
        "hist_days": 703,
    },
    {
        "id": "1987_블랙먼데이",
        "label": "1987 10월 블랙먼데이",
        "season": "가을", "ff_pos": "high", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "extreme",
        "credit": "tight", "yield_curve": "flattening",
        "semiconductor": "up", "dollar": "strong", "external_shock": "none",
        "comment": "강달러 + 고밸류 + 금리 인상. 1987년 10월 단일 일자 하락.\n정확한 트리거는 없었다. 거품이 스스로 무너졌다.",
        "quote": "연극은 끝이 있고 거품은 꺼지며 불균형은 균형으로 간다.",
        "aftermath": "단일 일자 -22%. 그러나 1년 내 회복하고 90년대 강세장으로 갔다.",
        "era_type": "event",
        "historical_duration_days": 90,
        "hist_start": "1987-08-25",
        "hist_end": "1987-11-30",
        "hist_days": 97,
    },
    {
        "id": "1989_일본버블",
        "label": "1989 일본 버블 정점",
        "season": "여름", "ff_pos": "high", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "extreme",
        "credit": "tight", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "neutral", "external_shock": "none",
        "comment": "1989년 12월 닛케이 38915. 그 후 30년 회복 못함.\n위대한 기업이라 해서 주가가 항상 오르는 거 아니다.",
        "quote": "위대한 기업의 주가도 박살나는 걸 침체라 한다.",
        "aftermath": "닛케이 30년이 넘도록 전고점 회복 못 했다. 잃어버린 30년이다.",
        "era_type": "period",
        "historical_duration_days": 365,
        "hist_start": "1988-01-01",
        "hist_end": "1989-12-29",
        "hist_days": 728,
    },
    # ===== 1990년대 =====
    {
        "id": "1990_걸프전",
        "label": "1990 걸프전",
        "season": "가을", "ff_pos": "mid", "ff_action": "cutting",
        "inflation_trend": "stable", "valuation": "normal",
        "credit": "widening", "yield_curve": "flattening",
        "semiconductor": "down", "dollar": "neutral", "external_shock": "geopolitical",
        "comment": "이라크의 쿠웨이트 침공. 유가 3개월 60퍼센트 급등.\n지정학 충격이 침체로 이어진 케이스.",
        "quote": "전쟁 이슈는 모든 것을 덮는다. 끝나면 진실이 드러난다.",
        "aftermath": "90년 7월~91년 3월 공식 침체로 들어갔다.",
        "era_type": "event",
        "historical_duration_days": 180,
        "hist_start": "1990-08-02",
        "hist_end": "1991-02-28",
        "hist_days": 210,
    },
    {
        "id": "1990_91_침체",
        "label": "1990~91 미국 침체",
        "season": "겨울", "ff_pos": "high", "ff_action": "cutting",
        "inflation_trend": "peaking", "valuation": "normal",
        "credit": "widening", "yield_curve": "recovering",
        "semiconductor": "down", "dollar": "neutral", "external_shock": "financial",
        "comment": "걸프전 + 저축대부조합 위기. 90년 7월부터 91년 3월까지 공식 침체.\n지정학 충격이 신용 위기로 옮긴 결말이다.",
        "quote": "악재가 다 나오면 그때부터 사면 된다.",
        "aftermath": "91~94년 회복기. 그린스펀이 점진 인하했다.",
        "era_type": "period",
        "historical_duration_days": 240,
        "hist_start": "1990-07-01",
        "hist_end": "1991-03-31",
        "hist_days": 273,
    },
    {
        "id": "1994_연착륙",
        "label": "1994 그린스펀 연착륙",
        "season": "봄", "ff_pos": "mid", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "normal",
        "credit": "tight", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "neutral", "external_shock": "none",
        "comment": "금리 올려도 경기가 버틴다. 횡보가 길어도 방향은 위다.",
        "quote": "연착륙이면 당연히 주가는 올라야 한다. 실적이 예상만큼 안빠질 것이니까.",
        "aftermath": "95~99년 5년 강세장. 결국 닷컴 광기로 진입했다.",
        "era_type": "period",
        "historical_duration_days": 365,
        "hist_start": "1994-02-01",
        "hist_end": "1995-02-28",
        "hist_days": 392,
    },
    {
        "id": "1995_96_골디락스",
        "label": "1995~96 골디락스",
        "season": "여름", "ff_pos": "mid", "ff_action": "hold",
        "inflation_trend": "stable", "valuation": "normal",
        "credit": "normal", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "neutral", "external_shock": "none",
        "comment": "94년 연착륙 직후. 인플레 안정, 성장 견고, 시장 평온.\n여름의 한가운데. 그러나 이 위에서 광기가 시작된다.",
        "quote": "주가는 끌어내리지 않으면 계속 오른다.",
        "aftermath": "96~99년 거품 본격 진행. 96년 경고도 다 무시됐다.",
        "era_type": "period",
        "historical_duration_days": 730,
        "hist_start": "1995-03-01",
        "hist_end": "1996-11-30",
        "hist_days": 640,
    },
    {
        "id": "1996_12월_과열경고",
        "label": "1996 12월 그린스펀 과열 경고",
        "season": "여름", "ff_pos": "mid", "ff_action": "hold",
        "inflation_trend": "stable", "valuation": "high",
        "credit": "tight", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "strong", "external_shock": "none",
        "comment": "그린스펀이 시장이 비이성적으로 과열됐다고 경고했다.\n발언 후 시장은 3년 더 올랐다. 거품 경고가 작동하지 않는 단계다.",
        "quote": "사람들이 모두 위험을 안다는 것은 더 이상 위험이 아니라는 뜻이다.",
        "aftermath": "시장 3년 더 올랐다. 결국 1999 광기로 진입했다.",
        "era_type": "moment",
        "historical_duration_days": 7,
        "hist_start": "1996-12-05",
        "hist_end": "1997-04-30",
        "hist_days": 146,
    },
    {
        "id": "1998_LTCM",
        "label": "1998 LTCM 위기",
        "season": "가을", "ff_pos": "mid", "ff_action": "cutting",
        "inflation_trend": "cooling", "valuation": "high",
        "credit": "widening", "yield_curve": "flattening",
        "semiconductor": "flat", "dollar": "strong", "external_shock": "financial",
        "comment": "신흥국 발 위기 + LTCM 파산. 그린스펀 긴급 인하.\n그러나 시장은 곧 회복하고 99년 광기로 들어갔다.",
        "quote": "위기가 끝나면 광기가 시작된다.",
        "aftermath": "98~99년 그린스펀 긴급 인하 후 닷컴 광기 가속됐다.",
        "era_type": "event",
        "historical_duration_days": 90,
        "hist_start": "1998-08-01",
        "hist_end": "1998-10-31",
        "hist_days": 91,
    },
    {
        "id": "1999_닷컴광기",
        "label": "1999 닷컴 광기",
        "season": "여름", "ff_pos": "mid", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "extreme",
        "credit": "tight", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "strong", "external_shock": "none",
        "comment": "거품의 마지막 1년. 99년 PER도 28 수준이었다.\n꽃이 아름다울수록 죽음은 가깝다. 그러나 그 1년이 길다.",
        "quote": "꽃이 아름다울수록 죽음은 더 가깝다.",
        "aftermath": "2000~02년 나스닥 -78%. 회복에 15년 걸렸다.",
        "era_type": "period",
        "historical_duration_days": 450,
        "hist_start": "1999-01-01",
        "hist_end": "2000-03-24",
        "hist_days": 448,
    },
    # ===== 2000년대 =====
    {
        "id": "2000_3월_정점",
        "label": "2000 3월 닷컴 정점",
        "season": "가을", "ff_pos": "high", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "extreme",
        "credit": "widening", "yield_curve": "inverted",
        "semiconductor": "down", "dollar": "strong", "external_shock": "none",
        "comment": "고점에서의 금리인하 + 밸류 극단. 2000년 3월의 지문이다.\n역사상 이 정도 고평가된 PER에서 숏 잡아 실패한 적 없다.",
        "quote": "역사상 이 정도 고평가된 PER에서 숏을 잡아 실패한 사례가 없다.",
        "aftermath": "2000~02년 닷컴 붕괴. 9.11에 이어 침체로 들어갔다.",
        "era_type": "moment",
        "historical_duration_days": 14,
        "hist_start": "2000-03-24",
        "hist_end": "2000-05-31",
        "hist_days": 68,
    },
    {
        "id": "2000_02_붕괴",
        "label": "2000~2002 닷컴 붕괴",
        "season": "겨울", "ff_pos": "mid", "ff_action": "cutting",
        "inflation_trend": "cooling", "valuation": "high",
        "credit": "widening", "yield_curve": "recovering",
        "semiconductor": "down", "dollar": "strong", "external_shock": "none",
        "comment": "주가가 빠지면서 PER이 더 높아졌다. 실적이 더 빠르게 빠졌으니까.\n나스닥 78퍼센트 하락. 회복에 15년 걸렸다.",
        "quote": "주가가 붕괴될 때 주가보다 실적이 더 빠른 속도로 붕괴되기에 per이 50, 60, 70, 80 된다.",
        "aftermath": "2003년 반도체 바닥 찍고 강세장 시작했다.",
        "era_type": "period",
        "historical_duration_days": 930,
        "hist_start": "2000-04-01",
        "hist_end": "2002-10-09",
        "hist_days": 922,
    },
    {
        "id": "2003_SOX바닥",
        "label": "2003 반도체 바닥 반전",
        "season": "봄", "ff_pos": "low", "ff_action": "hold",
        "inflation_trend": "cooling", "valuation": "normal",
        "credit": "tight", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "weak", "external_shock": "none",
        "comment": "반도체가 먼저 돌아섰다. 시장은 아직 의심 중.\n그러나 반도체가 가장 먼저 저점을 본다.",
        "quote": "그 어떤 섹터보다도 반도체가 가장 먼저 저점을 볼 것이다.",
        "aftermath": "2003~07년 4년 강세장. 그 사이 부동산 거품이 자랐다.",
        "era_type": "moment",
        "historical_duration_days": 14,
        "hist_start": "2002-10-09",
        "hist_end": "2003-06-30",
        "hist_days": 264,
    },
    {
        "id": "2004_06_점진긴축",
        "label": "2004~06 그린스펀 점진 긴축",
        "season": "여름", "ff_pos": "mid", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "high",
        "credit": "tight", "yield_curve": "flattening",
        "semiconductor": "up", "dollar": "weak", "external_shock": "none",
        "comment": "17번 연속 인상. 그러나 시장은 계속 올랐다.\n점진 긴축이 자산가격에 늦게 반영되는 패턴. 결말은 2007년이었다.",
        "quote": "긴축이 끝나기 전엔 오를 수 있으나 긴축의 청구서는 결국 도착한다.",
        "aftermath": "2007년 신용 디커플링으로 이어지고 2008년에 터졌다.",
        "era_type": "period",
        "historical_duration_days": 730,
        "hist_start": "2004-06-30",
        "hist_end": "2006-06-29",
        "hist_days": 729,
    },
    {
        "id": "2006_07_말기랠리",
        "label": "2006~07 말기 랠리",
        "season": "가을", "ff_pos": "high", "ff_action": "hold",
        "inflation_trend": "stable", "valuation": "high",
        "credit": "tight", "yield_curve": "inverted",
        "semiconductor": "up", "dollar": "neutral", "external_shock": "none",
        "comment": "금리 높은데 시장이 버틴다. 06년 말처럼 보인다.\n시간이 있지만 방향은 정해졌다.",
        "quote": "연극은 끝이 있고 거품은 꺼지며 불균형은 균형으로 간다.",
        "aftermath": "2007년 7월 신용 디커플링이 시작됐고 2008년에 패닉으로 갔다.",
        "era_type": "period",
        "historical_duration_days": 365,
        "hist_start": "2006-07-01",
        "hist_end": "2007-07-15",
        "hist_days": 379,
    },
    {
        "id": "2007_7월_디커플링",
        "label": "2007 7월 신용 디커플링",
        "season": "가을", "ff_pos": "high", "ff_action": "hold",
        "inflation_trend": "stable", "valuation": "high",
        "credit": "widening", "yield_curve": "inverted",
        "semiconductor": "down", "dollar": "weak", "external_shock": "financial",
        "comment": "신용이 먼저 깨졌다. 주식은 아직 잠잠하다.\n채권시장이 비명을 지를 때가 진짜 가을 시작이다.",
        "quote": "채권시장의 붕괴는 세상물정 모르는 나스닥 동생을 보고 느낀 걱정의 결과다.",
        "aftermath": "2008년 9월 리만 패닉. S&P -57%까지 갔다.",
        "era_type": "moment",
        "historical_duration_days": 14,
        "hist_start": "2007-07-15",
        "hist_end": "2007-10-09",
        "hist_days": 86,
    },
    {
        "id": "2008_9월_리만",
        "label": "2008 9월 리만 패닉",
        "season": "겨울", "ff_pos": "mid", "ff_action": "cutting",
        "inflation_trend": "cooling", "valuation": "normal",
        "credit": "panic", "yield_curve": "recovering",
        "semiconductor": "down", "dollar": "strong", "external_shock": "financial",
        "comment": "역실적장세다. 실적이 빠지고 주가가 빠지고 실업률이 올라간다.\n근데 여기서부터 매수가 정석이다.",
        "quote": "실업률 올라갈 때부터 주식을 사기 시작하면 된다.",
        "aftermath": "2009년 3월 바닥 찍고 사상 최장 강세장 시작했다.",
        "era_type": "event",
        "historical_duration_days": 180,
        "hist_start": "2008-09-15",
        "hist_end": "2009-03-09",
        "hist_days": 175,
    },
    {
        "id": "2009_3월_바닥",
        "label": "2009 3월 PMI 반전",
        "season": "봄", "ff_pos": "low", "ff_action": "hold",
        "inflation_trend": "stable", "valuation": "low",
        "credit": "widening", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "weak", "external_shock": "none",
        "comment": "선행지표가 먼저 돌아섰다. 시장은 아직 공포 중.\n역실적장세에선 매수가 정석이다.",
        "quote": "역실적장세에선 매수가 정석이다.",
        "aftermath": "2009~20년 11년 강세장. 사상 최장 강세장이었다.",
        "era_type": "moment",
        "historical_duration_days": 14,
        "hist_start": "2009-03-09",
        "hist_end": "2009-12-31",
        "hist_days": 297,
    },
    # ===== 2010년대 =====
    {
        "id": "2011_8월_등급강등",
        "label": "2011 8월 미국 신용등급 강등",
        "season": "가을", "ff_pos": "low", "ff_action": "hold",
        "inflation_trend": "stable", "valuation": "high",
        "credit": "widening", "yield_curve": "normal",
        "semiconductor": "down", "dollar": "weak", "external_shock": "financial",
        "comment": "S&P가 미국 AAA 첫 박탈. 정치발 신용 충격.\n단기 패닉 후 회복했지만 부채 누적의 기점이었다.",
        "quote": "미국 정부부채의 분명한 문제는 앞으로 있을 다양한 변수에 대비할 수 없는 수준이라는 것이다.",
        "aftermath": "단기 패닉 후 회복했다. 그러나 부채 누적은 그 후로 가속됐다.",
        "era_type": "event",
        "historical_duration_days": 60,
        "hist_start": "2011-08-05",
        "hist_end": "2011-10-31",
        "hist_days": 87,
    },
    {
        "id": "2013_테이퍼탠트럼",
        "label": "2013 5월 테이퍼 탠트럼",
        "season": "여름", "ff_pos": "low", "ff_action": "hold",
        "inflation_trend": "stable", "valuation": "high",
        "credit": "widening", "yield_curve": "normal",
        "semiconductor": "up", "dollar": "neutral", "external_shock": "none",
        "comment": "버냉키의 양적완화 축소 시사. 채권시장이 격렬하게 반응.\n그러나 시장은 곧 회복했다. 진짜 긴축이 아니었기 때문에.",
        "quote": "말이 아니라 행동을 봐야 한다.",
        "aftermath": "단기 채권 발작 후 시장 회복했다. 양적완화 축소는 천천히 진행됐다.",
        "era_type": "event",
        "historical_duration_days": 120,
        "hist_start": "2013-05-22",
        "hist_end": "2013-09-30",
        "hist_days": 131,
    },
    {
        "id": "2018_1월_변동성폭발",
        "label": "2018 1월 변동성 폭발",
        "season": "여름", "ff_pos": "low", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "high",
        "credit": "tight", "yield_curve": "normal",
        "semiconductor": "down", "dollar": "weak", "external_shock": "financial",
        "comment": "2월 5일 빅스 일일 116퍼센트 급등. 변동성 매도 상품들 한꺼번에 청산됐다.\n변동성 자체가 충격이 되는 패턴. 평온이 폭력으로 전환되는 순간이다.",
        "quote": "공짜 점심은 없다. 낮은 변동성에 베팅한 모든 것이 한 번에 깨진다.",
        "aftermath": "2018년 한 해 박스권. 12월에 추가 급락이 왔다.",
        "era_type": "event",
        "historical_duration_days": 90,
        "hist_start": "2018-01-26",
        "hist_end": "2018-04-30",
        "hist_days": 94,
    },
    {
        "id": "2018_12월_피봇",
        "label": "2018 12월 파월 1차 피봇",
        "season": "가을", "ff_pos": "mid", "ff_action": "hiking",
        "inflation_trend": "stable", "valuation": "normal",
        "credit": "widening", "yield_curve": "flattening",
        "semiconductor": "down", "dollar": "strong", "external_shock": "none",
        "comment": "파월이 4번 인상하고 시장이 흔들리자 멈췄다.\n연준의 본질은 금융 안정이지 인플레 잡이가 아니다.",
        "quote": "연준의 목표가 물가 안정이라 하는 사람은 연준을 모르는 사람이다.",
        "aftermath": "2019년 파월 유턴. 신고가 갱신했지만 2020년 코로나로 갔다.",
        "era_type": "moment",
        "historical_duration_days": 14,
        "hist_start": "2018-10-01",
        "hist_end": "2019-01-31",
        "hist_days": 122,
    },
]
assert len(ERA_LIBRARY) == 32, f"ERA_LIBRARY 항목 수 불일치: {len(ERA_LIBRARY)}"

ERA_DIM_WEIGHTS = {
    "season": 3, "ff_pos": 2, "ff_action": 2,
    "inflation_trend": 2, "valuation": 3, "credit": 2,
    "yield_curve": 2, "semiconductor": 1, "dollar": 1, "external_shock": 2,
}  # weight 합 = 20
ERA_MATCH_THRESHOLD = 0.55


# ═══ V3.11.0: DTW 진행도 측정 시스템 ═══
try:
    from fastdtw import fastdtw as _fastdtw  # type: ignore
    _DTW_AVAILABLE = True
except Exception:
    _DTW_AVAILABLE = False
try:
    from scipy.spatial.distance import euclidean as _scipy_euclidean  # type: ignore
except Exception:
    def _scipy_euclidean(u, v):
        import math
        return math.sqrt(sum((float(a)-float(b))**2 for a, b in zip(u, v)))

DTW_CACHE_FILE = SD / "cache" / "dtw_series.parquet"
DTW_LONG_RANGE_TICKERS_YF = {
    "spx": "^GSPC",
    "vix": "^VIX",
    "dxy": "DX-Y.NYB",
}
DTW_LONG_RANGE_FRED = {
    "cpi":     "CPIAUCSL",
    "ff":      "FEDFUNDS",
    "tb3m":    "TB3MS",
    "t10y":    "GS10",
    "indpro":  "INDPRO",
    "hy_oas":  "BAMLH0A0HYM2",
}


@st.cache_data(ttl=86400, show_spinner=False)
def _load_long_range_series(api_key):
    """1928년 이래 가용 시계열 8종 로드. parquet 캐싱 (1일 갱신).
    실패 시 dict 일부 또는 빈 dict 반환. None 안 던짐.
    """
    out = {}
    DTW_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        if DTW_CACHE_FILE.exists():
            try:
                _df = pd.read_parquet(DTW_CACHE_FILE)
                for _c in _df.columns:
                    out[_c] = _df[_c].dropna()
                # 1일 이상 지났으면 background refresh — 일단 cached 반환
                _mtime = DTW_CACHE_FILE.stat().st_mtime
                if (datetime.now().timestamp() - _mtime) < 86400:
                    return out
            except Exception:
                pass
    except Exception:
        pass
    # Fresh fetch
    try:
        if api_key:
            try:
                _fd = ffred_parallel(DTW_LONG_RANGE_FRED, api_key, start="1928-01-01")
                for _name, _s in (_fd or {}).items():
                    if _s is not None and len(_s) > 0:
                        out[_name] = _s.dropna()
            except Exception: pass
        try:
            import yfinance as _yf_dtw
            for _name, _tkr in DTW_LONG_RANGE_TICKERS_YF.items():
                try:
                    _h = _yf_dtw.Ticker(_tkr).history(period="max", auto_adjust=False)
                    if _h is not None and len(_h) > 0 and "Close" in _h.columns:
                        _s = _h["Close"].dropna()
                        try: _s.index = _s.index.tz_localize(None)
                        except Exception: pass
                        out[_name] = _s
                except Exception: continue
        except Exception: pass
        # short_rate hybrid: FF for 1954+, TB3M for earlier
        try:
            _ff = out.get("ff"); _tb = out.get("tb3m")
            if _ff is not None and _tb is not None:
                _cut = pd.Timestamp("1954-07-01")
                _early = _tb[_tb.index < _cut]
                _late = _ff[_ff.index >= _cut]
                _hybrid = pd.concat([_early, _late]).sort_index()
                _hybrid = _hybrid[~_hybrid.index.duplicated(keep="last")]
                out["short_rate"] = _hybrid
        except Exception: pass
        # CPI YoY (월간 변화율 12)
        try:
            _cpi = out.get("cpi")
            if _cpi is not None and len(_cpi) >= 13:
                out["cpi_yoy"] = (_cpi / _cpi.shift(12) - 1).dropna() * 100
        except Exception: pass
        # parquet 저장
        try:
            if out:
                _df_save = pd.DataFrame({_k: _s for _k, _s in out.items()})
                _df_save.to_parquet(DTW_CACHE_FILE)
        except Exception: pass
    except Exception: pass
    return out


def _available_series_for_era(era, long_range_series):
    """era 기간 내 가용 시계열 dict 반환. 월간 FRED 시리즈는 일간 ffill 로 정렬 —
    짧은 era (~100일) 에서도 monthly 시리즈가 30+ 포인트 확보됨."""
    try:
        start = pd.to_datetime(era["hist_start"])
        end = pd.to_datetime(era["hist_end"])
    except Exception:
        return {}
    available = {}
    if not long_range_series: return available
    for name, s in long_range_series.items():
        if s is None or len(s) == 0: continue
        try:
            era_slice = s[(s.index >= start) & (s.index <= end)].dropna()
            if len(era_slice) < 3: continue  # 최소 3개 raw 관측
            try:
                _daily = pd.date_range(start=start, end=end, freq="D")
                era_slice = era_slice.reindex(_daily, method="ffill").dropna()
            except Exception: pass
            if len(era_slice) < 10: continue
            available[name] = era_slice
        except Exception: continue
    return available


def _zscore_normalize(arr_like):
    import numpy as _np
    try:
        a = _np.array([float(x) for x in arr_like if x is not None and not pd.isna(x)])
        if len(a) < 10: return None
        mu = float(_np.mean(a)); sd = float(_np.std(a))
        if sd == 0: return None
        return (a - mu) / sd
    except Exception:
        return None


_DTW_CANONICAL_KEYS = ["spx", "vix", "dxy", "cpi", "ff", "t10y", "indpro", "hy_oas"]
_DTW_DISPLAY_NAMES = {
    "spx": "SPX", "vix": "VIX", "dxy": "DXY", "cpi": "CPI",
    "ff": "FF", "t10y": "T10Y", "indpro": "INDPRO", "hy_oas": "HY OAS",
}
_DTW_NAME_TO_CURR_KEY = {
    "spx":    "spx_s",
    "vix":    "vix_s",
    "dxy":    "dxy_s",
    "cpi":    "cpi_s",
    "ff":     "ff_s",
    "t10y":   "t10y_s",
    "indpro": "indpro_s",
    "hy_oas": "hy_s",
}


def measure_era_progress(era_id, long_range_series, current_raw_data, recent_days=90):
    """DTW 슬라이딩 윈도우로 era 진행도 측정.
    반환 dict: {progress_pct, current_day_in_era, total_era_days, confidence,
                series_used_count, series_used_names, series_missing_names,
                dtw_distance, reason}
    """
    import numpy as _np
    _all_disp = [_DTW_DISPLAY_NAMES[k] for k in _DTW_CANONICAL_KEYS]
    era = next((e for e in ERA_LIBRARY if e.get("id") == era_id), None)
    if era is None:
        return {"progress_pct": None, "confidence": "unavailable", "reason": "era 정의 없음",
                "series_used_count": 0, "series_used_names": [],
                "series_missing_names": _all_disp, "total_era_days": None}
    total_days = era.get("hist_days")
    if not _DTW_AVAILABLE:
        return {"progress_pct": None, "confidence": "unavailable", "reason": "fastdtw 미설치",
                "series_used_count": 0, "series_used_names": [],
                "series_missing_names": _all_disp, "total_era_days": total_days}
    available = _available_series_for_era(era, long_range_series or {})
    # 8 canonical 만 사용
    avail_canon = {k: v for k, v in available.items() if k in _DTW_CANONICAL_KEYS}
    n_series = len(avail_canon)
    if n_series < 3:
        _avail_disp = [_DTW_DISPLAY_NAMES[k] for k in avail_canon.keys()]
        _miss_disp = [d for d in _all_disp if d not in _avail_disp]
        return {"progress_pct": None, "confidence": "unavailable",
                "reason": f"가용 시계열 {n_series}/8 < 최소 3",
                "series_used_count": n_series,
                "series_used_names": _avail_disp,
                "series_missing_names": _miss_disp,
                "total_era_days": total_days}
    curr_norms = []; hist_norms = []; used_keys = []
    for name in _DTW_CANONICAL_KEYS:
        if name not in avail_canon: continue
        hist_s = avail_canon[name]
        ck = _DTW_NAME_TO_CURR_KEY.get(name)
        if ck is None: continue
        curr_s = (current_raw_data or {}).get(ck)
        if curr_s is None or len(curr_s) < 3: continue
        try:
            # curr 도 일간 ffill — 월간 FRED 시계열 호환
            try:
                _last = curr_s.index.max()
                _idx_d = pd.date_range(end=_last, periods=recent_days, freq="D")
                _curr_recent = curr_s.reindex(_idx_d, method="ffill").dropna()
            except Exception:
                _curr_recent = curr_s.dropna().tail(recent_days)
            if len(_curr_recent) < 10: continue
            hist_arr = _np.array([float(x) for x in hist_s.values if x is not None and not pd.isna(x)])
            if len(hist_arr) < 10: continue  # 짧은 era (1987 블랙먼데이 97일) 호환
            _mu = float(_np.mean(hist_arr)); _sd = float(_np.std(hist_arr))
            if _sd == 0: continue
            curr_arr = _np.array([float(x) for x in _curr_recent.values if x is not None and not pd.isna(x)])
            curr_n = (curr_arr - _mu) / _sd
            hist_n = (hist_arr - _mu) / _sd
            if len(curr_n) < 10 or len(hist_n) < 10: continue
            curr_norms.append(curr_n); hist_norms.append(hist_n); used_keys.append(name)
        except Exception: continue
    n_used = len(used_keys)
    used_disp = [_DTW_DISPLAY_NAMES[k] for k in used_keys]
    miss_disp = [d for d in _all_disp if d not in used_disp]
    if n_used < 3:
        return {"progress_pct": None, "confidence": "unavailable",
                "reason": f"정규화 후 {n_used} < 3",
                "series_used_count": n_used,
                "series_used_names": used_disp,
                "series_missing_names": miss_disp,
                "total_era_days": total_days}
    confidence = "high" if n_used >= 5 else "medium"
    # 다변량 결합 (각 시계열 같은 길이로 trim)
    min_curr = min(len(s) for s in curr_norms)
    min_hist = min(len(s) for s in hist_norms)
    if min_curr < 10 or min_hist < 30:
        return {"progress_pct": None, "confidence": "unavailable",
                "reason": "trim 후 길이 부족",
                "series_used_count": n_used,
                "series_used_names": used_disp,
                "series_missing_names": miss_disp,
                "total_era_days": total_days}
    curr_mat = _np.column_stack([s[-min_curr:] for s in curr_norms])
    hist_mat = _np.column_stack([s[-min_hist:] for s in hist_norms])
    # 슬라이딩 윈도우 DTW — hist 의 각 끝점 t 에서 curr_window 와 매칭
    best_t = None; best_dist = float("inf")
    window = min_curr
    # 성능: 너무 자주 호출 X — step 으로 sampling
    _step = max(1, (min_hist - window) // 60)  # 최대 ~60 비교
    _t = window
    while _t <= min_hist:
        try:
            _hist_w = hist_mat[_t - window:_t]
            _dist, _ = _fastdtw(curr_mat, _hist_w, dist=_scipy_euclidean)
            if _dist < best_dist:
                best_dist = _dist; best_t = _t
        except Exception: pass
        _t += _step
    if best_t is None:
        return {"progress_pct": None, "confidence": "unavailable",
                "reason": "DTW 매칭 실패",
                "series_used_count": n_used,
                "series_used_names": used_disp,
                "series_missing_names": miss_disp,
                "total_era_days": total_days}
    progress_pct = best_t / max(min_hist, 1) * 100
    current_day = int(best_t * (total_days / max(min_hist, 1))) if total_days else None
    return {
        "progress_pct": round(progress_pct, 1),
        "current_day_in_era": current_day,
        "total_era_days": total_days,
        "confidence": confidence,
        "series_used_count": n_used,
        "series_used_names": used_disp,
        "series_missing_names": miss_disp,
        "dtw_distance": round(best_dist, 2),
        "reason": None,
    }


@st.cache_data(ttl=3600, show_spinner=False)
def _measure_era_progress_cached(era_id, _curr_keys_signature):
    """era_progress streamlit cache wrapper. 인자 sig 가 같으면 캐시."""
    return None  # placeholder; main() 에서 직접 호출



def _era_distance_match(current_state, top_n=3):
    """current_state ↔ ERA_LIBRARY 차원별 가중 일치도. 점수 내림차순 top_n 반환.
    각 결과: {era, score, matched_dims, unmatched_dims}
    """
    if current_state is None:
        return []
    results = []
    weight_total = sum(ERA_DIM_WEIGHTS.values())
    for era in ERA_LIBRARY:
        score = 0; matched = []
        for dim, w in ERA_DIM_WEIGHTS.items():
            cur_v = current_state.get(dim); era_v = era.get(dim)
            if cur_v is not None and cur_v == era_v:
                score += w; matched.append(dim)
        unmatched = [d for d in ERA_DIM_WEIGHTS if d not in matched]
        results.append({"era": era, "score": score / weight_total,
                        "matched_dims": matched, "unmatched_dims": unmatched})
    results.sort(key=lambda x: -x["score"])
    return results[:top_n]


def _find_common_dims(matches):
    """top N 매치 간 공통 일치 차원 추출. 1개 미만이면 빈 list."""
    if len(matches) < 2:
        return []
    common = set(matches[0].get("matched_dims", []))
    for m in matches[1:]:
        common &= set(m.get("matched_dims", []))
    return list(common)


# ─── era enum → 클러스터 점수 추정 (💡6) ─────────────────────────────────
# ERA_LIBRARY 의 enum 차원 (valuation/credit/yield_curve 등) 을 5클러스터 점수로 매핑.
# 추정값이라 카드에 "추정 표기" 명시 필수.
_ENUM_TO_SCORE = {
    "valuation":   {"extreme": 18, "high": 35, "normal": 55, "low": 80},
    "credit":      {"panic": 15, "widening": 35, "normal": 60, "tight": 75},
    "yield_curve": {"inverted": 25, "recovering": 50, "normal": 70},
    "external_shock": {"geopolitical": 20, "oil": 30, "financial": 35, "none": 70},
    "ff_pos":      {"high": 40, "mid": 55, "low": 70},
    "inflation_trend": {"accelerating": 25, "stable": 55, "peaking": 50, "cooling": 70, "unknown": 50},
    "semiconductor": {"down": 30, "flat": 50, "up": 70},
    "dollar":      {"strong": 40, "neutral": 55, "weak": 65},
}

def _era_cluster_estimate(era):
    """era dict 의 enum 차원 → 5클러스터 추정 점수. 차원 없으면 None."""
    if not era: return None
    def _avg(*vals):
        vals = [v for v in vals if v is not None]
        return round(sum(vals) / len(vals), 1) if vals else None
    bond  = _avg(_ENUM_TO_SCORE["yield_curve"].get(era.get("yield_curve")),
                 _ENUM_TO_SCORE["ff_pos"].get(era.get("ff_pos")))
    val   = _ENUM_TO_SCORE["valuation"].get(era.get("valuation"))
    stres = _avg(_ENUM_TO_SCORE["credit"].get(era.get("credit")),
                 _ENUM_TO_SCORE["external_shock"].get(era.get("external_shock")))
    real  = _avg(_ENUM_TO_SCORE["inflation_trend"].get(era.get("inflation_trend")),
                 _ENUM_TO_SCORE["dollar"].get(era.get("dollar")))
    semi  = _ENUM_TO_SCORE["semiconductor"].get(era.get("semiconductor"))
    return {"채권/금리": bond, "밸류에이션": val, "스트레스": stres, "실물": real, "반도체": semi}


def _enrich_match(match):
    """1개 매치 dict 에 era 메타 + 시간거리 + aftermath + 추정 클러스터 추가."""
    from datetime import datetime as _dt
    era = match["era"]
    try: era_year = int(era["id"][:4])
    except Exception: era_year = None
    return {
        "era_id": era["id"],
        "label": era["label"],
        "score": match["score"],
        "matched_dims": match.get("matched_dims", []),
        "unmatched_dims": match.get("unmatched_dims", []),
        "comment": era.get("comment"),
        "quote": era.get("quote"),
        "aftermath": era.get("aftermath", "—"),
        "era_type": era.get("era_type", "period"),
        "historical_duration_days": era.get("historical_duration_days"),
        "years_ago": (_dt.now().year - era_year) if era_year else None,
        # enum dims (UI 표시용)
        "season":          era.get("season"),
        "ff_pos":          era.get("ff_pos"),
        "ff_action":       era.get("ff_action"),
        "valuation":       era.get("valuation"),
        "credit":          era.get("credit"),
        "yield_curve":     era.get("yield_curve"),
        "semiconductor":   era.get("semiconductor"),
        "dollar":          era.get("dollar"),
        "external_shock":  era.get("external_shock"),
        "inflation_trend": era.get("inflation_trend"),
        # 클러스터 추정
        "cluster_estimate": _era_cluster_estimate(era),
    }


def _era_match_with_threshold(current_state, top_n=3, threshold=ERA_MATCH_THRESHOLD):
    """거리 매칭 + threshold 미달 시 fallthrough. matches 항목에 enrich 메타 포함."""
    raw_matches = _era_distance_match(current_state, top_n=top_n)
    if not raw_matches or raw_matches[0]["score"] < threshold:
        return {
            "era": None, "label": None,
            "score": raw_matches[0]["score"] if raw_matches else 0.0,
            "matches": [],
            "comment": "깔끔하게 매칭되는 역사적 선례가 없다.\n무리하게 비유하지 않는다. 차원을 분리해서 봐라.",
            "quote": None,
            "common_dims": [],
            "current_state": current_state or {},
        }
    enriched = [_enrich_match(m) for m in raw_matches]
    top = raw_matches[0]
    return {
        "era": top["era"]["id"],
        "label": top["era"]["label"],
        "score": top["score"],
        "matches": enriched,
        "comment": top["era"]["comment"],
        "quote": top["era"]["quote"],
        "common_dims": _find_common_dims(raw_matches),
        "current_state": current_state or {},
    }


def _history_match_trend_multi(obs_jsonl_path, days_list=(1, 7, 30, 90)):
    """observations.jsonl 에서 N일 전 history_era_top1 다단계 조회.
    반환 dict: {N: {"era", "score", "actual_days"} or None}
    허용 오차: 요청 일수의 30% (최소 2일).
    """
    import json as _json
    from datetime import datetime as _dt, timedelta as _td
    result = {d: None for d in days_list}
    try:
        if not obs_jsonl_path or not Path(obs_jsonl_path).exists():
            return result
        with open(obs_jsonl_path, "r", encoding="utf-8") as f:
            lines = f.readlines()[-2000:]
        rows = []
        for line in lines:
            try:
                row = _json.loads(line)
                d_str = (row.get("date") or "").split()[0] if row.get("date") else ""
                if not d_str: continue
                rd = _dt.fromisoformat(d_str)
                rows.append((rd, row))
            except Exception:
                continue
        if not rows: return result
        now = _dt.now()
        for d in days_list:
            target = now - _td(days=d)
            tolerance = max(2, int(d * 0.3))
            best = min(rows, key=lambda x: abs((x[0] - target).total_seconds()))
            if abs((best[0] - target).days) > tolerance:
                continue
            era_id = best[1].get("history_era_top1")
            if era_id is None: continue
            result[d] = {
                "era": era_id,
                "score": best[1].get("history_score_top1", 0.0) or 0.0,
                "actual_days": (now - best[0]).days,
            }
        return result
    except Exception:
        return result


_DIM_KO = {
    "season": "계절",
    "ff_pos": "기준금리 위치",
    "ff_action": "연준 행동",
    "inflation_trend": "인플레",
    "valuation": "밸류",
    "credit": "신용",
    "yield_curve": "장단기금리",
    "semiconductor": "반도체",
    "dollar": "달러",
    "external_shock": "외부 충격",
}


def _era_consecutive_days(obs_jsonl_path, era_id):
    """현재 era 가 연속 며칠째 1위인지. 가장 최근부터 거꾸로 추적.
    V3.10.4 변경: 모든 백필/실측 row 를 (date, era) tuple 로 모음. 같은 날짜 중복은 last 우선.
    None (임계 미달) 은 갭으로 취급 — 카운트 증감 없이 통과 (다른 era 만나면 break).
    obs.jsonl 의 history_era_top1 필드 기반.
    """
    if era_id is None: return 0
    try:
        if not obs_jsonl_path or not Path(obs_jsonl_path).exists():
            return 0
        import json as _json
        with open(obs_jsonl_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        # 모든 row 수집 (None 도 포함, 갭 처리용)
        date_to_era = {}
        for line in lines:
            try:
                r = _json.loads(line)
                if r.get("_backfill_marker"): continue  # 마커는 skip
                d = (r.get("date") or "").split()[0]
                if not d: continue
                e = r.get("history_era_top1")  # None 허용
                # 같은 날짜 중복: 명시적 era 가 있는 게 우선, 둘 다 있으면 last
                cur = date_to_era.get(d)
                if cur is None or e is not None:
                    date_to_era[d] = e
            except Exception:
                continue
        if not date_to_era: return 0
        sorted_dates = sorted(date_to_era.keys())
        count = 0
        for d in reversed(sorted_dates):
            era_at_d = date_to_era[d]
            if era_at_d == era_id:
                count += 1
            elif era_at_d is None:
                continue  # 갭 — None 은 통과 (카운트 증감 없음)
            else:
                break  # 다른 era — 종결
        return count
    except Exception:
        return 0


# ═══ V3.10.4: 일회성 730일 백필 시스템 ═══
def _series_at_date(s, target_date):
    """DatetimeIndex 시리즈에서 target_date 이하 가장 최근 값 반환. 없으면 None.
    월간/일간 무관 — production _ago(s, days) 와 동일 로직."""
    try:
        if s is None or len(s) == 0: return None
        sub = s[s.index <= target_date]
        if len(sub) == 0: return None
        return float(sub.iloc[-1])
    except Exception: return None


def _trim_series_at_offset(s, offset):
    """offset 만큼 뒤로 잘라낸 시리즈. DatetimeIndex 면 date-based,
    그 외 iloc-based. helpers 와 동일 분기 — calendar/iloc 혼동 방지."""
    if s is None: return None
    if hasattr(s, "index") and isinstance(s.index, pd.DatetimeIndex):
        if offset <= 0: return s
        ref = s.index[-1] - pd.Timedelta(days=offset)
        return s[s.index <= ref]
    # fallback: iloc-based
    if offset <= 0: return s
    if len(s) <= offset: return s.iloc[:0]
    return s.iloc[:len(s) - offset]


def _safe_iloc_at(s, offset):
    """시점 가변 값 접근. DatetimeIndex 면 date-based (월간/일간 자동 호환).
    그 외 fallback: iloc-based. offset = today 기준 days 전."""
    try:
        if s is None or len(s) == 0: return None
        if hasattr(s, "index") and isinstance(s.index, pd.DatetimeIndex):
            target = s.index[-1] - pd.Timedelta(days=offset)
            return _series_at_date(s, target)
        if len(s) <= offset: return None
        return float(s.iloc[-1 - offset])
    except Exception:
        return None


def _pct_change_at(s, offset, lookback_days):
    """offset 시점 vs offset+lookback 변화율 (%). DatetimeIndex 면 date-based."""
    try:
        if s is None or len(s) == 0: return None
        if hasattr(s, "index") and isinstance(s.index, pd.DatetimeIndex):
            ref = s.index[-1] - pd.Timedelta(days=offset)
            cur = _series_at_date(s, ref)
            prev = _series_at_date(s, ref - pd.Timedelta(days=lookback_days))
            if cur is None or prev is None or prev == 0: return None
            return (cur / prev - 1) * 100
        if len(s) <= offset + lookback_days: return None
        cur = float(s.iloc[-1 - offset])
        prev = float(s.iloc[-1 - offset - lookback_days])
        if prev == 0: return None
        return (cur / prev - 1) * 100
    except Exception:
        return None


def _abs_change_at(s, offset, lookback_days):
    """offset 시점 vs offset+lookback 절대 변화. DatetimeIndex 면 date-based."""
    try:
        if s is None or len(s) == 0: return None
        if hasattr(s, "index") and isinstance(s.index, pd.DatetimeIndex):
            ref = s.index[-1] - pd.Timedelta(days=offset)
            cur = _series_at_date(s, ref)
            prev = _series_at_date(s, ref - pd.Timedelta(days=lookback_days))
            if cur is None or prev is None: return None
            return cur - prev
        if len(s) <= offset + lookback_days: return None
        cur = float(s.iloc[-1 - offset])
        prev = float(s.iloc[-1 - offset - lookback_days])
        return cur - prev
    except Exception:
        return None


def _percentile_at(s, offset, lookback_days):
    """offset 시점 값이 lookback 윈도우 내 percentile. DatetimeIndex 면 date-based."""
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
        if len(s) <= offset + lookback_days: return None
        cur = float(s.iloc[-1 - offset])
        window = s.iloc[max(0, len(s) - 1 - offset - lookback_days):len(s) - offset]
        if len(window) < 30: return None
        return float((window <= cur).sum()) / len(window) * 100
    except Exception:
        return None


def _evaluate_season_at_offset(raw, offset):
    """offset 시점의 4계절 점수 (간소화 9박스 평가). (season, scores_dict) 또는 (None, None).
    auto_season() 의 핵심 로직 추출 + 시간 가변. 정확도 trade-off 있음 (백필 전용)."""
    qqq = raw.get("qqq_s"); ff = raw.get("ff_s"); hy = raw.get("hy_s")
    unrate = raw.get("unrate_s"); inv3m10y = raw.get("t10y3m_s"); cpi = raw.get("cpi_yoy_s")
    cape = raw.get("cape_s"); fpe = raw.get("fpe_s"); vix = raw.get("vix_s")
    sox = raw.get("sox_s"); spx = raw.get("spx_s"); rsp = raw.get("rsp_s"); spy = raw.get("spy_s")
    wti = raw.get("wti_s")
    # QQQ DD from 52w high (date-based)
    qqq_dd = None
    try:
        _qqq_t = _trim_series_at_offset(qqq, offset)
        if _qqq_t is not None and len(_qqq_t) >= 252:
            window = _qqq_t.iloc[-252:]
            cur = float(_qqq_t.iloc[-1])
            high_52w = float(window.max())
            if high_52w > 0: qqq_dd = (cur / high_52w - 1) * 100
    except Exception: pass
    # 실업률 절대 + 3M 변화
    unemp_now = _safe_iloc_at(unrate, offset)
    unemp_3m = _abs_change_at(unrate, offset, 90)
    # FF 6M 변화 + percentile
    ff_6m = _abs_change_at(ff, offset, 180)
    ff_pos_pct = _percentile_at(ff, offset, 252 * 10)
    # HY OAS 절대 + 6M 변화 + 정점 대비
    hy_now = _safe_iloc_at(hy, offset)
    hy_6m = _abs_change_at(hy, offset, 180)
    hy_pct = (hy_now * 100) if (hy_now is not None and hy_now <= 1.0) else hy_now
    # 10Y-3M 역전 상태 (간단 분류, date-based)
    inv_now = _safe_iloc_at(inv3m10y, offset)
    inv_180_min = None
    try:
        _inv_t = _trim_series_at_offset(inv3m10y, offset)
        if _inv_t is not None and len(_inv_t) >= 252:
            inv_180_min = float(_inv_t.iloc[-252:].min())
    except Exception: pass
    # CPI YoY
    cpi_now = _safe_iloc_at(cpi, offset)
    cpi_3m_chg = _abs_change_at(cpi, offset, 90) if cpi is not None else None
    # PE
    fpe_now = _safe_iloc_at(fpe, offset)
    cape_now = _safe_iloc_at(cape, offset)
    # VIX 90D max (date-based)
    vix_90max = None
    try:
        _vix_t = _trim_series_at_offset(vix, offset)
        if _vix_t is not None and len(_vix_t) >= 30:
            if hasattr(_vix_t, "index") and isinstance(_vix_t.index, pd.DatetimeIndex):
                _ref = _vix_t.index[-1]
                w = _vix_t[(_vix_t.index >= _ref - pd.Timedelta(days=90)) & (_vix_t.index <= _ref)]
            else:
                w = _vix_t.iloc[-90:]
            if len(w) > 0: vix_90max = float(w.max())
    except Exception: pass
    # SOX/SPX 1M, 3M, 6M
    sox_3m = _pct_change_at(sox, offset, 63)
    spx_3m = _pct_change_at(spx, offset, 63)
    sox_6m = _pct_change_at(sox, offset, 126)
    spx_6m = _pct_change_at(spx, offset, 126)
    sox_1m = _pct_change_at(sox, offset, 22)
    spx_1m = _pct_change_at(spx, offset, 22)
    # RSP/QQQ 6M
    rsp_6m = _pct_change_at(rsp, offset, 126)
    qqq_6m = _pct_change_at(qqq, offset, 126)
    # SPY/RSP 1M
    spy_1m = _pct_change_at(spy, offset, 22)
    rsp_1m = _pct_change_at(rsp, offset, 22)
    # WTI 3M
    wti_3m = _pct_change_at(wti, offset, 63)

    scores = {"봄": 0, "여름": 0, "가을": 0, "겨울": 0}
    # ── 봄 박스 (간소화) ──
    if inv_180_min is not None and inv_now is not None and inv_180_min < -0.2 and inv_now > inv_180_min * 0.5:
        scores["봄"] += 1
    if hy_now is not None and hy_pct is not None and hy_pct < 4 and hy_6m is not None and hy_6m < 0:
        scores["봄"] += 1
    if ff_pos_pct is not None and ff_pos_pct < 30 and ff_6m is not None and ff_6m < 0:
        scores["봄"] += 1
    if unemp_now is not None and (unemp_now >= 4 or (unemp_3m is not None and unemp_3m > 0.5)):
        scores["봄"] += 1
    if qqq_dd is not None and qqq_dd < -25:
        scores["봄"] += 1
    if (fpe_now is not None and fpe_now <= 18) or (cape_now is not None and cape_now <= 25):
        scores["봄"] += 1
    if cpi_3m_chg is not None and cpi_now is not None and cpi_3m_chg < 0 and cpi_now < 3:
        scores["봄"] += 1
    # ── 여름 박스 ──
    if inv_now is not None and inv_now > 0 and (inv_180_min is None or inv_180_min >= -0.05):
        scores["여름"] += 1
    if hy_pct is not None and hy_pct < 4:
        scores["여름"] += 1
    if ff_6m is not None and abs(ff_6m) < 0.5:
        scores["여름"] += 1
    if fpe_now is not None and fpe_now < 22:
        scores["여름"] += 1
    if unemp_3m is not None and unemp_3m <= 0:
        scores["여름"] += 1
    if sox_6m is not None and spx_6m is not None and sox_6m > spx_6m:
        scores["여름"] += 1
    if rsp_6m is not None and qqq_6m is not None and rsp_6m > 0 and qqq_6m > 0:
        scores["여름"] += 1
    # ── 가을 박스 ──
    if inv_now is not None and inv_now < -0.05:
        scores["가을"] += 1
    if hy_6m is not None and hy_6m > 0:
        scores["가을"] += 1
    if ff_pos_pct is not None and ff_pos_pct >= 70:
        scores["가을"] += 1
    if (fpe_now is not None and fpe_now >= 22) or (cape_now is not None and cape_now >= 35):
        scores["가을"] += 1
    if hy_6m is not None and hy_now is not None and hy_pct is not None and hy_6m > 0 and hy_pct > 3.5:
        scores["가을"] += 1
    if wti_3m is not None and spx_3m is not None and wti_3m > 15 and spx_3m < 0:
        scores["가을"] += 1
    if sox_3m is not None and spx_3m is not None and spx_6m is not None and sox_3m < spx_3m and spx_6m > 0:
        scores["가을"] += 1
    if cape_now is not None and cape_now >= 35:
        scores["가을"] += 1
    # ── 겨울 박스 ──
    if hy_pct is not None and hy_pct > 5:
        scores["겨울"] += 1
    if ff_6m is not None and ff_6m < 0:
        scores["겨울"] += 1
    if qqq_dd is not None and qqq_dd < -20 and spx_1m is not None and abs(spx_1m) < 3:
        scores["겨울"] += 1
    if unemp_3m is not None and unemp_3m > 0.5:
        scores["겨울"] += 1
    if spy_1m is not None and rsp_1m is not None and spy_1m > 0 and rsp_1m < 0:
        scores["겨울"] += 1
    if sox_1m is not None and spx_1m is not None and qqq_dd is not None and sox_1m > spx_1m and qqq_dd < -15:
        scores["겨울"] += 1

    if all(v == 0 for v in scores.values()):
        return None, None
    best = max(scores.items(), key=lambda x: x[1])
    return best[0], scores


def _build_state_at_offset_full(raw, offset):
    """offset 시점의 10차원 enum (시간 가변). None 가능."""
    season, _ = _evaluate_season_at_offset(raw, offset)
    if season is None: return None

    # FF 위치 / 행동
    # ff_pos: 10y → 5y → 3y → 2y fallback (사용자 fetch 기간이 5y default 라 10y 미달)
    _ff_s_loc = raw.get("ff_s")
    ff_pos_pct = None
    for _lb in (252*10, 252*5, 252*3, 252*2, 252):
        ff_pos_pct = _percentile_at(_ff_s_loc, offset, _lb)
        if ff_pos_pct is not None: break
    ff_3m_chg = _abs_change_at(raw.get("ff_s"), offset, 90)
    ff_pos = ("high" if ff_pos_pct is not None and ff_pos_pct >= 70 else
              "low" if ff_pos_pct is not None and ff_pos_pct < 30 else
              "mid" if ff_pos_pct is not None else None)
    ff_action = ("hiking" if ff_3m_chg is not None and ff_3m_chg > 0.25 else
                 "cutting" if ff_3m_chg is not None and ff_3m_chg < -0.25 else
                 "hold" if ff_3m_chg is not None else None)

    # 인플레 추세
    cpi = raw.get("cpi_yoy_s")
    cpi_now = _safe_iloc_at(cpi, offset); cpi_3m_chg = _abs_change_at(cpi, offset, 90)
    if cpi_now is None or cpi_3m_chg is None:
        infl = "unknown"
    elif cpi_3m_chg > 0.3:
        infl = "accelerating"
    elif cpi_now >= 4.0 and cpi_3m_chg <= 0:
        infl = "peaking"
    elif cpi_3m_chg < -0.2:
        infl = "cooling"
    elif abs(cpi_3m_chg) <= 0.2 and cpi_now < 3.5:
        infl = "stable"
    else:
        infl = "unknown"

    # 밸류 (fpe 우선, 부재 시 cape 단독)
    fpe_now = _safe_iloc_at(raw.get("fpe_s"), offset)
    cape_now = _safe_iloc_at(raw.get("cape_s"), offset)
    if fpe_now is not None:
        if fpe_now >= 25 or (cape_now is not None and cape_now >= 35):
            valuation = "extreme"
        elif fpe_now >= 22 or (cape_now is not None and cape_now >= 28):
            valuation = "high"
        elif fpe_now < 18:
            valuation = "low"
        else:
            valuation = "normal"
    elif cape_now is not None:
        if cape_now >= 35:
            valuation = "extreme"
        elif cape_now >= 28:
            valuation = "high"
        elif cape_now < 20:
            valuation = "low"
        else:
            valuation = "normal"
    else:
        valuation = None

    # 신용
    hy_now = _safe_iloc_at(raw.get("hy_s"), offset)
    hy_6m = _abs_change_at(raw.get("hy_s"), offset, 180)
    hy_pct = (hy_now * 100) if (hy_now is not None and hy_now <= 1.0) else hy_now
    if hy_pct is None:
        cred = "normal"
    elif hy_pct < 3.5:
        cred = "tight"
    elif hy_pct < 5.0 and (hy_6m is None or hy_6m <= 0):
        cred = "normal"
    elif hy_6m is not None and hy_6m > 0 and hy_pct < 7:
        cred = "widening"
    elif hy_pct >= 7:
        cred = "panic"
    else:
        cred = "normal"

    # yield_curve (10Y-3M 기준, date-based)
    inv3 = raw.get("t10y3m_s")
    inv_now = _safe_iloc_at(inv3, offset)
    inv_min_252 = None
    try:
        _inv3_t = _trim_series_at_offset(inv3, offset)
        if _inv3_t is not None and len(_inv3_t) >= 252:
            inv_min_252 = float(_inv3_t.iloc[-252:].min())
    except Exception: pass
    if inv_now is None:
        yc = None
    elif inv_now < -0.05 and inv_min_252 is not None and inv_now <= inv_min_252 * 0.5:
        yc = "deepening"
    elif inv_now < -0.05:
        yc = "deep_stable"
    elif inv_now < 0:
        yc = "entering"
    elif inv_min_252 is not None and inv_min_252 < -0.05 and inv_now >= 0:
        yc = "recovering"
    else:
        yc = "normal"
    yc_simple = ("inverted" if yc in ("entering", "deepening", "deep_stable") else
                 "recovering" if yc == "recovering" else
                 "normal" if yc == "normal" else None)

    # 반도체
    sox_3m = _pct_change_at(raw.get("sox_s"), offset, 63)
    spx_3m = _pct_change_at(raw.get("spx_s"), offset, 63)
    if sox_3m is None or spx_3m is None:
        semi = None
    elif sox_3m > spx_3m + 1:
        semi = "up"
    elif sox_3m < spx_3m - 1:
        semi = "down"
    else:
        semi = "flat"

    # 달러
    dxy_now = _safe_iloc_at(raw.get("dxy_s"), offset)
    if dxy_now is None: doll = "neutral"
    elif dxy_now < 95: doll = "weak"
    elif dxy_now < 102: doll = "neutral"
    else: doll = "strong"

    # 외부충격
    wti_3m = _pct_change_at(raw.get("wti_s"), offset, 63)
    if wti_3m is not None and wti_3m > 50:
        sh = "geopolitical"
    elif wti_3m is not None and wti_3m > 30:
        sh = "oil"
    elif hy_6m is not None and hy_6m > 1.5:
        sh = "financial"
    else:
        sh = "none"

    return {
        "season":           season.lstrip("초늦") if season else None,
        "ff_pos":           ff_pos,
        "ff_action":        ff_action,
        "inflation_trend":  infl,
        "valuation":        valuation,
        "credit":           cred,
        "yield_curve":      yc_simple,
        "semiconductor":    semi,
        "dollar":           doll,
        "external_shock":   sh,
    }


def _is_already_backfilled(obs_path):
    """백필 마커 존재 확인."""
    try:
        if not Path(obs_path).exists(): return False
        import json as _json
        with open(obs_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    if _json.loads(line).get("backfilled") is True:
                        return True
                except Exception:
                    continue
        return False
    except Exception:
        return False


def _read_backfill_marker(obs_path):
    """가장 최근 _backfill_marker 행 반환. 없으면 None."""
    try:
        if not Path(obs_path).exists(): return None
        import json as _json
        markers = []
        with open(obs_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    r = _json.loads(line)
                    if r.get("_backfill_marker") is True:
                        markers.append(r)
                except Exception: continue
        if not markers: return None
        markers.sort(key=lambda r: r.get("ts") or r.get("date") or "")
        return markers[-1]
    except Exception:
        return None


def _backfill_observations(raw_data, days_back, obs_path, force=False):
    """과거 days_back 일치 매칭 시뮬 → obs.jsonl prepend. 마커 존재 시 skip (force=True 면 무시).
    반환: dict {ok, fail, blocked_by_earliest, total_attempted, marker_written}.

    V3.10.4 (force=True 시): 기존 백필/마커 행 모두 삭제 후 새로 채움 (clean restart).
    earliest 차단 기준: 실측 행만 (backfilled 무시) — 옛 백필이 차단 기준 되는 문제 해결.
    """
    import json as _json
    from datetime import datetime as _dt, timedelta as _td
    if not force and _is_already_backfilled(obs_path):
        return {"ok": 0, "fail": 0, "blocked_by_earliest": 0, "total_attempted": 0, "marker_written": False, "skipped": True}
    Path(obs_path).parent.mkdir(parents=True, exist_ok=True)

    existing = []
    earliest = None
    if Path(obs_path).exists():
        try:
            with open(obs_path, "r", encoding="utf-8") as f:
                for line in f:
                    try: existing.append(_json.loads(line))
                    except Exception: continue
            # force=True: 기존 백필 + 마커 모두 제거 (clean restart)
            if force:
                existing = [r for r in existing if not (r.get("backfilled") or r.get("_backfill_marker"))]
            dts = []
            for r in existing:
                try:
                    d = (r.get("date") or "").split()[0]
                    # earliest 계산: 실측 행 + history_era_top1 있는 것만 (옛 백필은 위에서 제거됨)
                    if d and r.get("history_era_top1") and not r.get("backfilled"):
                        dts.append(_dt.fromisoformat(d))
                except Exception: continue
            if dts: earliest = min(dts)
        except Exception: pass

    today = _dt.now()
    new_rows = []; failed = 0; blocked = 0
    for off in range(days_back, 0, -1):
        target = today - _td(days=off)
        if earliest is not None and target >= earliest: blocked += 1; continue
        state = _build_state_at_offset_full(raw_data, off)
        if state is None: failed += 1; continue
        matches = _era_distance_match(state, top_n=3)
        if not matches: failed += 1; continue
        # V3.10.4 fix: 백필은 임계 무시하고 raw top1 항상 박음 — 라이브 obs 는 그대로 (threshold 적용).
        # 사이클 진행도 metric 이 score 살짝 미달인 날들도 같은 era 로 잡을 수 있게.
        top1 = matches[0]
        top1_id = top1["era"]["id"]; top1_sc = top1["score"]
        season_then, scores_then = _evaluate_season_at_offset(raw_data, off)
        new_rows.append({
            "ts": target.isoformat(timespec="seconds"),
            "date": target.strftime("%Y-%m-%d"),
            "backfilled": True,
            "season": season_then,
            "season_score_spring": (scores_then or {}).get("봄"),
            "season_score_summer": (scores_then or {}).get("여름"),
            "season_score_autumn": (scores_then or {}).get("가을"),
            "season_score_winter": (scores_then or {}).get("겨울"),
            "history_era_top1": top1_id,
            "history_score_top1": round(top1_sc, 3) if top1_sc is not None else None,
            "history_era_top2": (matches[1]["era"]["id"] if len(matches) > 1 else None),
            "history_score_top2": (round(matches[1]["score"], 3) if len(matches) > 1 else None),
            "history_era_top3": (matches[2]["era"]["id"] if len(matches) > 2 else None),
            "history_score_top3": (round(matches[2]["score"], 3) if len(matches) > 2 else None),
            # V3.10.4: 그 시점 매크로 상태 10차원 enum (진단/사후분석)
            "state_season":           state.get("season"),
            "state_ff_pos":           state.get("ff_pos"),
            "state_ff_action":        state.get("ff_action"),
            "state_inflation_trend":  state.get("inflation_trend"),
            "state_valuation":        state.get("valuation"),
            "state_credit":           state.get("credit"),
            "state_yield_curve":      state.get("yield_curve"),
            "state_semiconductor":    state.get("semiconductor"),
            "state_dollar":           state.get("dollar"),
            "state_external_shock":   state.get("external_shock"),
        })
    # 마커 row — 항상 1행 추가 (0 성공이어도 재실행 차단). force=True 시도 항상 새 마커 박힘.
    marker_row = {
        "ts": today.isoformat(timespec="seconds"),
        "date": today.strftime("%Y-%m-%d"),
        "backfilled": True,
        "_backfill_marker": True,
        "_backfill_days_attempted": days_back,
        "_backfill_ok": len(new_rows),
        "_backfill_fail": failed,
        "_backfill_blocked": blocked,
    }
    new_rows.append(marker_row)
    all_rows = new_rows + existing
    all_rows.sort(key=lambda r: (r.get("date") or "") + (r.get("ts") or ""))
    with open(obs_path, "w", encoding="utf-8") as f:
        for r in all_rows:
            f.write(_json.dumps(r, ensure_ascii=False, default=str) + "\n")
    return {
        "ok": len(new_rows) - 1,  # 마커 제외
        "fail": failed,
        "blocked_by_earliest": blocked,
        "total_attempted": days_back,
        "marker_written": True,
        "skipped": False,
    }


def _build_raw_data_for_backfill(fd, yd, fpe_s_local=None, cape_s_local=None, cpi_yoy_s_local=None):
    """main() 의 fd / yd 에서 백필용 raw 시리즈 dict 빌드.
    V3.10.4: CPI YoY 자동 빌드 (월간 → 일간 ffill) — inflation_trend 차원 작동."""
    _spx = yd.get("SPX")
    if _spx is None: _spx = yd.get("^GSPC")
    _vix = fd.get("VIXCLS")
    if _vix is None: _vix = yd.get("^VIX")
    # CPI YoY 일간 리샘플 (월간 → ffill) — backfill 의 inflation_trend 차원 활성화
    _cpi_yoy = cpi_yoy_s_local
    if _cpi_yoy is None:
        _cpi_m = fd.get("CPIAUCSL")
        if _cpi_m is not None and len(_cpi_m) >= 13:
            try:
                _yoy_m = (_cpi_m / _cpi_m.shift(12) - 1) * 100
                _yoy_m = _yoy_m.dropna()
                if len(_yoy_m) > 0:
                    _cpi_yoy = _yoy_m.resample("D").ffill()
            except Exception: pass
    return {
        "ff_s":      fd.get("FEDFUNDS"),
        "hy_s":      fd.get("HY"),
        "t10y3m_s":  fd.get("T10Y3M"),
        "t10y2y_s":  fd.get("T10Y2Y"),
        "wti_s":     fd.get("WTI"),
        "spx_s":     _spx,
        "sox_s":     yd.get("SOXX"),
        "qqq_s":     yd.get("QQQ"),
        "rsp_s":     yd.get("RSP"),
        "spy_s":     yd.get("SPY"),
        "dxy_s":     yd.get("DX-Y.NYB"),
        "vix_s":     _vix,
        "cpi_yoy_s": _cpi_yoy,
        "fpe_s":     fpe_s_local,
        "cape_s":    cape_s_local,
        "unrate_s":  fd.get("UNRATE"),
        "payems_s":  fd.get("PAYEMS"),
        "umich_s":   fd.get("UMCSENT"),
        "drccl_s":   fd.get("DRCCLACBS"),
        "xle_s":     yd.get("XLE"),
        "xlk_s":     yd.get("XLK"),
        # V3.11.1: DTW 8 canonical 보강 (cpi raw, t10y, indpro)
        "cpi_s":     fd.get("CPIAUCSL"),
        "t10y_s":    fd.get("DGS10"),
        "indpro_s":  fd.get("INDPRO"),
    }


# ═══ V3.12.0 임의 시점 매크로 조회 ═══
@st.cache_data(ttl=86400, show_spinner=False)
def _build_long_range_raw(api_key, start="1990-01-01"):
    """시점 조회 전용 raw_data 페치. yfinance period="max" + FRED start=1990.
    사이드바 '관찰 기간' (3/5/10년) 과 무관 — query 탭 에서만 사용. 24h 캐싱."""
    out = {}
    _fred_targets = {
        "ff_s": "FEDFUNDS", "hy_s": "BAMLH0A0HYM2",
        "t10y3m_s": "T10Y3M", "t10y2y_s": "T10Y2Y",
        "wti_s": "DCOILWTICO", "cpi_s": "CPIAUCSL",
        "t10y_s": "DGS10", "indpro_s": "INDPRO",
        "unrate_s": "UNRATE", "payems_s": "PAYEMS",
        "umich_s": "UMCSENT", "drccl_s": "DRCCLACBS",
        "vix_fred_s": "VIXCLS",
    }
    if api_key:
        try:
            from fredapi import Fred as _Fred
            _fr = _Fred(api_key=api_key)
            for _k, _sid in _fred_targets.items():
                try:
                    _s = _fr.get_series(_sid, observation_start=start).dropna()
                    if len(_s) > 0: out[_k] = _s
                except Exception: continue
        except Exception: pass
    _yf_targets = {
        "spx_s": "^GSPC", "qqq_s": "QQQ", "sox_s": "SOXX",
        "rsp_s": "RSP", "spy_s": "SPY", "dxy_s": "DX-Y.NYB",
        "xle_s": "XLE", "xlk_s": "XLK", "vix_yf_s": "^VIX",
    }
    try:
        import yfinance as _yf
        for _k, _tkr in _yf_targets.items():
            try:
                _h = _yf.Ticker(_tkr).history(period="max", auto_adjust=True)
                if _h is None or len(_h) == 0: continue
                _s = _h["Close"].dropna()
                try: _s.index = _s.index.tz_localize(None)
                except Exception: pass
                out[_k] = _s
            except Exception: continue
    except Exception: pass
    # vix_s: FRED 우선, 없으면 yfinance fallback
    out["vix_s"] = out.get("vix_fred_s") if out.get("vix_fred_s") is not None else out.get("vix_yf_s")
    # CPI YoY 합성 (월간 → 일간 ffill)
    try:
        _cpi = out.get("cpi_s")
        if _cpi is not None and len(_cpi) >= 13:
            _yoy = (_cpi / _cpi.shift(12) - 1) * 100
            out["cpi_yoy_s"] = _yoy.dropna().resample("D").ffill()
    except Exception: pass
    out.setdefault("cape_s", None)
    out.setdefault("fpe_s", None)
    out.setdefault("te_s", None)
    out.setdefault("eg_s", None)
    # STEP A-2 / STEP 5-1 / STEP 5-3 / STEP 5-8: historical_loader 합본 적재
    # — HY OAS Wayback 합본 (1996-12~) / CAPE multpl (1990~) /
    #   trailing earnings te+eg / forward_pe 자동 누적 + daily ffill
    try:
        from historical_loader import load_hy_oas_history, load_cape_history, load_forward_pe_history
        _hy_hist = load_hy_oas_history()
        if _hy_hist is not None and len(_hy_hist) > 0:
            out["hy_s"] = _hy_hist  # FRED 직접 fetch 결과 override (3년 제한 우회)
        _cape_hist = load_cape_history()
        if _cape_hist is not None and len(_cape_hist) > 0:
            out["cape_s"] = _cape_hist
        _fpe_hist = load_forward_pe_history()
        if _fpe_hist is not None and len(_fpe_hist) > 0:
            try:
                _idx = pd.date_range(start=_fpe_hist.index.min(),
                                      end=pd.Timestamp.now().normalize(), freq="D")
                out["fpe_s"] = _fpe_hist.reindex(_idx, method="ffill")
            except Exception:
                out["fpe_s"] = _fpe_hist
        try:
            _te_path = SD / "cache" / "trailing_earnings_history.json"
            if _te_path.exists():
                with open(_te_path, "r", encoding="utf-8") as _f:
                    _te_obj = json.load(_f)
                _te_df = pd.DataFrame(_te_obj.get("data") or [])
                if len(_te_df) > 0:
                    _te_df["date"] = pd.to_datetime(_te_df["date"])
                    _te_df = _te_df.set_index("date").sort_index()
                    if "te" in _te_df.columns:
                        _te = _te_df["te"].dropna()
                        if len(_te) > 0: out["te_s"] = _te
                    if "eg" in _te_df.columns:
                        _eg = _te_df["eg"].dropna()
                        if len(_eg) > 0: out["eg_s"] = _eg
        except Exception: pass
    except Exception:
        pass
    return out


def _month_last_business_day(year, month):
    """그 달 마지막 영업일 (월~금 중 마지막). 미래월 이면 today."""
    from datetime import date as _d, timedelta as _tdt
    today = _d.today()
    if year > today.year or (year == today.year and month > today.month):
        return today
    # 다음 달 1일에서 -1일
    if month == 12:
        nxt = _d(year + 1, 1, 1)
    else:
        nxt = _d(year, month + 1, 1)
    last = nxt - _tdt(days=1)
    while last.weekday() >= 5:
        last -= _tdt(days=1)
    if last > today:
        last = today
    return last


def _compute_on_the_fly(target_date, raw_data):
    """target_date 시점 매크로 즉석 계산 (raw 시계열 기반).
    반환 스키마는 _query_macro_at_month 와 동일 (source="on_the_fly"). DTW V1 생략."""
    from datetime import date as _d
    today = _d.today()
    days_offset = max(0, (today - target_date).days)
    season_then, scores_then = _evaluate_season_at_offset(raw_data, days_offset)
    if season_then is None:
        return {
            "source": "unavailable",
            "query_date": target_date.isoformat(),
            "actual_date": None,
            "season": None, "season_scores": {},
            "macro_score": None,
            "history_match": None,
            "dtw_progress": None,
            "active_boxes": {},
            "reason": "raw 시계열 부족 — 9박스 평가 불가",
        }
    state_then = _build_state_at_offset_full(raw_data, days_offset)
    matches = _era_distance_match(state_then, top_n=3) if state_then else []
    hist_match = None
    if matches:
        _m1 = matches[0]; _m2 = matches[1] if len(matches) > 1 else None
        _m3 = matches[2] if len(matches) > 2 else None
        hist_match = {
            "era_top1":   _m1["era"]["id"],
            "label_top1": _m1["era"].get("label"),
            "comment_top1": _m1["era"].get("comment"),
            "score_top1": _m1["score"],
            "era_top2":   _m2["era"]["id"] if _m2 else None,
            "score_top2": _m2["score"] if _m2 else None,
            "era_top3":   _m3["era"]["id"] if _m3 else None,
            "score_top3": _m3["score"] if _m3 else None,
        }
    return {
        "source": "on_the_fly",
        "query_date": target_date.isoformat(),
        "actual_date": target_date.isoformat(),
        "season": season_then,
        "season_scores": dict(scores_then or {}),
        "macro_score": None,  # 즉석 mac_score 계산 V1 생략 (full_score 함수 시간 가변 X)
        "history_match": hist_match,
        "dtw_progress": None,  # V1: 과거 시점 DTW 별도 처리 필요 — 다음 버전
        "active_boxes": dict(scores_then or {}),
        "reason": None,
    }


def _season_emoji(season):
    return {"봄": "🌸", "여름": "🌞", "가을": "🍂", "겨울": "❄️"}.get(season, "📊")


def _strip_season_prefix(season):
    """'초여름'/'늦가을' → '여름'/'가을' 등 접두사 제거."""
    if not season: return season
    s = season
    while s and s[0] in ("초", "늦"):
        s = s[1:]
    return s


def _month_business_days(year, month):
    """그 달 모든 영업일 (월~금) 리스트. 미래일 절단."""
    from datetime import date as _d, timedelta as _tdt
    today = _d.today()
    if month == 12:
        nxt = _d(year + 1, 1, 1)
    else:
        nxt = _d(year, month + 1, 1)
    end = nxt - _tdt(days=1)
    if end > today:
        end = today
    out = []
    cur = _d(year, month, 1)
    while cur <= end:
        if cur.weekday() < 5:
            out.append(cur)
        cur += _tdt(days=1)
    return out


def _derive_season_from_boxes(box_states):
    """V3.12.1+ (B): 36박스 결과에서 일별 best season 산정.
    - 점수 = fire / valid (None 박스 분모 제외).
    - best = ratio desc → fire abs desc → 정의서 우선순위 (겨울>가을>여름>봄).
    - 접두사: V3.8 — 인접 계절 fire ≥ 4 시 '초'/'늦' 부착.
    - 반환 (season_with_prefix or None, fire_dict)."""
    if not box_states: return None, {}
    if not all(len(lst) > 0 for lst in box_states.values()): return None, {}
    fire = {"봄": 0, "여름": 0, "가을": 0, "겨울": 0}
    valid = {"봄": 0, "여름": 0, "가을": 0, "겨울": 0}
    for _se, _lst in box_states.items():
        for _lbl, _b in _lst:
            if _b is None: continue
            valid[_se] += 1
            if _b: fire[_se] += 1
    ratios = {_se: (fire[_se] / valid[_se] if valid[_se] else 0.0) for _se in fire}
    if max(ratios.values()) <= 0:
        return None, fire
    # 정의서 우선순위 (겨울 가장 보수, 봄 가장 후순) — 동점 시 보수적 선택
    _priority = {"겨울": 0, "가을": 1, "여름": 2, "봄": 3}
    best = sorted(fire.keys(), key=lambda s: (-ratios[s], -fire[s], _priority[s]))[0]
    # 인접 계절 fire ≥ 4 → prefix. _v8_prefix 와 동일 룰:
    #   nxt 우세 (앞으로 진행) → "늦" / prv 우세 (뒤에서 옴) → "초"
    # 이전 버전은 swap 됐었음 — 수정.
    _cycle = ["봄", "여름", "가을", "겨울"]
    _bi = _cycle.index(best)
    _nxt = _cycle[(_bi + 1) % 4]; _prv = _cycle[(_bi - 1) % 4]
    label = best
    if fire.get(_nxt, 0) >= 4: label = "늦" + best
    elif fire.get(_prv, 0) >= 4: label = "초" + best
    return label, fire


def _eval_one_day(target_date, raw_data, obs_rows_by_date):
    """단일 영업일 평가. live → backfill → on_the_fly 우선순위.
    box_states: 그 시점 36박스 boolean (raw_data 기반 mirror) — 항상 raw 로 재계산.
    season: V3.12.1+ (B) 부터 box_states 기반으로 재산정 (obs.jsonl row 미수정).
    box_states fail 시 chosen.season 폴백."""
    from datetime import date as _d
    d_iso = target_date.isoformat()
    rows = obs_rows_by_date.get(d_iso) or []
    live = [r for r in rows if not r.get("backfilled")]
    bf   = [r for r in rows if r.get("backfilled")]
    chosen = None; src = None
    if live:
        chosen = live[-1]; src = "live"
    elif bf:
        chosen = bf[-1]; src = "backfill"
    _today = _d.today()
    _off = max(0, (_today - target_date).days)
    try:
        # V3.12.1: 9박스 (auto_season label 1:1) — 계절별 9개 균일
        _box_states = _diagnose_9boxes_at_offset(raw_data, _off)
    except Exception as _be:
        print(f"[box diag fail {target_date}] {type(_be).__name__}: {_be}")
        _box_states = {}
    # STEP C: 단기 경보 카드 5종 — 박스와 무관 별도 출력
    try:
        from short_term_alerts import evaluate_short_term_alerts
        _alerts = evaluate_short_term_alerts(target_date, raw_data)
    except Exception as _ae:
        print(f"[alerts fail {target_date}] {type(_ae).__name__}: {_ae}")
        _alerts = {}
    # V3.12.1+ (B): box_states 기반 일별 season 재산정
    _box_season, _box_fire = _derive_season_from_boxes(_box_states)
    if chosen is not None:
        _ss_obs = {
            "봄":   chosen.get("season_score_spring") or 0,
            "여름": chosen.get("season_score_summer") or 0,
            "가을": chosen.get("season_score_autumn") or 0,
            "겨울": chosen.get("season_score_winter") or 0,
        }
        # box-derived 우선, 실패 시 obs row 폴백
        _final_season = _box_season if _box_season is not None else chosen.get("season")
        _final_scores = _box_fire if _box_fire else _ss_obs
        return {
            "date": d_iso,
            "source": src,
            "season": _final_season,
            "season_scores": _final_scores,
            "history_top1_id": chosen.get("history_era_top1"),
            "history_top1_score": chosen.get("history_score_top1"),
            "history_top2_id": chosen.get("history_era_top2"),
            "history_top2_score": chosen.get("history_score_top2"),
            "history_top3_id": chosen.get("history_era_top3"),
            "history_top3_score": chosen.get("history_score_top3"),
            "macro_score": chosen.get("mac_score"),
            "active_boxes": _final_scores,
            "box_states": _box_states,
            "alerts": _alerts,
        }
    otf = _compute_on_the_fly(target_date, raw_data)
    if otf.get("source") == "unavailable":
        return {"date": d_iso, "source": "unavailable", "season": None,
                "season_scores": {}, "history_top1_id": None,
                "history_top1_score": None, "history_top2_id": None,
                "history_top2_score": None, "history_top3_id": None,
                "history_top3_score": None, "macro_score": None,
                "active_boxes": {}, "box_states": _box_states,
                "alerts": _alerts}
    _hm = otf.get("history_match") or {}
    # box-derived 우선, 실패 시 on_the_fly 결과 폴백
    _otf_season = _box_season if _box_season is not None else otf.get("season")
    _otf_scores = _box_fire if _box_fire else (otf.get("season_scores") or {})
    return {
        "date": d_iso,
        "source": "on_the_fly",
        "season": _otf_season,
        "season_scores": _otf_scores,
        "history_top1_id":   _hm.get("era_top1"),
        "history_top1_score": _hm.get("score_top1"),
        "history_top2_id":   _hm.get("era_top2"),
        "history_top2_score": _hm.get("score_top2"),
        "history_top3_id":   _hm.get("era_top3"),
        "history_top3_score": _hm.get("score_top3"),
        "macro_score": None,
        "active_boxes": _otf_scores,
        "box_states": _box_states,
        "alerts": _alerts,
    }


def _compute_month_summary(daily_results):
    """일별 결과 → 요약 통계 dict.
    변동성: era 전환 횟수 + 계절 전환 횟수 기반 (V3.12.1 직후 임계 재조정)."""
    import numpy as _np
    from collections import Counter as _Cnt
    valid = [r for r in daily_results if r.get("season")]
    season_counter = _Cnt(_strip_season_prefix(r["season"]) for r in valid if r.get("season"))
    dominant = season_counter.most_common(1)[0][0] if season_counter else None
    score_arrays = {k: [] for k in ("봄", "여름", "가을", "겨울")}
    for r in valid:
        ss = r.get("season_scores") or {}
        for k in score_arrays:
            v = ss.get(k)
            if v is not None: score_arrays[k].append(v)
    means = {k: (float(_np.mean(v)) if v else 0.0) for k, v in score_arrays.items()}
    stds  = {k: (float(_np.std(v))  if v else 0.0) for k, v in score_arrays.items()}
    era_counter = _Cnt(r["history_top1_id"] for r in daily_results if r.get("history_top1_id"))
    era_avg = {}
    for eid in era_counter:
        scs = [r["history_top1_score"] for r in daily_results
               if r.get("history_top1_id") == eid and r.get("history_top1_score") is not None]
        era_avg[eid] = float(_np.mean(scs)) if scs else 0.0
    # era / 계절 전환 횟수 (연속 변경 지점)
    n_era_tr = 0
    _prev_era = None
    for r in daily_results:
        _e = r.get("history_top1_id")
        if _e is None: continue
        if _prev_era is not None and _e != _prev_era: n_era_tr += 1
        _prev_era = _e
    n_season_tr = 0
    _prev_se = None
    for r in daily_results:
        _se = _strip_season_prefix(r.get("season") or "") or None
        if _se is None: continue
        if _prev_se is not None and _se != _prev_se: n_season_tr += 1
        _prev_se = _se
    if n_era_tr <= 1 and n_season_tr <= 1: vol = "낮음"
    elif n_era_tr <= 4 and n_season_tr <= 2: vol = "중간"
    else: vol = "높음"
    avg_std = float(_np.mean(list(stds.values()))) if stds else 0.0
    # 박스별 valid/fire 분리: value 가 None 인 일자는 valid 에서 제외 (평가 불가)
    box_fire = {"봄": {}, "여름": {}, "가을": {}, "겨울": {}}
    box_valid_days = 0  # 박스 시스템 자체가 동작한 일수 (top-level)
    box_eval_failed_days = 0
    for r in daily_results:
        bs = r.get("box_states")
        if bs is None or bs == {}:
            box_eval_failed_days += 1
            continue
        if all(len(lst) == 0 for lst in bs.values()):
            box_eval_failed_days += 1
            continue
        box_valid_days += 1
        for _se, _list in bs.items():
            for _lbl, _b in _list:
                _slot = box_fire[_se].setdefault(_lbl, {"fire": 0, "valid": 0, "label": _lbl})
                if _b is None: continue  # 평가 불가 — 분모에 안 들어감
                _slot["valid"] += 1
                if _b: _slot["fire"] += 1
    box_aggregated = {}
    for _se in ("봄", "여름", "가을", "겨울"):
        _items = []
        for _lbl, _slot in box_fire[_se].items():
            _items.append((_lbl, _slot["fire"], _slot["valid"]))  # per-box valid
        box_aggregated[_se] = _items
    # ── STEP C: 단기 경보 카드 5종 월별 집계 ──
    _alert_cards = ("변동성_폭발", "급락", "신용_급격_확장", "단기_역전", "변동성_클러스터")
    _alert_fire_by_card = {c: 0 for c in _alert_cards}
    _alert_daily_fires = []
    for r in daily_results:
        a = r.get("alerts") or {}
        if not a: continue
        n = 0
        for c in _alert_cards:
            if a.get(c):
                _alert_fire_by_card[c] += 1
                n += 1
        _alert_daily_fires.append(n)
    _alert_avg = (sum(_alert_daily_fires) / len(_alert_daily_fires)) if _alert_daily_fires else 0.0
    _alert_max = max(_alert_daily_fires) if _alert_daily_fires else 0
    if _alert_avg >= 2.0: _alert_sev = ("shock", "🔴 충격")
    elif _alert_avg >= 1.0: _alert_sev = ("alert", "🟠 경계")
    elif _alert_avg >= 0.3: _alert_sev = ("warn", "🟡 주의")
    else: _alert_sev = ("none", "🟢 잠잠")
    return {
        "season_distribution": dict(season_counter),
        "dominant_season": dominant,
        "season_scores_mean": means,
        "season_scores_std": stds,
        "top1_era_distribution": dict(era_counter),
        "top1_era_avg_score": era_avg,
        "volatility": vol,
        "avg_score_std": round(avg_std, 2),
        "n_era_transitions": n_era_tr,
        "n_season_transitions": n_season_tr,
        "box_aggregated": box_aggregated,
        "box_valid_days": box_valid_days,
        "box_eval_failed_days": box_eval_failed_days,
        "alerts_fire_by_card": _alert_fire_by_card,
        "alerts_avg_per_day": _alert_avg,
        "alerts_max_in_day": _alert_max,
        "alerts_severity_key": _alert_sev[0],
        "alerts_severity_label": _alert_sev[1],
        "alerts_n_eval_days": len(_alert_daily_fires),
    }


def _compute_timeline(daily_results):
    """일별 era 변화를 연속 구간으로 압축."""
    timeline = []; cur = None
    for r in daily_results:
        eid = r.get("history_top1_id")
        if eid is None: continue
        if cur is None or cur["era_id"] != eid:
            if cur is not None: timeline.append(cur)
            cur = {
                "from_date": r["date"],
                "to_date": r["date"],
                "era_id": eid,
                "season": _strip_season_prefix(r.get("season") or ""),
                "n_days": 1,
            }
        else:
            cur["to_date"] = r["date"]
            cur["n_days"] += 1
    if cur is not None: timeline.append(cur)
    return timeline


def _inv_state_at_offset(s, offset, lookback=252):
    """V3.12.1: _inv_state 의 시점 가변 버전. date-based trim 후 production 로직 동일.
    offset=0 이면 _inv_state 와 동일 출력."""
    if s is None: return None
    s2 = _trim_series_at_offset(s, offset)
    if s2 is None: return None
    s2 = s2.dropna()
    if len(s2) < 60: return None
    cur = float(s2.iloc[-1])
    tail = s2.iloc[-min(len(s2), lookback):]
    peak_pp = float(tail.min())
    s_60d = float(s2.iloc[-60]) if len(s2) >= 60 else None
    s_6m  = float(s2.iloc[-126]) if len(s2) >= 126 else None
    if s_60d is not None and s_60d >= 0 and cur < 0: return "entering"
    if cur < 0 and s_60d is not None and (cur - s_60d) <= -0.10: return "deepening"
    if cur < 0 and s_6m is not None and s_6m < 0 and peak_pp < 0:
        rec_pct = (cur - peak_pp) / abs(peak_pp) * 100
        if rec_pct < 50: return "deep_stable"
    if peak_pp < 0:
        rec_pct = (cur - peak_pp) / abs(peak_pp) * 100
        if rec_pct >= 50: return "recovering"
    if cur > 0 and peak_pp >= 0: return "normal"
    return None


def _diagnose_9boxes_at_offset(raw, offset):
    """V8.0 머지 — 시점 가변 V8 1층 40박스 평가. 라벨은 V8_BOX_LABELS 와 1:1.
    UI 호환: checks dict {계절: [(label, bool), ...]} 형식 반환.
    """
    try:
        v8_res = V8L1.evaluate_v8_layer1(raw, offset)
        boxes = v8_res.get("boxes", {})
    except Exception as _e:
        print(f"[V8 box diag fail offset={offset}] {type(_e).__name__}: {_e}")
        return {}
    out = {}
    for season in ("봄", "여름", "가을", "겨울"):
        items = []
        for box_id in V8_SEASON_BOXES[season]:
            label = V8_BOX_LABELS.get(box_id, box_id)
            val = boxes.get(box_id)
            items.append((label, val is True))
        out[season] = items
    return out



def _diagnose_box_booleans(raw, offset):
    """V3.12.1 진단: 그 시점 9박스 boolean 식 + raw 입력값 dump.
    _evaluate_season_at_offset 의 box 식을 mirror — 어느 박스가 fire 했는지 확인."""
    qqq = raw.get("qqq_s"); ff = raw.get("ff_s"); hy = raw.get("hy_s")
    unrate = raw.get("unrate_s"); inv3m10y = raw.get("t10y3m_s"); cpi = raw.get("cpi_yoy_s")
    cape = raw.get("cape_s"); fpe = raw.get("fpe_s"); vix = raw.get("vix_s")
    sox = raw.get("sox_s"); spx = raw.get("spx_s"); rsp = raw.get("rsp_s"); spy = raw.get("spy_s")
    wti = raw.get("wti_s")
    qqq_dd = None
    try:
        _qqq_t = _trim_series_at_offset(qqq, offset)
        if _qqq_t is not None and len(_qqq_t) >= 252:
            window = _qqq_t.iloc[-252:]
            cur = float(_qqq_t.iloc[-1])
            high_52w = float(window.max())
            if high_52w > 0: qqq_dd = (cur / high_52w - 1) * 100
    except Exception: pass
    unemp_now = _safe_iloc_at(unrate, offset)
    unemp_3m = _abs_change_at(unrate, offset, 90)
    ff_6m = _abs_change_at(ff, offset, 180)
    ff_pos_pct = _percentile_at(ff, offset, 252 * 10)
    hy_now = _safe_iloc_at(hy, offset)
    hy_6m = _abs_change_at(hy, offset, 180)
    hy_pct = (hy_now * 100) if (hy_now is not None and hy_now <= 1.0) else hy_now
    inv_now = _safe_iloc_at(inv3m10y, offset)
    inv_180_min = None
    try:
        _inv_t = _trim_series_at_offset(inv3m10y, offset)
        if _inv_t is not None and len(_inv_t) >= 252:
            inv_180_min = float(_inv_t.iloc[-252:].min())
    except Exception: pass
    cpi_now = _safe_iloc_at(cpi, offset)
    cpi_3m_chg = _abs_change_at(cpi, offset, 90) if cpi is not None else None
    fpe_now = _safe_iloc_at(fpe, offset)
    cape_now = _safe_iloc_at(cape, offset)
    sox_3m = _pct_change_at(sox, offset, 63)
    spx_3m = _pct_change_at(spx, offset, 63)
    sox_6m = _pct_change_at(sox, offset, 126)
    spx_6m = _pct_change_at(spx, offset, 126)
    sox_1m = _pct_change_at(sox, offset, 22)
    spx_1m = _pct_change_at(spx, offset, 22)
    rsp_6m = _pct_change_at(rsp, offset, 126)
    qqq_6m = _pct_change_at(qqq, offset, 126)
    spy_1m = _pct_change_at(spy, offset, 22)
    rsp_1m = _pct_change_at(rsp, offset, 22)
    wti_3m = _pct_change_at(wti, offset, 63)
    raw_inputs = {
        "qqq_dd": qqq_dd, "unemp_now": unemp_now, "unemp_3m": unemp_3m,
        "ff_6m": ff_6m, "ff_pos_pct": ff_pos_pct,
        "hy_now": hy_now, "hy_pct": hy_pct, "hy_6m": hy_6m,
        "inv_now": inv_now, "inv_180_min": inv_180_min,
        "cpi_now": cpi_now, "cpi_3m_chg": cpi_3m_chg,
        "fpe_now": fpe_now, "cape_now": cape_now,
        "sox_3m": sox_3m, "spx_3m": spx_3m, "sox_6m": sox_6m, "spx_6m": spx_6m,
        "sox_1m": sox_1m, "spx_1m": spx_1m,
        "rsp_6m": rsp_6m, "qqq_6m": qqq_6m,
        "spy_1m": spy_1m, "rsp_1m": rsp_1m, "wti_3m": wti_3m,
    }
    spring = [
        ("S1 inv 회복",      bool(inv_180_min is not None and inv_now is not None and inv_180_min < -0.2 and inv_now > inv_180_min * 0.5)),
        ("S2 hy 안정",        bool(hy_now is not None and hy_pct is not None and hy_pct < 4 and hy_6m is not None and hy_6m < 0)),
        ("S3 ff 인하",        bool(ff_pos_pct is not None and ff_pos_pct < 30 and ff_6m is not None and ff_6m < 0)),
        ("S4 실업 압력",      bool(unemp_now is not None and (unemp_now >= 4 or (unemp_3m is not None and unemp_3m > 0.5)))),
        ("S5 qqq DD>25",      bool(qqq_dd is not None and qqq_dd < -25)),
        ("S6 밸류 매력",      bool((fpe_now is not None and fpe_now <= 18) or (cape_now is not None and cape_now <= 25))),
        ("S7 cpi 둔화",       bool(cpi_3m_chg is not None and cpi_now is not None and cpi_3m_chg < 0 and cpi_now < 3)),
    ]
    summer = [
        ("Su1 inv 정상",      bool(inv_now is not None and inv_now > 0 and (inv_180_min is None or inv_180_min >= -0.05))),
        ("Su2 hy 타이트",     bool(hy_pct is not None and hy_pct < 4)),
        ("Su3 ff 안정",       bool(ff_6m is not None and abs(ff_6m) < 0.5)),
        ("Su4 fpe<22",        bool(fpe_now is not None and fpe_now < 22)),
        ("Su5 실업 안정",     bool(unemp_3m is not None and unemp_3m <= 0)),
        ("Su6 SOX>SPX 6M",    bool(sox_6m is not None and spx_6m is not None and sox_6m > spx_6m)),
        ("Su7 RSP/QQQ 동반",  bool(rsp_6m is not None and qqq_6m is not None and rsp_6m > 0 and qqq_6m > 0)),
    ]
    autumn = [
        ("A1 inv 역전",       bool(inv_now is not None and inv_now < -0.05)),
        ("A2 hy 확장",        bool(hy_6m is not None and hy_6m > 0)),
        ("A3 ff 고점권",      bool(ff_pos_pct is not None and ff_pos_pct >= 70)),
        ("A4 밸류 과열",      bool((fpe_now is not None and fpe_now >= 22) or (cape_now is not None and cape_now >= 35))),
        ("A5 신용 + 절대",    bool(hy_6m is not None and hy_now is not None and hy_pct is not None and hy_6m > 0 and hy_pct > 3.5)),
        ("A6 WTI/SPX 디커플", bool(wti_3m is not None and spx_3m is not None and wti_3m > 15 and spx_3m < 0)),
        ("A7 SOX 약세",       bool(sox_3m is not None and spx_3m is not None and spx_6m is not None and sox_3m < spx_3m and spx_6m > 0)),
        ("A8 cape 35+",       bool(cape_now is not None and cape_now >= 35)),
    ]
    winter = [
        ("W1 hy 5+",          bool(hy_pct is not None and hy_pct > 5)),
        ("W2 ff 인하",        bool(ff_6m is not None and ff_6m < 0)),
        ("W3 qqq DD + 횡보",  bool(qqq_dd is not None and qqq_dd < -20 and spx_1m is not None and abs(spx_1m) < 3)),
        ("W4 실업 0.5+",      bool(unemp_3m is not None and unemp_3m > 0.5)),
        ("W5 SPY/RSP 디커플", bool(spy_1m is not None and rsp_1m is not None and spy_1m > 0 and rsp_1m < 0)),
        ("W6 SOX 강 + DD",    bool(sox_1m is not None and spx_1m is not None and qqq_dd is not None and sox_1m > spx_1m and qqq_dd < -15)),
    ]
    scores = {
        "봄":   sum(1 for _, b in spring if b),
        "여름": sum(1 for _, b in summer if b),
        "가을": sum(1 for _, b in autumn if b),
        "겨울": sum(1 for _, b in winter if b),
    }
    return {"raw_inputs": raw_inputs, "spring": spring, "summer": summer,
            "autumn": autumn, "winter": winter, "scores": scores}


def _print_box_diagnostic(raw_data, target_dates, label_prefix=""):
    """target_dates 각각에 대해 9박스 boolean dump 를 stdout 출력."""
    from datetime import date as _d
    today = _d.today()
    print(f"\n{'='*60}\n{label_prefix} 9박스 진단 출력 ({len(target_dates)} 날짜)\n{'='*60}")
    for _td in target_dates:
        try:
            _off = max(0, (today - _td).days)
            _diag = _diagnose_box_booleans(raw_data, _off)
            print(f"\n--- {_td.isoformat()} (offset {_off}일) ---")
            print("[raw inputs]")
            for k, v in _diag["raw_inputs"].items():
                _vstr = (f"{v:.3f}" if isinstance(v, (int, float)) else "None") if v is not None else "None"
                print(f"  {k:<14}= {_vstr}")
            for _se_key, _se_label in [("spring", "봄"), ("summer", "여름"),
                                        ("autumn", "가을"), ("winter", "겨울")]:
                _sc = _diag["scores"][_se_label]
                print(f"[{_se_label}] {_sc}점")
                for _lbl, _b in _diag[_se_key]:
                    print(f"  {'✓' if _b else ' '} {_lbl}")
            print(f"[scores] {_diag['scores']}")
        except Exception as _e:
            print(f"  진단 실패 {_td}: {type(_e).__name__}: {_e}")
    print(f"{'='*60}\n")


def _query_macro_at_month(year, month, raw_data, obs_path):
    """V3.12.1: 월 전체 영업일 분포 기반 매크로 진단. live/backfill/on_the_fly 혼합 가능."""
    from datetime import date as _d
    days = _month_business_days(year, month)
    # obs.jsonl 한 번 읽고 date → rows 인덱스 빌드
    obs_by_date = {}
    try:
        if obs_path and Path(obs_path).exists():
            with Path(obs_path).open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line: continue
                    try: r = json.loads(line)
                    except Exception: continue
                    if r.get("_backfill_marker"): continue
                    d = (r.get("date") or "").split()[0]
                    if d: obs_by_date.setdefault(d, []).append(r)
    except Exception: pass
    daily = [_eval_one_day(_dd, raw_data, obs_by_date) for _dd in days]
    # 정렬 보장
    daily.sort(key=lambda r: r["date"])
    summary = _compute_month_summary(daily)
    timeline = _compute_timeline(daily)
    # V3.12.1 진단: 2025-04 (트럼프 관세 충격) — 가을 미발동 원인 검토 console dump
    if year == 2025 and month == 4:
        from datetime import date as _diag_d
        try:
            _print_box_diagnostic(
                raw_data,
                [_diag_d(2025, 4, 4), _diag_d(2025, 4, 8), _diag_d(2025, 4, 15)],
                label_prefix="[2025-04 가을 미발동 진단]",
            )
        except Exception as _de:
            print(f"[box diag fail] {type(_de).__name__}: {_de}")
    return {
        "year": year,
        "month": month,
        "n_business_days": len(days),
        "daily_results": daily,
        "summary": summary,
        "timeline": timeline,
    }


def _render_query_result(result):
    """V3.12.1: 월 전체 분포 기반 렌더링."""
    from collections import Counter as _Cnt
    year = result.get("year"); month = result.get("month")
    n_days = result.get("n_business_days") or 0
    summary = result.get("summary") or {}
    timeline = result.get("timeline") or []
    daily = result.get("daily_results") or []
    if n_days == 0 or not daily:
        st.warning(f"{year}년 {month}월 평가 가능한 영업일 없음.")
        return
    src_counter = _Cnt(r.get("source") for r in daily)
    src_label = {"live": "✅실측", "backfill": "📊백필",
                 "on_the_fly": "⚙️즉석", "unavailable": "❌부재"}
    _src_str = " · ".join(f"{src_label.get(s, s)} {n}일" for s, n in src_counter.most_common())
    st.markdown(f"### 📅 {year}년 {month}월 조회 ({n_days} 영업일)")
    st.caption(f"데이터 출처: {_src_str}")

    # unavailable only
    if all(r.get("source") == "unavailable" for r in daily):
        st.warning("이 달 모든 영업일에서 raw 시계열이 부족 — 9박스 평가 불가.")
        return

    # 섹션 1: 계절 분포
    st.markdown("#### 📊 계절 분포")
    dist = summary.get("season_distribution") or {}
    dominant = summary.get("dominant_season")
    vol = summary.get("volatility", "?")
    valid_n = sum(dist.values())
    pct_dom = (dist.get(dominant, 0) / valid_n * 100) if (dominant and valid_n) else 0
    if dominant:
        # V8 prefix (초/늦) — dominant 계절 일자만 모아서 일별 season_scores 로 _v8_prefix 호출 후 다수결.
        # daily.season 자체는 prefix 미포함이라 r["season"] 다수결 불가 → r["season_scores"] 직접 사용.
        from collections import Counter as _PCnt
        _pref_cnt = _PCnt()
        for _r in daily:
            if _strip_season_prefix(_r.get("season") or "") != dominant: continue
            _ss = _r.get("season_scores") or {}
            try: _pref_cnt[_v8_prefix(dominant, _ss)] += 1
            except Exception: _pref_cnt[""] += 1
        _q_prefix = _pref_cnt.most_common(1)[0][0] if _pref_cnt else ""
        _q_label = _q_prefix + dominant
        st.markdown(f"**대표 계절: {_season_emoji(dominant)} {_q_label} ({pct_dom:.0f}%)**")
    if dist:
        _dist_str = " / ".join(f"{_season_emoji(k)} {k} {v}일" for k, v in sorted(dist.items(), key=lambda x: -x[1]))
        st.caption(f"분포: {_dist_str}")
    _n_era_tr = summary.get("n_era_transitions", 0)
    _n_se_tr = summary.get("n_season_transitions", 0)
    st.caption(f"변동성: **{vol}** — era {_n_era_tr}회 / 계절 {_n_se_tr}회 전환")

    # 섹션 2: 4계절 평균 점수
    st.markdown("#### 📊 4계절 평균 점수 (월 평균 ±std)")
    means = summary.get("season_scores_mean") or {}
    stds  = summary.get("season_scores_std") or {}
    _lines = []
    for k in ("봄", "여름", "가을", "겨울"):
        _lines.append(f"  {_season_emoji(k)} {k}: {means.get(k, 0):.1f} ±{stds.get(k, 0):.1f}")
    st.code("\n".join(_lines), language=None)

    # 4계절 박스 체크리스트 (계절 판단 탭 구조 — fire 빈도 기반)
    box_agg = summary.get("box_aggregated") or {}
    box_n = summary.get("box_valid_days") or 0
    box_failed = summary.get("box_eval_failed_days") or 0
    st.markdown("#### ☑️ 4계절 박스 체크리스트 (월 내 fire 빈도)")
    if box_n == 0:
        st.warning(
            f"박스 평가 실패 — {box_failed}일 모두 raw 부족 또는 _diagnose_box_booleans 예외. "
            "터미널 로그 [box diag fail YYYY-MM-DD] 확인."
        )
    else:
        if box_failed > 0:
            st.caption(f"⚠️ {box_failed}일 raw 부족으로 박스 평가 제외. 표시는 평가 성공 {box_n}일 기준.")
        # 계절 판단 탭 mirror — 2열 (봄+가을 / 여름+겨울)
        _bx_g = C["green"]; _bx_m = C["muted"]
        _se_icons = {"봄": "🌸", "여름": "☀️", "가을": "🍂", "겨울": "❄️"}
        _bx_c1, _bx_c2 = st.columns(2)
        for _se in ("봄", "가을", "여름", "겨울"):
            _items = box_agg.get(_se) or []
            _target = _bx_c1 if _se in ("봄", "가을") else _bx_c2
            _avg = means.get(_se, 0)
            _is_dom = (_se == dominant)
            _hdr_color = SC.get(_se, _bx_m)
            _sub = SL.get(_se, "")
            _dom_mark = " <span style='color:" + _hdr_color + ";font-weight:700'>(대표)</span>" if _is_dom else ""
            with _target:
                st.markdown(
                    f"<div style='font-size:var(--mac-fs-md);font-weight:700;margin-top:6px'>"
                    f"{_se_icons[_se]} {_se} <span style='color:{_bx_m};font-weight:500'>"
                    f"(평균 {_avg:.1f}점)</span>{_dom_mark} — "
                    f"<span style='color:{_hdr_color}'>{_sub}</span></div>",
                    unsafe_allow_html=True,
                )
                if not _items:
                    st.markdown(f"<span style='color:{_bx_m}'>박스 정보 없음</span>", unsafe_allow_html=True)
                else:
                    for _lbl, _fire, _valid in _items:
                        if _valid == 0:
                            _check = "⊘"; _color = _bx_m
                            _detail = "평가 불가 — 데이터 없음"
                        else:
                            _pct = _fire * 100 / _valid
                            _on = (_pct >= 50)
                            _check = "✅" if _on else "⬜"
                            _color = _bx_g if _on else _bx_m
                            _detail = f"{_fire}/{_valid}일"
                        st.markdown(
                            f"<span style='color:{_color};font-size:var(--mac-fs-md)'>"
                            f"{_check} {_lbl} <span style='color:{_bx_m};font-size:var(--mac-fs-sm)'>"
                            f"({_detail})</span></span>",
                            unsafe_allow_html=True,
                        )
                st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    # ── STEP C: 단기 경보 보조 라인 (박스 다수결과 무관 별도 출력) ──
    # 단기 경보 = 1M 윈도우 변동성 일반 신호. 메인 계절 변경 X.
    _alert_sev_key = summary.get("alerts_severity_key") or "none"
    if _alert_sev_key != "none":
        _alert_sev_label = summary.get("alerts_severity_label") or "🟢 잠잠"
        _alert_by_card = summary.get("alerts_fire_by_card") or {}
        _card_short = {
            "변동성_폭발": "VIX 폭발", "급락": "SPX 급락",
            "신용_급격_확장": "HY 확장", "단기_역전": "단기 역전",
            "변동성_클러스터": "변동성 클러스터",
        }
        _fired = [(_card_short.get(k, k), v) for k, v in _alert_by_card.items() if v > 0]
        _fired.sort(key=lambda x: -x[1])
        _n_fired = len(_fired)
        if _alert_sev_key == "warn":
            _names = _fired[0][0] if _fired else "?"
            _line = f"단기 경보 {_alert_sev_label} — {_names}"
        elif _alert_sev_key == "alert":
            _names = ", ".join(n for n, _ in _fired[:3])
            _line = f"단기 경보 {_alert_sev_label} — {_names}"
        else:
            _line = f"단기 경보 {_alert_sev_label} — {_n_fired}종 fire"
        st.caption(_line)

    # 섹션 3: 역사적 유사국면 (월 평균 매칭률)
    era_dist = summary.get("top1_era_distribution") or {}
    era_avg = summary.get("top1_era_avg_score") or {}
    if era_dist:
        st.markdown("#### 🔍 역사적 유사국면 (등장 일수 기준 top 3)")
        _top = sorted(era_dist.items(), key=lambda x: -x[1])[:3]
        _medals = ["🥇", "🥈", "🥉"]
        for _i, (_eid, _n) in enumerate(_top):
            _avg = era_avg.get(_eid, 0)
            _e = next((e for e in ERA_LIBRARY if e.get("id") == _eid), None)
            _lbl = _e.get("label") if _e else _eid
            st.markdown(f"{_medals[_i]} 등장 {_n}일 — **{_lbl}** (평균 매칭률 {_avg*100:.0f}%)")
    else:
        st.caption("🔍 역사 매칭 결과 없음 (raw 부족 또는 era 정의 외)")

    # 섹션 4: timeline
    if len(timeline) > 1:
        st.markdown("#### 🎯 1위 era 변화 timeline")
        for _seg in timeline:
            _e = next((e for e in ERA_LIBRARY if e.get("id") == _seg["era_id"]), None)
            _lbl = _e.get("label") if _e else _seg["era_id"]
            _se = _seg.get("season") or ""
            _se_str = f" · {_season_emoji(_se)} {_se}" if _se else ""
            st.markdown(f"`{_seg['from_date']} ~ {_seg['to_date']}` "
                        f"({_seg['n_days']}일) — {_lbl}{_se_str}")
    elif len(timeline) == 1:
        _seg = timeline[0]
        _e = next((e for e in ERA_LIBRARY if e.get("id") == _seg["era_id"]), None)
        _lbl = _e.get("label") if _e else _seg["era_id"]
        st.caption(f"🎯 월 내내 동일 era 유지: **{_lbl}** ({_seg['n_days']}일)")


_QUERY_SCHEMA_VERSION = "v14-2026-04-29-mraw-prefix-fix"  # bump 시 캐시 강제 무효


@st.cache_data(ttl=3600, show_spinner="월 전체 영업일 평가 중...")
def _query_macro_month_cached_v6(year, month, api_key, _obs_mtime, _schema_ver):
    """월별 결과 1시간 캐시. _schema_ver 는 explicit 인자 (default 사용 X) 로
    Streamlit cache key 에 확실하게 포함시킴. 함수명도 v6 로 박아 옛 캐시 우회.
    raw_data 는 V651.M.raw_data (라이브와 동일 소스) 사용 — 외부 시리즈 (cape/hy/fpe) 풀 적재.
    long_range raw 는 cape/hy/fpe 가 None setdefault 라 V8 박스 점등 약화. 통일 후 라이브 일치."""
    _v651_init()
    if V651.M.raw_data is None:
        try:
            cached = _load_pickled_raw_if_fresh()
            if cached is not None: V651.M.raw_data = cached
            else:
                V651.M.build_raw_data(verbose=False)
                _save_pickled_raw(V651.M.raw_data)
        except Exception: pass
    raw_data = V651.M.raw_data if V651.M.raw_data is not None else _build_long_range_raw(api_key)
    return _query_macro_at_month(year, month, raw_data, str(OBS_JSONL))


# ═══ V3.10.1 시계열 탭 — 역사 매칭 시각화 4종 ═══
def _render_era_score_overlay(_obs_df):
    """top 1/2/3 매칭률 추이 (3-line, %). 1/2위 격차로 전환기 진단."""
    if _obs_df is None or _obs_df.empty:
        st.info("아직 관측 스냅샷이 없다.")
        return
    _need = ["history_score_top1", "history_score_top2", "history_score_top3"]
    _avail = [c for c in _need if c in _obs_df.columns]
    if not _avail:
        st.info("아직 history_score 기록이 쌓이지 않았다. 앱 방문할 때마다 1점씩 누적됨.")
        return
    _df = _obs_df[["ts"] + _avail].dropna(how="all", subset=_avail).sort_values("ts")
    if _df.empty:
        st.info("history_score 유효 데이터 없음.")
        return
    _fig = go.Figure()
    _styles = [(C["red"], "🥇 1위", "history_score_top1"),
               (C["gold"], "🥈 2위", "history_score_top2"),
               (C["muted"], "🥉 3위", "history_score_top3")]
    for _color, _label, _col in _styles:
        if _col not in _df.columns: continue
        _fig.add_trace(go.Scatter(
            x=_df["ts"], y=_df[_col] * 100, mode="lines+markers",
            line=dict(color=_color, width=1.8), marker=dict(size=5),
            name=_label
        ))
    _ly_so = _ly("top 1/2/3 매칭률 추이 (%)", 380)
    _ly_so["yaxis"] = dict(title="매칭률 (%)", range=[0, 100], gridcolor="rgba(128,128,128,0.15)")
    _ly_so["legend"] = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    _fig.update_layout(**_ly_so)
    st.plotly_chart(_fig, use_container_width=True, key="chart_era_score_overlay")
    # 1/2위 격차 분석 (최근값)
    if {"history_score_top1", "history_score_top2"}.issubset(_df.columns):
        _last = _df.dropna(subset=["history_score_top1", "history_score_top2"]).tail(1)
        if not _last.empty:
            _gap = (float(_last["history_score_top1"].iloc[0]) - float(_last["history_score_top2"].iloc[0])) * 100
            if _gap < 5:
                st.caption(f"⚠️ 1위와 2위 격차 {_gap:.1f}%p — 전환기 가능성")
            elif _gap > 15:
                st.caption(f"✅ 1위와 2위 격차 {_gap:.1f}%p — 강한 매칭")
            else:
                st.caption(f"📊 1위와 2위 격차 {_gap:.1f}%p — 안정")


def _render_era_timeline(_obs_df):
    """1위 era 변화 타임라인 (콜러풀 띠 차트)."""
    if _obs_df is None or _obs_df.empty:
        st.info("아직 관측 스냅샷이 없다.")
        return
    if "history_era_top1" not in _obs_df.columns:
        st.info("아직 history_era_top1 기록 없음.")
        return
    _cols = ["ts", "history_era_top1"]
    if "history_score_top1" in _obs_df.columns:
        _cols.append("history_score_top1")
    _df = _obs_df[_cols].dropna(subset=["history_era_top1"]).sort_values("ts").reset_index(drop=True)
    if _df.empty:
        st.info("history_era_top1 유효 데이터 없음.")
        return
    _eras = list(dict.fromkeys(_df["history_era_top1"].tolist()))
    _palette = [C["gold"], C["blue"], C["red"], C["green"], "#9C27B0", "#FF8A00",
                "#3498DB", "#E74C3C", "#2ECC71", "#F39C12", "#16A085", C["muted"]]
    _era_colors = {_e: _palette[_i % len(_palette)] for _i, _e in enumerate(_eras)}
    # era 변화 segment 별 grouping
    _df["chg"] = (_df["history_era_top1"] != _df["history_era_top1"].shift(1)).fillna(True).astype(int)
    _df["seg"] = _df["chg"].cumsum()
    _fig = go.Figure()
    for _seg_id, _seg in _df.groupby("seg"):
        _era = _seg["history_era_top1"].iloc[0]
        _scores = _seg["history_score_top1"].tolist() if "history_score_top1" in _seg.columns else [0.0] * len(_seg)
        _txt = [f"{_era} ({(_s or 0)*100:.0f}%)" for _s in _scores]
        _fig.add_trace(go.Scatter(
            x=_seg["ts"], y=[_era] * len(_seg),
            mode="lines+markers",
            line=dict(color=_era_colors[_era], width=8),
            marker=dict(size=8, color=_era_colors[_era]),
            name=_era, showlegend=False,
            text=_txt,
            hovertemplate="%{x|%Y-%m-%d}<br>%{text}<extra></extra>",
        ))
    _ly_tl = _ly("1위 era 변화 타임라인", max(360, 60 + len(_eras) * 26))
    _ly_tl["yaxis"] = dict(title="era", gridcolor="rgba(128,128,128,0.15)", categoryorder="array", categoryarray=_eras[::-1])
    _fig.update_layout(**_ly_tl)
    st.plotly_chart(_fig, use_container_width=True, key="chart_era_timeline")
    _changes = max(0, int(_df["chg"].sum()) - 1)
    st.caption(f"기간 내 era 변화 {_changes}회 · 총 {len(_eras)}개 era 매칭됨.")


def _render_era_dim_overlay(_obs_df):
    """10개 차원 매칭 (1/0) overlay (offset stack). V3.10.0 obs 신규 필드 기반."""
    if _obs_df is None or _obs_df.empty:
        st.info("아직 관측 스냅샷이 없다.")
        return
    _dims = ["season", "ff_pos", "ff_action", "inflation_trend", "valuation",
             "credit", "yield_curve", "semiconductor", "dollar", "external_shock"]
    _cols = [(d, f"history_dim_match_{d}") for d in _dims]
    _avail = [(d, c) for d, c in _cols if c in _obs_df.columns]
    if not _avail:
        st.info("아직 차원 매칭 기록이 쌓이지 않았다. V3.10.0 신규 obs 필드 — 방문 누적 필요.")
        return
    _df = _obs_df[["ts"] + [c for _, c in _avail]].sort_values("ts").reset_index(drop=True)
    if _df.empty:
        st.info("차원 매칭 유효 데이터 없음.")
        return
    _palette = [C["gold"], C["blue"], C["red"], C["green"], "#9C27B0",
                "#FF8A00", "#3498DB", "#E74C3C", "#2ECC71", C["muted"]]
    _fig = go.Figure()
    _y_ticks = []
    for _i, (_dim, _col) in enumerate(_avail):
        _ko = _DIM_KO.get(_dim, _dim)
        _y_offset = _i * 1.2
        _y_ticks.append((_y_offset + 0.5, _ko))
        _fig.add_trace(go.Scatter(
            x=_df["ts"], y=_df[_col].fillna(0).astype(float) + _y_offset,
            mode="lines+markers",
            line=dict(color=_palette[_i % len(_palette)], width=1.5, shape="hv"),
            marker=dict(size=4),
            name=_ko, hovertemplate=f"{_ko}: %{{y:.0f}}<extra></extra>",
        ))
    _ly_dim = _ly("차원별 매칭 추이 (10차원, 1=매칭/0=미매칭)", max(420, 60 + len(_avail) * 32))
    _ly_dim["legend"] = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    _ly_dim["yaxis"] = dict(
        tickmode="array",
        tickvals=[t[0] for t in _y_ticks],
        ticktext=[t[1] for t in _y_ticks],
        gridcolor="rgba(128,128,128,0.15)",
    )
    _fig.update_layout(**_ly_dim)
    st.plotly_chart(_fig, use_container_width=True, key="chart_era_dim_overlay")
    st.caption("각 차원의 1위 era 매칭 여부 추이. 자주 미매칭 차원 → 시그널 약화 패턴.")


def _render_era_stability_heatmap(_obs_df):
    """era 안정성 heatmap (X=시간, Y=era, 색=매칭률; 1위 점유 시점만 색칠)."""
    if _obs_df is None or _obs_df.empty:
        st.info("아직 관측 스냅샷이 없다.")
        return
    if "history_era_top1" not in _obs_df.columns or "history_score_top1" not in _obs_df.columns:
        st.info("아직 history_era_top1 / score 기록 없음.")
        return
    _df = _obs_df[["ts", "history_era_top1", "history_score_top1"]].dropna().sort_values("ts").reset_index(drop=True)
    if _df.empty:
        st.info("history 유효 데이터 없음.")
        return
    _counts = _df["history_era_top1"].value_counts()
    _eras = list(_counts.head(12).index)  # top 12 빈도만
    _ts_list = _df["ts"].tolist()
    _z = []
    for _e in _eras:
        _row = []
        for _re, _s in zip(_df["history_era_top1"], _df["history_score_top1"]):
            _row.append(float(_s) if (_re == _e) else None)
        _z.append(_row)
    _fig = go.Figure(data=go.Heatmap(
        z=_z, x=_ts_list, y=_eras,
        colorscale="Viridis", zmin=0, zmax=1,
        colorbar=dict(title="매칭률"),
        hoverongaps=False,
        hovertemplate="%{y}<br>%{x|%Y-%m-%d}<br>매칭률: %{z:.0%}<extra></extra>",
    ))
    _ly_hm = _ly(f"era 안정성 heatmap (1위 점유 era · top {len(_eras)} 빈도)", max(360, 60 + len(_eras) * 28))
    _fig.update_layout(**_ly_hm)
    st.plotly_chart(_fig, use_container_width=True, key="chart_era_heatmap")
    st.caption("각 era 가 1위였던 시점만 색 표시. 색이 길면 안정 점유, 띄엄띄엄이면 시그널 노이즈.")


# ═══ V3.11.1: 달력 계절성 (Seasonal Chart) ═══
_SEASONAL_SYM_MAP = {"S&P 500": "^GSPC", "나스닥 100": "^NDX", "필라델피아 반도체": "^SOX"}


@st.cache_data(ttl=86400, show_spinner=False)
def _seasonal_fetch(symbol, start_year):
    """달력 계절성용 yfinance 시세. 24h 캐싱."""
    try:
        import yfinance as _yf
        _df = _yf.download(symbol,
                           start=f"{start_year}-01-01",
                           end=datetime.now().strftime("%Y-%m-%d"),
                           progress=False, auto_adjust=False)
        return _df
    except Exception:
        return None


def _render_seasonal_overlay(_obs_df=None):
    """월별 누적수익률 overlay — 최근 N년을 day-of-year 축에 겹쳐 비교.
    과거 연도: 색상 9종 순환 (작년=0 인덱스 기준) + opacity 0.7
    현재 연도: 빨강 굵게 (width 3, opacity 1.0)
    + N년 평균 라인 (검정 점선)"""
    import numpy as _np
    sym_choice = st.radio("종목 선택", options=list(_SEASONAL_SYM_MAP.keys()),
                          horizontal=True, key="cal_seasonal_overlay_sym")
    years = st.slider("비교 연도 수", min_value=3, max_value=60, value=5,
                      key="cal_seasonal_overlay_years")
    symbol = _SEASONAL_SYM_MAP[sym_choice]
    cur_year = datetime.now().year
    start_year = cur_year - years + 1
    _df = _seasonal_fetch(symbol, start_year)
    if _df is None or _df.empty:
        st.info(f"{sym_choice} ({symbol}) 데이터 못 받음.")
        return
    _close = _df["Close"]
    if isinstance(_close, pd.DataFrame):
        _close = _close.iloc[:, 0]
    _fig = go.Figure()
    _past_palette = [
        ("#3b82f6", 0.7),  # 파랑
        ("#10b981", 0.7),  # 초록
        ("#a855f7", 0.7),  # 보라
        ("#f59e0b", 0.7),  # 주황
        ("#ec4899", 0.7),  # 분홍
        ("#06b6d4", 0.7),  # 청록
        ("#84cc16", 0.7),  # 라임
        ("#f97316", 0.7),  # 진주황
        ("#8b5cf6", 0.7),  # 진보라
    ]
    _cur_color = "#dc2626"
    # 1. 과거 연도 trace
    _past_doy_to_returns = {}  # {doy: [returns from each past year]}
    for _y in range(start_year, cur_year):
        _yd = _close[_close.index.year == _y]
        if len(_yd) < 5: continue
        _first = float(_yd.iloc[0])
        if _first == 0: continue
        _cum = (_yd / _first - 1) * 100
        _doy = _yd.index.dayofyear
        _past_idx = cur_year - _y - 1  # 작년=0, 재작년=1
        _color, _opacity = _past_palette[_past_idx % len(_past_palette)]
        _fig.add_trace(go.Scatter(
            x=_doy, y=_cum.values, mode="lines", name=str(_y),
            line=dict(color=_color, width=1.5),
            opacity=_opacity,
            hovertemplate=f"<b>{_y}</b><br>%{{x}}일째<br>%{{y:.2f}}%<extra></extra>",
        ))
        for _d, _v in zip(_doy, _cum.values):
            try:
                _past_doy_to_returns.setdefault(int(_d), []).append(float(_v))
            except Exception: continue
    # 2. N년 평균 라인 (검정 점선) — 현재 연도 직전에 추가
    if _past_doy_to_returns:
        _avg_doys = sorted(_past_doy_to_returns.keys())
        _avg_vals = [float(_np.mean(_past_doy_to_returns[_d])) for _d in _avg_doys]
        _past_n = max(0, cur_year - start_year)
        _fig.add_trace(go.Scatter(
            x=_avg_doys, y=_avg_vals, mode="lines", name=f"{_past_n}년 평균",
            line=dict(color="#000000", width=2.5, dash="dash"),
            opacity=1.0,
            hovertemplate=f"<b>{_past_n}년 평균</b><br>%{{x}}일째<br>%{{y:.2f}}%<extra></extra>",
        ))
    # 3. 현재 연도 trace (마지막에 — 위에 그려지도록)
    _yd_cur = _close[_close.index.year == cur_year]
    if len(_yd_cur) >= 5:
        _first_cur = float(_yd_cur.iloc[0])
        if _first_cur != 0:
            _cum_cur = (_yd_cur / _first_cur - 1) * 100
            _doy_cur = _yd_cur.index.dayofyear
            _fig.add_trace(go.Scatter(
                x=_doy_cur, y=_cum_cur.values, mode="lines", name=str(cur_year),
                line=dict(color=_cur_color, width=3),
                opacity=1.0,
                hovertemplate=f"<b>{cur_year}</b><br>%{{x}}일째<br>%{{y:.2f}}%<extra></extra>",
            ))
    _month_starts = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
    _month_labels = ["1월", "2월", "3월", "4월", "5월", "6월",
                     "7월", "8월", "9월", "10월", "11월", "12월"]
    _ly_so = _ly(f"{sym_choice} 최근 {years}년 월별 누적 수익률 비교", 450)
    _ly_so["xaxis"] = dict(title="월", tickmode="array",
                            tickvals=_month_starts, ticktext=_month_labels, range=[1, 366])
    _ly_so["yaxis"] = dict(title="연초 대비 누적 수익률 (%)", gridcolor="rgba(128,128,128,0.15)")
    _ly_so["legend"] = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    _ly_so["hovermode"] = "x unified"
    _fig.update_layout(**_ly_so)
    _fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    st.plotly_chart(_fig, use_container_width=True,
                    key=f"chart_seasonal_overlay_{symbol}_{years}")
    if len(_yd_cur) > 0:
        _ret = (float(_yd_cur.iloc[-1]) / float(_yd_cur.iloc[0]) - 1) * 100
        _doy_last = int(_yd_cur.index[-1].dayofyear)
        st.caption(f"📅 {cur_year}년 현재: {_doy_last}일째, 누적 {_ret:+.2f}%")


def _render_seasonal_heatmap(_obs_df=None):
    """월별 평균 수익률 히트맵 — 행=연도, 열=월, 색=수익률 부호."""
    import numpy as _np
    sym_choice = st.radio("종목 선택", options=list(_SEASONAL_SYM_MAP.keys()),
                          horizontal=True, key="cal_seasonal_heatmap_sym")
    years = st.slider("비교 연도 수", min_value=3, max_value=60, value=5,
                      key="cal_seasonal_heatmap_years")
    symbol = _SEASONAL_SYM_MAP[sym_choice]
    cur_year = datetime.now().year
    start_year = cur_year - years + 1
    _df = _seasonal_fetch(symbol, start_year)
    if _df is None or _df.empty:
        st.info(f"{sym_choice} ({symbol}) 데이터 못 받음.")
        return
    _close = _df["Close"]
    if isinstance(_close, pd.DataFrame):
        _close = _close.iloc[:, 0]
    _monthly = _close.resample("ME").last() if hasattr(_close, "resample") else _close
    _ret = _monthly.pct_change() * 100
    _years_list = list(range(start_year, cur_year + 1))
    _matrix = []
    for _y in _years_list:
        _row = []
        for _m in range(1, 13):
            _v = _ret[(_ret.index.year == _y) & (_ret.index.month == _m)]
            _row.append(float(_v.iloc[0]) if len(_v) > 0 else _np.nan)
        _matrix.append(_row)
    _matrix = _np.array(_matrix, dtype=float)
    _text = []
    for _row in _matrix:
        _tr = []
        for _v in _row:
            _tr.append("" if _np.isnan(_v) else f"{_v:+.1f}")
        _text.append(_tr)
    _month_labels = ["1월", "2월", "3월", "4월", "5월", "6월",
                     "7월", "8월", "9월", "10월", "11월", "12월"]
    _fig = go.Figure(data=go.Heatmap(
        z=_matrix, x=_month_labels, y=[str(_y) for _y in _years_list],
        text=_text, texttemplate="%{text}", textfont={"size": 11},
        colorscale=[[0.0, "#991b1b"], [0.4, "#fca5a5"], [0.5, "#ffffff"],
                    [0.6, "#86efac"], [1.0, "#166534"]],
        zmid=0, hoverongaps=False,
        hovertemplate="%{y}년 %{x}<br>%{z:+.2f}%<extra></extra>",
    ))
    _ly_hm = _ly(f"{sym_choice} 월별 수익률 히트맵 (최근 {years}년)",
                  300 + 25 * len(_years_list))
    _ly_hm["yaxis"] = dict(autorange="reversed")
    _fig.update_layout(**_ly_hm)
    st.plotly_chart(_fig, use_container_width=True,
                    key=f"chart_seasonal_heatmap_{symbol}_{years}")
    _avg = _np.nanmean(_matrix, axis=0)
    if not _np.all(_np.isnan(_avg)):
        _best = int(_np.nanargmax(_avg)); _worst = int(_np.nanargmin(_avg))
        st.caption(
            f"📊 최근 {years}년 평균 — "
            f"최강 **{_month_labels[_best]}** ({_avg[_best]:+.2f}%) · "
            f"최약 **{_month_labels[_worst]}** ({_avg[_worst]:+.2f}%)"
        )



# 하위 호환: 기존 5개 인자 (season, ff_pos, val_score, semi_dir, wti_surge) 그대로.
# 신규 인자는 keyword-only / None 허용. mode 인자는 시그니처만 유지 (본체 미참조).
def season_history_match(season, ff_pos, val_score, semi_dir, wti_surge,
                         mode="일반",
                         hy_now=None, hy_6m_chg=None, inv_state=None,
                         dxy_now=None, cpi_yoy_now=None,
                         cpi_yoy_3m_chg=None, ff_3m_chg=None,
                         wti_3m=None):
    _base_season = season.lstrip("초늦") if season else None
    state = _build_current_state(
        season=_base_season,
        ff_pos=ff_pos,
        val_score=val_score,
        semi_dir=semi_dir,
        wti_3m=wti_3m if wti_3m is not None else (35 if wti_surge else 0),
        hy_now=hy_now,
        hy_6m_chg=hy_6m_chg,
        inv_state=inv_state,
        dxy_now=dxy_now,
        cpi_yoy_now=cpi_yoy_now,
        cpi_yoy_3m_chg=cpi_yoy_3m_chg,
        ff_3m_chg=ff_3m_chg,
        ff_6m_chg=None,
    )
    return _era_match_with_threshold(state)

def calc_mk_score(qqq_dd, soxx_dd, vix, fg, cash_pct, tqqq_ratio):
    """미어캣 스코어: 매집 환경 온도. 높을수록 매수환경 좋음."""
    items = {
        "qqq":  (_nm(qqq_dd,  [(-25,25),(-15,15),(-10,8),(-5,2),(0,0)]), 35),
        "soxx": (_nm(soxx_dd, [(-35,15),(-25,10),(-15,6),(-5,1),(0,0)]), 15),
        "vix":  (_nm(vix,     [(12,0),(20,3),(25,6),(35,12),(40,15)]), 15),
        "fg":   (_nm(fg,      [(15,10),(25,7),(50,4),(75,1),(80,0)]), 10),
        "cash": (_nm(cash_pct*100, [(0,0),(5,3),(15,8),(25,12),(30,15)]) if cash_pct is not None else None, 15),
        "ratio":(_nm(tqqq_ratio*100, [(200,10),(300,6),(400,3),(480,1),(500,0)]) if tqqq_ratio is not None else None, 10),
    }
    tw = 0; ts = 0
    for k, (sc, w) in items.items():
        if sc is not None: ts += sc; tw += w
    total = round(ts / tw * 100, 1) if tw > 0 else None
    detail = {}
    for k, (sc, w) in items.items():
        if sc is not None:
            detail[k] = {"raw": round(sc, 2), "weight": w, "contrib": round(sc, 2)}
    return total, detail

# ═══ JUDGE ═══
def j_sp(bp):
    if bp is None: return ("—", C["muted"], "")
    if bp < -40: return ("역전 심화", C["red"], "형이 많이 걱정하고 있다. 역전의 고점은 주가의 저점 근처다.")
    if bp < 0: return ("역전", C["red"], "형이 걱정하기 시작했다. 빌려줄수록 손해니까 대출을 줄인다.")
    if bp < 30: return ("역전 해소 중", C["gold"], "겨울이 왔거나 봄이 시작됐다. 둘 중 하나다.")
    return ("정상", C["green"], "형이 안심하고 있다. 동생은 신나서 놀고 있다.")
def j_dxy(v):
    if v is None: return ("—", C["muted"], "")
    if v >= 110: return ("극단 강세", C["red"], "달러가 고점이면 주가가 바닥 근처다. 22년 9월이 그랬다. 내가 그때 숏에서 롱으로 넘어갔다.")
    if v >= 105: return ("강세", C["red"], "공포가 달러를 올리고 올라간 달러가 공포를 키운다. 달러루프.")
    if v >= 95: return ("중립", C["gold"], "달러 중립. 다른 지표를 봐라.")
    return ("약세", C["green"], "달러가 빠지고 있다. 위험자산에 우호적.")
# VIX: 역발상 지표 — 공포=매수기회(초록), 낙관=과열(빨강)
def j_vix(v):
    if v is None: return ("—", C["muted"], "")
    if v >= 35: return ("공포 극대", C["green"], "조롱에 사고 자랑에 팔아라. 지금이 조롱이다.")
    if v >= 25: return ("공포", C["gold"], "시장이 흔들리고 있다. 아직 공포는 아니다.")
    if v >= 20: return ("중립", C["muted"], "조용하다. 근데 조용한 게 안전하다는 뜻은 아니다. 폭풍 전의 고요일 수 있다.")
    return ("극도의 낙관", C["red"], "들썩들썩 떠들썩 와글와글. 그 후엔 우르르 쾅쾅이다.")
def j_hy(v):
    if v is None: return ("—", C["muted"], "")
    bp = v * 100
    if bp >= 500: return ("신용경색", C["red"], "잠수함이 깊이 잠수하니 여기저기서 물이 터진다.")
    if bp >= 400: return ("경고", C["red"], "신용이 조이고 있다. 기업이 돈을 못 구한다.")
    if bp <= 300: return ("자만", C["gold"], "회사가 망할 수 있다는 생각을 안 한다. 위험하다.")
    return ("중립", C["gold"], "채권시장이 태평하다. 위험을 무시하고 있다. 이 상태가 오래가면 그 자체가 경고다.")
# 섹터 디커플링 4사분면 — XLE/XLK vs SPY 3M 상대수익률
# 임계값: 1999~2025 분포 검증 결과 P75≈±6/±3.5, 보수적으로 ±5/±3 (강) / ±2 (약)
# 반환: (라벨, 색, 코멘트, 사분면코드) — 코드는 export 전용, UI는 라벨만 사용
def _sector_quad(xle, xlk):
    if xle is None or xlk is None: return ("—", C["muted"], "", None)
    if xle > 5 and xlk < -3:
        return ("인플레 회귀", C["red"], "에너지가 기술을 이기고 있다. 인플레가 돌아오는 거다. 유가→인플레→연준→시장. 가을의 확인 신호.", "Q2")
    if xle < -3 and xlk > 5:
        return ("성장 회귀", C["green"], "기술이 에너지를 이기고 있다. 시장이 인플레를 잊었다. 봄이거나 여름 초입이다.", "Q4")
    if xle < -2 and xlk < -2:
        return ("동반 약세", C["red"], "에너지도 기술도 시장에 지고 있다. 소수 종목만 버티는 좁은 시장이다. 2000년 말기와 같다.", "Q3")
    if xle > 2 and xlk > 2:
        return ("균형 강세", C["gold"], "에너지도 기술도 시장을 이기고 있다. 유동성이 넘친다. 거품의 후기 증상이다.", "Q1")
    return ("균형 / 노이즈", C["muted"], "뚜렷한 로테이션 없다. 노이즈다. 다른 지표를 봐라.", "Q0")

# ═══ UI ═══

def _spark_svg(values, color="#FFC107", height=36, width=200, dates=None):
    """순수 SVG sparkline. values: list of float (None 무시). 의존성 없음, 가벼움.
    dates: list of datetime/Timestamp/date (선택). 주어지면 하단 캡션에 '시작 ~ 끝' 표시.
    n 의미: sparkline 에 실제로 그려진 데이터 포인트 개수 (일간=거래일, 월간=발표, 분기=분기, obs=방문)."""
    if not values: return ""
    if dates is None: dates = [None] * len(values)
    pairs = [(d, v) for d, v in zip(dates, values)
             if v is not None and not (isinstance(v, float) and (v != v))]
    if len(pairs) < 2: return ""
    clean = [v for _, v in pairs]
    clean_dates = [d for d, _ in pairs]
    vmin = min(clean); vmax = max(clean)
    rng = vmax - vmin
    if rng == 0: rng = 1
    n = len(clean)
    pts = []
    for i, v in enumerate(clean):
        x = i * width / (n - 1) if n > 1 else 0
        y = height - (v - vmin) / rng * (height - 4) - 2
        pts.append(f"{x:.1f},{y:.1f}")
    poly = " ".join(pts)
    fill_poly = f"0,{height} {poly} {width},{height}"
    cur_v = clean[-1]; first_v = clean[0]
    chg_str = f"{(cur_v - first_v):+.2f}" if abs(cur_v - first_v) < 100 else f"{cur_v - first_v:+.0f}"
    # 기간 표기: 시작일 ~ 끝일 (둘 다 있을 때만)
    def _fmt(d):
        if d is None: return ""
        try:
            if hasattr(d, "date"): d = d.date()
            return d.strftime("%y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        except Exception:
            return str(d)[:10]
    range_str = ""
    d0, d1 = clean_dates[0], clean_dates[-1]
    if d0 is not None and d1 is not None:
        range_str = f"{_fmt(d0)} ~ {_fmt(d1)}"
    return (f'<div style="margin-top:6px;border-top:1px solid rgba(128,128,128,0.18);padding-top:4px">'
            f'<svg width="100%" height="{height}" viewBox="0 0 {width} {height}" preserveAspectRatio="none" style="display:block">'
            f'<polygon points="{fill_poly}" fill="{color}" fill-opacity="0.15"/>'
            f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="1.4"/>'
            f'</svg>'
            f'<div style="font-size:9px;color:#888;display:flex;justify-content:space-between;margin-top:1px">'
            f'<span>{vmin:.2f}</span><span>n={n}p · Δ{chg_str}</span><span>{vmax:.2f}</span></div>'
            + (f'<div style="font-size:9px;color:#666;text-align:center;margin-top:1px">{range_str}</div>' if range_str else "")
            + '</div>')

def icard(label, value, status, color, detail="", trend="", mode="일반", sparkline=""):
    ld = bsl(label, mode); sd = bsl(status, mode)
    _t = C["text"]; _c = C["card"]; _b = C["border"]; _br = C["bright"]; _m = C["muted"]
    det = ""
    if detail: det = f"<div style='font-size:var(--mac-fs-sm);color:{_t};margin-top:6px;line-height:1.4;border-top:1px solid {_b};padding-top:6px'>{detail}</div>"
    trd = ""
    if trend: trd = trend
    # 라벨 옆 인라인 ⓘ (label 원문 기준 lookup)
    _rng = _CARD_RANGES.get(label)
    _rng_inline = f' <span class="gtp" tabindex="0">ⓘ<span class="gtxt">{_rng}</span></span>' if _rng else ""
    st.markdown(f"""<div class="maccard" tabindex="0" style="background:{_c};border:1px solid {_b};border-radius:8px;
        padding:12px 16px;border-left:3px solid {color};min-height:120px;position:relative">
        <div style="font-size:var(--mac-fs-sm);color:{_m};margin-bottom:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="{ld}">{ld}{_rng_inline}</div>
        <div style="font-size:var(--mac-fs-large);font-weight:700;color:{_br};margin:2px 0">{value}</div>
        <div style="font-size:var(--mac-fs-md);color:{color};font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="{sd}">{sd}</div>
        {trd}{det}{sparkline}</div>""", unsafe_allow_html=True)

def sgauge(score, label, quote=None, info=None, subline=None, sub_color=None):
    _c = C["card"]; _b = C["border"]; _br = C["bright"]; _m = C["muted"]
    _ii = _tip(info) if info else ""
    if score is None:
        _fallback = "데이터 부족" if quote is None else "QQQ/SOXX 데이터 대기 중"
        return f"<div style='background:{_c};border:1px solid {_b};border-radius:12px;padding:20px 24px;text-align:center'><div style='color:{_m};font-size:var(--mac-fs-md)'>{label}</div><div style='color:{_m};font-size:var(--mac-fs-sm);margin-top:8px'>{_fallback}</div></div>"
    gc = C["green"] if score >= 75 else C["blue"] if score >= 50 else C["orange"] if score >= 25 else C["red"]
    _qt = ""
    if quote:
        _qt = f"<div style='font-size:var(--mac-fs-sm);color:{_m};margin-top:8px;font-style:italic;line-height:1.4;min-height:32px'>{quote}</div>"
    _sub = ""
    if subline:
        _sc = sub_color or _m
        _sub = f"<div style='font-size:var(--mac-fs-sm);color:{_sc};margin-top:2px;font-weight:600'>{subline}</div>"
    return f"""<div style="background:{_c};border:1px solid {_b};border-radius:12px;padding:20px 24px;text-align:center">
        <div style="font-size:var(--mac-fs-md);color:{_m}">{label}</div>
        <div style="font-size:var(--mac-fs-display);font-weight:700;color:{gc};margin:6px 0">{score:.0f}{_ii}</div>{_sub}
        <div style="background:{_b};border-radius:4px;height:10px;margin:10px 0">
            <div style="background:{gc};width:{min(score,100):.0f}%;height:10px;border-radius:4px"></div></div>
        <div style="font-size:var(--mac-fs-sm);color:{_m}">0 과열 ← → 매수환경 100</div>{_qt}</div>"""

def mx22(g, m):
    if g is None or m is None: return "스코어 데이터 부족"
    if g >= 50 and m >= 50: return "트리거 접근 + 거시 겨울. 매집 확신 최대. 산다."
    if g < 50 and m >= 50: return "가격은 빠졌는데 거시가 아직 과열. 기술적 조정이다. 속지 마라."
    if g >= 50 and m < 50: return "거시는 겨울인데 가격이 아직 안 빠졌다. 곧 온다. 현금 들고 기다려라."
    return "과열이고 가격도 높다. 가지치기 구간이다. DCA만 해라."

def _ly(t="", h=350):
    return dict(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=h,
                title=dict(text=t, font=dict(size=14)), font=dict(size=11),
                legend=dict(orientation="h", yanchor="top", y=-0.12, font=dict(size=10)),
                margin=dict(l=50, r=20, t=40, b=50), xaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
                yaxis=dict(gridcolor="rgba(128,128,128,0.15)"))

# ═══ 쉬운모드 해설 ═══
HELP_TAB0 = """여기는 전체 요약이다. 거시 스코어가 높을수록 시장이 싸고 무섭다(= 사기 좋다).
낮을수록 시장이 비싸고 신난다(= 조심해야 한다).
카드의 색깔이 빨간색이면 경고, 초록색이면 괜찮다.
추세 화살표(↑↓)는 방향을 보여준다. 숫자보다 방향이 중요하다."""

HELP_TAB1 = """채권이 형이다. 형이 걱정하면 동생(주식)이 아프다.
- 장단기금리차가 마이너스(역전)면: 형이 걱정하는 거다. 침체가 온다.
- 0 위로 올라오면(역전 해소): 겨울이 왔거나 봄이 시작된다.
- 연방기금금리가 내려가고 있으면: 연준이 경기를 살리려는 거다. 주식에 좋다.
- 연방기금금리가 올라가고 있으면: 연준이 인플레를 잡으려는 거다. 주식에 나쁘다.

핵심: 금리의 방향(올라가냐 내려가냐)이 수준(몇 %냐)보다 중요하다."""

HELP_TAB2 = """주식이 비싼지 싼지를 본다.
- PE가 높으면: 비싸다. 이익 대비 주가가 높다.
- PE 28 이상이면: 역사적으로 여기서 터지지 않은 적이 없다. 140년간.
- 배당수익률이 낮으면: 비싸다. 같은 이야기를 다른 각도에서 본 것.
- CAPE(10년 평균)이 35 이상이면: 2000년 닷컴 때밖에 없었다.

핵심: 비싸다고 내일 떨어지는 게 아니다. 근데 방향은 확실하다. 타이밍만 모른다."""

HELP_TAB3 = """반도체가 시장보다 먼저 움직인다.
- SOX/SPX 비율이 올라가면: 반도체가 시장을 이기고 있다. 경기 회복 신호.
- SOX/SPX 비율이 내려가면: 반도체가 시장에 지고 있다. 경기 하강 신호.
- 1M/3M/6M/1Y의 %p 숫자: 반도체가 시장보다 몇 % 더 올랐는지(+) 또는 덜 올랐는지(-).

핵심: SOXL 매집 타이밍의 맥락을 여기서 본다."""

HELP_TAB4 = """시장에도 사계절이 있다.
- 봄(금융장세): 실적은 나쁜데 돈이 풀린다. 주가 오른다. 여기서 사면 된다.
- 여름(실적장세): 실적이 따라온다. 주가도 오른다. 정상.
- 가을(역금융장세): 실적은 좋은데 긴축 시작. 주가 떨어진다. 가지치기 구간.
- 겨울(역실적장세): 실적도 떨어진다. 근데 하락이 둔화된다. 바닥 매집 준비.

9박스 중 가장 많이 켜진 계절이 지금 계절이다 (5점부터 의미 있음, 7점 이상이면 확정).

핵심: 계절을 안다고 매매를 바꾸면 안 된다. DCA는 사계절 내내 한다."""

HELP_TAB5 = """한 달에 한 번 기록한다. 지금이 무슨 계절이고 왜 그런지 3줄 적는다.
데이터는 자동으로 붙는다. 나중에 돌아보면 내가 뭘 보고 있었는지 알 수 있다."""

# ═══ V8.0 40박스 툴팁 (2026-04-29 머지) ═══
# 키: (계절, 박스 라벨). 라벨은 auto_season() checks dict 와 1:1 일치해야 함.
# V8_BOX_LABELS 와 동일.
_SEASON_BOX_HELP = {
    # ─────────────────── 봄 (11박스) ───────────────────
    ("봄", "채권: 역전이 풀리는 중인데 고용도 살아난다"):
        "3개월물과 10년물 역전이 풀리는 중이다. 그런데 실업률도 떨어지고 있다.\n"
        "같은 정상화라도 실업률이 오르면 겨울이다. 내려가면 봄이다.\n"
        "봄과 겨울은 채권 신호가 같다. 구분하는 건 고용이다.",
    ("봄", "연준: 저점에서 인하한다"):
        "기준금리가 지난 10년 중 하위 30% 이내인데 그 자리에서 또 인하한다.\n"
        "고점에서의 인하는 가을이다. 저점에서의 인하만 봄으로 친다.\n"
        "연준이 바닥에서 돈을 푼다는 건 최악은 지났다는 뜻이다.",
    ("봄", "달러: 빠르게 떨어진다"):
        "달러지수가 6개월간 5% 이상 떨어졌다.\n"
        "달러가 떨어지기 시작하면 무섭게 떨어진다. 숏 잡지 마라.\n"
        "강달러가 풀린다는 건 유동성이 돌아온다는 뜻이다. 주식 살 때다.",
    ("봄", "채권금리곡선: 빠르게 개선 중이다"):
        "채권금리곡선 스프레드가 3개월간 30bp 이상 올라갔다.\n"
        "주가는 위치가 아닌 속도에 반응한다. 채권도 마찬가지다.\n"
        "수준이 아직 역전이어도 속도가 양수면 봄이다. 방향이 바뀐 거다.",
    ("봄", "신용: 공포가 물러나는 중이다"):
        "하이일드 스프레드가 6개월간 1%p 이상 줄었다. 1년 내 고점이 5% 이상이었다.\n"
        "회사가 망할 수 있다는 공포가 물러나는 중이다.\n"
        "스프레드가 줄어드는 속도가 빠를수록 봄이 강하다.",
    ("봄", "공포: 극단을 찍고 진정됐다"):
        "VIX가 90일 내 35를 넘었다가 지금 25 아래로 내려왔다.\n"
        "VIX 35에 지수 사서 실패한 사람 본 적 있니? 없다.\n"
        "극단의 공포가 진정되는 시점이 봄이다. 두려움에 저항하는 자만 돈을 번다.",
    ("봄", "실업률: 4%를 넘었거나 급등했다"):
        "실업률이 4%를 넘었거나 최근 3개월간 0.5%p 이상 올랐다.\n"
        "실업률 올라갈 때부터 주식을 사기 시작하면 된다. 그 출발 신호다.\n"
        "사람들은 이 숫자를 보고 겁먹는다. 그래서 싸게 살 수 있다.",
    ("봄", "바닥: 고점에서 20% 이상 빠졌다"):
        "S&P500이 52주 고점에서 20% 이상 빠진 상태다.\n"
        "두려울 때 사라. 깊은 조정 없이는 진짜 바닥 아니다.\n"
        "물려도 되는 시기다. 지금 안 사면 언제 사게?",
    ("봄", "실적: 나쁘지만 바닥은 지났다"):
        "기업 실적이 전년 대비 마이너스다. 그런데 3개월 전보다는 나아지고 있다.\n"
        "역실적장세에선 매수가 정석이다.\n"
        "실적이 빠지는 속도가 줄어든다는 건 바닥을 지났다는 뜻이다.",
    ("봄", "밸류: 거품이 다 빠졌다"):
        "Shiller CAPE 22 이하다.\n"
        "위에서 내려온 자리다. 거품이 다 빠졌다는 뜻이다.\n"
        "이 수준에서 주식 사서 장기로 손해 본 적 거의 없다.",
    ("봄", "반도체: 가장 먼저 저점을 본다"):
        "반도체가 시장보다 1개월 수익률이 좋다. S&P는 -15% 이상, 반도체는 -20% 이상 빠진 상태다.\n"
        "단순 반도체 강세가 아니다. 진짜 조정장 안에서의 반도체 선행이다.\n"
        "그 어떤 섹터보다도 반도체가 가장 먼저 저점을 본다.",
    # ─────────────────── 여름 (9박스) ───────────────────
    ("여름", "채권: 형이 안심하고 있다"):
        "3개월물과 10년물이 정상이다. 장기가 단기보다 높다. 1년 내 역전도 없었다.\n"
        "채권 형이 안심하고 있는 상태다.\n"
        "채권시장이 편안하면 주식시장도 편안하다.",
    ("여름", "연준: 건드리지 않고 있다"):
        "기준금리 6개월 변화가 ±0.5%p 이내다.\n"
        "인상도 인하도 아닌 정체. 시장이 가장 편안한 자리다.\n"
        "연준이 조용하면 주가는 실적만 따라간다.",
    ("여름", "신용: 아무도 걱정하지 않는다"):
        "하이일드 스프레드 4% 미만이다.\n"
        "회사가 망할 수 있다는 생각을 안 한다.\n"
        "위험하다. 아무도 걱정 안 할 때가 가장 걱정해야 할 때다.",
    ("여름", "공포: 없다"):
        "VIX 1개월 평균 20 미만이다.\n"
        "시장에 공포가 없다. 여름의 본질이다.\n"
        "그러나 공포가 없다는 건 보험이 싸다는 뜻이기도 하다.",
    ("여름", "고용: 견고하다"):
        "실업률 안 오르고 신규 일자리 월 10만 이상이다.\n"
        "노동시장이 견고하다. 소비도 따라온다.\n"
        "미국 GDP의 70%가 소비다. 고용이 버티면 소비가 버틴다.",
    ("여름", "시장 폭: 전 업종 동반 강세"):
        "동일가중 S&P(RSP)와 시총가중 S&P 둘 다 6개월 양수다.\n"
        "메가캡만 끌고 가는 가짜 강세가 아니다. 전 업종이 함께 간다.\n"
        "진짜 여름은 시장 폭이 건강하다.",
    ("여름", "실적: 좋고 더 좋아지고 있다"):
        "기업 실적 성장률 5% 이상이고 가속 중이다.\n"
        "주가는 위치가 아닌 속도에 반응한다. 속도가 양수면 여름이다.\n"
        "실적이 좋은 건 당연하다. 더 빨라지고 있느냐가 관건이다.",
    ("여름", "밸류: 정당화 가능하다"):
        "Shiller CAPE 30 미만이다.\n"
        "비싸지 않다는 뜻이 아니다. 아직 광기는 아니라는 뜻이다.\n"
        "30 넘으면 이 박스가 꺼진다. 그게 정직한 출력이다.",
    ("여름", "반도체: 시장을 끌고 간다"):
        "반도체가 시장보다 6개월 수익률이 좋다.\n"
        "반도체가 시장을 끌고 갈 때가 진짜 여름이다.\n"
        "반도체 산업의 주가는 시장에 선행한다. 반도체가 강하면 시장도 강하다.",
    # ─────────────────── 가을 (11박스) ───────────────────
    ("가을", "채권: 역전이 시작됐거나 깊어지고 있다"):
        "3개월물과 10년물 역전이 신규 진입했거나 더 깊어지는 중이다.\n"
        "채권시장의 붕괴는 세상물정 모르는 나스닥 동생을 보고 느낀 걱정의 결과다.\n"
        "가을의 첫 신호다.",
    ("가을", "연준: 고점에서 내리거나 올리고 있다"):
        "기준금리가 10년 중 상위 30%인데 인하했다. 또는 6개월간 50bp 이상 올렸다.\n"
        "고점에서의 금리인하는 주가 하락을 이끈다. 가을의 본질이다.\n"
        "연준이 올리고 싶다 해도 올릴 수 있다는 뜻이 아니다.",
    ("가을", "달러: 비정상이다"):
        "달러지수가 6개월간 8% 넘게 올랐거나 108을 넘었다.\n"
        "지금 달러는 비정상이다. 강달러는 부채리스크에 영향을 줘서 사람들을 불안하게 만든다.\n"
        "불안감이 다시 달러수요로 이어져 더 강달러가 되는 달러루프다.",
    ("가을", "채권금리곡선: 빠르게 악화 중이다"):
        "채권금리곡선 스프레드가 3개월간 30bp 이상 떨어졌다.\n"
        "채권이 형이다. 형이 빠르게 나빠지고 있으면 동생도 곧 당한다.\n"
        "속도가 음수면 가을이다. 느리게 나빠지는 건 조정, 빠르게 나빠지는 건 전환이다.",
    ("가을", "신용: 슬슬 벌어진다"):
        "하이일드 스프레드가 6개월간 80bp 이상 또는 3개월간 50bp 이상 벌어졌다.\n"
        "회사가 망할 수 있다는 생각이 슬슬 돌기 시작한다.\n"
        "아직 크게 벌어진 건 아니다. 그래서 위험하다. 사람들이 무시하니까.",
    ("가을", "경기활동: 둔화 중이다"):
        "시카고 연준 경기활동지수(CFNAI) 3개월 평균이 음수다.\n"
        "105개 거시 지표를 합성한 지수가 평균 아래로 내려갔다.\n"
        "경기가 둔화되기 시작했다. 주가는 경기를 따라가게 되어있다.",
    ("가을", "반도체: 먼저 꺾였다"):
        "반도체 3개월 수익률이 시장보다 나쁘다. 시장 6개월은 아직 양수인데 반도체 1개월은 음수다.\n"
        "반도체가 먼저 꺾였다. 시장은 아직 모른다.\n"
        "반도체가 선행한다. 안 그랬던 적이 없다.",
    ("가을", "시장 폭: 메가캡만 끌고 간다"):
        "시총가중 SPY가 동일가중 RSP보다 3개월 수익률이 4%p 이상 높고 RSP는 음수다.\n"
        "거대주만 끌고 가는 가짜 강세다. 시장 폭이 무너진 신호다.\n"
        "빈약한 기둥에 무거운 지붕이 얹힌 상태다.",
    ("가을", "실적: 좋지만 느려지고 있다"):
        "실적 성장률은 아직 양수다. 그러나 3개월 전보다 1%p 이상 둔화됐다.\n"
        "주가는 위치가 아닌 속도에 반응하는 방정식이다. 속도가 느려지면 떨어진다.\n"
        "실적이 좋다고 안심하지 마라. 느려지고 있느냐가 전부다.",
    ("가을", "밸류: 어떤 잣대로 봐도 비싸다"):
        "후행 PER 28 이상이거나 Shiller CAPE 32 이상이다.\n"
        "셋 중 하나만 극단이어도 켜진다. 어떤 잣대로 봐도 비싸다는 뜻이다.\n"
        "주가가 붕괴될 때 주가보다 실적이 더 빠른 속도로 붕괴되기에 PER이 50, 60, 70, 80 된다.",
    ("가을", "CAPE: 역사가 말한다"):
        "Shiller CAPE 35 이상이거나, 32 이상이면서 20년 백분위 85% 이상이다.\n"
        "역사상 이 정도 고평가된 PER에서 숏을 잡아 실패한 사례가 없다. 140년간 예외 없었다.\n"
        "항룡유회(亢龍有悔)다. 극에 이른 것은 반드시 내려온다.",
    # ─────────────────── 겨울 (9박스) ───────────────────
    ("겨울", "채권: 역전이 풀리는데 경제가 무너진다"):
        "역전이 풀리는 중이다. 그런데 실업률이 3개월간 0.3%p 이상 올랐다.\n"
        "봄과 같은 신호이지만 실적 빠지고 실업률 오를 때 함께 잡히면 겨울이다.\n"
        "역전이 풀리는 이유가 다르다. 봄은 회복, 겨울은 연준이 급하게 인하해서.",
    ("겨울", "연준: 급하게 내리고 있다"):
        "기준금리가 3개월간 내렸다. 그런데 금리 수준이 아직 중립 이상이다.\n"
        "저점에서 내리면 봄이고 고점/중립에서 내리면 겨울이다.\n"
        "연준이 급하게 내린다는 건 뭔가 터졌다는 뜻이다.",
    ("겨울", "신용: 이미 깨졌다"):
        "하이일드 스프레드 5% 이상이다.\n"
        "신용시장이 이미 깨진 상태다. 가을 다음 단계다.\n"
        "채권시장은 거의 파멸이다. 주식시장이 그걸 모른다.",
    ("겨울", "공포: 한 달 내내 지속된다"):
        "VIX 30 이상이고 1개월 평균도 25 이상이다.\n"
        "공포가 일시 스파이크가 아니다. 한 달 내내 유지된다. 진짜 패닉이다.\n"
        "주식은 지옥이다. 환영한다. 이게 주식이다.",
    ("겨울", "실업률: 빠르게 오르고 있다"):
        "실업률이 3개월간 0.5%p 이상 올랐다.\n"
        "미국 고용시장은 유연하다. 3.5%가 두 달 만에 14%가 되기도 한다.\n"
        "실업률이 튀기 시작하면 빠르다. 소프트랜딩은 힘들다.",
    ("겨울", "하락: 멈췄지만 바닥이다"):
        "S&P500이 -20% 이상 빠진 상태에서 1개월 변화가 ±3% 이내다.\n"
        "공포는 있는데 추가 하락은 멈췄다. 바닥을 다지는 자리다.\n"
        "건강한 신체에선 비만이 걱정이지만 죽을 병 걸리면 살은 저절로 빠진다.",
    ("겨울", "실적: 확실히 무너졌다"):
        "기업 실적 성장률이 -5% 이하다.\n"
        "단순 한 분기 빠진 게 아니다. 추세적 감소다. 진짜 역실적장세다.\n"
        "경기침체는 이미 시작됐다. 그리고 경기침체는 항상 투자의 적기다.",
    ("겨울", "달러: 시스템이 위험하다"):
        "달러지수 108 이상이고 3개월간 5% 넘게 올랐다.\n"
        "달러루프에 빠졌다. 이거 놔두면 시스템이 무너진다.\n"
        "연준이든 BOJ든 이 고리 끊기 위한 액션이 있을 거다. 안 그러면 인플레 전에 시스템이 위험하다.",
    ("겨울", "추세: 200일선 아래 두 달째"):
        "S&P500이 200일 이동평균선 아래에서 60일 이상 머물고 있다.\n"
        "약세장이 확정된 상태다. 단기 반등에 속지 마라.\n"
        "기다려라. 언제까지 기다려야 하는지 모르는 기다림이지만 기다려라.",
}

def easy_help(mode, text):
    """쉬운모드일 때만 해설 패널 표시."""
    if mode == "쉬운":
        with st.expander("📖 이 페이지 보는 법"):
            st.markdown(text)

# ═══ MAIN ═══
def main():
    st.set_page_config(page_title="미어캣의 관측소", page_icon="👁️", layout="wide", initial_sidebar_state="expanded")
    _auto_backup()  # 일 1회 핵심 파일 백업 (~/.meerkat/backups/YYYY-MM-DD/, 14일 유지)
    # 테마 CSS 변수 + 카드 호버
    st.markdown("""<style>
    /* ── 라이트모드 기본값 ── */
    .stApp {
        --mac-bg: #ffffff;
        --mac-card: #f0f2f6;
        --mac-text: #31333F;
        --mac-bright: #1a1a1a;
        --mac-muted: #6b7280;
        --mac-border: rgba(0,0,0,0.10);
        /* ── 폰트 사이즈 토큰 (6단계) ── */
        --mac-fs-display: 28px; /* 메인 스코어 (sgauge) */
        --mac-fs-large:   22px; /* 카드 value, 큰 표시 */
        --mac-fs-h3:      18px; /* h3 서브헤더, 강조 숫자 */
        --mac-fs-md:      13px; /* 섹션 헤더, 카드 status, 본문 */
        --mac-fs-sm:      12px; /* 카드 라벨, 디테일 */
        --mac-fs-xs:      11px; /* 단위, 캡션, 표 헤더 */
    }
    /* ── 다크모드 ── */
    @media (prefers-color-scheme: dark) {
        .stApp {
            --mac-bg: #0d1117;
            --mac-card: #161b22;
            --mac-text: #c9d1d9;
            --mac-bright: #e6edf3;
            --mac-muted: #8b949e;
            --mac-border: rgba(128,128,128,0.2);
        }
    }
    /* Streamlit 자체 테마 변수가 있으면 우선 사용 */
    [data-testid="stAppViewContainer"] {
        --mac-bg: var(--background-color, var(--mac-bg));
        --mac-card: var(--secondary-background-color, var(--mac-card));
        --mac-text: var(--text-color, var(--mac-text));
        --mac-bright: var(--text-color, var(--mac-bright));
    }
    /* ── 카드 호버 + 클릭(포커스) 확대 ── */
    .maccard {
        transition: transform 0.18s ease, box-shadow 0.18s ease;
        cursor: pointer;
        outline: none;
        margin-bottom: 10px;  /* 행 간 상하 간격 (줄바꿈 시) */
    }
    /* 컬럼 좌우 간격 확보 — st.columns 기본 gap이 좁다 */
    [data-testid="stHorizontalBlock"] {
        gap: 12px !important;
    }
    .maccard:hover {
        transform: scale(1.06);
        box-shadow: 0 8px 20px rgba(0,0,0,0.25);
        z-index: 100;
        position: relative;
    }
    .maccard:hover div {
        white-space: normal !important;
        overflow: visible !important;
        text-overflow: unset !important;
    }
    .maccard:focus {
        transform: scale(1.12);
        box-shadow: 0 12px 32px rgba(0,0,0,0.4);
        z-index: 200;
        position: relative;
        outline: 2px solid var(--mac-bright, #e6edf3);
        outline-offset: 2px;
    }
    .maccard:focus div {
        white-space: normal !important;
        overflow: visible !important;
        text-overflow: unset !important;
    }
    </style>""", unsafe_allow_html=True)
    st.sidebar.title("👁️ 미어캣의 관측소")
    st.sidebar.caption(f"V{VERSION} · 1층 40박스 + 2층 ANFCI/CAPE")
    cfg = lcfg()
    api_key = st.sidebar.text_input("FRED API 키", value=cfg.get("fred_api_key", ""), type="password")
    if api_key and api_key != cfg.get("fred_api_key", ""): scfg({"fred_api_key": api_key}); st.sidebar.success("저장", icon="✅")
    av_key = st.sidebar.text_input("Alpha Vantage API 키", value=cfg.get("av_api_key", ""), type="password")
    if av_key and av_key != cfg.get("av_api_key", ""): scfg({"av_api_key": av_key}); st.sidebar.success("저장", icon="✅")
    mo = ["일반", "쉬운", "병신"]
    mode = st.sidebar.radio("모드", mo, index=mo.index(cfg.get("mode", "일반")) if cfg.get("mode", "일반") in mo else 0, horizontal=True)
    if mode != cfg.get("mode", "일반"): scfg({"mode": mode})
    deep = st.sidebar.checkbox("👁️‍🗨️ 심안", value=cfg.get("deep", False))
    if deep != cfg.get("deep", False): scfg({"deep": deep})
    # ── 미니 시계열 토글 (모든 카드 하단에 sparkline 표시) ──
    show_minichart = st.sidebar.checkbox("📈 미니 시계열 표시", value=cfg.get("show_minichart", False),
                                         help="모든 카드 하단에 작은 시계열 차트(sparkline)를 표시한다.")
    if show_minichart != cfg.get("show_minichart", False): scfg({"show_minichart": show_minichart})
    if show_minichart:
        _mc_periods = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365}
        _mc_default = cfg.get("minichart_period", "3M")
        if _mc_default not in _mc_periods: _mc_default = "3M"
        minichart_period = st.sidebar.selectbox("    └ 기간", list(_mc_periods.keys()),
                                                index=list(_mc_periods.keys()).index(_mc_default))
        if minichart_period != cfg.get("minichart_period", "3M"): scfg({"minichart_period": minichart_period})
        minichart_days = _mc_periods[minichart_period]
    else:
        minichart_period = "3M"; minichart_days = 90
    st.sidebar.divider()
    lbo = ["3년", "5년", "10년"]
    lb = st.sidebar.selectbox("관찰 기간", lbo, index=lbo.index(cfg.get("lb", "5년")) if cfg.get("lb", "5년") in lbo else 1)
    if lb != cfg.get("lb", "5년"): scfg({"lb": lb})
    yfp = {"3년": "3y", "5년": "5y", "10년": "10y"}[lb]
    fst = (datetime.now() - timedelta(days={"3년": 3, "5년": 5, "10년": 10}[lb] * 365)).strftime("%Y-%m-%d")
    mk = lmk()
    # ── 수동 새로고침 ──
    st.sidebar.divider()
    if st.sidebar.button("🔄 데이터 강제 새로고침", use_container_width=True):
        st.cache_data.clear()
        _dc_clear()
        st.rerun()
    # 실패 소스만 재시도 — streamlit cache_data 만 클리어, 디스크 캐시 보존.
    # 정상 소스는 디스크 캐시 즉시 히트 (네트워크 호출 0). 실패 소스는 디스크 캐시
    # 가 애초에 저장 안 됐으므로 API 재호출 시도.
    if st.sidebar.button("🔁 실패 소스만 재시도", use_container_width=True,
                         help="정상 소스는 디스크 캐시에서 즉시 복원, 실패한 소스만 API 재호출"):
        try: ffred_daily.clear()
        except Exception: pass
        try: ffred_weekly.clear()
        except Exception: pass
        try: ffred_monthly.clear()
        except Exception: pass
        try: ffred_quarter.clear()
        except Exception: pass
        try: fyf_batch.clear()
        except Exception: pass
        st.rerun()
    # ── 자가 업데이트 (GitHub raw, git 불필요) ──
    if st.sidebar.button("⬇️ 업데이트 (GitHub)", use_container_width=True, help="GitHub 에서 최신 버전 다운로드. git 불필요."):
        with st.spinner("최신 버전 확인 중..."):
            _upd, _fail = _self_update()
        if _upd:
            st.sidebar.success(f"✅ 업데이트됨: {', '.join(_upd)} — 앱을 재시작하세요.")
            _check_update_available.clear()  # 캐시 무효화 — 다음 체크 시 최신 반영
        elif not _fail:
            st.sidebar.info("✅ 이미 최신 버전입니다.")
        if _fail:
            st.sidebar.error("⚠️ " + " / ".join(_fail))
    # 자동 체크: GitHub 최신 != 로컬 → 알림 (1시간 캐시)
    try:
        if _check_update_available():
            st.sidebar.warning("🔔 GitHub 에 최신 버전 있음 — 업데이트 권장")
    except Exception:
        pass
    if not api_key:
        st.title("👁️ 미어캣의 관측소"); st.info("사이드바에 FRED API 키를 입력하라.")
        st.markdown("---"); st.markdown(f"> *{dq()}*"); return

    # ── DATA ──
    with st.spinner("데이터 로딩 중..."):
        # FRED 시리즈 — 스코어 계산용(T10YIE, GDP)은 항상 로드
        fs = {"DGS2": "DGS2", "DGS10": "DGS10", "DGS3MO": "DTB3", "T10Y2Y": "T10Y2Y", "T10Y3M": "T10Y3M",
              "DGS20": "DGS20", "DGS30": "DGS30",
              "VIXCLS": "VIXCLS", "HY": "BAMLH0A0HYM2", "UNRATE": "UNRATE", "FEDFUNDS": "FEDFUNDS",
              "PAYEMS": "PAYEMS", "WTI": "DCOILWTICO", "KRW": "DEXKOUS",
              "T10YIE": "T10YIE", "GDP": "A191RL1Q225SBEA",
              "GDP_NOMINAL": "GDP", "WILSHIRE": "NCBEILQ027S",  # F4 버핏 지표 — 항상 로드 (Z.1 corp equity, 분기)
              "CFNAI": "CFNAI"}  # V3.3 시카고 연준 활동지수 — 항상 로드, 카드는 심안
        if deep: fs.update({"JTSJOL": "JTSJOL", "UNEMPLOY": "UNEMPLOY", "CPIAUCSL": "CPIAUCSL", "CPILFESL": "CPILFESL",
                            "PCEPI": "PCEPI", "PCEPILFE": "PCEPILFE",
                            "T5YIE": "T5YIE", "DRCCLACBS": "DRCCLACBS", "GFDEGDQ188S": "GFDEGDQ188S", "UMCSENT": "UMCSENT",
                            "INDPRO": "INDPRO"})  # V3.11.1: DTW 8 canonical
        fd = ffred_parallel(fs, api_key, fst)
        # yfinance — 배치 다운로드 (병렬 X, 단일 호출로 데이터 혼선 방지)
        # DXY는 DX-Y.NYB.
        # V3.5-hotfix: VIX/WTI/KRW 모두 yfinance primary + FRED fallback 구조.
        # 실시간성이 중요한 데이터라 yfinance 우선, 죽으면 FRED로 자동 폴백.
        yt = {"DXY": "DX-Y.NYB", "SOXX": "SOXX", "SPX": "^GSPC", "GOLD": "GC=F",
              "QQQ": "QQQ", "XLE": "XLE", "XLK": "XLK",
              "VIX_YF": "^VIX", "WTI_YF": "CL=F", "KRW_YF": "KRW=X",
              "TQQQ": "TQQQ", "SOXL": "SOXL", "VOO": "VOO", "SGOV": "SGOV",
              "MOVE": "^MOVE",
              # V3.8: 시장 폭 측정용 (겨울 #8 — 메가캡 의존)
              "SPY": "SPY", "RSP": "RSP"}
        yd = fyf_load(yt, yfp)
        # ── V3.5-hotfix: VIX/WTI/KRW 소스 일원화 (yfinance primary, FRED fallback) ──
        # 키 이름은 fd 안에서 그대로 유지 → 호출부 무수정.
        # 죽으면 FRED 시리즈 그대로 사용 → 가용성 오히려 향상.
        _vix_yf = yd.get("VIX_YF")
        if _vix_yf is not None and len(_vix_yf) > 0:
            fd["VIXCLS"] = _vix_yf
        _wti_yf = yd.get("WTI_YF")
        if _wti_yf is not None and len(_wti_yf) > 0:
            fd["WTI"] = _wti_yf
        _krw_yf = yd.get("KRW_YF")
        if _krw_yf is not None and len(_krw_yf) > 0:
            fd["KRW"] = _krw_yf
        # MOVE 지수 폴백: ^MOVE 실패 시 대체 티커 탐색
        _move_yf = yd.get("MOVE")
        if _move_yf is None or len(_move_yf) == 0:
            for _alt in ("MOVE", "MOVE.IX"):
                try:
                    _alt_raw = fyf_batch((_alt,), yfp)
                    _alt_s = _alt_raw.get(_alt)
                    if _alt_s is not None and len(_alt_s) > 0:
                        yd["MOVE"] = _alt_s
                        break
                except: pass

        # ── V3.7 관측 히스토리 (raw backfill + 매 방문 최근값 merge) ──
        # 첫 실행: max history 백필 (시리즈별 1회, 마커로 중복 방지). 실패해도 앱 진행.
        # 매 방문: fd/yd 의 최근 90일을 raw.jsonl 에 merge (백필 없어도 점진 축적).
        try:
            _bf_marker = _hist_load_marker()
            _bf_need = ([s for s in RAW_FRED_IDS if not _bf_marker["fred"].get(s)]
                        or [t for t in RAW_YF_TICKERS if not _bf_marker["yf"].get(t)])
            if _bf_need:
                with st.spinner("초기 raw 히스토리 백필 중... 한 번만 실행된다"):
                    _bfr = _hist_backfill_once(api_key)
                    try: _hist_load_raw_df.clear()
                    except: pass
            _hist_update_raw_latest(fd, yd, tail_days=90)
            try: _hist_load_raw_df.clear()
            except: pass
        except Exception as _hist_err:
            try: st.sidebar.caption(f"⚠️ raw 히스토리 갱신 실패: {type(_hist_err).__name__}")
            except: pass
    # API 호출 시각은 _dc_set → ~/.meerkat/last_fetch 에 기록됨
    # 데이터 신선도 + 실패 수집
    _fresh = {}  # {이름: (마지막날짜, 소스)}
    _fails = []
    # VIXCLS/WTI/KRW는 V3.5-hotfix에서 yfinance 우선 → 소스 라벨 보정
    _src_override = {
        "VIXCLS": "yfinance" if (_vix_yf is not None and len(_vix_yf) > 0) else "FRED",
        "WTI":    "yfinance" if (_wti_yf is not None and len(_wti_yf) > 0) else "FRED",
        "KRW":    "yfinance" if (_krw_yf is not None and len(_krw_yf) > 0) else "FRED",
    }
    for k, s in fd.items():
        if s is not None and len(s) > 0:
            _src = _src_override.get(k, "FRED")
            _fresh[k] = (str(s.index[-1].date()) if hasattr(s.index[-1], 'date') else str(s.index[-1]), _src)
        else: _fails.append(f"FRED:{k}")
    for k, s in yd.items():
        if k in ("VIX_YF", "WTI_YF", "KRW_YF"): continue  # fd에 이미 주입됨, 중복 카운트 방지
        if s is not None and len(s) > 0:
            _fresh[k] = (str(s.index[-1].date()) if hasattr(s.index[-1], 'date') else str(s.index[-1]), "yfinance")
        else: _fails.append(f"YF:{k}")
    # 사이드바 캐시 시각 표시
    try: _loaded_at = _LAST_FETCH.read_text("utf-8").strip()
    except:
        # last_fetch 파일이 없으면 캐시 파일 중 가장 최근 mtime 사용
        _loaded_at = "—"
        try:
            import re as _re
            _hex16 = _re.compile(r"^[0-9a-f]{16}\.json$")
            # 누적 히스토리 파일(forward_eps_history 등)은 제외 — 캐시가 아니라 영구 누적물
            _cache_files = [p for p in CACHE_DIR.glob("*.json") if _hex16.match(p.name)]
            if _cache_files:
                _latest = max(_cache_files, key=lambda p: p.stat().st_mtime)
                _loaded_at = datetime.fromtimestamp(_latest.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                _LAST_FETCH.write_text(_loaded_at, "utf-8")
        except: pass
    st.sidebar.caption(f"📊 마지막 API 호출: {_loaded_at}")
    _ok = len(_fresh); _fail = len(_fails)
    st.sidebar.caption(f"⏱ 소스: {_ok}개 정상{f' · {_fail}개 실패' if _fail else ''}")
    # 자동 백업 상태
    try:
        _bk_today = BKD / _date.today().isoformat()
        if _bk_today.exists():
            _bk_cnt = sum(1 for _ in _bk_today.rglob("*") if _.is_file())
            st.sidebar.caption(f"💾 오늘 백업: {_bk_cnt}개 파일 · `~/.meerkat/backups/{_date.today().isoformat()}/`")
    except Exception: pass

    # 거래일(d) → 캘린더 변환: 252→1Y, 126→6M, 63→3M, 21→1M, 10→2W, 5→1W
    def _d2off(d):
        if d >= 250: return pd.DateOffset(years=1)
        if d >= 120: return pd.DateOffset(months=6)
        if d >= 60:  return pd.DateOffset(months=3)
        if d >= 18:  return pd.DateOffset(months=1)
        if d >= 8:   return pd.DateOffset(weeks=2)
        return pd.DateOffset(weeks=1)
    def L(s):
        if s is not None and len(s) > 0: return float(s.iloc[-1])
        return None
    def _asof_old(s, d):
        if s is None or len(s) < 2: return None
        s2 = s.dropna()
        if len(s2) < 2: return None
        target = s2.index[-1] - _d2off(d)
        if target < s2.index[0]: return None
        v = s2.asof(target)
        return float(v) if v is not None and not pd.isna(v) else None
    def chg(s, d=126):
        n = L(s); o = _asof_old(s, d)
        if n is None or o is None or o == 0: return None
        return (n / o - 1) * 100
    def ca(s, d=63):
        n = L(s); o = _asof_old(s, d)
        if n is None or o is None: return None
        return n - o
    def yoy(s):
        n = L(s); o = _asof_old(s, 252)
        if n is None or o is None or o <= 0: return None
        return (n / o - 1) * 100

    def chg_common(s1, s2, d=63):
        """두 시리즈의 공통 최신일 기준으로 각각 d거래일 전 대비 % 변화율을 반환.
        SOX/SPY, XLE/SPY 등 상대 비교 시 기준일 어긋남 방지."""
        if s1 is None or s2 is None: return (None, None)
        s1d = s1.dropna(); s2d = s2.dropna()
        if len(s1d) == 0 or len(s2d) == 0: return (None, None)
        cm = s1d.index.intersection(s2d.index)
        if len(cm) <= d: return (None, None)
        _end = cm[-1]
        _start = cm[-d-1] if len(cm) > d else cm[0]
        def _ch(s):
            try:
                n = float(s.loc[_end]); o = float(s.loc[_start])
                if o == 0 or pd.isna(n) or pd.isna(o): return None
                return (n / o - 1) * 100
            except: return None
        return (_ch(s1d), _ch(s2d))

    t10y2y = L(fd.get("T10Y2Y")); t10y3m = L(fd.get("T10Y3M"))
    dgs2 = L(fd.get("DGS2")); dgs10 = L(fd.get("DGS10")); dgs3m = L(fd.get("DGS3MO"))
    # ── 금리 시리즈 날짜 동기화: 스프레드는 DGS3MO/DGS2/DGS10 공통 최신일 기준으로 재계산 ──
    # 각 시리즈의 최신 observation_date가 다를 수 있어(FRED 업데이트 타이밍 차이),
    # 공통 최신일을 찾아 세 금리와 스프레드를 모두 그 날짜에 맞춰 일관되게 만든다.
    def _last_date(s):
        if s is None or len(s) == 0: return None
        try:
            sd = s.dropna()
            if len(sd) == 0: return None
            return str(sd.index[-1].date()) if hasattr(sd.index[-1], "date") else str(sd.index[-1])
        except: return None
    _d3s_raw = fd.get("DGS3MO"); _d2s_raw = fd.get("DGS2"); _d10s_raw = fd.get("DGS10")
    _dgs3m_date = _last_date(_d3s_raw); _dgs2_date = _last_date(_d2s_raw); _dgs10_date = _last_date(_d10s_raw)
    _rate_common_date = None
    if _d3s_raw is not None and _d2s_raw is not None and _d10s_raw is not None:
        _s3 = _d3s_raw.dropna(); _s2 = _d2s_raw.dropna(); _s10 = _d10s_raw.dropna()
        _cm_rates = _s3.index.intersection(_s2.index).intersection(_s10.index)
        if len(_cm_rates) > 0:
            _ct = _cm_rates[-1]
            _rate_common_date = str(_ct.date()) if hasattr(_ct, "date") else str(_ct)
            dgs3m = float(_s3.loc[_ct]); dgs2 = float(_s2.loc[_ct]); dgs10 = float(_s10.loc[_ct])
            # 스프레드 재계산 (공통일 기준) — 단위 %p (기존 관행: *100 하여 bp)
            t10y2y = dgs10 - dgs2
            t10y3m = dgs10 - dgs3m
    vix = L(fd.get("VIXCLS")); hy = L(fd.get("HY")); unemp = L(fd.get("UNRATE")); ff = L(fd.get("FEDFUNDS"))
    dxy = L(yd.get("DXY")); gold = L(yd.get("GOLD"))
    krw = L(fd.get("KRW"))  # FRED DEXKOUS (원/달러)
    wti = L(fd.get("WTI"))  # FRED DCOILWTICO
    t3m2y = (dgs3m - dgs2) if (dgs3m is not None and dgs2 is not None) else None
    t10yie = L(fd.get("T10YIE")); rr = (dgs10 - t10yie) if (dgs10 is not None and t10yie is not None) else None

    # SOX/SPX — 공통 최신일 기준 (카드 단독값도 같은 날짜로 맞춤)
    sox_s = yd.get("SOXX"); spx_s = yd.get("SPX")
    sox = None; spx = None; sox_spx = None; sox_dd = None; sox_rel3 = None
    if sox_s is not None and spx_s is not None and len(sox_s) > 0 and len(spx_s) > 0:
        cm = sox_s.index.intersection(spx_s.index)
        if len(cm) > 0:
            _ce = cm[-1]
            sox = float(sox_s.loc[_ce]); spx = float(spx_s.loc[_ce])
            sox_spx = sox / spx if spx != 0 else None
    else:
        if sox_s is not None and len(sox_s) > 0: sox = float(sox_s.iloc[-1])
        if spx_s is not None and len(spx_s) > 0: spx = float(spx_s.iloc[-1])
    if sox_s is not None and len(sox_s) > 20: sox_dd = (float(sox_s.iloc[-1]) / float(sox_s.max()) - 1) * 100
    # 3M 변화율 — 공통일 기준 (sc3/spc3 둘 다 같은 end/start)
    sc3, spc3 = chg_common(sox_s, spx_s, 63)
    if sc3 is not None and spc3 is not None: sox_rel3 = sc3 - spc3
    # 섹터 디커플링: XLE/XLK vs SPY 3M 상대수익률 (%p) — SPY와 공통 최신일 맞춤
    _xle3, _spy3_for_xle = chg_common(yd.get("XLE"), spx_s, 63)
    _xlk3, _spy3_for_xlk = chg_common(yd.get("XLK"), spx_s, 63)
    xle_spy_3m = (_xle3 - _spy3_for_xle) if (_xle3 is not None and _spy3_for_xle is not None) else None
    xlk_spy_3m = (_xlk3 - _spy3_for_xlk) if (_xlk3 is not None and _spy3_for_xlk is not None) else None
    _sec_lbl, _sec_clr, _sec_d, _sec_q = _sector_quad(xle_spy_3m, xlk_spy_3m)
    # 카드 라벨/색 (UI + export 양쪽에서 재사용)
    _xle_lbl = "—"; _xle_clr = C["muted"]
    if xle_spy_3m is not None:
        if xle_spy_3m > 5:    _xle_lbl, _xle_clr = "강한 아웃퍼폼", C["red"]
        elif xle_spy_3m > 2:  _xle_lbl, _xle_clr = "아웃퍼폼", C["gold"]
        elif xle_spy_3m < -3: _xle_lbl, _xle_clr = "강한 언더퍼폼", C["green"]
        elif xle_spy_3m < -2: _xle_lbl, _xle_clr = "언더퍼폼", C["gold"]
        else:                 _xle_lbl, _xle_clr = "균형", C["muted"]
    _xlk_lbl = "—"; _xlk_clr = C["muted"]
    if xlk_spy_3m is not None:
        if xlk_spy_3m > 5:    _xlk_lbl, _xlk_clr = "강한 아웃퍼폼", C["green"]
        elif xlk_spy_3m > 2:  _xlk_lbl, _xlk_clr = "아웃퍼폼", C["gold"]
        elif xlk_spy_3m < -3: _xlk_lbl, _xlk_clr = "강한 언더퍼폼", C["red"]
        elif xlk_spy_3m < -2: _xlk_lbl, _xlk_clr = "언더퍼폼", C["gold"]
        else:                 _xlk_lbl, _xlk_clr = "균형", C["muted"]

    cpi_y = yoy(fd.get("CPIAUCSL")); cpic_y = yoy(fd.get("CPILFESL"))
    pce_y = yoy(fd.get("PCEPI")); pcec_y = yoy(fd.get("PCEPILFE"))
    # 심안 변수 — export용으로 항상 계산 (deep 아니면 FRED 데이터 없어서 None)
    jolts = L(fd.get("JTSJOL"))
    payems_s = fd.get("PAYEMS"); nfp = None
    if payems_s is not None and len(payems_s) > 1:
        # 월간 인접성 체크: iloc[-2]가 직전 월이 아니면(공표 지연/결측) NFP 의미 왜곡 → 스킵
        try:
            _gap = (payems_s.index[-1] - payems_s.index[-2]).days
            if _gap <= 45: nfp = float(payems_s.iloc[-1]) - float(payems_s.iloc[-2])
        except Exception: pass
    gdpv = L(fd.get("GDP"))
    um = L(fd.get("UMCSENT"))
    bei = L(fd.get("T5YIE"))
    cd = L(fd.get("DRCCLACBS"))
    dg = L(fd.get("GFDEGDQ188S"))
    # F4 버핏 지표: Z.1 NCBEILQ027S(비금융기업 주식 부채) / 명목 GDP × 100
    # WILL5000INDFC는 십억 달러 단위 (이미 시총). GDP는 십억 달러. 비율 그대로 %로 환산.
    # NCBEILQ027S: $백만 단위 → ÷1000 = $십억 (GDP 단위와 맞춤)
    _w5 = L(fd.get("WILSHIRE")); _gdpn = L(fd.get("GDP_NOMINAL"))
    buffett = round((_w5 / 1000) / _gdpn * 100, 1) if (_w5 is not None and _gdpn is not None and _gdpn > 0) else None
    t3m2y_bp = round(t3m2y * 100, 0) if t3m2y is not None else None
    vd = fval(av_key); tpe = vd.get("trailing_pe"); fpe = vd.get("forward_pe"); cape = vd.get("cape"); dy = vd.get("div_yield")
    # Forward PE 폴백 체인: 1) fval(AV/yfinance) → 2) Top10 시총가중 → 3) EPS비율 → 4) trailing PE
    spy_info = fspy_info()
    if fpe is None:
        # 2차: Top10 시총가중 (가장 정확, ~20.6 실제 ~20.96)
        _est = fest_fwd_pe()
        if _est is not None:
            fpe = _est; vd["forward_pe"] = fpe
            vd["source"] = vd.get("source", "없음") + " + Top10가중"
    if fpe is None and tpe is not None:
        # 3차: EPS 비율
        te = spy_info.get("trailingEps"); fe = spy_info.get("forwardEps")
        if te and fe and te > 0 and fe > 0:
            fpe = round(tpe * (te / fe), 1)
            vd["forward_pe"] = fpe
            vd["source"] = vd.get("source", "없음") + " + EPS계산"
    _fpe_is_copy = False
    if fpe is None and tpe is not None:
        # 4차: trailing PE 폴백 (표시용만. 스코어에선 null 처리)
        fpe = tpe; vd["forward_pe"] = fpe; _fpe_is_copy = True
        vd["source"] = vd.get("source", "없음") + " + TrailingPE폴백"
    # STEP 5-8 자동 누적: 1~3차 폴백으로 산출된 fpe 만 historical_loader 에 적재 (4차 제외)
    if fpe is not None and not _fpe_is_copy:
        try:
            from historical_loader import append_forward_pe_entry
            _src_label = vd.get("source", "auto")
            append_forward_pe_entry(
                date_str=datetime.now().strftime("%Y-%m-%d"),
                fpe=fpe,
                source=_src_label,
                feps=spy_info.get("forwardEps") if spy_info else None,
                spx=L(spx_s) if spx_s is not None else None,
            )
        except Exception: pass
    # 밸류에이션 카드 출처 노출용 문자열.
    # - _val_src_fwd: Forward PE 전용. 폴백 체인(multpl.com + Top10가중 등) 전부 포함.
    # - _val_src_base: trailing_pe/cape/div_yield 전용. Forward 체인 꼬리는 제거한 원본 소스.
    _val_src_fwd = vd.get("source") or "없음"
    _val_src_base = _val_src_fwd.split(" + ")[0]
    fg = ffg(); fgs = fg.get("score")
    _fg_src = fg.get("source", "?")
    if fgs is None: _fails.append("Fear&Greed")

    # FEDFUNDS 6M 변화 — 날짜 기반 (월간 데이터)
    def _ago_val(s, days):
        if s is None or len(s) < 2: return None
        target = s.index[-1] - timedelta(days=days)
        older = s[s.index <= target]
        return float(older.iloc[-1]) if len(older) > 0 else None
    ff_now_v = L(fd.get("FEDFUNDS")); ff_6m_v = _ago_val(fd.get("FEDFUNDS"), 180)
    ff6m_chg = (ff_now_v - ff_6m_v) if (ff_now_v is not None and ff_6m_v is not None) else None
    un_now_v = L(fd.get("UNRATE")); un_3m_v = _ago_val(fd.get("UNRATE"), 90)
    un3m_chg = (un_now_v - un_3m_v) if (un_now_v is not None and un_3m_v is not None) else None

    # V3.3 CFNAI MA3 — 시카고 연준 활동지수 3개월 이동평균
    _cfnai_s = fd.get("CFNAI")
    cfnai_ma3 = None
    _cfnai_ma3_s = None
    if _cfnai_s is not None and len(_cfnai_s) >= 3:
        _cfnai_ma3_s = _cfnai_s.dropna().rolling(3).mean().dropna()
        if len(_cfnai_ma3_s) > 0: cfnai_ma3 = float(_cfnai_ma3_s.iloc[-1])

    gd = {"t10y2y": t10y2y, "t10y3m": t10y3m,  # V3.6: 10Y-3M 신규
          "ff6m": ff6m_chg, "rr": rr, "fpe": None if _fpe_is_copy else fpe, "cape": cape, "hy": hy,
          "sdd": sox_dd, "sr3m": sox_rel3, "vix": vix, "fg": fgs, "u3m": un3m_chg, "gdp": L(fd.get("GDP")),
          "buffett": buffett, "cf": cfnai_ma3}

    # ── F1 역전 해소 변곡점 (2Y10Y, 3M10Y) ──
    _inv2y10y = _inv_recovery(fd.get("T10Y2Y"))
    _inv3m10y = _inv_recovery(fd.get("T10Y3M"))

    # ── F3 FF금리 historical 위치 (10Y rolling percentile) ──
    _ff_pos = _ff_position(fd.get("FEDFUNDS"), lookback_years=10)
    # 인하 사이클의 단계: 저점/중립/고점에 따라 의미가 다름
    # 봄 인하 = 저점에서 인하 (구제), 가을 인하 = 고점에서 인하 (위기 대응)
    _ff_stage = None  # "저점" / "중립" / "고점"
    if _ff_pos is not None:
        if _ff_pos >= 70:   _ff_stage = "고점"
        elif _ff_pos >= 30: _ff_stage = "중립"
        else:               _ff_stage = "저점"

    # ── F2 금리 인하 사이클 단계 ──
    _cut_info = _cut_cycle(fd.get("FEDFUNDS"))

    # ── F6 반사성 프록시 (Forward EPS 시계열 누적) ──
    _today_str = datetime.now().strftime("%Y-%m-%d")
    _feps_now = spy_info.get("forwardEps") if spy_info else None
    _spx_snap = float(spx_s.iloc[-1]) if (spx_s is not None and len(spx_s) > 0) else None
    _save_fwd_snapshot(_today_str, fpe, _feps_now, _spx_snap)
    _fwd_hist = _load_fwd_hist()
    _refl_30 = _reflexivity(_fwd_hist, days=30)
    _refl_90 = _reflexivity(_fwd_hist, days=90)

    # ── F5 가속도 (시장 5종: VIX·HY·2Y10Y·DXY·SOX/SPX) ──
    # _sox_spx_s는 main 후반에 계산되므로 여기서 미리 산출
    _ac_sox_spx_s = None
    _sox_pre = yd.get("SOXX"); _spx_pre = yd.get("SPX")
    if _sox_pre is not None and _spx_pre is not None and len(_sox_pre) > 0 and len(_spx_pre) > 0:
        _cm_pre = _sox_pre.index.intersection(_spx_pre.index)
        if len(_cm_pre) > 0: _ac_sox_spx_s = _sox_pre.loc[_cm_pre] / _spx_pre.loc[_cm_pre]
    _ac_vix    = _accel(fd.get("VIXCLS"), mode="abs", indicator="VIX")
    _ac_hy     = _accel(fd.get("HY"), mode="abs", indicator="HY")
    _ac_t2y10y = _accel(fd.get("T10Y2Y"), mode="abs", indicator="T10Y2Y")
    _ac_dxy    = _accel(yd.get("DXY"), mode="pct", indicator="DXY")
    _ac_soxspx = _accel(_ac_sox_spx_s, mode="pct", indicator="SOX/SPX")

    # ── 원본 지표 ΔΔ (§4) ──
    _raw_dd = {}
    _raw_dd_src = {
        "T10Y2Y": fd.get("T10Y2Y"),
        "VIX":    fd.get("VIXCLS"),
        "HY":     fd.get("HY"),
        "UNEMP":  fd.get("UNRATE"),
        "CPI":    yoy_s(fd.get("CPIAUCSL")),
    }
    for _rk, _rs in _raw_dd_src.items():
        if _rs is not None and len(_rs) >= 60:
            _rdd = compute_delta_delta(_rs)
            if _rdd:
                _rdd["label_raw"] = _dd_label_2nd_raw(_rdd["delta_delta"], _DD_RAW_THRESHOLDS[_rk])
            _raw_dd[_rk] = _rdd
        else:
            _raw_dd[_rk] = None

    gs, gs_detail = mac_sc(gd)

    # ── 미어캣 스코어 계산 ──
    # QQQ 52주 DD + 고점 (auto_season 내부 from_hi와 동일 계산이나 스코프 밖이라 재계산)
    _qqq_s = yd.get("QQQ"); _mk_qqq_dd = None; _mk_qqq_52w_high = None
    if _qqq_s is not None and len(_qqq_s) > 252:
        _mk_qqq_52w_high = float(_qqq_s.iloc[-252:].max())
        _mk_qqq_dd = (float(_qqq_s.iloc[-1]) / _mk_qqq_52w_high - 1) * 100
    # SOXX 52주 DD + 고점 (기존 sox_dd는 전체기간 max 기준이라 52주로 재계산)
    _sox_s = yd.get("SOXX"); _mk_soxx_dd = None; _mk_soxx_52w_high = None
    if _sox_s is not None and len(_sox_s) > 252:
        _mk_soxx_52w_high = float(_sox_s.iloc[-252:].max())
        _mk_soxx_dd = (float(_sox_s.iloc[-1]) / _mk_soxx_52w_high - 1) * 100
    # state.json에서 계좌 데이터
    _mk_cash = mk.get("cash_pct") if mk else None
    _mk_ratio = mk.get("tqqq_ratio") if mk else None
    _mk_soxl_ratio = mk.get("soxl_ratio") if mk else None
    # V3.6: TQQQ/SOXL 실시간 가격 → ratio 자체 계산 (snapshot 폴백)
    # 엑셀에는 평단/주식수만 두고, 현재가 + ratio는 yfinance로 갱신.
    # 우선순위: 프리/애프터 1m bar (fyf_live_price) → 일간 종가 (fyf_load) → snapshot
    _tq_live = yd.get("TQQQ"); _sx_live = yd.get("SOXL")
    _tq_daily = float(_tq_live.iloc[-1]) if _tq_live is not None and len(_tq_live) > 0 else None
    _sx_daily = float(_sx_live.iloc[-1]) if _sx_live is not None and len(_sx_live) > 0 else None
    _tq_extended = fyf_live_price("TQQQ")
    _sx_extended = fyf_live_price("SOXL")
    _tq_live_price = _tq_extended if _tq_extended is not None else _tq_daily
    _sx_live_price = _sx_extended if _sx_extended is not None else _sx_daily
    _tq_live_src = "extended" if _tq_extended is not None else ("daily" if _tq_daily is not None else None)
    _sx_live_src = "extended" if _sx_extended is not None else ("daily" if _sx_daily is not None else None)
    _ratio_is_live = False; _soxl_ratio_is_live = False
    # ── 확장 필드 (V3.2 쌍발 엔진 연동) ──
    _mk_tqqq_eval = mk.get("tqqq_eval") if mk else None
    _mk_soxl_eval = mk.get("soxl_eval") if mk else None
    _mk_sgov_val  = mk.get("sgov_val") if mk else None
    _mk_total_val = mk.get("total_val") if mk else None
    # V3.5-hotfix3: 원화 raw + yfinance KRW=X로 환산 (총자산 정의 이후 위치)
    _mk_krw_val   = mk.get("krw_val") if mk else None
    _fx_krw = None; _mk_krw_usd = None
    try:
        _ks = fd.get("KRW")
        if _ks is not None and len(_ks) > 0:
            _fx_krw = float(_ks.iloc[-1])
            if _mk_krw_val and _fx_krw > 0:
                _mk_krw_usd = _mk_krw_val / _fx_krw
    except: pass
    # 통합 현금/총자산 (KRW 환산분 포함) — 표시·점수 산출용 보정값
    _mk_cash_total = (_mk_sgov_val or 0) + (_mk_krw_usd or 0)
    _mk_total_val_adj = (_mk_total_val or 0) + (_mk_krw_usd or 0)
    _mk_cash_pct_adj = (_mk_cash_total / _mk_total_val_adj) if _mk_total_val_adj > 0 else None
    # KRW 환산값이 있으면 cash_pct를 보정값으로 덮어씀 → 미어캣 점수·6카드·export 모두 일관 반영
    if _mk_krw_usd is not None and _mk_krw_usd > 0 and _mk_cash_pct_adj is not None:
        _mk_cash = _mk_cash_pct_adj
    _mk_tqqq_cost = mk.get("tqqq_cost") if mk else None
    _mk_soxl_cost = mk.get("soxl_cost") if mk else None
    _mk_tqqq_shares = mk.get("tqqq_shares") if mk else None
    _mk_soxl_shares = mk.get("soxl_shares") if mk else None
    _mk_tqqq_price = mk.get("tqqq_price") if mk else None
    _mk_soxl_price = mk.get("soxl_price") if mk else None
    # V3.6: live 가격 우선 → ratio·eval·price 모두 갱신, snapshot은 폴백
    if _tq_live_price and _mk_tqqq_shares and _mk_tqqq_cost:
        _mk_tqqq_price = _tq_live_price
        _mk_tqqq_eval  = _tq_live_price * _mk_tqqq_shares
        _mk_ratio      = _mk_tqqq_eval / _mk_tqqq_cost
        _ratio_is_live = True
    if _sx_live_price and _mk_soxl_shares and _mk_soxl_cost:
        _mk_soxl_price = _sx_live_price
        _mk_soxl_eval  = _sx_live_price * _mk_soxl_shares
        _mk_soxl_ratio = _mk_soxl_eval / _mk_soxl_cost
        _soxl_ratio_is_live = True
    # live 갱신 시 총자산·현금비중도 재계산 (KRW 환산분 유지)
    if _ratio_is_live or _soxl_ratio_is_live:
        _mk_total_val = (_mk_tqqq_eval or 0) + (_mk_soxl_eval or 0) + (_mk_sgov_val or 0)
        _mk_total_val_adj = _mk_total_val + (_mk_krw_usd or 0)
        _mk_cash_total = (_mk_sgov_val or 0) + (_mk_krw_usd or 0)
        if _mk_total_val_adj > 0:
            _mk_cash_pct_adj = _mk_cash_total / _mk_total_val_adj
            _mk_cash = _mk_cash_pct_adj
    _mk_last_check = mk.get("last_check_date") if mk else None
    _mk_last_buy   = mk.get("last_buy_date") if mk else None
    _mk_ytd_count  = mk.get("trigger_count_ytd") if mk else None
    _mk_next_buy   = mk.get("next_buy_amount") if mk else None
    # V3.5-hotfix2: ratio 매도 파라미터 (외부 도구 프리셋 → state.json) — fallback은 V1.2 호환값
    _mk_trigger    = mk.get("trigger") if mk else None
    _mk_target     = mk.get("target") if mk else None
    # DD 트리거 단계 파라미터 (외부 도구 → state.json → 본 앱)
    # 매수배율 DD (DCA 증폭)
    _dd_caution = mk.get("dd_caution", -0.10) if mk else -0.10
    _dd_correction = mk.get("dd_correction", -0.15) if mk else -0.15
    _dd_crash = mk.get("dd_crash", -0.25) if mk else -0.25
    _soxl_dd1 = mk.get("soxl_dd1", -0.15) if mk else -0.15
    _soxl_dd2 = mk.get("soxl_dd2", -0.25) if mk else -0.25
    _soxl_dd3 = mk.get("soxl_dd3", -0.35) if mk else -0.35
    # 재투입 DD (SGOV → TQQQ 이체 트리거 + 속도/주)
    _reinv_dd_shallow = mk.get("reinv_dd_shallow", -0.10) if mk else -0.10
    _reinv_dd_mid     = mk.get("reinv_dd_mid", -0.20) if mk else -0.20
    _reinv_dd_deep    = mk.get("reinv_dd_deep", -0.30) if mk else -0.30
    _reinv_spd_shallow = mk.get("reinv_spd_shallow", 0.08) if mk else 0.08
    _reinv_spd_mid     = mk.get("reinv_spd_mid", 0.18) if mk else 0.18
    _reinv_spd_deep    = mk.get("reinv_spd_deep", 0.24) if mk else 0.24
    def _dd_stage(dd_pct, c1, c2, c3):
        """DD% → (단계명, 배율텍스트)"""
        if dd_pct is None: return "—", ""
        dd_frac = dd_pct / 100  # -7.8% → -0.078
        if dd_frac <= c3: return "폭락장", "×3.0"
        if dd_frac <= c2: return "조정장", "×2.0"
        if dd_frac <= c1: return "경계장", "×1.5"
        return "평시", "×1.0"
    ms, ms_detail = calc_mk_score(_mk_qqq_dd, _mk_soxx_dd, vix, fgs, _mk_cash, _mk_ratio)

    # Earnings 폴백 경로 기록
    _eg = spy_info.get("earningsGrowth"); _te = spy_info.get("trailingEps"); _fe = spy_info.get("forwardEps")
    if _eg is not None: _earn_src = f"earningsGrowth={_eg:.3f}"
    elif _te and _fe: _earn_src = f"EPS비교 (t={_te:.2f} f={_fe:.2f})"
    elif fpe and tpe: _earn_src = f"PE비율 (fpe/tpe={fpe/tpe:.2f})"
    else: _earn_src = "없음"; _fails.append("Earnings")
    # V3.8: fwd_pe_history 기반 fpe z-score / 3M 변화 계산 (24개월 미만이면 None — auto_season 내 fallback OR 분기로 처리)
    try:
        _fwd_pe_hist_for_z = _load_fwd_hist()
    except Exception:
        _fwd_pe_hist_for_z = []
    _fpe_for_z = None if _fpe_is_copy else fpe
    _fpe_z_in     = _fpe_zscore(_fpe_for_z, _fwd_pe_hist_for_z)
    _fpe_3mchg_in = _fpe_3m_change(_fwd_pe_hist_for_z)
    season_auto, season_conf, season_checks, season_scores = auto_season(
        fd, yd, ff, unemp, None if _fpe_is_copy else fpe, tpe, cape, wti, spy_info,
        fpe_z=_fpe_z_in, fpe_3m_chg=_fpe_3mchg_in,
    )

    # ── Export 데이터 (탭별 + 전체) ──
    _now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    _r2 = lambda v, d=2: round(v, d) if v is not None else None
    # ── 스코어 분해 (개별 기여도) ──
    _MAC_LABELS = {"t":"2Y10Y","tm":"3M10Y","f":"FF금리6M변화","r":"실질금리","p":"Forward PE","c":"CAPE",
                   "h":"HY스프레드","sd":"SOX고점대비","sr":"SOX상대3M","v":"VIX","g":"Fear&Greed","u":"실업률3M변화","gd":"GDP","bf":"버핏지표","cf":"CFNAI MA3"}
    _MK_LABELS = {"qqq":"QQQ DD","soxx":"SOXX DD","vix":"VIX","fg":"Fear&Greed","cash":"현금비중","ratio":"TQQQ Ratio"}
    export_mac_detail = {_MAC_LABELS.get(k,k): v for k,v in gs_detail.items()} if gs_detail else {}
    export_mk_detail = {_MK_LABELS.get(k,k): v for k,v in ms_detail.items()} if ms_detail else {}
    export_mac_clusters = mac_clusters(gs_detail)
    # ── 클러스터 괴리도 ──
    _cl_scores_list = [v["score"] for v in export_mac_clusters.values() if v.get("score") is not None]
    _cl_divergence = round(max(_cl_scores_list) - min(_cl_scores_list), 1) if len(_cl_scores_list) >= 2 else None
    _cl_max_name = max((it for it in export_mac_clusters.items() if it[1].get("score") is not None), key=lambda x: x[1]["score"])[0] if _cl_scores_list else None
    _cl_min_name = min((it for it in export_mac_clusters.items() if it[1].get("score") is not None), key=lambda x: x[1]["score"])[0] if _cl_scores_list else None
    # ── V3.4 디커플링 코멘트 / 스코어 속도 / 역사 매칭 ──
    _cl_decouple = cluster_decouple_comment(export_mac_clusters, _cl_divergence, mode)
    _cl_decouple_easy = cluster_decouple_comment(export_mac_clusters, _cl_divergence, "쉬운")
    mac_score_history_append(gs, mk=ms, clusters=export_mac_clusters, divergence=_cl_divergence, season=season_auto)
    _mac_history = mac_history_load()  # V3.5 export용 — 시계열 탭과 동일 데이터
    _mac_velocity = mac_score_velocity()
    _vel_lbl, _vel_clr_key, _vel_cmt = mac_velocity_label(_mac_velocity, mode)

    # ── ΔΔ (2차 도함수) 계산 ──
    # 거시 스코어 ΔΔ
    _mac_dd = compute_delta_delta(_mac_history) if _mac_history else None
    # 5클러스터 ΔΔ (observations.jsonl 기반)
    _obs_df_dd = _hist_load_obs_df()
    _CL_OBS_KEYS = {"채권/금리": "cluster_bond", "밸류에이션": "cluster_val",
                     "스트레스": "cluster_stress", "실물": "cluster_real", "반도체": "cluster_semi"}
    _cl_dd = {}
    if _obs_df_dd is not None and not _obs_df_dd.empty:
        for cn, ck in _CL_OBS_KEYS.items():
            if ck in _obs_df_dd.columns:
                _cseries = _obs_df_dd[ck].dropna()
                if len(_cseries) >= 60:
                    _cl_dd[cn] = compute_delta_delta(_cseries)
                else:
                    _cl_dd[cn] = None
            else:
                _cl_dd[cn] = None

    # 사계절 역사 매칭 입력 변수
    _ff_pos_pct = _ff_position(fd.get("FEDFUNDS"), lookback_years=10)
    _ff_pos_str = "고점권" if (_ff_pos_pct is not None and _ff_pos_pct >= 70) else "저점권" if (_ff_pos_pct is not None and _ff_pos_pct < 30) else "중립권" if _ff_pos_pct is not None else None
    _val_cluster_score = export_mac_clusters.get("밸류에이션", {}).get("score") if export_mac_clusters else None
    _semi_dir = "up" if (sox_rel3 is not None and sox_rel3 > 0) else "down" if (sox_rel3 is not None and sox_rel3 < 0) else "flat"
    _wti_s = fd.get("WTI"); _wti_surge = False
    if _wti_s is not None and len(_wti_s) >= 64:
        try: _wti_surge = (float(_wti_s.iloc[-1]) / float(_wti_s.iloc[-64]) - 1) * 100 > 30
        except: _wti_surge = False
    # ── V3.9.1 거리 매칭용 신규 인자 산출 ──
    _hy_s_hm = fd.get("HY")
    _hy_now_hm = float(_hy_s_hm.iloc[-1]) if (_hy_s_hm is not None and len(_hy_s_hm) > 0) else None
    _hy_6m_chg_hm = None
    if _hy_s_hm is not None and len(_hy_s_hm) >= 130:
        try:
            _hn = float(_hy_s_hm.iloc[-1]); _h6 = float(_hy_s_hm.iloc[-127])
            _hy_6m_chg_hm = (_hn - _h6) * (100 if _hn <= 1.0 else 1)
        except Exception: pass
    _inv_3m10y_hm = _inv_state(fd.get("T10Y3M"))
    _dxy_now_hm = None
    _dxy_s_hm = yd.get("DXY")
    if _dxy_s_hm is not None and len(_dxy_s_hm) > 0:
        try: _dxy_now_hm = float(_dxy_s_hm.iloc[-1])
        except Exception: pass
    _cpi_yoy_now_hm = None; _cpi_yoy_3m_chg_hm = None
    _cpi_s_hm = fd.get("CPIAUCSL")
    if _cpi_s_hm is not None and len(_cpi_s_hm) >= 16:
        try:
            _cn = float(_cpi_s_hm.iloc[-1]); _c12 = float(_cpi_s_hm.iloc[-13])
            _cpi_yoy_now_hm = (_cn / _c12 - 1) * 100
            _c3 = float(_cpi_s_hm.iloc[-4]); _c15 = float(_cpi_s_hm.iloc[-16])
            _cpi_yoy_3m_ago = (_c3 / _c15 - 1) * 100
            _cpi_yoy_3m_chg_hm = _cpi_yoy_now_hm - _cpi_yoy_3m_ago
        except Exception: pass
    _ff_3m_chg_hm = None
    _ff_s_hm = fd.get("FEDFUNDS")
    if _ff_s_hm is not None and len(_ff_s_hm) >= 2:
        try:
            from datetime import timedelta as _td_hm
            _t = _ff_s_hm.index[-1] - _td_hm(days=90)
            _older = _ff_s_hm[_ff_s_hm.index <= _t]
            if len(_older) > 0:
                _ff_3m_chg_hm = float(_ff_s_hm.iloc[-1]) - float(_older.iloc[-1])
        except Exception: pass
    _wti_3m_pct_hm = None
    if _wti_s is not None and len(_wti_s) >= 64:
        try: _wti_3m_pct_hm = (float(_wti_s.iloc[-1]) / float(_wti_s.iloc[-64]) - 1) * 100
        except Exception: pass
    _hist_match = season_history_match(
        season_auto, _ff_pos_str, _val_cluster_score, _semi_dir, _wti_surge,
        hy_now=_hy_now_hm, hy_6m_chg=_hy_6m_chg_hm, inv_state=_inv_3m10y_hm,
        dxy_now=_dxy_now_hm, cpi_yoy_now=_cpi_yoy_now_hm,
        cpi_yoy_3m_chg=_cpi_yoy_3m_chg_hm, ff_3m_chg=_ff_3m_chg_hm,
        wti_3m=_wti_3m_pct_hm,
    )
    _hist_match_easy = _hist_match  # V3.9.1: 거리 매칭은 mode 무관 — 동일 결과 alias 유지

    # ═══ V3.10.4: 일회성 730일 백필 + 사이드바 상태 ═══
    _force_bf = bool(st.session_state.pop("_force_backfill_rerun", False))
    try:
        _need_bf = _force_bf or not _is_already_backfilled(OBS_JSONL)
        if _need_bf:
            _raw_for_bf = _build_raw_data_for_backfill(fd, yd)
            with st.spinner("역사 매칭 백필 중 (730일, 보통 10~30초)..."):
                _bf_res = _backfill_observations(_raw_for_bf, 730, OBS_JSONL, force=_force_bf)
            if _bf_res.get("ok", 0) > 0:
                st.success(f"✅ 역사 매칭 백필 완료 — {_bf_res['ok']}일치 추가 "
                           f"(실패 {_bf_res['fail']}일 · 기존 데이터로 차단 {_bf_res['blocked_by_earliest']}일)")
                try: _hist_load_obs_df.clear()
                except Exception: pass
            elif _bf_res.get("blocked_by_earliest", 0) > 0:
                st.info(f"ℹ️ 백필 시도 종료 — 모든 730일이 기존 obs 데이터로 차단됨 "
                        f"({_bf_res['blocked_by_earliest']}일). 마커 박음, 재실행 안 함.")
            elif _bf_res.get("fail", 0) > 0:
                st.warning(f"⚠️ 역사 매칭 백필 — 0일 성공, raw 데이터 부족 {_bf_res['fail']}일. 마커 박음.")
            else:
                st.info(f"ℹ️ 백필 함수 호출됨, 처리 결과 없음.")
    except Exception as _bf_e:
        try: st.sidebar.caption(f"⚠️ 백필 실패: {type(_bf_e).__name__}: {_bf_e}")
        except Exception: pass

    # 사이드바 백필 상태 표시 + 강제 재실행 버튼
    try:
        _bf_done = _is_already_backfilled(OBS_JSONL)
        _bf_meta = _read_backfill_marker(OBS_JSONL) if _bf_done else None
        if _bf_done and _bf_meta:
            _ok = _bf_meta.get("_backfill_ok", 0); _fail = _bf_meta.get("_backfill_fail", 0)
            _blk = _bf_meta.get("_backfill_blocked", 0); _att = _bf_meta.get("_backfill_days_attempted", 0)
            st.sidebar.caption(f"📚 역사 매칭 백필: 완료 ({_ok}일 성공 / {_fail} 실패 / {_blk} 차단)")
        elif _bf_done:
            st.sidebar.caption("📚 역사 매칭 백필: 완료")
        else:
            st.sidebar.caption("📚 역사 매칭 백필: 미실행")
        if st.sidebar.button("🔁 백필 강제 재실행", help="마커 무시하고 백필 함수 재실행. 사이클 진행도 정확도 향상."):
            st.session_state["_force_backfill_rerun"] = True
            st.rerun()
    except Exception:
        pass
    # ── 2×2 사분면 ──
    _mx_key = ("g" if (gs is not None and gs >= 50) else "l") + ("g" if (ms is not None and ms >= 50) else "l")
    _MX_LABELS = {"gg": "매직존", "gl": "대기존", "lg": "경계존", "ll": "평시존"}
    _mx_label = _MX_LABELS.get(_mx_key, "?") if (gs is not None and ms is not None) else None
    # ── 주요 지표 Δ ──
    def _d_abs(s, days):
        n = L(s); o = _asof_old(s, days)
        if n is None or o is None: return None
        return n - o
    def _d_pct(s, days):
        n = L(s); o = _asof_old(s, days)
        if n is None or o is None or o == 0: return None
        return (n / o - 1) * 100
    def _darr(v):
        if v is None: return "→"
        return "↑" if v > 0 else "↓" if v < 0 else "→"
    _sox_spx_s = None
    if sox_s is not None and spx_s is not None and len(sox_s) > 0 and len(spx_s) > 0:
        _scm = sox_s.index.intersection(spx_s.index)
        if len(_scm) > 0: _sox_spx_s = sox_s.loc[_scm] / spx_s.loc[_scm]
    def _mk_delta(s, mode="abs", bp=False):
        f = _d_abs if mode == "abs" else _d_pct
        w = f(s, 5); m = f(s, 21)
        if bp:
            w = round(w * 100) if w is not None else None
            m = round(m * 100) if m is not None else None
        else:
            w = round(w, 2) if w is not None else None
            m = round(m, 2) if m is not None else None
        return {"1W": w, "1M": m, "1W_d": _darr(w), "1M_d": _darr(m)}
    _KEY_DELTAS = {
        "VIX": _mk_delta(fd.get("VIXCLS"), "abs"),
        "HY_bp": _mk_delta(fd.get("HY"), "abs", bp=True),
        "2Y10Y_bp": _mk_delta(fd.get("T10Y2Y"), "abs", bp=True),
        "DXY": _mk_delta(yd.get("DXY"), "pct"),
        "SOX_SPX": _mk_delta(_sox_spx_s, "pct"),
        "KRW": _mk_delta(fd.get("KRW"), "pct"),
    }
    # ── 전 지표 추세 (UI 카드의 ctrends 결과를 export용으로 수집) ──
    def _tr_export(s, mode="pct", P=None):
        tr = ctrends(s, mode=mode, P=P)
        return {k: (round(v[0], 2) if v[0] is not None else None) for k, v in tr.items()}
    _TRENDS = {
        # 일간 (1W/2W/1M/3M/6M/1Y)
        "2Y10Y_bp":   _tr_export(fd.get("T10Y2Y"), "abs_bp"),
        "3M10Y_bp":   _tr_export(fd.get("T10Y3M"), "abs_bp"),
        "DXY_pct":    _tr_export(yd.get("DXY"), "pct"),
        "KRW_pct":    _tr_export(fd.get("KRW"), "pct"),
        "FEDFUNDS":   _tr_export(fd.get("FEDFUNDS"), "abs"),
        "VIX":        _tr_export(fd.get("VIXCLS"), "abs"),
        "HY_bp":      _tr_export(fd.get("HY"), "abs_bp"),
        "UNRATE":     _tr_export(fd.get("UNRATE"), "abs"),
        "WTI_pct":    _tr_export(fd.get("WTI"), "pct"),
        "GOLD_pct":   _tr_export(yd.get("GOLD"), "pct"),
        "BEI_5Y":     _tr_export(fd.get("T5YIE"), "abs"),
        # 월간 (1M/3M/6M/1Y)
        "CPI_YoY_pp":      _tr_export(yoy_s(fd.get("CPIAUCSL")), "abs_pp", PM),
        "CPI_코어_YoY_pp": _tr_export(yoy_s(fd.get("CPILFESL")), "abs_pp", PM),
        "PCE_YoY_pp":      _tr_export(yoy_s(fd.get("PCEPI")), "abs_pp", PM),
        "PCE_코어_YoY_pp": _tr_export(yoy_s(fd.get("PCEPILFE")), "abs_pp", PM),
        "JOLTS_pct":   _tr_export(fd.get("JTSJOL"), "pct", PM),
        "NFP":         _tr_export(diff_s(fd.get("PAYEMS")), "abs", PM),
        "소비자신뢰": _tr_export(fd.get("UMCSENT"), "abs", PM),
        # 분기 (1Q/2Q/1Y)
        "GDP_pp":         _tr_export(fd.get("GDP"), "abs_pp", PQ),
        "카드연체율_pp": _tr_export(fd.get("DRCCLACBS"), "abs_pp", PQ),
        "국채GDP_pp":    _tr_export(fd.get("GFDEGDQ188S"), "abs_pp", PQ),
    }
    # ── 카드 구조화 export (대시보드 카드 1:1 대응) ──
    def _cn(c):
        # hex → 라벨 색상명
        if c == C["green"]: return "green"
        if c == C["red"]:   return "red"
        if c == C["gold"]:  return "yellow"
        if c == C["muted"]: return "gray"
        return "gray"
    _UNIT = {"abs_bp": "bp", "abs_pp": "pp", "pct": "%", "abs": "abs"}
    def _trend_card(s, mode="pct", P=None):
        if s is None: return None
        tr = ctrends(s, mode=mode, P=P)
        out = {}
        u = _UNIT.get(mode, "abs")
        for k, (v, _disp) in tr.items():
            if v is None: continue
            d = "up" if v > 0 else ("down" if v < 0 else "flat")
            out[k] = {"value": round(v, 2), "unit": u, "dir": d}
        return out if out else None
    def _card(value, label, color, comment, trend=None):
        d = {"value": value, "label": label, "color": _cn(color), "comment": comment or ""}
        if trend is not None: d["trend"] = trend
        return d
    # 일반 탭 카드 라벨/색상/코멘트 — 렌더 블록과 동일 로직 재계산
    # r1
    _2y10y_s, _2y10y_c, _2y10y_d = j_sp(t10y2y * 100 if t10y2y is not None else None)
    _dxy_s, _dxy_c, _dxy_d = j_dxy(dxy)
    if krw is not None:
        if krw >= 1450: _krw_s, _krw_c, _krw_d = "약세 극단", C["red"], "비정상이다. 전쟁이든 위기든 원인은 매번 다르지만 이 수준은 오래 안 간다."
        elif krw >= 1300: _krw_s, _krw_c, _krw_d = "약세", C["gold"], "달러가 비싸다. 환전은 불리한 구간. 급하지 않으면 기다려라."
        elif krw >= 1100: _krw_s, _krw_c, _krw_d = "정상", C["green"], "정상 범위다. 환전할 거면 여기서 해라."
        else: _krw_s, _krw_c, _krw_d = "강세", C["green"], "원화가 강하다. 달러가 싸다. 매집 재원 환전 최적 구간."
    else: _krw_s, _krw_c, _krw_d = "—", C["muted"], ""
    if ff is not None:
        if ff >= 5: _ff_s, _ff_c, _ff_d = "긴축", C["red"], "시스템이 위협받으면 연준은 반드시 돌아선다. 볼커 빼고 전부 그랬다."
        elif ff >= 3: _ff_s, _ff_c, _ff_d = "제한적", C["gold"], "끝이 보이는 긴축은 더 이상 주가를 끌어내리지 못한다."
        elif ff >= 1: _ff_s, _ff_c, _ff_d = "중립", C["gold"], "긴축도 완화도 아니다. 다른 지표를 봐라."
        else: _ff_s, _ff_c, _ff_d = "완화", C["green"], "제로금리 근처다. 돈이 풀리고 있다. 역실적장세에선 매수가 정석이다."
    else: _ff_s, _ff_c, _ff_d = "—", C["muted"], ""
    # r2
    if fpe is not None:
        if fpe >= 22: _fpe_s, _fpe_c, _fpe_d = "극단", C["red"], "이 수준에서 숏 잡아 실패한 적 없다. 140년간."
        elif fpe >= 18: _fpe_s, _fpe_c, _fpe_d = "정상", C["green"], "정상 범위다. 비싸지도 싸지도 않다. 이 구간에서는 밸류에이션이 매매 근거가 안 된다."
        else: _fpe_s, _fpe_c, _fpe_d = "정상", C["green"], "싸다. 역사적으로 이 밑에서 사면 거의 다 이겼다. 실적이 무너져서 싼 건지 공포로 싼 건지만 구분해라."
    else: _fpe_s, _fpe_c, _fpe_d = "—", C["muted"], ""
    _vix_s, _vix_c, _vix_d = j_vix(vix)
    _hy_s, _hy_c, _hy_d = j_hy(hy)
    if unemp is not None:
        if unemp >= 5: _un_s, _un_c, _un_d = "경고", C["red"], "실업률이 올라가고 있다. 여기서부터 사기 시작하면 된다."
        elif unemp >= 4: _un_s, _un_c, _un_d = "주의", C["gold"], "아직 견디고 있다. 근데 JOLTS를 봐라. 선행지표가 먼저 꺾인다."
        else: _un_s, _un_c, _un_d = "안정", C["green"], "고용이 강하다. 연준이 금리 내릴 명분이 없다."
    else: _un_s, _un_c, _un_d = "—", C["muted"], ""
    # r3
    if sox_rel3 is not None:
        _sox_s = "아웃퍼폼" if sox_rel3 > 0 else "언더퍼폼"
        _sox_c = C["green"] if sox_rel3 > 0 else C["gold"]
        _sox_d = "반도체가 시장을 이기고 있다. 봄의 선행 신호." if sox_rel3 > 0 else "반도체가 시장에 지고 있다. 겨울의 선행 신호."
    else: _sox_s, _sox_c, _sox_d = "—", C["muted"], ""
    if wti is not None:
        if wti >= 100: _wti_s, _wti_c, _wti_d = "위험", C["red"], "유가→인플레→연준→시장. 전부 연결되어 있다."
        elif wti >= 80: _wti_s, _wti_c, _wti_d = "부담", C["gold"], "유가가 오르고 있다. 인플레 재점화 리스크."
        elif wti >= 50: _wti_s, _wti_c, _wti_d = "정상", C["green"], "유가가 조용하다. 조용할 때 다른 걸 봐라. 유가는 움직이기 시작하면 빠르다."
        else: _wti_s, _wti_c, _wti_d = "수요 부진", C["red"], "수요가 죽었다. 디플레 압력. 건강한 신체에선 비만이 걱정일테지만 죽을 병 걸리면 살은 저절로 빠진다."
    else: _wti_s, _wti_c, _wti_d = "—", C["muted"], ""
    _fg_s = fg.get("rating", "—") or "—"
    _fg_c = C["green"] if (fgs and fgs < 25) else C["red"] if (fgs and fgs > 75) else C["gold"] if fgs is not None else C["muted"]
    _fg_d = ""
    if fgs is not None:
        if fgs < 20: _fg_d = "Extreme Fear. 욕심으로 사지 말고 두려움으로 사라."
        elif fgs > 80: _fg_d = "사람들이 확신에 차면 그 방향의 가격은 이미 과대평가된 거다."
    # 심안 d1
    if t3m2y is not None:
        if t3m2y > 0: _t32_s, _t32_c, _t32_d = "역전", C["red"], "연준이 너무 올렸다. 시장이 그렇게 말하고 있다."
        elif t3m2y > -0.5: _t32_s, _t32_c, _t32_d = "정상", C["green"], "역전 직전이거나 막 풀린 상태다. 긴축 오버슈팅 경계 구간."
        else: _t32_s, _t32_c, _t32_d = "정상", C["green"], "정상이다. 단기 금리가 초단기보다 충분히 높다. 긴축이 과하지 않다."
    else: _t32_s, _t32_c, _t32_d = "—", C["muted"], ""
    if rr is not None:
        if rr >= 3.0:
            _rr_s, _rr_c = "극단", C["red"]
            _rr_d = "볼커 영역이다. 시스템이 깨질 수 있다."
        elif rr >= 2.0:
            _rr_s, _rr_c = "강한 긴축", C["red"]
            _rr_d = "파월이 보는 긴축 강도가 높다. 경기를 죄고 있다."
        elif rr >= 1.0:
            _rr_s, _rr_c = "긴축적", C["gold"]
            _rr_d = "긴축적이지만 극단은 아니다. 주식이 버틸 수 있는 구간."
        elif rr > 0:
            _rr_s, _rr_c = "중립", C["gold"]
            _rr_d = "중립 근처다. 연준이 경기를 억누르지도 밀어주지도 않는 상태."
        else:
            _rr_s, _rr_c = "완화", C["green"]
            _rr_d = "실질금리 마이너스. 돈이 풀리는 토양이다."
    else:
        _rr_s, _rr_c, _rr_d = "—", C["muted"], ""
    if ff6m_chg is not None:
        _ff6_s = "완화" if ff6m_chg < 0 else "긴축"
        _ff6_c = C["green"] if ff6m_chg < 0 else C["red"]
        if ff6m_chg < -0.5: _ff6_d = "금리 인하 진행 중. 유동성이 풀리고 있다."
        elif ff6m_chg < -0.05: _ff6_d = "금리가 살짝 내려왔다. 방향 전환의 초입일 수 있다."
        elif ff6m_chg < 0.05: _ff6_d = "연준이 멈춰 있다. 데이터를 보고 있다는 뜻이다."
        elif ff6m_chg <= 0.5: _ff6_d = "아직 올리고 있다. 긴축 사이클이 끝나지 않았다."
        else: _ff6_d = "금리 인상 진행 중. 유동성 긴축."
    else: _ff6_s, _ff6_c, _ff6_d = "—", C["muted"], ""
    # 심안 d2 인플레 4종 — 추세+수준 라벨
    _INFL_T_E = {
        "CPIAUCSL": ["인플레가 안 꺾인다. 연준이 못 내린다.", "인플레가 끈적하다. 체감 물가가 안 내려오는 구간이다.", "방향은 맞다. 속도가 느릴 뿐이다.", "정상 궤도 근처다. 연준이 다른 데 눈 돌릴 여유가 생긴다.", "인플레가 꺾이고 있다. 봄이 가까워진다."],
        "CPILFESL": ["인플레가 안 꺾인다. 연준이 못 내린다.", "코어가 3% 위에 붙어 있으면 연준은 비둘기가 못 된다.", "코어가 내려오고 있다. PCE 코어와 같이 봐라. 둘 다 3% 밑이면 인하 조건 충족.", "코어 인플레 정상화. 이 상태가 유지되면 긴축 종료를 선언할 수 있다.", "인플레가 꺾이고 있다. 봄이 가까워진다."],
        "PCEPI":    ["인플레가 안 꺾인다. 연준이 못 내린다.", "끈적하다. 연준이 원하는 속도가 아니다. 인하 기대를 접어라.", "내려오고 있긴 한데 아직 목표(2%) 위다. 연준이 참을성을 시험받는 구간.", "거의 다 왔다. 이 구간이면 연준이 움직일 명분이 생긴다.", "인플레가 꺾이고 있다. 봄이 가까워진다."],
        "PCEPILFE": ["인플레가 안 꺾인다. 연준이 못 내린다.", "코어가 안 내려온다. 헤드라인이 내려와도 의미 없다. 연준은 코어를 본다.", "코어가 3% 밑이면 연준이 숨통이 트인다. 근데 2%까지는 아직 멀다.", "연준의 승리가 보이기 시작한다. 인하 사이클의 전제 조건이 갖춰지는 중.", "인플레가 꺾이고 있다. 봄이 가까워진다."],
    }
    def _infl_lc(val, fk):
        if val is None: return ("—", C["muted"], "")
        _ys = yoy_s(fd.get(fk))
        _3m = None
        if _ys is not None and len(_ys) > 3: _3m = float(_ys.iloc[-1]) - float(_ys.iloc[-4])
        _b = _INFL_T_E[fk]
        if val > 4: _cm = _b[0]
        elif val > 3: _cm = _b[1]
        elif val > 2.5: _cm = _b[2]
        elif val > 2: _cm = _b[3]
        else: _cm = _b[4]
        if _3m is None: return ("—", C["muted"], _cm)
        _up = _3m > 0
        if _up and val > 4: return ("↑ 과열", C["red"], _cm)
        if _up and val > 2.5: return ("↑ 고착", C["gold"], _cm)
        if _up: return ("↑ 반등", C["red"], _cm)
        if val > 4: return ("↓ 피크아웃", C["gold"], _cm)
        if val > 2.5: return ("↓ 둔화 중", C["gold"], _cm)
        return ("↓ 목표 근접", C["green"], _cm)
    _cpi_s, _cpi_c, _cpi_d = _infl_lc(cpi_y, "CPIAUCSL")
    _cpic_s, _cpic_c, _cpic_d = _infl_lc(cpic_y, "CPILFESL")
    _pce_s, _pce_c, _pce_d = _infl_lc(pce_y, "PCEPI")
    _pcec_s, _pcec_c, _pcec_d = _infl_lc(pcec_y, "PCEPILFE")
    # 심안 d3
    if jolts is not None:
        if jolts >= 8000: _jol_s, _jol_c, _jol_d = "과열", C["red"], "구인이 넘친다. 노동시장 과열. 연준이 인하를 미룰 근거."
        elif jolts >= 5000: _jol_s, _jol_c, _jol_d = "정상", C["green"], "구인이 정상 수준이다. 과열은 아닌데 냉각도 아니다."
        else:
            if jolts >= 4000: _jol_s, _jol_c, _jol_d = "냉각 시작", C["gold"], "구인이 줄고 있다. 기업이 채용을 멈추기 시작했다."
            else:             _jol_s, _jol_c, _jol_d = "냉각", C["red"], "구인이 말랐다. 실업률이 따라 올라간다. 사라."
    else: _jol_s, _jol_c, _jol_d = "—", C["muted"], ""
    if nfp is not None:
        if nfp >= 200:   _nfp_s, _nfp_c, _nfp_d = "호조", C["green"], "고용이 튼튼하다. 연준이 안 내릴 이유가 하나 더 생겼다."
        elif nfp >= 100: _nfp_s, _nfp_c, _nfp_d = "둔화", C["gold"], "나쁘진 않다. 근데 방향을 봐라. 3개월 연속 둔화면 신호다."
        elif nfp >= 0:   _nfp_s, _nfp_c, _nfp_d = "약화", C["gold"], "고용이 식고 있다. 마이너스는 아닌데 방향이 나쁘다."
        else:            _nfp_s, _nfp_c, _nfp_d = "수축", C["red"], "고용 마이너스. 이미 경기침체다."
    else: _nfp_s, _nfp_c, _nfp_d = "—", C["muted"], ""
    if gdpv is not None:
        if gdpv >= 3:    _gdp_s, _gdp_c, _gdp_d = "호조", C["green"], "경제가 뜨겁다. 이 숫자가 유지되면 연준이 내릴 이유가 없다."
        elif gdpv >= 2:  _gdp_s, _gdp_c, _gdp_d = "정상", C["green"], "버티고 있다. 이게 1% 밑으로 가면 그때 긴장해라."
        elif gdpv >= 0:  _gdp_s, _gdp_c, _gdp_d = "실속", C["gold"], "실속 구간이다. 고용이 같이 꺾이면 침체로 넘어간다."
        else:            _gdp_s, _gdp_c, _gdp_d = "침체", C["red"], "마이너스 성장. 침체다."
    else: _gdp_s, _gdp_c, _gdp_d = "—", C["muted"], ""
    if um is not None:
        if um >= 80: _um_s, _um_c, _um_d = "낙관", C["green"], "소비자가 낙관적이다. 경기 과열 신호. 연준이 내릴 이유가 줄어든다."
        elif um >= 60: _um_s, _um_c, _um_d = "보통", C["gold"], "보통이다. 숫자 자체보다 방향을 봐라. 3개월 연속 하락이면 경고."
        else: _um_s, _um_c, _um_d = "비관", C["red"], "소비자가 쫄았다. GDP의 70%가 소비다."
    else: _um_s, _um_c, _um_d = "—", C["muted"], ""
    # 심안 d4
    if cape is not None:
        if cape >= 35: _cape_s, _cape_c, _cape_d = "극단", C["red"], "35 넘는 건 역사상 2000년밖에 없었다."
        elif cape >= 25: _cape_s, _cape_c, _cape_d = "—", C["muted"], "역사적 평균(17)보다 높다. 비싸다. 근데 비싼 채로 10년 갈 수도 있다."
        else: _cape_s, _cape_c, _cape_d = "—", C["muted"], "정상 범위에 가까워지고 있다. 역사적으로 여기서 사면 10년 뒤에 웃는다."
    else: _cape_s, _cape_c, _cape_d = "—", C["muted"], ""
    if tpe is not None:
        if tpe >= 28: _tpe_s, _tpe_c, _tpe_d = "극단", C["red"], "터지기 전 PER은 28이었다. 60, 70은 터진 뒤의 PER이다."
        elif tpe >= 20: _tpe_s, _tpe_c, _tpe_d = "—", C["muted"], "비싸다. 근데 실적이 받쳐주면 유지 가능하다. 실적이 꺾이는 순간 이 숫자가 폭탄이 된다."
        else: _tpe_s, _tpe_c, _tpe_d = "—", C["muted"], "싸다. 역사적으로 여기서 산 사람은 거의 다 이겼다."
    else: _tpe_s, _tpe_c, _tpe_d = "—", C["muted"], ""
    if dy is not None:
        if dy < 1.5: _dy_s, _dy_c, _dy_d = "경고", C["red"], "지금보다 낮았던 건 2000년뿐이다."
        elif dy < 2.0: _dy_s, _dy_c, _dy_d = "—", C["muted"], "배당이 역사적 평균보다 낮다. 주가가 비싸다는 뜻이다."
        else: _dy_s, _dy_c, _dy_d = "—", C["muted"], "배당이 정상 근처다. 주가가 적정하거나 빠졌다는 신호."
    else: _dy_s, _dy_c, _dy_d = "—", C["muted"], ""
    if bei is not None:
        if bei >= 2.8: _bei_s, _bei_c, _bei_d = "인플레 경고", C["red"], "시장이 인플레 장기화를 가격에 넣고 있다. 연준 신뢰가 흔들리는 거다."
        elif bei >= 2.0: _bei_s, _bei_c, _bei_d = "안정", C["green"], ("기대 인플레가 안정적이다. 연준이 일을 하고 있다." if bei >= 2.2 else "목표 근처다. 이 수준이면 연준이 편하다.")
        else: _bei_s, _bei_c, _bei_d = "디플레 경고", C["red"], "디플레를 걱정하기 시작했다. 침체 냄새다."
    else: _bei_s, _bei_c, _bei_d = "—", C["muted"], ""
    # 심안 d5
    if cd is not None:
        if cd > 5:    _cd_s, _cd_c, _cd_d = "경고", C["red"], "카드값을 못 막는다. 저축률이 바닥이다."
        elif cd >= 3: _cd_s, _cd_c, _cd_d = "주의", C["gold"], "연체가 올라오고 있다. 아직 위기는 아닌데 소비자의 체력이 빠지고 있다."
        else:         _cd_s, _cd_c, _cd_d = "안정", C["green"], "소비자 건강하다. 아직은."
    else: _cd_s, _cd_c, _cd_d = "—", C["muted"], ""
    if dg is not None:
        if dg > 120: _dg_s, _dg_c, _dg_d = "경고", C["red"], f"GDP 대비 {dg:.0f}%. 적자의 지속가능성. 이건 구조적 약세다."
        elif dg >= 100: _dg_s, _dg_c, _dg_d = "—", C["muted"], "부채가 높다. 위기 때 재정으로 받칠 여력이 제한된다."
        else: _dg_s, _dg_c, _dg_d = "—", C["muted"], "재정 여력이 있다. 위기가 와도 정부가 돈을 풀 수 있다."
    else: _dg_s, _dg_c, _dg_d = "—", C["muted"], ""
    _gold_yoy_e = chg(yd.get("GOLD"), 252)
    if _gold_yoy_e is not None:
        if _gold_yoy_e > 25: _gld_s, _gld_c, _gld_d = "과열", C["red"], "금이 미쳤다. 달러 시스템에 대한 불신이 극에 달했다."
        elif _gold_yoy_e >= 10: _gld_s, _gld_c, _gld_d = "상승", C["gold"], "금이 꾸준히 오른다. 불안이 쌓이고 있다."
        elif _gold_yoy_e >= 0: _gld_s, _gld_c, _gld_d = "안정", C["green"], "금값은 달러에 대한 불신의 가격이다. 지금은 조용하다."
        else: _gld_s, _gld_c, _gld_d = "하락", C["red"], "금이 빠지고 있다. 달러 신뢰 회복이거나 유동성이 마르고 있다."
    else: _gld_s, _gld_c, _gld_d = "—", C["muted"], "금값은 달러에 대한 불신의 가격이다."
    if un3m_chg is not None:
        if un3m_chg > 0.5: _u3_s, _u3_c, _u3_d = "경고", C["red"], "3개월 새 0.5%p 이상 올랐다. 삼의 법칙(Sahm Rule) 발동 근처. 침체가 시작됐을 수 있다."
        elif un3m_chg > 0.3: _u3_s, _u3_c, _u3_d = "경고", C["red"], "실업률이 빠르게 올라가고 있다. 침체 신호."
        elif un3m_chg > 0: _u3_s, _u3_c, _u3_d = "주의", C["gold"], "실업률이 미세하게 올라가고 있다. 아직 경고 수준은 아니다. 방향만 확인."
        else: _u3_s, _u3_c, _u3_d = "안정", C["green"], "실업률이 안정적이거나 내려가고 있다. 고용이 버티고 있다."
    else: _u3_s, _u3_c, _u3_d = "—", C["muted"], ""
    # 미어캣 7카드 — DD 단계 + 계좌 카드 (export 용 라벨/색)
    _qdd_stg_e, _qdd_mult_e = _dd_stage(_mk_qqq_dd, _dd_caution, _dd_correction, _dd_crash)
    _sdd_stg_e, _sdd_mult_e = _dd_stage(_mk_soxx_dd, _soxl_dd1, _soxl_dd2, _soxl_dd3)
    def _stg_color(stg):
        if stg == "폭락장": return C["red"]
        if stg == "조정장": return C["green"]
        if stg == "경계장": return C["orange"]
        return C["text"] if stg != "—" else C["muted"]
    _qdd_lbl_e = f"{_qdd_stg_e} {_qdd_mult_e}".strip() if _qdd_stg_e != "—" else "—"
    _sdd_lbl_e = f"{_sdd_stg_e} {_sdd_mult_e}".strip() if _sdd_stg_e != "—" else "—"
    _qdd_clr_e = _stg_color(_qdd_stg_e); _sdd_clr_e = _stg_color(_sdd_stg_e)
    _cash_lbl_e = "—" if _mk_cash is None else "충실" if _mk_cash >= 0.20 else "정상" if _mk_cash >= 0.10 else "부족"
    _cash_clr_e = C["green"] if (_mk_cash is not None and _mk_cash >= 0.10) else C["gold"] if _mk_cash is not None else C["muted"]
    _tqr_lbl_e = "—" if _mk_ratio is None else "안전" if _mk_ratio < 2.0 else "주의" if _mk_ratio < 4.0 else "위험"
    _tqr_clr_e = C["green"] if (_mk_ratio is not None and _mk_ratio < 2.0) else C["gold"] if (_mk_ratio is not None and _mk_ratio < 4.0) else C["red"] if _mk_ratio is not None else C["muted"]
    _slr_lbl_e = "—" if _mk_soxl_ratio is None else "안전" if _mk_soxl_ratio < 2.0 else "주의" if _mk_soxl_ratio < 4.0 else "위험"
    _slr_clr_e = C["green"] if (_mk_soxl_ratio is not None and _mk_soxl_ratio < 2.0) else C["gold"] if (_mk_soxl_ratio is not None and _mk_soxl_ratio < 4.0) else C["red"] if _mk_soxl_ratio is not None else C["muted"]
    # F4 버핏 지표 라벨/색
    if buffett is not None:
        # NCBEILQ027S 기준 (Wilshire 대비 ~30% 높음)
        if buffett >= 220:   _bf_s, _bf_c, _bf_d = "극단 고평가", C["red"], "불균형은 균형으로 간다. 거품 정점 영역이다."
        elif buffett >= 190: _bf_s, _bf_c, _bf_d = "고평가", C["red"], "시총이 GDP를 한참 넘었다. 중력은 무시할 수 없다."
        elif buffett >= 160: _bf_s, _bf_c, _bf_d = "보통", C["gold"], "장기 평균 근처. 비싸지도 싸지도 않다."
        else:                _bf_s, _bf_c, _bf_d = "저평가", C["green"], "여기서 산 사람은 역사적으로 전부 이겼다."
    else: _bf_s, _bf_c, _bf_d = "—", C["muted"], ""

    # V3.3 CFNAI MA3 — 단계 라벨/색/코멘트 (실물 클러스터 선행 슬롯)
    if cfnai_ma3 is not None:
        if   cfnai_ma3 >= 0.30:  _cf_lbl, _cf_clr, _cf_d = "확장 강세", C["green"], "경제 활동이 추세를 넘어서고 있다. 85개 지표가 같은 방향이다. 숏 잡지 마라."
        elif cfnai_ma3 >= 0.00:  _cf_lbl, _cf_clr, _cf_d = "정상",      C["green"], "추세 수준이다. 경기가 나쁘지 않다. 방향을 봐라. 내려가기 시작하면 그때 긴장해라."
        elif cfnai_ma3 >= -0.30: _cf_lbl, _cf_clr, _cf_d = "주의",      C["gold"],  "추세 밑이다. 경기가 식고 있다. 아직 침체는 아닌데 방향이 나쁘다. GDP랑 실업률은 멀쩡해 보일 것이다. 후행이니까."
        elif cfnai_ma3 >= -0.70: _cf_lbl, _cf_clr, _cf_d = "경고",      C["red"],   "시카고 연준 침체 룰 직전이다. -0.70 밑으로 가면 역사상 침체 안 온 적 없다. 주식 사면 안 된다."
        else:                    _cf_lbl, _cf_clr, _cf_d = "침체",      C["red"],   "시카고 연준 룰 발동. 침체다. 2000년에 -1.12, 2008년에 -3.03이었다. 근데 여기서부터 역실적장세다. 실업률 올라갈 때부터 주식을 사기 시작하면 된다."
    else: _cf_lbl, _cf_clr, _cf_d = "—", C["muted"], ""

    # ── V3.8.2 신규 카드 변수 (deep 모드 OFF 시에도 obs/_CARDS 노출 위해 deep 블록 전에 계산) ──
    _gap_now = None; _gap_lbl = None; _gap_color = None; _gap_cmt = None; _gap_tr = None; _gap_disp = None; _gap_s = None
    _wti_3m_dash = None; _spx_3m_dash = None; _shock_state = None; _shock_color = None; _shock_cmt = None; _box_on = None; _shock_val = None; _box_tag = None
    # 노동격차 (JOLTS - UNEMPLOY, 천명 단위). UNEMPLOY 는 deep 모드에서만 fd 에 들어감
    try:
        _jolts_pre = fd.get("JTSJOL"); _unem_abs_pre = fd.get("UNEMPLOY")
        if _jolts_pre is not None and _unem_abs_pre is not None:
            _ci = _jolts_pre.index.intersection(_unem_abs_pre.index)
            if len(_ci) >= 12:
                _gap_s = (_jolts_pre.loc[_ci] - _unem_abs_pre.loc[_ci])
                _gap_now = float(_gap_s.iloc[-1])
                if   _gap_now >= 3000:
                    _gap_lbl, _gap_color = "과열", C["red"]
                    _gap_cmt = "구인이 차고 넘친다. 자연실업률 밑에선 임금이 오를 수밖에 없고 임금이 오르면 인플레는 안 잡힌다. 파월이 인하 못 하는 이유가 여기다."
                elif _gap_now >= 1500:
                    _gap_lbl, _gap_color = "정상", C["green"]
                    _gap_cmt = "노동시장 위쪽이다. 안정적이지만 균형이 빠르게 무너질 수 있다. 미국 고용은 매우 탄력적이다. 3.5에서 두달만에 14가 되기도 한다."
                elif _gap_now >= 0:
                    _gap_lbl, _gap_color = "균형", C["gold"]
                    _gap_cmt = "균형점이다. 0 깨고 내려가는 순간 사라. 잭슨홀에서 파월이 한 말이 이거다. 노동수요를 줄여 인플레 잡겠다고."
                else:
                    _gap_lbl, _gap_color = "침체 진입", C["red"]
                    _gap_cmt = "실업자가 구인을 넘었다. 매수 타이밍이다. 실업률 올라갈 때부터 주식을 사기 시작하면 된다. 22년 8월 격차 +6500K 였다. 그때부터 방향이 정해졌다."
                _gap_disp = f"{_gap_now/1000:+.1f}M" if abs(_gap_now) >= 1000 else f"{_gap_now:+.0f}K"
    except Exception:
        pass
    # 공급충격 디커플링 (WTI 3M / SPX 3M). 가을 #7 박스 진행도
    try:
        _wti_pre = fd.get("WTI")
        # NOTE: pandas Series 에 `or` 연산자 사용 금지 → ValueError 가 외부 try 에 삼켜져 _shock_state=None 결과
        _spx_pre = yd.get("SPX")
        if _spx_pre is None:
            _spx_pre = yd.get("^GSPC")
        if (_wti_pre is not None and _spx_pre is not None
            and len(_wti_pre) >= 64 and len(_spx_pre) >= 64):
            _w0 = float(_wti_pre.iloc[-64]); _s0 = float(_spx_pre.iloc[-64])
            if _w0 != 0 and _s0 != 0:
                _wti_3m_dash = (float(_wti_pre.iloc[-1]) / _w0 - 1) * 100
                _spx_3m_dash = (float(_spx_pre.iloc[-1]) / _s0 - 1) * 100
                _w, _s = _wti_3m_dash, _spx_3m_dash
                if _w > 50:
                    _shock_state = "지정학 충격"; _shock_color = C["red"]
                    _shock_cmt = f"이란/중동 프리미엄 정점. WTI {_w:+.1f}%다. 전쟁만큼 효과적인 재정정책 없다고 했다. 단기 급등 후 다시 빠진다. 패닉 사지 마라."
                elif _w > 30 and _s < -3:
                    _shock_state = "충격 활성"; _shock_color = C["red"]
                    _shock_cmt = f"공급충격 가을이다. 유가→인플레→연준→시장 연쇄가 시작됐다. 22년 봄 러우전과 같은 패턴이다. 그때 나스닥 30% 빠졌다. (WTI {_w:+.1f}% / SPX {_s:+.1f}%)"
                elif _w > 15 and -3 <= _s < 0:
                    _shock_state = "박스 발동 직전"; _shock_color = C["orange"]
                    _shock_cmt = f"WTI {_w:+.1f}% / SPX {_s:+.1f}%. 시장이 드디어 반응한다. 원래 호재가 작동 안 하는 게 하락장이고 악재가 작동 안 하는 게 반등장이다. 지금은 악재가 작동하기 시작했다."
                elif _w > 15 and _s >= 0:
                    _shock_state = "선행 신호"; _shock_color = C["gold"]
                    _shock_cmt = f"WTI {_w:+.1f}%인데 시장은 {_s:+.1f}%다. 충격이 아직 옮지 않았다. 거품이 모든 악재를 흡수하는 중이다. 꽃이 아름다울수록 죽음은 가깝다."
                else:
                    _shock_state = "조용"; _shock_color = C["muted"]
                    _shock_cmt = f"공급충격 신호 없다. 다른 지표 봐라. 유가가 인플레를 결정하고 인플레가 연준을 결정한다. 그 출발점이 잠잠하다. (WTI {_w:+.1f}% / SPX {_s:+.1f}%)"
                _box_on = (_w > 15) and (_s < 0)
                _box_tag = "🔴 박스 ON" if _box_on else "⚪ 박스 OFF"
                _shock_val = f"WTI {_w:+.1f}% · SPX {_s:+.1f}%"
    except Exception:
        pass
    # _CARDS 등록 (deep 모드 OFF 일 때도 export_dash 에 포함되도록)

    _CARDS = {
        # 일반 r1
        "2Y10Y_스프레드": _card(round(t10y2y * 100, 1) if t10y2y is not None else None, _2y10y_s, _2y10y_c, _2y10y_d, _trend_card(fd.get("T10Y2Y"), "abs_bp")),
        "DXY":           _card(_r2(dxy), _dxy_s, _dxy_c, _dxy_d, _trend_card(yd.get("DXY"), "pct")),
        "원달러":         _card(_r2(krw, 0), _krw_s, _krw_c, _krw_d, _trend_card(fd.get("KRW"), "pct")),
        "연방기금금리":   _card(_r2(ff), _ff_s, _ff_c, _ff_d, _trend_card(fd.get("FEDFUNDS"), "abs")),
        # 일반 r2
        "Forward_PE":    _card(fpe, _fpe_s, _fpe_c, _fpe_d),
        "VIX":           _card(_r2(vix), _vix_s, _vix_c, _vix_d, _trend_card(fd.get("VIXCLS"), "abs")),
        "HY_스프레드":    _card(round(hy * 100, 0) if hy is not None else None, _hy_s, _hy_c, _hy_d, _trend_card(fd.get("HY"), "abs_bp")),
        "실업률":        _card(_r2(unemp, 1), _un_s, _un_c, _un_d, _trend_card(fd.get("UNRATE"), "abs")),
        # 일반 r3
        "SOX_SPX":       _card(_r2(sox_spx, 4), _sox_s, _sox_c, _sox_d),
        "WTI":           _card(_r2(wti), _wti_s, _wti_c, _wti_d, _trend_card(fd.get("WTI"), "pct")),
        "Fear_Greed":    _card(_r2(fgs, 0), _fg_s, _fg_c, _fg_d),
        # 심안 d1
        "3M10Y_스프레드": (lambda r: _card(round(t10y3m * 100, 1) if t10y3m is not None else None, r[0], r[1], r[2], _trend_card(fd.get("T10Y3M"), "abs_bp")))(j_sp(t10y3m * 100 if t10y3m is not None else None)),
        "3M2Y_스프레드":  _card(round(t3m2y * 100, 1) if t3m2y is not None else None, _t32_s, _t32_c, _t32_d),
        "실질금리":       _card(_r2(rr), _rr_s, _rr_c, _rr_d),
        "FF_6M변화":     _card(_r2(ff6m_chg), _ff6_s, _ff6_c, _ff6_d),
        # 심안 d2 인플레
        "CPI_YoY":       _card(_r2(cpi_y), _cpi_s, _cpi_c, _cpi_d, _trend_card(yoy_s(fd.get("CPIAUCSL")), "abs_pp", PM)),
        "CPI코어_YoY":   _card(_r2(cpic_y), _cpic_s, _cpic_c, _cpic_d, _trend_card(yoy_s(fd.get("CPILFESL")), "abs_pp", PM)),
        "PCE_YoY":       _card(_r2(pce_y), _pce_s, _pce_c, _pce_d, _trend_card(yoy_s(fd.get("PCEPI")), "abs_pp", PM)),
        "PCE코어_YoY":   _card(_r2(pcec_y), _pcec_s, _pcec_c, _pcec_d, _trend_card(yoy_s(fd.get("PCEPILFE")), "abs_pp", PM)),
        # 심안 d3
        "JOLTS":         _card(_r2(jolts, 0), _jol_s, _jol_c, _jol_d, _trend_card(fd.get("JTSJOL"), "pct", PM)),
        "NFP":           _card(_r2(nfp, 0), _nfp_s, _nfp_c, _nfp_d, _trend_card(diff_s(fd.get("PAYEMS")), "abs", PM)),
        "GDP_성장률":    _card(_r2(gdpv), _gdp_s, _gdp_c, _gdp_d, _trend_card(fd.get("GDP"), "abs_pp", PQ)),
        "소비자신뢰":    _card(_r2(um, 1), _um_s, _um_c, _um_d, _trend_card(fd.get("UMCSENT"), "abs", PM)),
        # 심안 d4
        "Shiller_CAPE":  _card(_r2(cape), _cape_s, _cape_c, _cape_d),
        "Trailing_PE":   _card(_r2(tpe), _tpe_s, _tpe_c, _tpe_d),
        "배당수익률":    _card(_r2(dy), _dy_s, _dy_c, _dy_d),
        "버핏지표":      _card(_r2(buffett, 1), _bf_s, _bf_c, _bf_d, _trend_card(fd.get("WILSHIRE"), "pct", PQ)),
        "BEI_5Y":        _card(_r2(bei), _bei_s, _bei_c, _bei_d, _trend_card(fd.get("T5YIE"), "abs")),
        # 심안 d5
        "카드연체율":    _card(_r2(cd, 1), _cd_s, _cd_c, _cd_d, _trend_card(fd.get("DRCCLACBS"), "abs_pp", PQ)),
        "국채GDP":       _card(_r2(dg, 0), _dg_s, _dg_c, _dg_d, _trend_card(fd.get("GFDEGDQ188S"), "abs_pp", PQ)),
        "Gold":          _card(_r2(gold, 0), _gld_s, _gld_c, _gld_d, _trend_card(yd.get("GOLD"), "pct")),
        "실업률_3M변화": _card(_r2(un3m_chg, 2), _u3_s, _u3_c, _u3_d),
        # V3.3 CFNAI MA3 — 실물 클러스터 선행 슬롯 (점수 W=9)
        "CFNAI_MA3":     _card(_r2(cfnai_ma3, 2), _cf_lbl, _cf_clr, _cf_d, _trend_card(_cfnai_ma3_s, "abs", PM) if _cfnai_ma3_s is not None else ""),
        # 미어캣 7카드 — 계좌·DD 단계 (VIX/F&G는 위에 이미 있음)
        "QQQ_DD_52w":    _card(_r2(_mk_qqq_dd, 1), _qdd_lbl_e, _qdd_clr_e, "52주 고점 대비"),
        "SOXX_DD_52w":   _card(_r2(_mk_soxx_dd, 1), _sdd_lbl_e, _sdd_clr_e, "52주 고점 대비"),
        "현금비중":      _card(_r2(_mk_cash, 4) if _mk_cash is not None else None, _cash_lbl_e, _cash_clr_e, "SGOV/총자산"),
        "TQQQ_Ratio":    _card(_r2(_mk_ratio, 4) if _mk_ratio is not None else None, _tqr_lbl_e, _tqr_clr_e, "평가/투입"),
        "SOXL_Ratio":    _card(_r2(_mk_soxl_ratio, 4) if _mk_soxl_ratio is not None else None, _slr_lbl_e, _slr_clr_e, "평가/투입"),
        # 섹터 디커플링 (XLE/XLK vs SPY 3M 상대수익률)
        "XLE_SPY_3M":    _card(_r2(xle_spy_3m), _xle_lbl if xle_spy_3m is not None else "—", _xle_clr if xle_spy_3m is not None else C["muted"], _sec_d),
        "XLK_SPY_3M":    _card(_r2(xlk_spy_3m), _xlk_lbl if xlk_spy_3m is not None else "—", _xlk_clr if xlk_spy_3m is not None else C["muted"], _sec_d),
        "섹터_로테이션":  _card(_sec_q, _sec_lbl, _sec_clr, _sec_d),
    }
    # V3.8.2 신규 카드 등록 (deep 모드 OFF 시에도 export_dash 포함)
    if _gap_now is not None:
        _CARDS["노동격차"] = _card(_gap_disp, _gap_lbl, _gap_color, _gap_cmt, _trend_card(_gap_s, "abs", PM))
    if _shock_state is not None:
        _CARDS["공급충격_디커플링"] = _card(
            {"wti_3m_pct": round(_wti_3m_dash, 2), "spx_3m_pct": round(_spx_3m_dash, 2), "box_on": bool(_box_on)},
            _shock_state, _shock_color, _shock_cmt)
    # 계절 base/prefix 분리 (예: "초여름" → base="여름", prefix="초")
    _season_base = season_auto.lstrip("초늦") if season_auto else None
    _season_prefix = season_auto[0] if (season_auto and season_auto[0] in "초늦") else ""
    export_dash = {
        "date": _now_str, "version": VERSION,
        "거시_스코어": gs, "미어캣_스코어": ms,
        "계절": season_auto, "계절_원형": _season_base, "계절_접두사": _season_prefix,
        "확신도": season_conf,
        "계절_박스_총수": 9,
        "계절_박스_구조": "공통 5 + 고유 4",
        "QQQ_DD_단계": _qdd_stg_e, "QQQ_DD_배율": _qdd_mult_e,
        "SOXX_DD_단계": _sdd_stg_e, "SOXX_DD_배율": _sdd_mult_e,
        "2Y10Y_bp": _r2(t10y2y * 100, 1) if t10y2y is not None else None,
        "DXY": _r2(dxy), "KRW": _r2(krw, 0), "FF금리": _r2(ff),
        "Forward_PE": fpe, "VIX": _r2(vix),
        "HY_bp": _r2(hy * 100, 0) if hy is not None else None,
        "실업률": _r2(unemp, 1), "SOX_SPX": _r2(sox_spx, 4),
        "WTI": _r2(wti), "Fear_Greed": _r2(fgs, 0), "GOLD": _r2(gold, 0),
        "QQQ_DD_52w": _r2(_mk_qqq_dd, 1), "SOXX_DD_52w": _r2(_mk_soxx_dd, 1),
        "현금비중": _r2(_mk_cash, 4) if _mk_cash is not None else None,
        "TQQQ_ratio": _r2(_mk_ratio, 4) if _mk_ratio is not None else None,
        "SOXL_ratio": _r2(_mk_soxl_ratio, 4) if _mk_soxl_ratio is not None else None,
        "FPE_trailing복사": _fpe_is_copy,
        "거시_스코어_분해": export_mac_detail,
        "미어캣_스코어_분해": export_mk_detail,
        "거시_클러스터": export_mac_clusters,
        "클러스터_괴리도": _cl_divergence,
        "클러스터_최대": _cl_max_name,
        "클러스터_최소": _cl_min_name,
        "클러스터_코멘트": _cl_decouple,            # V3.4
        "클러스터_코멘트_쉬운": _cl_decouple_easy,   # V3.4
        "거시_속도": _mac_velocity,                # V3.4 (Δ30D)
        "거시_속도_라벨": _vel_lbl,                # V3.4
        "역사_매칭": _hist_match,                    # V3.4
        "역사_매칭_쉬운": _hist_match_easy,          # V3.4
        "거시_히스토리": _mac_history,             # V3.5 (date/score/mk/clusters/divergence/season)
        "히스토리_누적일수": len(_mac_history),       # V3.5
        "사분면": _mx_label,
        "사분면_키": _mx_key if (gs is not None and ms is not None) else None,
        "주요지표_방향성": _KEY_DELTAS,
        "추세": _TRENDS,
        "카드": _CARDS,
    }
    def _pick(keys):
        return {k: _CARDS[k] for k in keys if k in _CARDS}
    export_bond = {
        "date": _now_str, "3개월금리": _r2(dgs3m), "2년금리": _r2(dgs2), "10년금리": _r2(dgs10),
        "FF금리": _r2(ff), "2Y10Y_bp": _r2(t10y2y * 100, 1) if t10y2y is not None else None,
        "3M10Y_bp": _r2(t10y3m * 100, 1) if t10y3m is not None else None,
        "3M2Y_bp": t3m2y_bp, "실질금리": _r2(rr), "FF_6M변화": _r2(ff6m_chg),
        "data_dates": {"DGS3MO": _dgs3m_date, "DGS2": _dgs2_date, "DGS10": _dgs10_date, "공통_기준일": _rate_common_date},
        "카드": _pick(["2Y10Y_스프레드", "3M10Y_스프레드", "3M2Y_스프레드", "실질금리", "FF_6M변화", "연방기금금리"]),
    }
    export_valuation = {
        "date": _now_str, "Trailing_PE": tpe, "Forward_PE": fpe, "CAPE": cape,
        "배당수익률": dy, "소스": vd.get("source"), "FPE_trailing복사": _fpe_is_copy,
        "카드": _pick(["Forward_PE", "Trailing_PE", "Shiller_CAPE", "배당수익률"]),
    }
    export_semi = {
        "date": _now_str, "SOX_SPX비율": _r2(sox_spx, 4),
        "SOX_고점대비": _r2(sox_dd, 1), "SOX_SPX_3M상대": _r2(sox_rel3, 1),
        "카드": _pick(["SOX_SPX"]),
    }
    # V3.6: 쌍발 엔진 (QQQ/SOXX 가격·DD·트리거 + 계좌)
    export_engines = {
        "date": _now_str, "version": VERSION,
        "QQQ_DD_단계": _qdd_stg_e, "QQQ_DD_배율": _qdd_mult_e,
        "SOXX_DD_단계": _sdd_stg_e, "SOXX_DD_배율": _sdd_mult_e,
        "QQQ_DD_52w": _r2(_mk_qqq_dd, 1), "SOXX_DD_52w": _r2(_mk_soxx_dd, 1),
        "DD_기준_경계": _dd_caution, "DD_기준_조정": _dd_correction, "DD_기준_폭락": _dd_crash,
        "SOXL_DD_기준_1차": _soxl_dd1, "SOXL_DD_기준_2차": _soxl_dd2, "SOXL_DD_기준_3차": _soxl_dd3,
        "재투입_DD_얕은": _reinv_dd_shallow, "재투입_DD_중간": _reinv_dd_mid, "재투입_DD_깊은": _reinv_dd_deep,
        "재투입_속도_얕은": _reinv_spd_shallow, "재투입_속도_중간": _reinv_spd_mid, "재투입_속도_깊은": _reinv_spd_deep,
        # 계좌 (state.json + live)
        "총자산": _r2(_mk_total_val, 0) if _mk_total_val is not None else None,
        "총자산_KRW환산포함": _r2(_mk_total_val_adj, 0),
        "TQQQ_평가액": _r2(_mk_tqqq_eval, 0) if _mk_tqqq_eval is not None else None,
        "SOXL_평가액": _r2(_mk_soxl_eval, 0) if _mk_soxl_eval is not None else None,
        "SGOV_평가액": _r2(_mk_sgov_val, 0) if _mk_sgov_val is not None else None,
        "원화_raw": _mk_krw_val, "원화_USD환산": _r2(_mk_krw_usd, 0) if _mk_krw_usd is not None else None,
        "환율_KRW": _r2(_fx_krw, 1) if _fx_krw is not None else None,
        "현금비중": _r2(_mk_cash, 4) if _mk_cash is not None else None,
        "TQQQ_ratio": _r2(_mk_ratio, 4) if _mk_ratio is not None else None,
        "SOXL_ratio": _r2(_mk_soxl_ratio, 4) if _mk_soxl_ratio is not None else None,
        "TQQQ_ratio_라이브": _ratio_is_live, "TQQQ_가격소스": _tq_live_src,
        "SOXL_ratio_라이브": _soxl_ratio_is_live, "SOXL_가격소스": _sx_live_src,
        "TQQQ_주수": _mk_tqqq_shares, "SOXL_주수": _mk_soxl_shares,
        "TQQQ_평단투입": _r2(_mk_tqqq_cost, 0) if _mk_tqqq_cost is not None else None,
        "SOXL_평단투입": _r2(_mk_soxl_cost, 0) if _mk_soxl_cost is not None else None,
        "TQQQ_현재가": _r2(_mk_tqqq_price, 4) if _mk_tqqq_price is not None else None,
        "SOXL_현재가": _r2(_mk_soxl_price, 4) if _mk_soxl_price is not None else None,
        "trigger": _mk_trigger, "target": _mk_target,
        "최근_점검일": _mk_last_check, "최근_매수일": _mk_last_buy,
        "YTD_매수횟수": _mk_ytd_count, "다음_매수예정금": _mk_next_buy,
        "state_갱신": mk.get("updated") if mk else None,
    }
    export_inflation = {
        "date": _now_str, "CPI_YoY": _r2(cpi_y), "CPI코어_YoY": _r2(cpic_y),
        "PCE_YoY": _r2(pce_y), "PCE코어_YoY": _r2(pcec_y),
        "BEI_5Y": _r2(bei),
        "카드": _pick(["CPI_YoY", "CPI코어_YoY", "PCE_YoY", "PCE코어_YoY", "BEI_5Y"]),
    }
    export_employment = {
        "date": _now_str, "실업률": _r2(unemp, 1), "UNRATE_3M변화": _r2(un3m_chg, 2),
        "JOLTS": _r2(jolts, 0), "NFP": _r2(nfp, 0),
        "소비자신뢰": _r2(um, 1), "GDP": _r2(gdpv),
        "CFNAI_MA3": _r2(cfnai_ma3, 2),  # V3.3
        "카드": _pick(["실업률", "실업률_3M변화", "JOLTS", "NFP", "소비자신뢰", "GDP_성장률", "카드연체율", "CFNAI_MA3"]),
    }
    # 계절 체크리스트 상세 (16개 항목)
    # numpy.bool_ → Python bool 강제 변환 (JSON 인코더 호환)
    export_season_checks = {sn: [(lbl, bool(v)) for lbl, v in items] for sn, items in season_checks.items()}
    # JSON 부조화 방지 — 라벨 결정 과정 분리 명시.
    # 동률 (여름=가을 4=4) 시 raw_best=가을 + 히스테리시스 직전 라벨=여름 유지 → label="늦여름" 가능.
    # 점수 동률인데 라벨 "늦여름" 으로 보이는 부조화 추적 위해 raw_best/base/prefix/hysteresis 분리 export.
    _v8_diag = v651_today or {}
    export_season_d = {
        "date": _now_str, "계절": season_auto, "확신도": season_conf, "계절_점수": season_scores,
        "계절_원형_raw_best": _v8_diag.get("raw_season"),     # tiebreak 직후 (히스테리시스/전이 전)
        "계절_base_after_hyst": _v8_diag.get("base"),         # 히스테리시스 + 전이 적용 후 base
        "계절_접두사": _v8_diag.get("prefix", ""),             # prefix (초/늦/없음)
        "히스테리시스_적용": _v8_diag.get("hysteresis_held", False),
        "계절_체크리스트": export_season_checks,
        "계절_박스_총수": 9,
        "계절_박스_구조": "공통 5 + 고유 4",
        "확신도_임계": {
            "매우 높음": "best≥8 AND 차이≥3",
            "높음":     "best≥8 OR (best≥7 AND 차이≥2)",
            "보통":     "best 6~7",
            "낮음":     "best 5",
            "판정 불가": "best ≤4",
        },
        "역사_매칭": _hist_match,           # V3.4
        "역사_매칭_쉬운": _hist_match_easy,  # V3.4
    }
    # 시계열 원본 (최근 252일)
    def _ts_export(s, n=252):
        if s is None or len(s) == 0: return None
        sl = s.iloc[-n:]
        return {str(d.date()) if hasattr(d, 'date') else str(d): round(float(v), 4) for d, v in zip(sl.index, sl.values)}
    export_timeseries = {
        "T10Y2Y": _ts_export(fd.get("T10Y2Y")),
        "DGS2": _ts_export(fd.get("DGS2")),
        "DGS10": _ts_export(fd.get("DGS10")),
        "FEDFUNDS": _ts_export(fd.get("FEDFUNDS")),
        "VIXCLS": _ts_export(fd.get("VIXCLS")),
        "HY": _ts_export(fd.get("HY")),
        "UNRATE": _ts_export(fd.get("UNRATE")),
        "DXY": _ts_export(yd.get("DXY")),
        "SOXX": _ts_export(yd.get("SOXX")),
        "SPX": _ts_export(yd.get("SPX")),
        "QQQ": _ts_export(yd.get("QQQ")),
        "GOLD": _ts_export(yd.get("GOLD")),
        "KRW": _ts_export(fd.get("KRW")),
        "WTI": _ts_export(fd.get("WTI")),
        "CFNAI": _ts_export(fd.get("CFNAI")),  # V3.3
        "거시_히스토리": _mac_history,         # V3.5
    }
    # 관찰 기록 일괄
    _obs_all = []
    for _of in lobs():
        try: _obs_all.append(json.loads(_of.read_text("utf-8")))
        except: pass
    export_all = {
        **export_dash,
        "3M10Y_bp": _r2(t10y3m * 100, 1) if t10y3m is not None else None,
        "3M2Y_bp": t3m2y_bp, "실질금리": _r2(rr),
        "data_dates": {"DGS3MO": _dgs3m_date, "DGS2": _dgs2_date, "DGS10": _dgs10_date, "공통_기준일": _rate_common_date},
        "Trailing_PE": tpe, "CAPE": cape, "배당수익률": dy,
        "CPI_YoY": _r2(cpi_y), "CPI코어_YoY": _r2(cpic_y),
        "PCE_YoY": _r2(pce_y), "PCE코어_YoY": _r2(pcec_y),
        "JOLTS": _r2(jolts, 0), "NFP": _r2(nfp, 0),
        "GDP": _r2(gdpv), "소비자신뢰": _r2(um, 1),
        "BEI_5Y": _r2(bei), "카드연체율": _r2(cd, 1), "국채GDP비율": _r2(dg, 0),
        "FF_6M변화": _r2(ff6m_chg), "UNRATE_3M변화": _r2(un3m_chg),
        "SOXL_ratio": _r2(_mk_soxl_ratio, 4) if _mk_soxl_ratio is not None else None,
        "QQQ_DD_단계": _qdd_stg_e, "QQQ_DD_배율": _qdd_mult_e,
        "SOXX_DD_단계": _sdd_stg_e, "SOXX_DD_배율": _sdd_mult_e,
        "DD_기준_경계": _dd_caution, "DD_기준_조정": _dd_correction, "DD_기준_폭락": _dd_crash,
        "SOXL_DD_기준_1차": _soxl_dd1, "SOXL_DD_기준_2차": _soxl_dd2, "SOXL_DD_기준_3차": _soxl_dd3,
        "재투입_DD_얕은": _reinv_dd_shallow, "재투입_DD_중간": _reinv_dd_mid, "재투입_DD_깊은": _reinv_dd_deep,
        "재투입_속도_얕은": _reinv_spd_shallow, "재투입_속도_중간": _reinv_spd_mid, "재투입_속도_깊은": _reinv_spd_deep,
        "FPE_trailing복사": _fpe_is_copy, "PE소스": vd.get("source"),
        "계절_점수": season_scores, "계절_체크리스트": export_season_checks,
        "SOX_SPX비율": _r2(sox_spx, 4),
        "거시_스코어_분해": export_mac_detail,
        "미어캣_스코어_분해": export_mk_detail,
        "거시_클러스터": export_mac_clusters,
        "클러스터_괴리도": _cl_divergence,
        "클러스터_최대": _cl_max_name,
        "클러스터_최소": _cl_min_name,
        "사분면": _mx_label,
        "사분면_키": _mx_key if (gs is not None and ms is not None) else None,
        "주요지표_방향성": _KEY_DELTAS,
        "추세": _TRENDS,
    }
    _ds = datetime.now().strftime('%Y%m%d')
    def _jbtn(data, prefix, label="📥 JSON Export", key_sfx=""):
        st.download_button(label, json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
                           f"{prefix}_{_ds}.json", "application/json", key=f"exp_{prefix}{key_sfx}")
    def _export_html(data, title="미어캣의 관측소", charts=None, section=None):
        """HTML 리포트 생성. charts: list of (fig, title) plotly figures. section: 섹션명(None=전체)."""
        _skip = {"date", "version", "계절_점수", "거시_스코어_분해", "미어캣_스코어_분해",
                 "거시_클러스터", "계절_체크리스트", "시계열", "관찰_기록", "주요지표_방향성", "추세", "카드",
                 "거시_히스토리"}  # V3.5: 별도 섹션으로 렌더
        rows = "".join(f"<tr><td style='padding:6px 12px;border-bottom:1px solid #ddd'>{k}</td>"
                       f"<td style='padding:6px 12px;border-bottom:1px solid #ddd;font-weight:600'>"
                       f"{v if v is not None else '—'}</td></tr>"
                       for k, v in data.items() if k not in _skip and not isinstance(v, (dict, list)))
        # 스코어 분해 테이블
        gd_html = ""
        if data.get("거시_스코어_분해"):
            gd_rows = "".join(f"<tr><td>{k}</td><td>{v.get('raw','—')}</td><td>{v.get('weight','—')}</td><td><strong>{v.get('contrib','—')}</strong></td></tr>"
                              for k, v in data["거시_스코어_분해"].items())
            gd_html = f"<h2>거시 스코어 분해</h2><table><tr><th>지표</th><th>원점수(0~10)</th><th>가중치</th><th>기여점수</th></tr>{gd_rows}</table>"
        md_html = ""
        if data.get("미어캣_스코어_분해"):
            md_rows = "".join(f"<tr><td>{k}</td><td>{v.get('raw','—')}</td><td>{v.get('weight','—')}</td><td><strong>{v.get('contrib','—')}</strong></td></tr>"
                              for k, v in data["미어캣_스코어_분해"].items())
            md_html = f"<h2>미어캣 스코어 분해</h2><table><tr><th>지표</th><th>원점수</th><th>가중치</th><th>기여점수</th></tr>{md_rows}</table>"
        # 클러스터 분해 테이블
        cl_html = ""
        if data.get("거시_클러스터"):
            for cname, cdata in data["거시_클러스터"].items():
                cs = cdata.get("score"); drivers = cdata.get("drivers", {})
                cl_html += f"<h3 style='color:#a5d6a7;margin-top:20px'>{cname} — {cs if cs is not None else '—'}/100</h3>"
                if drivers:
                    dr = "".join(f"<tr><td>{dk}</td><td>{dv.get('raw','—')}</td><td>{dv.get('weight','—')}</td><td><strong>{dv.get('contrib','—')}</strong></td></tr>" for dk, dv in drivers.items())
                    cl_html += f"<table><tr><th>지표</th><th>원점수</th><th>가중치</th><th>기여</th></tr>{dr}</table>"
            if cl_html: cl_html = f"<h2>거시 뷰 — 5클러스터</h2>{cl_html}"
        # 계절 체크리스트 테이블
        sc_html = ""
        if data.get("계절_체크리스트"):
            for sn, items in data["계절_체크리스트"].items():
                sc_html += f"<h3 style='margin-top:16px'>{sn} ({sum(1 for _,v in items if v)}/9)</h3>"
                sc_html += "".join(f"<div>{'✅' if v else '⬜'} {lbl}</div>" for lbl, v in items)
            if sc_html: sc_html = f"<h2>계절 체크리스트</h2>{sc_html}"
        # 전 지표 추세 테이블 (일간/월간/분기별로 분리)
        tr_html = ""
        if data.get("추세"):
            _DAILY = ["2Y10Y_bp","3M10Y_bp","DXY_pct","KRW_pct","FEDFUNDS","VIX","HY_bp","UNRATE","WTI_pct","GOLD_pct","BEI_5Y"]
            _MONTHLY = ["CPI_YoY_pp","CPI_코어_YoY_pp","PCE_YoY_pp","PCE_코어_YoY_pp","JOLTS_pct","NFP","소비자신뢰"]
            _QUARTERLY = ["GDP_pp","카드연체율_pp","국채GDP_pp"]
            def _trtbl(keys, periods, title):
                rows_t = ""
                for k in keys:
                    v = data["추세"].get(k)
                    if not v: continue
                    cells = "".join(f"<td style='text-align:right'>{v.get(p) if v.get(p) is not None else '—'}</td>" for p in periods)
                    rows_t += f"<tr><td>{k}</td>{cells}</tr>"
                if not rows_t: return ""
                hdr = "".join(f"<th style='text-align:right'>{p}</th>" for p in periods)
                return f"<h3>{title}</h3><table><tr><th>지표</th>{hdr}</tr>{rows_t}</table>"
            tr_html = _trtbl(_DAILY, ["1W","2W","1M","3M","6M","1Y"], "일간 (1W~1Y)")
            tr_html += _trtbl(_MONTHLY, ["1M","3M","6M","1Y"], "월간 (1M~1Y)")
            tr_html += _trtbl(_QUARTERLY, ["1Q","2Q","1Y"], "분기 (1Q~1Y)")
            if tr_html: tr_html = f"<h2>전 지표 추세</h2>{tr_html}"
        # 카드 구조화 테이블
        card_html = ""
        if data.get("카드"):
            _color_hex = {"green": "#1D9E75", "red": "#A32D2D", "yellow": "#EF9F27", "gray": "#8b949e"}
            crows = ""
            for cname, cv in data["카드"].items():
                if not isinstance(cv, dict): continue
                _val = cv.get("value")
                _lbl = cv.get("label", "—")
                _clr = _color_hex.get(cv.get("color", "gray"), "#8b949e")
                _cmt = cv.get("comment", "")
                crows += (f"<tr><td>{cname}</td>"
                          f"<td style='text-align:right'>{_val if _val is not None else '—'}</td>"
                          f"<td style='text-align:center;color:{_clr};font-weight:700'>{_lbl}</td>"
                          f"<td style='font-size:13px;color:#b0bec5'>{_cmt}</td></tr>")
            if crows:
                card_html = f"<h2>카드 상세 (라벨 / 코멘트)</h2><table><tr><th>지표</th><th>값</th><th>라벨</th><th>코멘트</th></tr>{crows}</table>"
        # 주요 지표 방향성 테이블
        delta_html = ""
        if data.get("주요지표_방향성"):
            _dnames = {"VIX": "VIX", "HY_bp": "HY (bp)", "2Y10Y_bp": "10Y-2Y (bp)", "DXY": "DXY (%)", "SOX_SPX": "SOX/SPX (%)", "KRW": "원/달러 (%)"}
            dr = "".join(f"<tr><td>{_dnames.get(k,k)}</td><td style='text-align:center'>{v.get('1W_d','→')} {v.get('1W','—')}</td><td style='text-align:center'>{v.get('1M_d','→')} {v.get('1M','—')}</td></tr>"
                         for k, v in data["주요지표_방향성"].items())
            delta_html = f"<h2>주요 지표 방향성 (Δ)</h2><table><tr><th>지표</th><th>1W</th><th>1M</th></tr>{dr}</table>"
        if data.get("사분면") or data.get("클러스터_괴리도") is not None:
            delta_html += "<h2>시장 구조</h2>"
            if data.get("사분면"): delta_html += f"<p><strong>2×2 사분면:</strong> {data['사분면']} ({data.get('사분면_키','')})</p>"
            if data.get("클러스터_괴리도") is not None: delta_html += f"<p><strong>클러스터 괴리도:</strong> {data['클러스터_괴리도']} ({data.get('클러스터_최대','?')} vs {data.get('클러스터_최소','?')})</p>"
        # 차트 HTML
        chart_html = ""
        if charts:
            for idx, (fig, ct) in enumerate(charts):
                chart_html += f"<h2>{ct}</h2>"
                chart_html += pio.to_html(fig, full_html=False, include_plotlyjs=(idx==0))
        sec_title = f"{title} — {section}" if section else title
        return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{sec_title} {_now_str}</title>
<style>body{{font-family:'Segoe UI',sans-serif;max-width:1000px;margin:40px auto;padding:0 20px;background:#1a1a2e;color:#e0e0e0}}
h1{{border-bottom:2px solid #4CAF50;padding-bottom:10px;color:#4CAF50}}
h2{{color:#81c784;margin-top:30px;border-bottom:1px solid #333;padding-bottom:6px}}
h3{{color:#b0bec5;margin-top:16px}}
table{{border-collapse:collapse;width:100%;margin:16px 0}}
th{{text-align:left;padding:8px 12px;background:#263238;border-bottom:2px solid #4CAF50;color:#a5d6a7}}
td{{padding:6px 12px;border-bottom:1px solid #333}}
tr:hover{{background:#263238}}
.summary{{display:flex;gap:20px;flex-wrap:wrap;margin:20px 0}}
.card{{background:#263238;border:1px solid #333;border-radius:8px;padding:16px;min-width:140px;text-align:center}}
.card .label{{font-size:12px;color:#999}}.card .val{{font-size:24px;font-weight:700;color:#4CAF50}}</style></head>
<body><h1>\\U0001f441\\ufe0f {sec_title} V{VERSION}</h1><p>{_now_str} | 계절: {season_auto} ({season_conf})</p>
<div class="summary"><div class="card"><div class="label">거시 스코어</div><div class="val">{gs if gs is not None else '—'}</div></div>
<div class="card"><div class="label">미어캣 스코어</div><div class="val">{ms if ms is not None else '—'}</div></div>
<div class="card"><div class="label">계절</div><div class="val">{season_auto}</div></div>
<div class="card"><div class="label">사분면</div><div class="val">{_mx_label or '—'}</div></div>
<div class="card"><div class="label">괴리도</div><div class="val">{_cl_divergence if _cl_divergence is not None else '—'}</div></div></div>
<h2>지표 현황</h2><table><tr><th>지표</th><th>값</th></tr>{rows}</table>
{gd_html}{cl_html}{md_html}{sc_html}{card_html}{delta_html}{tr_html}{chart_html}
<hr><p style="color:#666;font-size:12px">미어캣의 관측소 V{VERSION} · 자동 생성</p></body></html>"""

    # ── state.json에 거시 데이터 기록 (외부 도구 연동용) ──
    # V3.7-hotfix6: 실시간 현재가 5종 추가 (엑셀 가계부 자동 갱신용)
    def _last(ticker):
        _s = yd.get(ticker)
        if _s is None or len(_s) == 0: return None
        try: return float(_s.iloc[-1])
        except: return None
    _tqqq_live = _last("TQQQ")
    _soxl_live = _last("SOXL")
    _qqq_live  = _last("QQQ")
    _voo_live  = _last("VOO")
    _sgov_live = _last("SGOV")
    # V4.5: 한국 ETF 종가 (pykrx) — TIGER 381180, K-QLD 418660
    # pykrx 는 KRX 웹 스크래핑 기반이라 간헐적 실패 가능 → None 시 외부 도구가 기존 셀 값 유지.
    _tiger_krw = fetch_krx_etf_close("381180")
    _kqld_krw  = fetch_krx_etf_close("418660")
    sstate({
        "date": datetime.now().strftime("%Y-%m-%d"),
        "season": season_auto, "confidence": season_conf,
        "mac_score": gs, "meerkat_score": ms,
        "vix": _r2(vix), "fear_greed": _r2(fgs, 0),
        "qqq_dd_52w": _r2(_mk_qqq_dd, 1), "soxx_dd_52w": _r2(_mk_soxx_dd, 1),
        # 52w 고점 절대값 (엑셀 수기 입력 제거용 — 외부 도구 dash_xl 오버라이드 소스)
        "qqq_52w_high":  _r2(_mk_qqq_52w_high, 4) if _mk_qqq_52w_high is not None else None,
        "soxx_52w_high": _r2(_mk_soxx_52w_high, 4) if _mk_soxx_52w_high is not None else None,
        # V3.7-hotfix6: 실시간 현재가 (총계 B5/B12/B20 + 주간점검 T17 + VOO 가계부용)
        "tqqq_live_price": _r2(_tqqq_live, 4) if _tqqq_live is not None else None,
        "soxl_live_price": _r2(_soxl_live, 4) if _soxl_live is not None else None,
        "sgov_live_price": _r2(_sgov_live, 4) if _sgov_live is not None else None,
        "qqq_live_price":  _r2(_qqq_live, 4)  if _qqq_live  is not None else None,
        "voo_live_price":  _r2(_voo_live, 4)  if _voo_live  is not None else None,
        # V4.5: 한국 ETF 종가 (총계 B30/B31 신설)
        "tiger_price_krw": _r2(_tiger_krw, 0) if _tiger_krw is not None else None,
        "kqld_price_krw":  _r2(_kqld_krw, 0)  if _kqld_krw  is not None else None,
        **( {"fx_krw": round(float(fd["KRW"].iloc[-1]), 2)} if fd.get("KRW") is not None and len(fd["KRW"].dropna()) > 0 else {} ),
        **( {"ffr": round(float(fd["FEDFUNDS"].iloc[-1]), 4)} if fd.get("FEDFUNDS") is not None and len(fd["FEDFUNDS"].dropna()) > 0 else {} ),
    })

    # ── V3.7 관측 스냅샷 (가공 지표 + 백필 불가 원본 → observations.jsonl append) ──
    try:
        _obs_cl = {}
        for _cln, _cv in (export_mac_clusters or {}).items():
            _sv = _cv.get("score") if isinstance(_cv, dict) else None
            if _sv is not None:
                try: _obs_cl[_cln] = float(_sv)
                except: pass
        # V3.10.0 history match enriched helpers
        _hm_m = _hist_match.get("matches") or []
        _hm_top1 = _hm_m[0] if _hm_m else {}
        _hm_md1 = set(_hm_top1.get("matched_dims", []))
        # V3.10.4: 현재 매크로 상태 enum 10차원 (진단/사후분석용)
        _hm_state = _hist_match.get("current_state") or {}
        # V3.11.0: DTW 진행도 1회 계산 (라이브 obs row 만)
        _dtw_progress = None
        try:
            if _DTW_AVAILABLE and _hm_top1.get("era_id"):
                _dtw_long_obs = _load_long_range_series(api_key)
                _dtw_curr_obs = _build_raw_data_for_backfill(fd, yd)
                _dtw_progress = measure_era_progress(_hm_top1["era_id"], _dtw_long_obs, _dtw_curr_obs)
        except Exception: _dtw_progress = None
        _obs_row = {
            "ts":      datetime.now().isoformat(timespec="seconds"),
            "date":    _date.today().isoformat(),
            "version": VERSION,
            "_tool_version": VERSION,
            "_tool_name":    "미어캣의 관측소",
            "_author":       __author__,
            # 핵심 스코어
            "mac_score":     float(gs) if gs is not None else None,
            "meerkat_score": float(ms) if ms is not None else None,
            "mac_velocity":  float(_mac_velocity) if _mac_velocity is not None else None,
            # 5 클러스터 점수
            "cluster_bond":    _obs_cl.get("채권/금리"),
            "cluster_val":     _obs_cl.get("밸류에이션"),
            "cluster_stress":  _obs_cl.get("스트레스"),
            "cluster_real":    _obs_cl.get("실물"),
            "cluster_semi":    _obs_cl.get("반도체"),
            "cluster_divergence": float(_cl_divergence) if _cl_divergence is not None else None,
            "cluster_max": _cl_max_name, "cluster_min": _cl_min_name,
            # 사계절 (V3.8 9박스)
            "season":              season_auto,                                       # 합쳐진 라벨 (호환성 유지)
            "season_base":         (season_auto.lstrip("초늦") if season_auto else None),
            "season_prefix":       (season_auto[0] if (season_auto and season_auto[0] in "초늦") else ""),
            "season_confidence":   season_conf,
            "season_score_spring": (season_scores or {}).get("봄"),
            "season_score_summer": (season_scores or {}).get("여름"),
            "season_score_autumn": (season_scores or {}).get("가을"),
            "season_score_winter": (season_scores or {}).get("겨울"),
            "season_max_score":    9,
            # 2×2 매트릭스
            "matrix_quadrant":     _mx_label,
            "matrix_key":          _mx_key if (gs is not None and ms is not None) else None,
            # 백필 불가 원본 (API 가 과거 히스토리 안 줌 → 관측만 기록)
            "fear_greed":  float(fgs) if fgs is not None else None,
            "forward_pe":  float(fpe) if fpe is not None else None,
            "trailing_pe": float(tpe) if tpe is not None else None,
            "cape":        float(cape) if cape is not None else None,
            "dividend_yield": float(dy) if dy is not None else None,
            # 52w DD (파생)
            "qqq_dd_52w":  float(_mk_qqq_dd) if _mk_qqq_dd is not None else None,
            "soxx_dd_52w": float(_mk_soxx_dd) if _mk_soxx_dd is not None else None,
            # 가공 매크로 지표 (raw 만으로는 표현 안 되는 카드 값)
            "cfnai_ma3":     float(cfnai_ma3) if cfnai_ma3 is not None else None,
            "ff6m_chg":      float(ff6m_chg) if ff6m_chg is not None else None,
            "un3m_chg":      float(un3m_chg) if un3m_chg is not None else None,
            "real_rate":     float(rr) if rr is not None else None,
            "buffett_ratio": float(buffett) if buffett is not None else None,
            "sox_rel3":      float(sox_rel3) if sox_rel3 is not None else None,
            "xle_spy_3m":    float(xle_spy_3m) if xle_spy_3m is not None else None,
            "xlk_spy_3m":    float(xlk_spy_3m) if xlk_spy_3m is not None else None,
            # V3.8.2 노동격차 + 공급충격 디커플링
            "labor_gap_K":     float(_gap_now) if _gap_now is not None else None,
            "labor_gap_label": _gap_lbl,
            "wti_3m_pct":      float(_wti_3m_dash) if _wti_3m_dash is not None else None,
            "spx_3m_pct":      float(_spx_3m_dash) if _spx_3m_dash is not None else None,
            "shock_state":     _shock_state,
            "shock_box_on":    bool(_box_on) if _box_on is not None else None,
            # F1 역전 해소 진행률
            "f1_2y10y_recovery_pct": float(_inv2y10y["recovery_pct"]) if _inv2y10y is not None else None,
            "f1_3m10y_recovery_pct": float(_inv3m10y["recovery_pct"]) if _inv3m10y is not None else None,
            # F2 인하 사이클
            "f2_cum_cut_bp": float(_cut_info["cum_cut_bp"]) if (_cut_info and _cut_info.get("active")) else None,
            "f2_months":     float(_cut_info["months"])     if (_cut_info and _cut_info.get("active")) else None,
            "f2_stage":      _cut_info["stage"]             if (_cut_info and _cut_info.get("active")) else None,
            # F3 FF금리 historical percentile
            "f3_ff_position": float(_ff_pos_pct) if _ff_pos_pct is not None else None,
            # F5 가속도 모니터 (5종 ratio + 종합)
            "f5_vix_ratio":     (_ac_vix.get("ratio")     if _ac_vix     else None),
            "f5_hy_ratio":      (_ac_hy.get("ratio")      if _ac_hy      else None),
            "f5_t10y2y_ratio":  (_ac_t2y10y.get("ratio")  if _ac_t2y10y  else None),
            "f5_dxy_ratio":     (_ac_dxy.get("ratio")     if _ac_dxy     else None),
            "f5_soxspx_ratio":  (_ac_soxspx.get("ratio")  if _ac_soxspx  else None),
            # F6 Forward EPS 추세 (30일 미만이면 가용 max lookback 으로 fallback)
            "f6_eps_chg_30d":   float(_refl_30["eps_chg"]) if (_refl_30 and _refl_30.get("eps_chg") is not None) else None,
            "f6_spx_chg_30d":   float(_refl_30["spx_chg"]) if (_refl_30 and _refl_30.get("spx_chg") is not None) else None,
            "f6_lookback_days": int(_refl_30["n"])         if (_refl_30 and _refl_30.get("n") is not None) else None,
            # 클러스터별 Δ30D / ΔΔ (5클러스터 + 거시 종합)
            "mac_delta":         (_mac_dd.get("delta")         if _mac_dd     else None),
            "mac_delta_delta":   (_mac_dd.get("delta_delta")   if _mac_dd     else None),
            "cluster_bond_delta":      (_cl_dd.get("채권/금리",   {}) or {}).get("delta")       if _cl_dd.get("채권/금리")   else None,
            "cluster_bond_dd":         (_cl_dd.get("채권/금리",   {}) or {}).get("delta_delta") if _cl_dd.get("채권/금리")   else None,
            "cluster_val_delta":       (_cl_dd.get("밸류에이션", {}) or {}).get("delta")       if _cl_dd.get("밸류에이션") else None,
            "cluster_val_dd":          (_cl_dd.get("밸류에이션", {}) or {}).get("delta_delta") if _cl_dd.get("밸류에이션") else None,
            "cluster_stress_delta":    (_cl_dd.get("스트레스",   {}) or {}).get("delta")       if _cl_dd.get("스트레스")   else None,
            "cluster_stress_dd":       (_cl_dd.get("스트레스",   {}) or {}).get("delta_delta") if _cl_dd.get("스트레스")   else None,
            "cluster_real_delta":      (_cl_dd.get("실물",       {}) or {}).get("delta")       if _cl_dd.get("실물")       else None,
            "cluster_real_dd":         (_cl_dd.get("실물",       {}) or {}).get("delta_delta") if _cl_dd.get("실물")       else None,
            "cluster_semi_delta":      (_cl_dd.get("반도체",     {}) or {}).get("delta")       if _cl_dd.get("반도체")     else None,
            "cluster_semi_dd":         (_cl_dd.get("반도체",     {}) or {}).get("delta_delta") if _cl_dd.get("반도체")     else None,
            # V3.10.0 역사 매칭 — top 3 enriched + 차원별 매칭 (10개 차원, 0/1)
            "history_era_top1":             _hist_match.get("era"),
            "history_era_label":            _hist_match.get("label"),
            "history_score_top1":           (round(float(_hist_match["score"]), 3) if _hist_match.get("score") is not None else None),
            "history_era_top2":             (_hm_m[1].get("era_id") if len(_hm_m) > 1 else None),
            "history_score_top2":           (round(float(_hm_m[1]["score"]), 3) if len(_hm_m) > 1 else None),
            "history_era_top3":             (_hm_m[2].get("era_id") if len(_hm_m) > 2 else None),
            "history_score_top3":           (round(float(_hm_m[2]["score"]), 3) if len(_hm_m) > 2 else None),
            # V3.10.0 신규 — top 1 enriched 메타
            "history_aftermath_top1":       _hm_top1.get("aftermath"),
            "history_years_ago_top1":       _hm_top1.get("years_ago"),
            "history_era_type_top1":        _hm_top1.get("era_type"),
            "history_duration_days_top1":   _hm_top1.get("historical_duration_days"),
            "history_unmatched_top1":       (",".join(_hm_top1.get("unmatched_dims", [])) if _hm_top1 else None),
            "history_common_dims":          ",".join(_hist_match.get("common_dims") or []),
            # V3.10.0 차원별 매칭 (top 1 기준, 1=매칭/0=미매칭, None=매칭없음)
            "history_dim_match_season":          ((1 if "season" in _hm_md1 else 0) if _hm_top1 else None),
            "history_dim_match_ff_pos":          ((1 if "ff_pos" in _hm_md1 else 0) if _hm_top1 else None),
            "history_dim_match_ff_action":       ((1 if "ff_action" in _hm_md1 else 0) if _hm_top1 else None),
            "history_dim_match_inflation_trend": ((1 if "inflation_trend" in _hm_md1 else 0) if _hm_top1 else None),
            "history_dim_match_valuation":       ((1 if "valuation" in _hm_md1 else 0) if _hm_top1 else None),
            "history_dim_match_credit":          ((1 if "credit" in _hm_md1 else 0) if _hm_top1 else None),
            "history_dim_match_yield_curve":     ((1 if "yield_curve" in _hm_md1 else 0) if _hm_top1 else None),
            "history_dim_match_semiconductor":   ((1 if "semiconductor" in _hm_md1 else 0) if _hm_top1 else None),
            "history_dim_match_dollar":          ((1 if "dollar" in _hm_md1 else 0) if _hm_top1 else None),
            "history_dim_match_external_shock":  ((1 if "external_shock" in _hm_md1 else 0) if _hm_top1 else None),
            # V3.10.4 현재 상태 enum (진단용)
            "state_season":           _hm_state.get("season"),
            "state_ff_pos":           _hm_state.get("ff_pos"),
            "state_ff_action":        _hm_state.get("ff_action"),
            "state_inflation_trend":  _hm_state.get("inflation_trend"),
            "state_valuation":        _hm_state.get("valuation"),
            "state_credit":           _hm_state.get("credit"),
            "state_yield_curve":      _hm_state.get("yield_curve"),
            "state_semiconductor":    _hm_state.get("semiconductor"),
            "state_dollar":           _hm_state.get("dollar"),
            "state_external_shock":   _hm_state.get("external_shock"),
            # V3.11.0 DTW 진행도 (실측 행에만 의미 있음)
            "era_progress_pct":          (_dtw_progress.get("progress_pct") if _dtw_progress else None),
            "era_progress_current_day":  (_dtw_progress.get("current_day_in_era") if _dtw_progress else None),
            "era_progress_confidence":   (_dtw_progress.get("confidence") if _dtw_progress else None),
            "era_progress_series_count": (_dtw_progress.get("series_used_count") if _dtw_progress else None),
        }
        _hist_append_observation(_obs_row)
        try: _hist_load_obs_df.clear()
        except: pass
    except Exception as _obs_err:
        # 조용히 먹지 말 것 — 스냅샷이 영구히 빈 채로 있어 놓치기 쉽다
        try: st.sidebar.caption(f"⚠️ 관측 스냅샷 저장 실패: {type(_obs_err).__name__}: {_obs_err}")
        except: pass

    _fig_bond = None; _fig_semi = None; _fig_season = None; _fig_mx = None
    tabs = st.tabs([bsl(t, mode) for t in ["📡 대시보드", "🚀 쌍발 엔진", "📈 채권/금리", "💰 밸류에이션", "🏭 반도체", "🌡️ 계절 판단", "📋 관찰 기록", "📈 시계열", "🔮 시점 조회"]])
    st.markdown(TIP_CSS, unsafe_allow_html=True)

    # ── 카드 → 시리즈 매핑 (미니 시계열용). label 기준 자동 lookup ──
    # ("raw", key) = raw.jsonl 시리즈 / ("obs", key) = observations.jsonl 가공 지표
    # 2026-04-29 미니시계열 전수 점검 — obs only 11일치 한계 우회 + CPI/PCE/GDP 라벨-그래프 일관성.
    # ("calc", fn) — fn(raw_df) → pd.Series. raw 백필 (~수십년) 데이터로 즉시 계산.
    def _yoy_pct(df, col):
        if col not in df.columns: return pd.Series(dtype=float)
        s = df[col].dropna()
        if len(s) < 13: return pd.Series(dtype=float)
        return ((s / s.shift(12) - 1) * 100).dropna()
    def _qoq_annualized(df, col):
        if col not in df.columns: return pd.Series(dtype=float)
        s = df[col].dropna()
        if len(s) < 2: return pd.Series(dtype=float)
        return (((s / s.shift(1)) ** 4 - 1) * 100).dropna()
    def _dd_52w(df, col):
        if col not in df.columns: return pd.Series(dtype=float)
        s = df[col].dropna()
        if len(s) < 252: return pd.Series(dtype=float)
        return ((s / s.rolling(252).max() - 1) * 100).dropna()
    def _pct_chg(df, col, lookback):
        if col not in df.columns: return pd.Series(dtype=float)
        s = df[col].dropna()
        if len(s) < lookback + 1: return pd.Series(dtype=float)
        return ((s / s.shift(lookback) - 1) * 100).dropna()
    def _abs_chg(df, col, lookback):
        if col not in df.columns: return pd.Series(dtype=float)
        s = df[col].dropna()
        if len(s) < lookback + 1: return pd.Series(dtype=float)
        return (s - s.shift(lookback)).dropna()
    _CARD_SERIES_MAP = {
        # 미어캣 상황판 — 52w drawdown
        "QQQ DD":          ("calc", lambda df: _dd_52w(df, "QQQ")),
        "SOXX DD":         ("calc", lambda df: _dd_52w(df, "SOXX")),
        # V3.4 핵심 5
        "VIX":             ("raw", "VIXCLS"),
        "Fear & Greed":    ("obs", "fear_greed"),  # CNN API only
        "10Y-2Y 스프레드":  ("raw", "T10Y2Y"),
        "WTI 유가":         ("raw", "DCOILWTICO"),
        "원/달러":          ("raw", "DEXKOUS"),
        # 시장 온도 (심안)
        "Forward PE":      ("obs", "forward_pe"),  # multpl 외부 only
        "DXY":             ("raw", "DX-Y.NYB"),
        "연방기금금리":     ("raw", "FEDFUNDS"),
        "하이일드 스프레드": ("raw", "BAMLH0A0HYM2"),
        # 시장/실물 — SOX/SPX 3M 상대수익률
        "SOX/SPX":         ("calc", lambda df: (_pct_chg(df, "SOXX", 63) - _pct_chg(df, "^GSPC", 63)).dropna()),
        "실업률":           ("raw", "UNRATE"),
        # 채권/금리 심화
        "10Y-3M 스프레드":  ("raw", "T10Y3M"),
        # 2Y-3M: DGS3MO 백필 누락 → T10Y3M - T10Y2Y 합성 (= -(2Y-3M), 부호 반대지만 정확. 카드 라벨 부합 위해 부호 반전)
        "2Y-3M 스프레드":   ("calc", lambda df: (
            (df["T10Y3M"] - df["T10Y2Y"]).dropna()
            if ("T10Y3M" in df.columns and "T10Y2Y" in df.columns) else pd.Series(dtype=float)
        )),
        # 실질금리 = FEDFUNDS - CPI YoY (둘 다 월간)
        "실질금리":         ("calc", lambda df: (
            (df["FEDFUNDS"].dropna() - _yoy_pct(df, "CPIAUCSL").reindex(df["FEDFUNDS"].dropna().index, method="nearest")).dropna()
            if ("FEDFUNDS" in df.columns and "CPIAUCSL" in df.columns) else pd.Series(dtype=float)
        )),
        "FF 6M 변화":       ("calc", lambda df: _abs_chg(df, "FEDFUNDS", 6)),
        # 인플레 4종 — YoY % (헤드라인과 일치)
        "CPI YoY":         ("calc", lambda df: _yoy_pct(df, "CPIAUCSL")),
        "CPI 코어 YoY":    ("calc", lambda df: _yoy_pct(df, "CPILFESL")),
        "PCE YoY":         ("calc", lambda df: _yoy_pct(df, "PCEPI")),
        "PCE 코어 YoY":    ("calc", lambda df: _yoy_pct(df, "PCEPILFE")),
        # 고용/경기
        "JOLTS":           ("raw", "JTSJOL"),
        "NFP":             ("raw", "PAYEMS"),
        "GDP 성장률":       ("raw", "GDP"),  # raw_df "GDP" 컬럼 = FRED A191RL1Q225SBEA = 실질 GDP 성장률 (연율화 %)
        "소비자신뢰":       ("raw", "UMCSENT"),
        "CFNAI MA3":       ("calc", lambda df: (
            df["CFNAI"].dropna().rolling(3).mean().dropna()
            if "CFNAI" in df.columns else pd.Series(dtype=float)
        )),
        # 밸류에이션/구조
        "Shiller CAPE":    ("obs", "cape"),  # multpl 외부 only
        "Trailing PE":     ("obs", "trailing_pe"),  # multpl 외부 only
        "배당수익률":       ("raw", "DIVIDEND_YIELD"),
        "BEI 5Y":          ("raw", "T5YIE"),
        # 버핏 지표 — raw.jsonl 에 명목 GDP 백필 없음 (raw_df "GDP" 는 실질 성장률 %).
        # obs 만 사용 (~11일치 한계). 향후 nominal GDP 백필 추가 시 calc 로 이동 가능.
        "버핏 지표":        ("obs", "buffett_ratio"),
        # 구조/기타
        "카드 연체율":      ("raw", "DRCCLACBS"),
        "국채/GDP":         ("raw", "GFDEGDQ188S"),
        "Gold":            ("raw", "GC=F"),
        "실업률 3M 변화":   ("calc", lambda df: _abs_chg(df, "UNRATE", 3)),
        # 섹터 로테이션 — 3M 상대수익률
        "XLE-SPY 3M":      ("calc", lambda df: (_pct_chg(df, "XLE", 63) - _pct_chg(df, "SPY", 63)).dropna()),
        "XLK-SPY 3M":      ("calc", lambda df: (_pct_chg(df, "XLK", 63) - _pct_chg(df, "SPY", 63)).dropna()),
        "섹터 로테이션":     ("calc", lambda df: (_pct_chg(df, "XLK", 63) - _pct_chg(df, "SPY", 63)).dropna()),
        # 추세/사이클 (역전 자체 시계열)
        "2Y10Y 역전 해소":  ("raw", "T10Y2Y"),
        "3M10Y 역전 해소":  ("raw", "T10Y3M"),
        # FF금리 위치 = FEDFUNDS 직근 10년(120개월) percentile
        "FF금리 위치":      ("calc", lambda df: (
            (df["FEDFUNDS"].dropna().rolling(120, min_periods=24).rank(pct=True).dropna() * 100)
            if "FEDFUNDS" in df.columns else pd.Series(dtype=float)
        )),
        # 인하 사이클: FEDFUNDS 직근 24개월 max 대비 차이 (bp). 음수 = 인하 진행
        "인하 사이클":      ("calc", lambda df: (
            ((df["FEDFUNDS"].dropna() - df["FEDFUNDS"].dropna().rolling(24, min_periods=6).max()) * 100).dropna()
            if "FEDFUNDS" in df.columns else pd.Series(dtype=float)
        )),
        "Forward EPS 추세": ("obs", "f6_eps_chg_30d"),  # forward EPS 외부 합성
        # 노동격차 + 공급충격 (V3.8.2 부터 calc)
        "노동격차 (구인-실업자)": ("calc", lambda df: (
            (df["JTSJOL"] - df["UNEMPLOY"]).dropna()
            if ("JTSJOL" in df.columns and "UNEMPLOY" in df.columns) else pd.Series(dtype=float)
        )),
        "공급충격 디커플링":      ("calc", lambda df: (
            ((df["DCOILWTICO"] / df["DCOILWTICO"].shift(64) - 1) * 100).dropna()
            if "DCOILWTICO" in df.columns else pd.Series(dtype=float)
        )),
    }

    # 미니 시계열용 시리즈 캐시 (raw_df + obs_df, lazy)
    _mc_raw_df = None; _mc_obs_df = None
    if show_minichart:
        try:
            _mc_raw_df = _hist_load_raw_df()
            _mc_obs_df = _hist_load_obs_df()
        except Exception:
            _mc_raw_df = None; _mc_obs_df = None

    def _spark_for(label, color):
        """label 에 해당하는 sparkline SVG 반환 (토글 OFF 또는 매핑 없음 또는 데이터 부족이면 빈 문자열).
        자동 fallback: 선택 기간 내 점이 5개 미만이면 전체 시계열로 확장 (월간/분기 시리즈 가시성 보장)."""
        if not show_minichart: return ""
        m = _CARD_SERIES_MAP.get(label)
        if m is None: return ""
        src, key = m
        try:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=minichart_days)
            if src == "raw" and _mc_raw_df is not None and key in _mc_raw_df.columns:
                full = _mc_raw_df[key].dropna()
                sub = full[full.index >= cutoff]
                # fallback: 점 5개 미만이면 전체 시계열 (월간/분기 데이터 가시성)
                if len(sub) < 5: sub = full
                vals = sub.values.tolist()
                dts  = list(sub.index)
            elif src == "obs" and _mc_obs_df is not None and key in _mc_obs_df.columns:
                sub_full = _mc_obs_df[["ts", key]].dropna()
                _ts = pd.to_datetime(sub_full["ts"])
                if hasattr(_ts.dt, "tz_localize") and getattr(_ts.dt, "tz", None) is not None:
                    _ts = _ts.dt.tz_localize(None)
                sub_full = sub_full.assign(ts=_ts).sort_values("ts")
                sub = sub_full[sub_full["ts"] >= cutoff]
                if len(sub) < 5: sub = sub_full
                vals = sub[key].values.tolist()
                dts  = list(sub["ts"])
            elif src == "calc" and _mc_raw_df is not None and callable(key):
                # V3.8.2: raw_df 에서 즉시 계산. obs 누적 대기 없음.
                full = key(_mc_raw_df)
                if full is None or len(full) == 0: return ""
                full = full.dropna()
                sub = full[full.index >= cutoff]
                if len(sub) < 5: sub = full
                vals = sub.values.tolist()
                dts  = list(sub.index)
            else:
                return ""
            return _spark_svg(vals, color=color, dates=dts)
        except Exception:
            return ""

    # ── 카드 캡처+미러링: dashboard에서 렌더한 icard 인자를 _CARD_RENDER에 저장,
    #    탭(채권/밸류/반도체/계절)에서 같은 키로 재렌더링.
    _CARD_RENDER = {}
    def _ic(label, value, status, color, detail="", trend=""):
        _CARD_RENDER[label] = (label, value, status, color, detail, trend)
        icard(label, value, status, color, detail, trend, mode, sparkline=_spark_for(label, color))
    def _re(key):
        args = _CARD_RENDER.get(key)
        if args is None:
            st.markdown(f"<div style='color:{C['muted']};font-size:var(--mac-fs-xs);padding:8px'>— ({key})</div>", unsafe_allow_html=True)
            return
        # 미러링 시에도 동일 sparkline 적용
        icard(*args, mode, sparkline=_spark_for(args[0], args[3]))
    def _mirror_grid(keys, ncols=4):
        cols = st.columns(ncols)
        for i, k in enumerate(keys):
            with cols[i % ncols]:
                _re(k)

    # ═══ TAB 0: 대시보드 ═══
    with tabs[0]:
        easy_help(mode, HELP_TAB0)

        # ── V3.6 공식 개편 적응기 배지 (VERSION_STARTED 부터 60일 이내만) ──
        try:
            _vstart_d = _date.fromisoformat(VERSION_STARTED)
            _days_elapsed = (_date.today() - _vstart_d).days
            if 0 <= _days_elapsed <= 60:
                _days_left = 60 - _days_elapsed
                st.markdown(
                    f"<div style='background:rgba(200,80,80,0.08);border:1px solid rgba(200,80,80,0.35);border-left:3px solid rgba(200,80,80,0.75);"
                    f"border-radius:8px;padding:8px 14px;margin-bottom:12px;font-size:var(--mac-fs-sm);color:{C['text']}'>"
                    f"<b>🔧 공식 개편 적응기</b> ({_days_left}일 남음) — "
                    f"3.6 시작일 {VERSION_STARTED}. Δ30D/ΔΔ 는 윈도우에 3.5 데이터 섞이는 동안 표시 제한. "
                    f"구 기록은 <span style='color:{C['muted']}'>시계열 탭에서 회색 점선으로 병기</span>된다."
                    f"</div>", unsafe_allow_html=True)
        except Exception:
            pass

        # ── 상단 1행 4열: 거시 + 미어캣 + 사계절 + 한줄평 ──
        s1, s2, s3, s4 = st.columns([1, 1, 1, 1])
        _mq_text, _mq_band = mq(ms)
        with s1:
            # V3.4 속도(Δ30D) 서브라인
            _vel_color = C.get(_vel_clr_key, C["muted"]) if _vel_clr_key else C["muted"]
            if _mac_velocity is not None:
                _vel_sub = f"Δ30D {_mac_velocity:+.1f} {_vel_lbl}"
                if _mac_dd:
                    _vel_sub += f" · ΔΔ {_mac_dd['delta_delta']:+.1f} {_mac_dd['delta_delta_label']}"
            else:
                _vel_sub = _vel_lbl  # "축적 중"
            st.markdown(sgauge(gs, bsl("거시 스코어", mode), info=_tip_mac(gs) if gs is not None else None, subline=_vel_sub, sub_color=_vel_color), unsafe_allow_html=True)
        with s2: st.markdown(sgauge(ms, bsl("미어캣 스코어", mode), quote=_mq_text, info=_tip_mk(ms) if ms is not None else None), unsafe_allow_html=True)
        with s3:
            _cd = C["card"]; _bd = C["border"]; _m = C["muted"]; _t = C["text"]
            _sb = (season_auto.lstrip("초늦") if season_auto else None) or "—"
            s_col = SC.get(_sb, C["gold"]) if _sb in SC else C["gold"]
            s_disp = season_label(season_auto, mode) if season_auto else "—"
            _si = _tip(_tip_season(season_auto)) if season_auto else ""
            # V3.4 역사 매칭 — 사계절 카드 하단에 era만 노출 (코멘트/인용은 아래 별도 카드)
            _era_h = ""
            if _hist_match.get("era"):
                _era_h = f"<div style='font-size:var(--mac-fs-sm);color:{s_col};margin-top:6px;font-weight:600'>≈ {_hist_match['era']}</div>"
            st.markdown(f"""<div style="background:{_cd};border:2px solid {s_col};border-radius:12px;padding:20px 24px;text-align:center">
                <div style="font-size:var(--mac-fs-sm);color:{_m}">사계절 자동 판정</div>
                <div style="font-size:var(--mac-fs-large);font-weight:700;color:{s_col};margin:8px 0">{s_disp}{_si}</div>
                <div style="font-size:var(--mac-fs-md);color:{_t}">확신도: {season_conf}</div>{_era_h}</div>""", unsafe_allow_html=True)
        with s4:
            interp = mx22(gs, ms); sqt, sband = sq(gs)
            scc = SC.get({"과열": "여름", "가을": "가을", "겨울": "겨울", "바닥": "겨울"}.get(sband, "가을"), C["muted"])
            st.markdown(f"""<div style="background:{_cd};border:1px solid {_bd};border-radius:12px;padding:20px 24px">
                <div style="font-size:var(--mac-fs-md);color:{_m}">2×2 매트릭스{_tip(_tip_mx(gs, ms))}</div>
                <div style="font-size:var(--mac-fs-md);color:{_t};margin:10px 0;line-height:1.5">{interp}</div>
                <div style="border-top:1px solid {_bd};padding-top:10px;margin-top:10px">
                <div style="font-size:var(--mac-fs-sm);color:{_m}">거시 한줄평</div>
                <div style="font-size:var(--mac-fs-md);color:{scc};font-style:italic;margin-top:6px">{sqt}</div></div></div>""", unsafe_allow_html=True)

        _fig_radar = None  # 아래에서 생성

        # ── 2×2 매트릭스 차트 + Δ 방향성 ──
        _mx_c1, _mx_c2 = st.columns([1, 1])
        with _mx_c1:
            if gs is not None and ms is not None:
                _fig_mx = go.Figure()
                _fig_mx.add_shape(type="rect", x0=50, y0=50, x1=100, y1=100, fillcolor="rgba(29,158,117,0.12)", line_width=0)
                _fig_mx.add_shape(type="rect", x0=50, y0=0, x1=100, y1=50, fillcolor="rgba(50,102,173,0.10)", line_width=0)
                _fig_mx.add_shape(type="rect", x0=0, y0=50, x1=50, y1=100, fillcolor="rgba(216,90,48,0.10)", line_width=0)
                _fig_mx.add_shape(type="rect", x0=0, y0=0, x1=50, y1=50, fillcolor="rgba(139,148,158,0.06)", line_width=0)
                _mx_colors = {"gg": C["green"], "gl": C["blue"], "lg": C["orange"], "ll": C["muted"]}
                _fig_mx.add_trace(go.Scatter(x=[gs], y=[ms], mode="markers+text",
                    marker=dict(size=14, color=_mx_colors.get(_mx_key, C["muted"]), symbol="circle",
                                line=dict(width=2, color="white")),
                    text=[f" {_mx_label}"], textposition="middle right",
                    textfont=dict(size=12), showlegend=False))
                _fig_mx.add_annotation(x=75, y=85, text="매직존", showarrow=False, font=dict(size=11, color="rgba(29,158,117,0.5)"))
                _fig_mx.add_annotation(x=75, y=15, text="대기존", showarrow=False, font=dict(size=11, color="rgba(50,102,173,0.5)"))
                _fig_mx.add_annotation(x=25, y=85, text="경계존", showarrow=False, font=dict(size=11, color="rgba(216,90,48,0.5)"))
                _fig_mx.add_annotation(x=25, y=15, text="평시존", showarrow=False, font=dict(size=11, color="rgba(139,148,158,0.4)"))
                _fig_mx.add_hline(y=50, line_dash="dot", line_color="rgba(128,128,128,0.3)")
                _fig_mx.add_vline(x=50, line_dash="dot", line_color="rgba(128,128,128,0.3)")
                _lyt = _ly("2×2 매트릭스", 280)
                _lyt["xaxis"] = dict(title="거시", range=[0, 100], gridcolor="rgba(128,128,128,0.15)")
                _lyt["yaxis"] = dict(title="미어캣", range=[0, 100], gridcolor="rgba(128,128,128,0.15)")
                _fig_mx.update_layout(**_lyt)
                st.plotly_chart(_fig_mx, use_container_width=True, key="chart_mx")
            else:
                st.markdown(f"<div style='background:{C['card']};border:1px solid {C['border']};border-radius:8px;padding:40px;text-align:center;color:{C['muted']}'>스코어 데이터 대기 중</div>", unsafe_allow_html=True)
        with _mx_c2:
            _DELTA_RIB = {"VIX": True, "HY_bp": True, "2Y10Y_bp": False, "DXY": True, "SOX_SPX": False, "KRW": True}
            _DELTA_NAMES = {"VIX": "VIX", "HY_bp": "HY", "2Y10Y_bp": "10Y-2Y", "DXY": "DXY", "SOX_SPX": "SOX/SPX", "KRW": "원/달러"}
            def _dcol(k, v):
                if v is None or v == 0: return C["muted"]
                bad_up = _DELTA_RIB.get(k, False)
                return (C["red"] if v > 0 else C["green"]) if bad_up else (C["green"] if v > 0 else C["red"])
            _dtrows = ""
            for k, d in _KEY_DELTAS.items():
                w = d.get("1W"); m = d.get("1M"); wa = d.get("1W_d", "→"); ma = d.get("1M_d", "→")
                wc = _dcol(k, w); mc = _dcol(k, m); nm = _DELTA_NAMES.get(k, k)
                _wt = f"{wa} {w}" if w is not None else "—"
                _mt = f"{ma} {m}" if m is not None else "—"
                _row_tip = _tip(_TD_ROW.get(k, "")) if _TD_ROW.get(k) else ""
                _dtrows += f"<tr style='border-bottom:1px solid {C['border']}'><td style='padding:4px 8px;color:{C['text']};font-size:var(--mac-fs-md)'>{nm}{_row_tip}</td><td style='padding:4px 8px;text-align:center;color:{wc};font-weight:600;font-size:var(--mac-fs-md)'>{_wt}</td><td style='padding:4px 8px;text-align:center;color:{mc};font-weight:600;font-size:var(--mac-fs-md)'>{_mt}</td></tr>"
            _tip_1w = _tip(_TIP_1W); _tip_1m = _tip(_TIP_1M)
            _dtable_h = f"<table style='width:100%;border-collapse:collapse'><tr style='border-bottom:1px solid {C['border']}'><th style='text-align:left;padding:4px 8px;color:{C['muted']};font-size:var(--mac-fs-xs)'>지표</th><th style='padding:4px 8px;color:{C['muted']};font-size:var(--mac-fs-xs)'>1W{_tip_1w}</th><th style='padding:4px 8px;color:{C['muted']};font-size:var(--mac-fs-xs)'>1M{_tip_1m}</th></tr>{_dtrows}</table>"
            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{C['muted']};margin-bottom:6px;font-weight:600'>Δ 주요 지표 방향성</div>{_dtable_h}", unsafe_allow_html=True)

        # ── 미어캣 상황판 7카드 ──
        st.markdown(f"<div style='height:12px'></div>", unsafe_allow_html=True)
        _m = C["muted"]; _cd = C["card"]; _bd = C["border"]; _t = C["text"]; _br = C["bright"]
        st.caption("📋 관찰용. 매매는 프로토콜 트리거가 결정한다.")
        def _mkcard(label, val, unit="", color=None):
            _cc = color or _t
            _spark_html = _spark_for(label, _cc)  # 토글 ON 시 자동 sparkline
            return f"""<div style="background:{_cd};border:1px solid {_bd};border-radius:8px;padding:10px 14px;text-align:center;min-height:70px">
                <div style="font-size:var(--mac-fs-sm);color:{_m}">{label}</div>
                <div style="font-size:var(--mac-fs-large);font-weight:700;color:{_cc};margin:4px 0">{val}</div>
                <div style="font-size:var(--mac-fs-xs);color:{_m}">{unit}</div>{_spark_html}</div>"""
        mk6 = st.columns(7)
        # QQQ DD (트리거 단계 표시)
        _qdd_v = f"{_mk_qqq_dd:.1f}%" if _mk_qqq_dd is not None else "—"
        _qdd_stg, _qdd_mult = _dd_stage(_mk_qqq_dd, _dd_caution, _dd_correction, _dd_crash)
        _qdd_c = C["red"] if _qdd_stg == "폭락장" else C["green"] if _qdd_stg == "조정장" else C["orange"] if _qdd_stg == "경계장" else _t
        _qdd_unit = f"{_qdd_stg} {_qdd_mult}" if _qdd_stg != "—" else "52주 고점 대비"
        with mk6[0]: st.markdown(_mkcard("QQQ DD", _qdd_v, _qdd_unit, _qdd_c), unsafe_allow_html=True)
        # SOXX DD (트리거 단계 표시)
        _sdd_v = f"{_mk_soxx_dd:.1f}%" if _mk_soxx_dd is not None else "—"
        _sdd_stg, _sdd_mult = _dd_stage(_mk_soxx_dd, _soxl_dd1, _soxl_dd2, _soxl_dd3)
        _sdd_c = C["red"] if _sdd_stg == "폭락장" else C["green"] if _sdd_stg == "조정장" else C["orange"] if _sdd_stg == "경계장" else _t
        _sdd_unit = f"{_sdd_stg} {_sdd_mult}" if _sdd_stg != "—" else "52주 고점 대비"
        with mk6[1]: st.markdown(_mkcard("SOXX DD", _sdd_v, _sdd_unit, _sdd_c), unsafe_allow_html=True)
        # VIX
        _vix_v = f"{vix:.1f}" if vix is not None else "—"
        _vix_c = C["green"] if (vix is not None and vix >= 35) else C["orange"] if (vix is not None and vix >= 25) else _t
        with mk6[2]: st.markdown(_mkcard("VIX", _vix_v, "", _vix_c), unsafe_allow_html=True)
        # F&G — 툴팁과 동일 밴드: <25 green, 25~75 gold, >75 red
        _fg_v = f"{fgs:.0f}" if fgs is not None else "—"
        _fg_c = C["green"] if (fgs is not None and fgs < 25) else C["red"] if (fgs is not None and fgs > 75) else C["gold"] if fgs is not None else _m
        _fg_unit = "CNN" if fg.get("source") == "CNN" else ""
        with mk6[3]: st.markdown(_mkcard("Fear & Greed", _fg_v, _fg_unit, _fg_c), unsafe_allow_html=True)
        # 현금비중
        _cash_v = f"{_mk_cash*100:.1f}%" if _mk_cash is not None else "—"
        with mk6[4]: st.markdown(_mkcard("현금비중", _cash_v, "SGOV/총자산", C["blue"] if _mk_cash is not None else _m), unsafe_allow_html=True)
        # TQQQ Ratio
        _rat_v = f"{_mk_ratio:.2f}" if _mk_ratio is not None else "—"
        _rat_c = C["green"] if (_mk_ratio is not None and _mk_ratio < 2.0) else _t
        with mk6[5]: st.markdown(_mkcard("TQQQ R", _rat_v, "평가/투입", _rat_c), unsafe_allow_html=True)
        # SOXL Ratio
        _sr_v = f"{_mk_soxl_ratio:.2f}" if _mk_soxl_ratio is not None else "—"
        _sr_c = C["green"] if (_mk_soxl_ratio is not None and _mk_soxl_ratio < 2.0) else _t
        with mk6[6]: st.markdown(_mkcard("SOXL R", _sr_v, "평가/투입", _sr_c), unsafe_allow_html=True)

        st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

        # ── 거시 뷰: 5클러스터 레이더 ──
        _clusters = mac_clusters(gs_detail)
        if _clusters and any(v["score"] is not None for v in _clusters.values()):
            _rc1, _rc2 = st.columns([1, 2])
            with _rc1:
                _cl_rows = ""; _tc = C["text"]
                for cn, cv in _clusters.items():
                    _cvs = cv["score"]
                    _cv_s = f"{_cvs:.0f}" if _cvs is not None else "—"
                    _cv_c = C["green"] if (_cvs is not None and _cvs >= 60) else C["orange"] if (_cvs is not None and _cvs >= 40) else C["red"] if _cvs is not None else C["muted"]
                    _cn_tip = _tip(_TIP_CLUSTER_MAP[cn]) if cn in _TIP_CLUSTER_MAP else ""
                    # ΔΔ 서브라인
                    _cdd = _cl_dd.get(cn)
                    _cdd_sub = ""
                    if _cdd:
                        _cdd_sub = (f"<div style='font-size:var(--mac-fs-xs);color:{_m};padding-left:4px'>"
                                    f"Δ {_cdd['delta']:+.1f} {_cdd['delta_label']} · "
                                    f"ΔΔ {_cdd['delta_delta']:+.1f} {_cdd['delta_delta_label']}</div>")
                    _cl_rows += (f"<div style='padding:4px 0'>"
                                 f"<div style='display:flex;justify-content:space-between;font-size:var(--mac-fs-md)'>"
                                 f"<span style='color:{_tc}'>{cn}{_cn_tip}</span>"
                                 f"<span style='color:{_cv_c};font-weight:700'>{_cv_s}</span></div>"
                                 f"{_cdd_sub}</div>")
                _title_h = f"<div style='color:{_br};border-left:4px solid {C['green']};padding-left:12px;font-size:var(--mac-fs-h3);font-weight:700;margin-bottom:16px'>{bsl('미어캣의 관측소',mode)}{_tip(_TIP_MAC_EYE)}</div>"
                _div_h = ""
                if _cl_divergence is not None:
                    _div_c = C["orange"] if _cl_divergence >= 40 else C["gold"] if _cl_divergence >= 25 else C["muted"]
                    _div_h = (f"<div style='margin-top:12px;padding-top:10px;border-top:1px solid {C['border']};display:flex;justify-content:space-between;align-items:center;font-size:var(--mac-fs-sm)'>"
                              f"<span style='color:{C['muted']}'>클러스터 괴리도{_tip(_TIP_DIVERGENCE)}</span>"
                              f"<span style='color:{_div_c};font-weight:700;font-size:var(--mac-fs-h3)'>{_cl_divergence:.1f}</span></div>")
                    # V3.4 디커플링 자동 코멘트 — 좌측, max vs min 우측, 같은 라인에서 시작
                    _decouple_txt = cluster_decouple_comment(_clusters, _cl_divergence, mode)
                    _decouple_html = _decouple_txt.replace("\n", "<br>") if _decouple_txt else ""
                    _maxmin_html = ""
                    if _cl_max_name and _cl_min_name:
                        _maxmin_html = (f"<span style='color:{C['muted']};font-size:var(--mac-fs-xs);white-space:nowrap;"
                                        f"margin-left:12px;align-self:flex-end'>"
                                        f"{_cl_max_name} {max(_cl_scores_list):.0f} vs {_cl_min_name} {min(_cl_scores_list):.0f}</span>")
                    if _decouple_html or _maxmin_html:
                        _div_h += (f"<div style='display:flex;justify-content:space-between;align-items:flex-end;margin-top:8px;gap:12px'>"
                                   f"<div style='color:{_t};font-size:var(--mac-fs-md);line-height:1.5;text-align:left;flex:1'>{_decouple_html}</div>"
                                   f"{_maxmin_html}</div>")
                st.markdown(f"<div style='display:flex;flex-direction:column;justify-content:center;min-height:280px'>{_title_h}{_cl_rows}{_div_h}</div>", unsafe_allow_html=True)
            with _rc2:
                _cl_names = [cn for cn in _clusters]; _cl_vals = [_clusters[cn]["score"] or 0 for cn in _cl_names]
                _fig_radar = go.Figure(go.Scatterpolar(r=_cl_vals + [_cl_vals[0]], theta=_cl_names + [_cl_names[0]],
                    fill='toself', fillcolor='rgba(76,175,80,0.15)', line=dict(color=C["green"], width=2),
                    marker=dict(size=6, color=C["green"])))
                _fig_radar.update_layout(**_ly("", 280), polar=dict(
                    bgcolor="rgba(0,0,0,0)", radialaxis=dict(visible=True, range=[0, 100], showticklabels=False, gridcolor="rgba(128,128,128,0.2)"),
                    angularaxis=dict(gridcolor="rgba(128,128,128,0.2)", color=C["text"])))
                st.plotly_chart(_fig_radar, use_container_width=True, key="chart_radar")

        # ── V3.4 핵심 지표 5장 (심안 OFF에서도 항상 표시) ──
        st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_m};margin:14px 0 6px;font-weight:600'>핵심 지표</div>", unsafe_allow_html=True)
        kr = st.columns(5)
        def _raw_dd_trend(key, base_trend):
            """원본 지표 ΔΔ를 기존 trend 바 뒤에 추가."""
            _rd = _raw_dd.get(key)
            if _rd:
                return base_trend + (f"<div style='font-size:var(--mac-fs-xs);color:{_m};margin-top:2px'>"
                    f"ΔΔ {_rd['delta_delta']:+.2f} {_rd['label_raw']}</div>")
            return base_trend
        with kr[0]:
            _v = f"{vix:.1f}" if vix is not None else "—"
            s, c, d = j_vix(vix)
            _ic("VIX", _v, s, c, d, _raw_dd_trend("VIX", tbar(ctrends(fd.get("VIXCLS"), mode="abs"), "VIX")))
        with kr[1]:
            fgd_parts = []
            if fgs:
                if fgs < 20: fgd_parts.append("Extreme Fear. 욕심으로 사지 말고 두려움으로 사라.")
                elif fgs > 80: fgd_parts.append("사람들이 확신에 차면 그 방향의 가격은 이미 과대평가된 거다.")
            fgd_parts.append("출처: CNN" if fg.get("source") == "CNN" else "출처: 없음 (CNN 호출 실패)")
            fgd = " · ".join(fgd_parts)
            _v = f"{fgs:.0f}" if fgs is not None else "—"
            _r = fg.get("rating", "—") or "—"
            _ic("Fear & Greed", _v, _r,
                C["green"] if (fgs and fgs < 25) else C["red"] if (fgs and fgs > 75) else C["gold"] if fgs is not None else C["muted"], fgd, "")
        with kr[2]:
            _v = f"{t10y2y*100:.0f}bp" if t10y2y is not None else "—"
            s, c, d = j_sp(t10y2y * 100 if t10y2y is not None else None)
            _ic("10Y-2Y 스프레드", _v, s, c, d, _raw_dd_trend("T10Y2Y", tbar(ctrends(fd.get("T10Y2Y"), mode="abs_bp"), "10Y-2Y")))
        with kr[3]:
            wd = ""; wlbl = "—"; wclr = C["muted"]
            if wti is not None:
                if wti >= 100: wlbl, wclr, wd = "위험", C["red"], "유가→인플레→연준→시장. 전부 연결되어 있다."
                elif wti >= 80: wlbl, wclr, wd = "부담", C["gold"], "유가가 오르고 있다. 인플레 재점화 리스크."
                elif wti >= 50: wlbl, wclr, wd = "정상", C["green"], "유가가 조용하다. 조용할 때 다른 걸 봐라. 유가는 움직이기 시작하면 빠르다."
                else: wlbl, wclr, wd = "수요 부진", C["red"], "수요가 죽었다. 디플레 압력. 건강한 신체에선 비만이 걱정일테지만 죽을 병 걸리면 살은 저절로 빠진다."
            _v = f"${wti:.1f}" if wti is not None else "—"
            _ic("WTI 유가", _v, wlbl, wclr, wd, tbar(ctrends(fd.get("WTI"), mode="pct"), "WTI"))
        with kr[4]:
            kd = ""; klbl = "—"; kclr = C["muted"]
            if krw is not None:
                if krw >= 1450: klbl, kclr, kd = "약세 극단", C["red"], "비정상이다. 전쟁이든 위기든 원인은 매번 다르지만 이 수준은 오래 안 간다."
                elif krw >= 1300: klbl, kclr, kd = "약세", C["gold"], "달러가 비싸다. 환전은 불리한 구간. 급하지 않으면 기다려라."
                elif krw >= 1100: klbl, kclr, kd = "정상", C["green"], "정상 범위다. 환전할 거면 여기서 해라."
                else: klbl, kclr, kd = "강세", C["green"], "원화가 강하다. 달러가 싸다. 매집 재원 환전 최적 구간."
                dc = chg(yd.get("DXY"), 126); kc = chg(fd.get("KRW"), 126)
                if dc is not None and kc is not None:
                    if dc < -3 and kc < -3: kd = "DXY↓ + 원/달러↓ = 전쟁 프리미엄 해소. 달러를 살 시기가 온다."
                    elif dc < -3 and kc > 0: kd = "DXY↓ + 원/달러↑ = 한국 고유 리스크. 원화가 혼자 약하다."
            _v = f"₩{krw:,.0f}" if krw is not None else "—"
            _ic("원/달러", _v, klbl, kclr, kd, tbar(ctrends(fd.get("KRW"), mode="pct"), "KRW"))

        # ── V3.4 가속도 모니터 (심안 OFF에서도 항상 표시) — 역사 매칭은 계절 탭으로 이동 ──
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        ar = st.columns([1, 2])
        with ar[0]:
            # F5 가속도 모니터 — 시장 5종 (VIX/HY는 역발상)
            _flip_v34 = {"↑↑":"↓↓", "↑":"↓", "→":"→", "↓":"↑", "↓↓":"↑↑"}
            def _ac_inv_v34(ac):
                # '*' 클립 마커 보존하면서 core 기호만 flip
                if ac is None: return None
                lbl = ac["label"]
                star = "*" if lbl.endswith("*") else ""
                core = lbl[:-1] if star else lbl
                return {**ac, "label": _flip_v34.get(core, core) + star}
            _ac_items_v34 = [
                ("VIX",     _ac_inv_v34(_ac_vix)),
                ("HY",      _ac_inv_v34(_ac_hy)),
                ("2Y10Y",   _ac_t2y10y),
                ("DXY",     _ac_dxy),
                ("SOX/SPX", _ac_soxspx),
            ]
            _ac_lines_v34 = []
            _accel_cnt_v34 = 0; _decel_cnt_v34 = 0
            for _nm_a, _ac in _ac_items_v34:
                _tt = ""  # 클립 시 hover tooltip
                if _ac is None: _lab = "—"
                else:
                    _lab = _ac["label"]
                    if "↑" in _lab: _accel_cnt_v34 += 1
                    elif "↓" in _lab: _decel_cnt_v34 += 1
                    if _ac.get("clipped") and _ac.get("raw_ratio") is not None:
                        _tt = f' title="분모 불안정 (raw ratio: {_ac["raw_ratio"]:.1f}, 표시값 ±10으로 클립)"'
                _ac_lines_v34.append(f"<div style='font-size:var(--mac-fs-xs);color:{_t};display:flex;justify-content:space-between'><span>{_nm_a}</span><span{_tt} style='font-weight:600'>{_lab}</span></div>")
            _ac_html_v34 = "".join(_ac_lines_v34)
            if _accel_cnt_v34 >= 3:   _f5_lbl, _f5_clr = "가속 우세", C["green"]
            elif _decel_cnt_v34 >= 3: _f5_lbl, _f5_clr = "감속 우세", C["red"]
            else:                     _f5_lbl, _f5_clr = "혼재", C["gold"]
            _f5_tip = _CARD_RANGES.get("가속도 모니터", "")
            _f5_tip_html = f'<div style="position:absolute;top:6px;right:8px;font-size:var(--mac-fs-sm);line-height:1"><span class="gtp gtp-r" tabindex="0">ⓘ<span class="gtxt">{_f5_tip}</span></span></div>' if _f5_tip else ""
            _cd = C["card"]; _bd = C["border"]
            _ac_card = f"""<div class="maccard" tabindex="0" style="background:{_cd};border:1px solid {_bd};border-radius:8px;
                padding:12px 16px;border-left:3px solid {_f5_clr};min-height:120px;position:relative">
                {_f5_tip_html}<div style="font-size:var(--mac-fs-sm);color:{_m};margin-bottom:4px;padding-right:18px">{bsl("가속도 모니터", mode)}</div>
                {_ac_html_v34}
                <div style="font-size:var(--mac-fs-md);color:{_f5_clr};font-weight:600;margin-top:6px;border-top:1px solid {_bd};padding-top:4px">{_f5_lbl}</div>
                <div style="font-size:var(--mac-fs-sm);color:{_t};margin-top:6px;line-height:1.4;border-top:1px solid {_bd};padding-top:6px">주가는 위치가 아닌 속도에 반응한다. 속도의 속도가 바뀌는 지점이 진짜 전환점이다.</div></div>"""
            st.markdown(_ac_card, unsafe_allow_html=True)
            _CARDS["F5_가속도_모니터"] = _card(
                {n: ({
                        "label":     a["label"],
                        "ratio":     a.get("ratio"),
                        "raw_ratio": a.get("raw_ratio"),
                        "clipped":   a.get("clipped", False),
                    } if a else {"label": "—", "ratio": None, "raw_ratio": None, "clipped": False})
                 for n, a in _ac_items_v34},
                _f5_lbl, _f5_clr,
                "주가는 위치가 아닌 속도에 반응한다. 속도의 속도가 바뀌는 지점이 진짜 전환점이다."
            )
        with ar[1]:
            # ── §2c ΔΔ 매트릭스 카드 ──
            _dd_tip = _TIP_DD_MATRIX
            _dd_tip_html = f' <span class="gtp" tabindex="0">ⓘ<span class="gtxt">{_dd_tip}</span></span>' if _dd_tip else ""
            _dd_cl_names = ["채권/금리", "밸류에이션", "스트레스", "실물", "반도체"]
            _dd_rows_html = ""
            _n_accel = 0; _n_decel = 0; _n_angst = 0
            for _dcn in _dd_cl_names:
                _dcs = export_mac_clusters.get(_dcn, {}).get("score") if export_mac_clusters else None
                _dcd = _cl_dd.get(_dcn)
                _s_val = f"{_dcs:.0f}" if _dcs is not None else "—"
                if _dcd:
                    _s_d = f"{_dcd['delta']:+.1f}"
                    _s_dd = f"{_dcd['delta_delta']:+.1f}"
                    _d_lbl = _dcd["delta_label"].split(" ")[-1]  # ↑↑/↑/↓/↓↓
                    _dd_lbl = _dcd["delta_delta_label"]
                    _dd_val = _dcd["delta_delta"]
                    _d_val = _dcd["delta"]
                    if _dd_val > _DD_STEADY: _n_accel += 1
                    if _dd_val < -_DD_STEADY: _n_decel += 1
                    if _d_val > 0 and _dd_val < -_DD_STEADY: _n_angst += 1
                    _state = f"{_d_lbl} {_dd_lbl}"
                else:
                    _s_d = "—"; _s_dd = "—"; _state = "축적 중"
                _dd_rows_html += (f"<tr style='border-bottom:1px solid {C['border']}'>"
                    f"<td style='padding:3px 6px;color:{_t};font-size:var(--mac-fs-xs)'>{_dcn}</td>"
                    f"<td style='padding:3px 6px;text-align:right;color:{_br};font-weight:600;font-size:var(--mac-fs-xs)'>{_s_val}</td>"
                    f"<td style='padding:3px 6px;text-align:right;color:{_t};font-size:var(--mac-fs-xs)'>{_s_d}</td>"
                    f"<td style='padding:3px 6px;text-align:right;color:{_t};font-size:var(--mac-fs-xs)'>{_s_dd}</td>"
                    f"<td style='padding:3px 6px;color:{_m};font-size:var(--mac-fs-xs)'>{_state}</td></tr>")
            # 거시 종합 행
            _gs_val = f"{gs:.0f}" if gs is not None else "—"
            if _mac_dd:
                _gs_d = f"{_mac_dd['delta']:+.1f}"
                _gs_dd = f"{_mac_dd['delta_delta']:+.1f}"
                _gs_state = f"{_mac_dd['delta_label'].split(' ')[-1]} {_mac_dd['delta_delta_label']}"
            else:
                _gs_d = "—"; _gs_dd = "—"; _gs_state = "축적 중"
            _dd_rows_html += (f"<tr style='border-top:2px solid {C['border']}'>"
                f"<td style='padding:3px 6px;color:{_br};font-weight:700;font-size:var(--mac-fs-xs)'>거시 종합</td>"
                f"<td style='padding:3px 6px;text-align:right;color:{_br};font-weight:700;font-size:var(--mac-fs-xs)'>{_gs_val}</td>"
                f"<td style='padding:3px 6px;text-align:right;color:{_br};font-size:var(--mac-fs-xs)'>{_gs_d}</td>"
                f"<td style='padding:3px 6px;text-align:right;color:{_br};font-size:var(--mac-fs-xs)'>{_gs_dd}</td>"
                f"<td style='padding:3px 6px;color:{_br};font-size:var(--mac-fs-xs)'>{_gs_state}</td></tr>")
            # §2d 매트릭스 해석 코멘트
            _any_dd = any(_cl_dd.get(cn) for cn in _dd_cl_names)
            if _any_dd:
                if _n_accel == 5:                          _dd_cmt_key = "all_accel"
                elif _n_decel == 5:                        _dd_cmt_key = "all_decel"
                elif _n_angst >= 3:                        _dd_cmt_key = "angstblute"
                elif _n_accel >= 3 and _n_decel == 0:      _dd_cmt_key = "broad_accel"
                elif _n_decel >= 3 and _n_accel == 0:      _dd_cmt_key = "broad_decel"
                else:                                      _dd_cmt_key = "mixed"
            else:
                _dd_cmt_key = "mixed"
            _dd_cmt = _DD_MATRIX_COMMENT.get(_dd_cmt_key, "")
            _dd_cmt_html = f"<div style='margin-top:8px;font-size:var(--mac-fs-xs);color:{_t};line-height:1.5'>{_dd_cmt}</div>" if _dd_cmt else ""
            # Angstblüte 경고 배너
            _angst_html = ""
            if _n_angst >= 3:
                _angst_html = (f"<div style='margin-top:6px;padding:6px 10px;background:rgba(255,152,0,0.15);border-left:3px solid {C['orange']};"
                               f"border-radius:4px;font-size:var(--mac-fs-xs);color:{C['orange']}'>"
                               f"⚠ Angstblüte — Δ↑ + ΔΔ↘ 클러스터 {_n_angst}개. 겉은 개선이지만 속도가 꺾이고 있다.</div>")
            # 색상 결정
            if _n_angst >= 3: _dd_border_clr = C["orange"]
            elif _n_accel >= 3: _dd_border_clr = C["green"]
            elif _n_decel >= 3: _dd_border_clr = C["red"]
            else: _dd_border_clr = C["gold"]
            _dd_matrix_card = f"""<div class="maccard" tabindex="0" style="background:{_cd};border:1px solid {_bd};border-radius:8px;
                padding:12px 16px;border-left:3px solid {_dd_border_clr};min-height:120px;position:relative">
                <div style="font-size:var(--mac-fs-sm);color:{_m};margin-bottom:8px">{bsl("2차 도함수 매트릭스", mode)}{_dd_tip_html}</div>
                <table style="width:100%;border-collapse:collapse">
                <tr style="border-bottom:1px solid {C['border']}">
                    <th style="text-align:left;padding:3px 6px;color:{_m};font-size:var(--mac-fs-xs)">클러스터</th>
                    <th style="text-align:right;padding:3px 6px;color:{_m};font-size:var(--mac-fs-xs)">점수</th>
                    <th style="text-align:right;padding:3px 6px;color:{_m};font-size:var(--mac-fs-xs)">Δ30D</th>
                    <th style="text-align:right;padding:3px 6px;color:{_m};font-size:var(--mac-fs-xs)">ΔΔ</th>
                    <th style="padding:3px 6px;color:{_m};font-size:var(--mac-fs-xs)">상태</th>
                </tr>{_dd_rows_html}</table>{_angst_html}{_dd_cmt_html}</div>"""
            st.markdown(_dd_matrix_card, unsafe_allow_html=True)
            _CARDS["DD_매트릭스"] = _card(
                {"n_accel": _n_accel, "n_decel": _n_decel, "n_angst": _n_angst, "key": _dd_cmt_key},
                _dd_cmt_key, _dd_border_clr,
                f"가속 {_n_accel} / 감속 {_n_decel} / Angst {_n_angst}")
        # ── 심안 OFF에서는 대시보드 핵심(상단 4카드 + 미어캣 7카드 + 5클러스터/레이더 + Δ방향성)까지만.
        #    + V3.4: 핵심 지표 5장, 가속도 모니터가 항상 표시. (역사 매칭은 계절 탭으로 이동)
        #    심안 ON에서 추가되는 것: Forward PE/DXY/FF/HY/SOX-SPX/실업률 + 채권/밸류/반도체/인플레/고용 심화 + 추세/사이클(F1·F3) + 사이클/반사성(F2·F6).
        #    스코어 계산은 deep과 무관 (mac_sc/미어캣/클러스터 모두 base 데이터로 항상 계산됨).
        if deep:
            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_m};margin:10px 0;font-weight:600'>시장 온도 (보강)</div>", unsafe_allow_html=True)
            r1 = st.columns(4)
            with r1[0]:
                ps = "—"; pc = C["muted"]; pdesc = ""
                if fpe is not None:
                    if fpe >= 22: ps, pc, pdesc = "극단", C["red"], "이 수준에서 숏 잡아 실패한 적 없다. 140년간."
                    elif fpe >= 18: ps, pc, pdesc = "정상", C["green"], "정상 범위다. 비싸지도 싸지도 않다. 이 구간에서는 밸류에이션이 매매 근거가 안 된다."
                    else: ps, pc, pdesc = "정상", C["green"], "싸다. 역사적으로 이 밑에서 사면 거의 다 이겼다. 실적이 무너져서 싼 건지 공포로 싼 건지만 구분해라."
                _v = f"{fpe:.1f}" if fpe is not None else "—"
                pdesc = (pdesc + " · " if pdesc else "") + f"출처: {_val_src_fwd}"
                _ic("Forward PE", _v, ps, pc, pdesc, "")
            with r1[1]:
                _v = f"{dxy:.1f}" if dxy is not None else "—"
                s, c, d = j_dxy(dxy)
                _ic("DXY", _v, s, c, d, tbar(ctrends(yd.get("DXY"), mode="pct"), "DXY"))
            with r1[2]:
                fd_d = ""; flbl = "—"; fclr = C["muted"]
                if ff is not None:
                    if ff >= 5: flbl, fclr, fd_d = "긴축", C["red"], "시스템이 위협받으면 연준은 반드시 돌아선다. 볼커 빼고 전부 그랬다."
                    elif ff >= 3: flbl, fclr, fd_d = "제한적", C["gold"], "끝이 보이는 긴축은 더 이상 주가를 끌어내리지 못한다."
                    elif ff >= 1: flbl, fclr, fd_d = "중립", C["gold"], "긴축도 완화도 아니다. 다른 지표를 봐라."
                    else: flbl, fclr, fd_d = "완화", C["green"], "제로금리 근처다. 돈이 풀리고 있다. 역실적장세에선 매수가 정석이다."
                _v = f"{ff:.2f}%" if ff is not None else "—"
                _ic("연방기금금리", _v, flbl, fclr, fd_d, tbar(ctrends(fd.get("FEDFUNDS"), mode="abs"), "FEDFUNDS"))
            with r1[3]:
                _v = f"{hy*100:.0f}bp" if hy is not None else "—"
                s, c, d = j_hy(hy)
                _ic("하이일드 스프레드", _v, s, c, d, _raw_dd_trend("HY", tbar(ctrends(fd.get("HY"), mode="abs_bp"), "HY")))

            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_m};margin:10px 0;font-weight:600'>시장 / 실물 (보강)</div>", unsafe_allow_html=True)
            r3 = st.columns(4)
            with r3[0]:
                sd = ""
                if sox_rel3 is not None:
                    if sox_rel3 > 0: sd = "반도체가 시장을 이기고 있다. 봄의 선행 신호."
                    else: sd = "반도체가 시장에 지고 있다. 겨울의 선행 신호."
                _v = f"{sox_spx:.3f}" if sox_spx is not None else "—"
                _ic("SOX/SPX", _v,
                    "아웃퍼폼" if (sox_rel3 and sox_rel3 > 0) else "언더퍼폼" if sox_rel3 is not None else "—",
                    C["green"] if (sox_rel3 and sox_rel3 > 0) else C["gold"] if sox_rel3 is not None else C["muted"], sd, "")
            with r3[1]:
                ud = ""; ulbl = "—"; uclr = C["muted"]
                if unemp is not None:
                    if unemp >= 5: ulbl, uclr, ud = "경고", C["red"], "실업률이 올라가고 있다. 여기서부터 사기 시작하면 된다."
                    elif unemp >= 4: ulbl, uclr, ud = "주의", C["gold"], "아직 견디고 있다. 근데 JOLTS를 봐라. 선행지표가 먼저 꺾인다."
                    else: ulbl, uclr, ud = "안정", C["green"], "고용이 강하다. 연준이 금리 내릴 명분이 없다."
                _v = f"{unemp:.1f}%" if unemp is not None else "—"
                _ic("실업률", _v, ulbl, uclr, ud, _raw_dd_trend("UNEMP", tbar(ctrends(fd.get("UNRATE"), mode="abs"), "UNRATE")))
            # V3.8.2 공급충격 디커플링 (deep 블록 전 사전 계산된 _shock_* 변수 사용)
            if _shock_state is not None:
                with r3[2]:
                    _ic("공급충격 디커플링", _shock_val, _shock_state, _shock_color,
                        f"{_shock_cmt} ({_box_tag})", "")

            st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
            st.markdown(f"<h3 style='color:{_br};border-left:4px solid {C['purple']};padding-left:12px;font-size:var(--mac-fs-h3)'>{bsl('심안',mode)}</h3>", unsafe_allow_html=True)
            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_m};margin:10px 0;font-weight:600'>채권/금리 심화</div>", unsafe_allow_html=True)
            d1 = st.columns(4)
            with d1[0]:
                _v = f"{t10y3m*100:.0f}bp" if t10y3m is not None else "—"
                s2, c2, d2 = j_sp(t10y3m * 100 if t10y3m is not None else None)
                _ic("10Y-3M 스프레드", _v, s2, c2, d2, tbar(ctrends(fd.get("T10Y3M"), mode="abs_bp"), "10Y-3M"))
            with d1[1]:
                td = ""
                if t3m2y is not None:
                    if t3m2y > 0: td = "연준이 너무 올렸다. 시장이 그렇게 말하고 있다."
                    elif t3m2y > -0.5: td = "역전 직전이거나 막 풀린 상태다. 긴축 오버슈팅 경계 구간."
                    else: td = "정상이다. 단기 금리가 초단기보다 충분히 높다. 긴축이 과하지 않다."
                _v = f"{t3m2y*100:.0f}bp" if t3m2y is not None else "—"
                _ic("2Y-3M 스프레드", _v,
                      "역전" if (t3m2y and t3m2y > 0) else "정상", C["red"] if (t3m2y and t3m2y > 0) else C["green"], td, "")
            with d1[2]:
                rd = ""
                if rr is not None:
                    if rr >= 2: rd = "파월이 보는 긴축 강도가 높다. 경기를 죄고 있다."
                    elif rr >= 1: rd = "긴축적이지만 극단은 아니다. 주식이 버틸 수 있는 구간."
                    elif rr > 0: rd = "중립 근처다. 연준이 경기를 억누르지도 밀어주지도 않는 상태."
                    else: rd = "실질금리 마이너스. 돈이 풀리는 토양이다."
                _v = f"{rr:.2f}%" if rr is not None else "—"
                _ic("실질금리", _v,
                      _rr_s, _rr_c, rd, "")
            with d1[3]:
                _ffd = ""
                if ff6m_chg is not None:
                    if ff6m_chg < -0.5: _ffd = "금리 인하 진행 중. 유동성이 풀리고 있다."
                    elif ff6m_chg < -0.05: _ffd = "금리가 살짝 내려왔다. 방향 전환의 초입일 수 있다."
                    elif ff6m_chg < 0.05: _ffd = "연준이 멈춰 있다. 데이터를 보고 있다는 뜻이다."
                    elif ff6m_chg <= 0.5: _ffd = "아직 올리고 있다. 긴축 사이클이 끝나지 않았다."
                    else: _ffd = "금리 인상 진행 중. 유동성 긴축."
                _v = f"{ff6m_chg:+.2f}%p" if ff6m_chg is not None else "—"
                _ic("FF 6M 변화", _v,
                      "완화" if (ff6m_chg and ff6m_chg < 0) else "긴축" if ff6m_chg else "—",
                      C["green"] if (ff6m_chg and ff6m_chg < 0) else C["red"] if ff6m_chg else C["muted"], _ffd, "")

            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{C['muted']};margin:10px 0;font-weight:600'>인플레 4종</div>", unsafe_allow_html=True)
            d2 = st.columns(4)
            _INFL_T = {
                "CPIAUCSL": ["인플레가 안 꺾인다. 연준이 못 내린다.",
                             "인플레가 끈적하다. 체감 물가가 안 내려오는 구간이다.",
                             "방향은 맞다. 속도가 느릴 뿐이다.",
                             "정상 궤도 근처다. 연준이 다른 데 눈 돌릴 여유가 생긴다.",
                             "인플레가 꺾이고 있다. 봄이 가까워진다."],
                "CPILFESL": ["인플레가 안 꺾인다. 연준이 못 내린다.",
                             "코어가 3% 위에 붙어 있으면 연준은 비둘기가 못 된다.",
                             "코어가 내려오고 있다. PCE 코어와 같이 봐라. 둘 다 3% 밑이면 인하 조건 충족.",
                             "코어 인플레 정상화. 이 상태가 유지되면 긴축 종료를 선언할 수 있다.",
                             "인플레가 꺾이고 있다. 봄이 가까워진다."],
                "PCEPI":    ["인플레가 안 꺾인다. 연준이 못 내린다.",
                             "끈적하다. 연준이 원하는 속도가 아니다. 인하 기대를 접어라.",
                             "내려오고 있긴 한데 아직 목표(2%) 위다. 연준이 참을성을 시험받는 구간.",
                             "거의 다 왔다. 이 구간이면 연준이 움직일 명분이 생긴다.",
                             "인플레가 꺾이고 있다. 봄이 가까워진다."],
                "PCEPILFE": ["인플레가 안 꺾인다. 연준이 못 내린다.",
                             "코어가 안 내려온다. 헤드라인이 내려와도 의미 없다. 연준은 코어를 본다.",
                             "코어가 3% 밑이면 연준이 숨통이 트인다. 근데 2%까지는 아직 멀다.",
                             "연준의 승리가 보이기 시작한다. 인하 사이클의 전제 조건이 갖춰지는 중.",
                             "인플레가 꺾이고 있다. 봄이 가까워진다."],
            }
            for i, (lbl, val, fk, rk) in enumerate([
                ("CPI YoY", cpi_y, "CPIAUCSL", "CPI"),
                ("CPI 코어 YoY", cpic_y, "CPILFESL", "CPI"),
                ("PCE YoY", pce_y, "PCEPI", "PCE"),
                ("PCE 코어 YoY", pcec_y, "PCEPILFE", "PCE")]):
                with d2[i]:
                    dd = ""
                    if val is not None:
                        _b = _INFL_T[fk]
                        if val > 4: dd = _b[0]
                        elif val > 3: dd = _b[1]
                        elif val > 2.5: dd = _b[2]
                        elif val > 2: dd = _b[3]
                        else: dd = _b[4]
                    _v = f"{val:.1f}%" if val is not None else "—"
                    _yoy_s = yoy_s(fd.get(fk))
                    _tr = tbar(ctrends(_yoy_s, mode="abs_pp", P=PM), rk)
                    # 3M YoY 변화량으로 추세 방향 판정
                    _3m_chg = None
                    if _yoy_s is not None and len(_yoy_s) > 3:
                        _3m_chg = float(_yoy_s.iloc[-1]) - float(_yoy_s.iloc[-4])
                    _ilbl = "—"; _iclr = C["muted"]
                    if val is not None and _3m_chg is not None:
                        _up = _3m_chg > 0
                        if _up and val > 4:        _ilbl, _iclr = "↑ 과열", C["red"]
                        elif _up and val > 2.5:    _ilbl, _iclr = "↑ 고착", C["gold"]
                        elif _up:                  _ilbl, _iclr = "↑ 반등", C["red"]
                        elif (not _up) and val > 4:    _ilbl, _iclr = "↓ 피크아웃", C["gold"]
                        elif (not _up) and val > 2.5:  _ilbl, _iclr = "↓ 둔화 중", C["gold"]
                        else:                          _ilbl, _iclr = "↓ 목표 근접", C["green"]
                    # CPI YoY 카드에만 원본 지표 ΔΔ 추가
                    if lbl == "CPI YoY":
                        _tr = _raw_dd_trend("CPI", _tr)
                    _ic(lbl, _v, _ilbl, _iclr, dd, _tr)

            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{C['muted']};margin:10px 0;font-weight:600'>고용/경기</div>", unsafe_allow_html=True)
            d3 = st.columns(4)
            with d3[0]:
                _v = f"{jolts/1000:.0f}K" if jolts is not None else "—"
                _tr = tbar(ctrends(fd.get("JTSJOL"), mode="pct", P=PM), "JOLTS")
                jd = ""; jlbl = "—"; jclr = C["muted"]
                if jolts is not None:
                    if jolts >= 8000: jlbl, jclr, jd = "과열", C["red"], "구인이 넘친다. 노동시장 과열. 연준이 인하를 미룰 근거."
                    elif jolts >= 5000: jlbl, jclr, jd = "정상", C["green"], "구인이 정상 수준이다. 과열은 아닌데 냉각도 아니다."
                    else:
                        if jolts >= 4000: jlbl, jclr, jd = "냉각 시작", C["gold"], "구인이 줄고 있다. 기업이 채용을 멈추기 시작했다."
                        else:             jlbl, jclr, jd = "냉각", C["red"], "구인이 말랐다. 실업률이 따라 올라간다. 사라."
                _ic("JOLTS", _v, jlbl, jclr, jd, _tr)
            with d3[1]:
                nd = ""; nlbl = "—"; nclr = C["muted"]
                if nfp is not None:
                    if nfp >= 200: nlbl, nclr, nd = "호조", C["green"], "고용이 튼튼하다. 연준이 안 내릴 이유가 하나 더 생겼다."
                    elif nfp >= 100: nlbl, nclr, nd = "둔화", C["gold"], "나쁘진 않다. 근데 방향을 봐라. 3개월 연속 둔화면 신호다."
                    elif nfp >= 0: nlbl, nclr, nd = "약화", C["gold"], "고용이 식고 있다. 마이너스는 아닌데 방향이 나쁘다."
                    else: nlbl, nclr, nd = "수축", C["red"], "고용 마이너스. 이미 경기침체다."
                _v = f"{nfp:+.0f}K" if nfp is not None else "—"
                _tr = tbar(ctrends(diff_s(fd.get("PAYEMS")), mode="abs", P=PM), "NFP")
                _ic("NFP", _v, nlbl, nclr, nd, _tr)
            with d3[2]:
                _v = f"{gdpv:.1f}%" if gdpv is not None else "—"
                _tr = tbar(ctrends(fd.get("GDP"), mode="abs_pp", P=PQ), "GDP")
                gd2 = ""; glbl = "—"; gclr = C["muted"]
                if gdpv is not None:
                    if gdpv >= 3: glbl, gclr, gd2 = "호조", C["green"], "경제가 뜨겁다. 이 숫자가 유지되면 연준이 내릴 이유가 없다."
                    elif gdpv >= 2: glbl, gclr, gd2 = "정상", C["green"], "버티고 있다. 이게 1% 밑으로 가면 그때 긴장해라."
                    elif gdpv >= 0: glbl, gclr, gd2 = "실속", C["gold"], "실속 구간이다. 고용이 같이 꺾이면 침체로 넘어간다."
                    else: glbl, gclr, gd2 = "침체", C["red"], "마이너스 성장. 침체다."
                _ic("GDP 성장률", _v, glbl, gclr, gd2, _tr)
            with d3[3]:
                _v = f"{um:.1f}" if um is not None else "—"
                _tr = tbar(ctrends(fd.get("UMCSENT"), mode="abs", P=PM), "UMCSENT")
                ud2 = ""; umlbl = "—"; umclr = C["muted"]
                if um is not None:
                    if um >= 80: umlbl, umclr, ud2 = "낙관", C["green"], "소비자가 낙관적이다. 경기 과열 신호. 연준이 내릴 이유가 줄어든다."
                    elif um >= 60: umlbl, umclr, ud2 = "보통", C["gold"], "보통이다. 숫자 자체보다 방향을 봐라. 3개월 연속 하락이면 경고."
                    else: umlbl, umclr, ud2 = "비관", C["red"], "소비자가 쫄았다. GDP의 70%가 소비다."
                _ic("소비자신뢰", _v, umlbl, umclr, ud2, _tr)

            # V3.3 CFNAI MA3 — 실물 클러스터 선행 슬롯 (점수 W=9, 라벨/색상은 _CARDS 등록 직전에 미리 계산됨)
            d3b = st.columns(4)
            with d3b[0]:
                _v = f"{cfnai_ma3:+.2f}" if cfnai_ma3 is not None else "—"
                _cf_tr = tbar(ctrends(_cfnai_ma3_s, mode="abs", P=PM), "CFNAI") if _cfnai_ma3_s is not None else ""
                _ic("CFNAI MA3", _v, _cf_lbl, _cf_clr, _cf_d, _cf_tr)

            # V3.8.2 노동격차 (deep 블록 전 사전 계산된 _gap_* 변수 사용)
            if _gap_now is not None:
                with d3b[1]:
                    _gap_tr = tbar(ctrends(_gap_s, mode="abs", P=PM), "JOLTS")
                    _ic("노동격차 (구인-실업자)", _gap_disp, _gap_lbl, _gap_color, _gap_cmt, _gap_tr)

            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{C['muted']};margin:10px 0;font-weight:600'>밸류에이션/구조</div>", unsafe_allow_html=True)
            d4 = st.columns(4)
            with d4[0]:
                _v = f"{cape:.1f}" if cape is not None else "—"
                cad = ""
                if cape is not None:
                    if cape >= 35: cad = "35 넘는 건 역사상 2000년밖에 없었다."
                    elif cape >= 25: cad = "역사적 평균(17)보다 높다. 비싸다. 근데 비싼 채로 10년 갈 수도 있다."
                    else: cad = "정상 범위에 가까워지고 있다. 역사적으로 여기서 사면 10년 뒤에 웃는다."
                cad = (cad + " · " if cad else "") + f"출처: {_val_src_base}"
                _ic("Shiller CAPE", _v,
                      "극단" if (cape and cape >= 35) else "—", C["red"] if (cape and cape >= 35) else C["muted"], cad, "")
            with d4[1]:
                _v = f"{tpe:.1f}" if tpe is not None else "—"
                tpd = ""
                if tpe is not None:
                    if tpe >= 28: tpd = "터지기 전 PER은 28이었다. 60, 70은 터진 뒤의 PER이다."
                    elif tpe >= 20: tpd = "비싸다. 근데 실적이 받쳐주면 유지 가능하다. 실적이 꺾이는 순간 이 숫자가 폭탄이 된다."
                    else: tpd = "싸다. 역사적으로 여기서 산 사람은 거의 다 이겼다."
                tpd = (tpd + " · " if tpd else "") + f"출처: {_val_src_base}"
                _ic("Trailing PE", _v,
                      "극단" if (tpe and tpe >= 28) else "—", C["red"] if (tpe and tpe >= 28) else C["muted"], tpd, "")
            with d4[2]:
                _v = f"{dy:.2f}%" if dy is not None else "—"
                dyd = ""
                if dy is not None:
                    if dy < 1.5: dyd = "지금보다 낮았던 건 2000년뿐이다."
                    elif dy < 2.0: dyd = "배당이 역사적 평균보다 낮다. 주가가 비싸다는 뜻이다."
                    else: dyd = "배당이 정상 근처다. 주가가 적정하거나 빠졌다는 신호."
                dyd = (dyd + " · " if dyd else "") + f"출처: {_val_src_base}"
                _ic("배당수익률", _v,
                      "경고" if (dy and dy < 1.5) else "—", C["red"] if (dy and dy < 1.5) else C["muted"], dyd, "")
            with d4[3]:
                _v = f"{bei:.2f}%" if bei is not None else "—"
                _tr = tbar(ctrends(fd.get("T5YIE"), mode="abs"), "BEI")
                bed = ""; belbl = "—"; beclr = C["muted"]
                if bei is not None:
                    if bei >= 2.8: belbl, beclr, bed = "인플레 경고", C["red"], "시장이 인플레 장기화를 가격에 넣고 있다. 연준 신뢰가 흔들리는 거다."
                    elif bei >= 2.0: belbl, beclr, bed = "안정", C["green"], ("기대 인플레가 안정적이다. 연준이 일을 하고 있다." if bei >= 2.2 else "목표 근처다. 이 수준이면 연준이 편하다.")
                    else: belbl, beclr, bed = "디플레 경고", C["red"], "디플레를 걱정하기 시작했다. 침체 냄새다."
                _ic("BEI 5Y", _v, belbl, beclr, bed, _tr)

            d4b = st.columns(4)
            with d4b[0]:
                _v = f"{buffett:.1f}%" if buffett is not None else "—"
                _tr = tbar(ctrends(fd.get("WILSHIRE"), mode="pct", P=PQ), "WILSHIRE") if fd.get("WILSHIRE") is not None else ""
                _ic("버핏 지표", _v, _bf_s, _bf_c, _bf_d, _tr)

            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_m};margin:10px 0;font-weight:600'>구조 / 기타</div>", unsafe_allow_html=True)
            d5 = st.columns(4)
            with d5[0]:
                _v = f"{cd:.1f}%" if cd is not None else "—"
                _tr = tbar(ctrends(fd.get("DRCCLACBS"), mode="abs_pp", P=PQ), "CARD")
                cdd = ""; cdlbl = "—"; cdclr = C["muted"]
                if cd is not None:
                    if cd > 5: cdlbl, cdclr, cdd = "경고", C["red"], "카드값을 못 막는다. 저축률이 바닥이다."
                    elif cd >= 3: cdlbl, cdclr, cdd = "주의", C["gold"], "연체가 올라오고 있다. 아직 위기는 아닌데 소비자의 체력이 빠지고 있다."
                    else: cdlbl, cdclr, cdd = "안정", C["green"], "소비자 건강하다. 아직은."
                _ic("카드 연체율", _v, cdlbl, cdclr, cdd, _tr)
            with d5[1]:
                _v = f"{dg:.0f}%" if dg is not None else "—"
                _tr = tbar(ctrends(fd.get("GFDEGDQ188S"), mode="abs_pp", P=PQ), "DEBT")
                dgd = ""
                if dg is not None:
                    if dg > 120: dgd = f"GDP 대비 {dg:.0f}%. 적자의 지속가능성. 이건 구조적 약세다."
                    elif dg >= 100: dgd = "부채가 높다. 위기 때 재정으로 받칠 여력이 제한된다."
                    else: dgd = "재정 여력이 있다. 위기가 와도 정부가 돈을 풀 수 있다."
                _ic("국채/GDP", _v,
                      "경고" if (dg and dg > 120) else "—", C["red"] if (dg and dg > 120) else C["muted"], dgd, _tr)
            with d5[2]:
                _v = f"${gold:,.0f}" if gold is not None else "—"
                _gold_yoy = chg(yd.get("GOLD"), 252)
                gld = ""; gldlbl = "—"; gldclr = C["muted"]
                if _gold_yoy is not None:
                    if _gold_yoy > 25: gldlbl, gldclr, gld = "과열", C["red"], "금이 미쳤다. 달러 시스템에 대한 불신이 극에 달했다."
                    elif _gold_yoy >= 10: gldlbl, gldclr, gld = "상승", C["gold"], "금이 꾸준히 오른다. 불안이 쌓이고 있다."
                    elif _gold_yoy >= 0: gldlbl, gldclr, gld = "안정", C["green"], "금값은 달러에 대한 불신의 가격이다. 지금은 조용하다."
                    else: gldlbl, gldclr, gld = "하락", C["red"], "금이 빠지고 있다. 달러 신뢰 회복이거나 유동성이 마르고 있다."
                else: gld = "금값은 달러에 대한 불신의 가격이다."
                _ic("Gold", _v, gldlbl, gldclr, gld, tbar(ctrends(yd.get("GOLD"), mode="pct"), "GOLD"))
            with d5[3]:
                _un3v = f"{un3m_chg:+.2f}%p" if un3m_chg is not None else "—"
                _und = ""; _unlbl = "—"; _unclr = C["muted"]
                if un3m_chg is not None:
                    if un3m_chg > 0.5: _unlbl, _unclr, _und = "경고", C["red"], "3개월 새 0.5%p 이상 올랐다. 삼의 법칙(Sahm Rule) 발동 근처. 침체가 시작됐을 수 있다."
                    elif un3m_chg > 0.3: _unlbl, _unclr, _und = "경고", C["red"], "실업률이 빠르게 올라가고 있다. 침체 신호."
                    elif un3m_chg > 0: _unlbl, _unclr, _und = "주의", C["gold"], "실업률이 미세하게 올라가고 있다. 아직 경고 수준은 아니다. 방향만 확인."
                    else: _unlbl, _unclr, _und = "안정", C["green"], "실업률이 안정적이거나 내려가고 있다. 고용이 버티고 있다."
                _ic("실업률 3M 변화", _un3v, _unlbl, _unclr, _und, "")

            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_m};margin:10px 0;font-weight:600'>섹터 로테이션</div>", unsafe_allow_html=True)
            d6 = st.columns(3)
            with d6[0]:
                _v = f"{xle_spy_3m:+.1f}%p" if xle_spy_3m is not None else "—"
                _ic("XLE-SPY 3M", _v, _xle_lbl, _xle_clr, "에너지 vs 시장. 에너지가 이기면 인플레 압력, 지면 디플레 압력. 방향만 봐라.", "")
            with d6[1]:
                _v = f"{xlk_spy_3m:+.1f}%p" if xlk_spy_3m is not None else "—"
                _ic("XLK-SPY 3M", _v, _xlk_lbl, _xlk_clr, "기술 vs 시장. 기술이 이기면 성장 기대, 지면 유동성 회수. SOX/SPX와 같이 봐라.", "")
            with d6[2]:
                _xle_str = f"XLE {xle_spy_3m:+.1f}" if xle_spy_3m is not None else "XLE —"
                _xlk_str = f"XLK {xlk_spy_3m:+.1f}" if xlk_spy_3m is not None else "XLK —"
                _v = f"{_xle_str} / {_xlk_str}"
                _ic("섹터 로테이션", _v, _sec_lbl, _sec_clr, _sec_d, "")

        if deep:
            # ═══ 추세/사이클 진단 (F1 + F3) — F5는 V3.4에서 always-on으로 이동 ═══
            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_m};margin:14px 0 6px;font-weight:600'>🔄 추세/사이클 진단</div>", unsafe_allow_html=True)
            d7 = st.columns(3)

            # ── F1: 2Y10Y 역전 해소 진행률 ──
            with d7[0]:
                if _inv2y10y is not None:
                    _rec_pct = _inv2y10y["recovery_pct"]
                    _peak = _inv2y10y["peak"]; _cur = _inv2y10y["cur"]; _rec_bp = _inv2y10y["recovery_bp"]
                    if _rec_pct >= 100:    _f1a_lbl, _f1a_clr, _f1a_msg = "역전 해소", C["green"], "형이 안심하고 있다. 정상 곡선이다."
                    elif _rec_pct >= 50:   _f1a_lbl, _f1a_clr, _f1a_msg = "해소 진행 중", C["gold"], "역전이 풀리고 있다. 겨울이 끝나가는 신호."
                    elif _rec_pct >= 10:   _f1a_lbl, _f1a_clr, _f1a_msg = "해소 초기", C["orange"], "역전의 고점은 주가의 저점 근처다. 풀리기 시작했다."
                    else:                  _f1a_lbl, _f1a_clr, _f1a_msg = "역전 지속", C["red"], "역전의 고점은 인플레의 고점 근처이고 주가의 저점 근처다."
                    _f1a_v = f"{_rec_pct:.0f}%"
                    _f1a_det = f"{_f1a_msg}<br><span style='color:{_m};font-size:var(--mac-fs-xs)'>peak {_peak:.0f}bp → 현재 {_cur:.0f}bp ({_rec_bp:+.0f}bp)</span>"
                    _ic("2Y10Y 역전 해소", _f1a_v, _f1a_lbl, _f1a_clr, _f1a_det, "")
                    _CARDS["F1_2Y10Y_역전해소"] = _card(round(_rec_pct, 0), _f1a_lbl, _f1a_clr, _f1a_msg)
                else:
                    _ic("2Y10Y 역전 해소", "—", "역전 없음", C["green"], "정상 곡선이다. 형이 안심하고 있다. 다음 역전이 시작될 때까지 여기는 볼 게 없다.", "")
                    _CARDS["F1_2Y10Y_역전해소"] = _card(None, "역전 없음", C["green"], "52주 내 역전 없음")

            # ── F1: 3M10Y 역전 해소 진행률 ──
            with d7[1]:
                if _inv3m10y is not None:
                    _rec_pct = _inv3m10y["recovery_pct"]
                    _peak = _inv3m10y["peak"]; _cur = _inv3m10y["cur"]; _rec_bp = _inv3m10y["recovery_bp"]
                    if _rec_pct >= 100:    _f1b_lbl, _f1b_clr, _f1b_msg = "역전 해소", C["green"], "형이 안심하고 있다. 정상 곡선이다."
                    elif _rec_pct >= 50:   _f1b_lbl, _f1b_clr, _f1b_msg = "해소 진행 중", C["gold"], "역전이 풀리고 있다. 겨울이 끝나가는 신호."
                    elif _rec_pct >= 10:   _f1b_lbl, _f1b_clr, _f1b_msg = "해소 초기", C["orange"], "역전의 고점은 주가의 저점 근처다. 풀리기 시작했다."
                    else:                  _f1b_lbl, _f1b_clr, _f1b_msg = "역전 지속", C["red"], "3개월물과 10년물이 역전되고 침체 안 온 적 있나? 없다."
                    _f1b_v = f"{_rec_pct:.0f}%"
                    _f1b_det = f"{_f1b_msg}<br><span style='color:{_m};font-size:var(--mac-fs-xs)'>peak {_peak:.0f}bp → 현재 {_cur:.0f}bp ({_rec_bp:+.0f}bp)</span>"
                    _ic("3M10Y 역전 해소", _f1b_v, _f1b_lbl, _f1b_clr, _f1b_det, "")
                    _CARDS["F1_3M10Y_역전해소"] = _card(round(_rec_pct, 0), _f1b_lbl, _f1b_clr, _f1b_msg)
                else:
                    _ic("3M10Y 역전 해소", "—", "역전 없음", C["green"], "정상 곡선이다. 형이 안심하고 있다. 다음 역전이 시작될 때까지 여기는 볼 게 없다.", "")
                    _CARDS["F1_3M10Y_역전해소"] = _card(None, "역전 없음", C["green"], "52주 내 역전 없음")

            # ── F3: FF금리 historical 위치 ──
            with d7[2]:
                if _ff_pos is not None:
                    if _ff_stage == "고점":   _f3_clr, _f3_lbl = C["red"],   "고점권 인하"
                    elif _ff_stage == "중립": _f3_clr, _f3_lbl = C["gold"],  "중립권"
                    else:                     _f3_clr, _f3_lbl = C["green"], "저점권 인하"
                    _f3_v = f"{_ff_pos:.0f}%ile"
                    _f3_det = "고점에서의 금리인하는 주가 하락을 이끈다. 저점에서의 금리인하는 봄이다. 같은 인하라도 출발점이 다르면 의미가 정반대다."
                    _ic("FF금리 위치", _f3_v, _f3_lbl, _f3_clr, _f3_det, "")
                    _CARDS["F3_FF금리_위치"] = _card(round(_ff_pos, 0), _f3_lbl, _f3_clr, _f3_det)
                else:
                    _ic("FF금리 위치", "—", "데이터 부족", C["muted"], "", "")
                    _CARDS["F3_FF금리_위치"] = _card(None, "데이터 부족", C["muted"], "")

            # F5 가속도 모니터는 V3.4부터 always-on 섹션으로 이동 (대시보드 상단)

            # ═══ 사이클 / 반사성 (F2 + F6) ═══
            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_m};margin:14px 0 6px;font-weight:600'>📅 사이클 / 반사성</div>", unsafe_allow_html=True)
            d8 = st.columns(4)

            # ── F2: 금리 인하 사이클 단계 ──
            with d8[0]:
                if _cut_info is None:
                    _ic("인하 사이클", "—", "데이터 부족", C["muted"], "", "")
                    _CARDS["F2_인하_사이클"] = _card(None, "데이터 부족", C["muted"], "")
                elif not _cut_info["active"]:
                    _ic("인하 사이클", "—", "비활성",
                          C["muted"],
                          "인하 사이클 아니다. 동결 또는 인상 중.",
                          "")
                    _CARDS["F2_인하_사이클"] = _card(None, "비활성", C["muted"], "인하 사이클 아니다. 동결 또는 인상 중.")
                else:
                    _cv = f"-{_cut_info['cum_cut_bp']:.0f}bp"
                    _cs = _cut_info["stage"]
                    if _cs == "초입":   _cc = C["green"]
                    elif _cs == "중반": _cc = C["gold"]
                    elif _cs == "후반": _cc = C["orange"]
                    else:               _cc = C["red"]  # 장기화
                    _F2_MSG = {
                        "초입":   "인하가 시작됐다. 시장은 환호한다. 근데 초입의 인하는 연준이 겁먹었다는 뜻이기도 하다.",
                        "중반":   "인하가 진행 중이다. 유동성이 풀리고 있다. 아직은 순풍이다.",
                        "후반":   "인하한 지 오래됐는데 경기가 안 살아난다. 더 이상 금리가 떨어지지 않을 것이라는 믿음이 생기기 전까지 주가는 하락한다.",
                        "장기화": "인하를 이만큼 했는데도 안 된다. 약이 안 듣는 거다. 항암치료도 환자가 버텨야 한다.",
                    }
                    _cd_det = (f"{_F2_MSG.get(_cs, '')}"
                               f"<br><span style='color:{_m};font-size:var(--mac-fs-xs)'>"
                               f"첫 인하 {_cut_info['months']}개월 전 ({_cut_info['start_date']}) · "
                               f"peak {_cut_info['peak']:.2f}% → 현재 {_cut_info['cur']:.2f}%</span>")
                    _ic("인하 사이클", _cv, _cs, _cc, _cd_det, "")
                    _CARDS["F2_인하_사이클"] = _card(
                        {"cum_cut_bp": _cut_info['cum_cut_bp'], "months": _cut_info['months'], "peak": _cut_info['peak'], "cur": _cut_info['cur'], "start_date": _cut_info['start_date']},
                        _cs, _cc, _F2_MSG.get(_cs, "")
                    )

            # ── F6: 반사성 프록시 (Forward EPS 30D 변화) ──
            with d8[1]:
                if _refl_30 is None:
                    _n = len([h for h in _fwd_hist if h.get("feps") is not None])
                    _ic("Forward EPS 추세", "축적 중", f"{_n}/5일",
                          C["muted"],
                          "데이터 쌓는 중이다. 방향을 말하려면 최소 30일은 있어야 한다. 기다려라.", "")
                    _CARDS["F6_Forward_EPS_추세"] = _card({"누적일수": _n, "필요": 5}, "축적 중", C["muted"], "데이터 누적 대기")
                else:
                    _ec = _refl_30["eps_chg"]; _n = _refl_30["n"]; _sc = _refl_30["spx_chg"]
                    # 30일 미만 fallback: 신뢰도 한 단계 약화 (green→gold, red→gold) + 라벨에 (임시) 표기
                    _is_partial = _n < 30
                    if _ec > 1.5:    _f6_lbl, _f6_clr = "컨센서스 급등", (C["gold"] if _is_partial else C["green"])
                    elif _ec > 0:    _f6_lbl, _f6_clr = "컨센서스 상향", (C["gold"] if _is_partial else C["green"])
                    elif _ec > -1.5: _f6_lbl, _f6_clr = "컨센서스 정체", C["gold"]
                    else:            _f6_lbl, _f6_clr = "컨센서스 하향", (C["gold"] if _is_partial else C["red"])
                    if _is_partial: _f6_lbl += " *"
                    _spx_str = f" · SPX {_sc:+.1f}%" if _sc is not None else ""
                    _partial_str = f" · ⚠️ {_n}일 기준 (30일 미달, 임시값)" if _is_partial else ""
                    _f6_det = ("22년 12월엔 안 보이던 혁명이 왜 지금 보이냐. 실적이 좋아서? 아니다. 가격이 올랐기 때문이다. 컨센서스가 가격을 따라가고 있으면 그건 분석이 아니라 반사성이다."
                               f"<br><span style='color:{_m};font-size:var(--mac-fs-xs)'>"
                               f"{_n}일 누적 · fEPS {_ec:+.2f}%{_spx_str}{_partial_str}</span>")
                    _ic("Forward EPS 추세", f"{_ec:+.2f}%", _f6_lbl, _f6_clr, _f6_det, "")
                    _CARDS["F6_Forward_EPS_추세"] = _card(
                        {"eps_chg_pct": round(_ec, 2), "spx_chg_pct": round(_sc, 2) if _sc is not None else None,
                         "n_days": _n, "is_partial": _is_partial},
                        _f6_lbl, _f6_clr,
                        "22년 12월엔 안 보이던 혁명이 왜 지금 보이냐. 실적이 좋아서? 아니다. 가격이 올랐기 때문이다. 컨센서스가 가격을 따라가고 있으면 그건 분석이 아니라 반사성이다."
                    )

        # ── V3.2 카드 추가 후 탭 export 재구성 (_pick은 호출 시점 _CARDS 스냅샷) ──
        export_bond["카드"] = _pick([
            "2Y10Y_스프레드", "3M10Y_스프레드", "3M2Y_스프레드", "실질금리", "FF_6M변화", "연방기금금리",
            "F1_2Y10Y_역전해소", "F1_3M10Y_역전해소", "F2_인하_사이클", "F3_FF금리_위치"
        ])
        export_valuation["카드"] = _pick([
            "Forward_PE", "Trailing_PE", "Shiller_CAPE", "배당수익률", "버핏지표", "F6_Forward_EPS_추세"
        ])
        export_dash["가속도_모니터"] = _CARDS.get("F5_가속도_모니터")

        # ── 데이터 Export (대시보드) ──
        st.markdown("---")
        _all_charts = [c for c in [(_fig_radar,"거시 뷰") if _fig_radar else None, (_fig_mx,"2×2 매트릭스") if _fig_mx else None, (_fig_bond,"장단기금리차") if _fig_bond else None, (_fig_semi,"SOX/SPX 비율") if _fig_semi else None, (_fig_season,"계절 점수") if _fig_season else None] if c]
        with st.expander("📦 JSON Export — 클로드 상담 · 데이터 분석용"):
            ej1, ej2, ej3 = st.columns(3)
            with ej1:
                _jbtn(export_all, "observatory_all", "📥 전체 (모든 섹션)", "_all")
                _jbtn(export_dash, "observatory_dash", "📥 대시보드 요약", "_dash")
            with ej2:
                _jbtn(export_inflation, "inflation", "📥 인플레이션", "_infl")
                _jbtn(export_employment, "employment", "📥 고용/실물", "_emp")
            with ej3:
                _jbtn(export_timeseries, "timeseries", "📥 시계열 원본 (252일)", "_ts")
                if _obs_all:
                    st.download_button("📥 관찰 기록 일괄", json.dumps(_obs_all, ensure_ascii=False, indent=2).encode("utf-8"), f"observations_{_ds}.json", "application/json", key="exp_obs_all")
        with st.expander("📄 HTML Export — 브라우저 열람 · 공유용"):
            eh1, eh2, eh3 = st.columns(3)
            with eh1: st.download_button("📥 전체 리포트 (차트 포함)", _export_html(export_all, charts=_all_charts).encode("utf-8"), f"observatory_{_ds}.html", "text/html", key="exp_html")
            with eh2: st.download_button("📥 인플레이션", _export_html(export_inflation, section="인플레이션").encode("utf-8"), f"inflation_{_ds}.html", "text/html", key="exp_infl_html")
            with eh3: st.download_button("📥 고용/실물", _export_html(export_employment, section="고용/실물").encode("utf-8"), f"employment_{_ds}.html", "text/html", key="exp_emp_html")

        # ── 진단 패널 ──
        with st.expander("🔧 진단"):
            _ok = len(_fresh); _fail = len(_fails)
            st.caption(f"**소스 현황**: {_ok}개 정상 · {_fail}개 실패{'  ⚠️ '+', '.join(_fails) if _fails else ''}")
            # 폴백 체인
            st.markdown(f"""<div style="background:{C['card']};border:1px solid {C['border']};border-radius:6px;padding:10px 14px;margin:8px 0;font-size:var(--mac-fs-md);color:{C['text']}">
<b>폴백 체인</b><br>
Forward PE: <code>{fpe}</code> ← {vd.get('source','?')}<br>
Fear &amp; Greed: <code>{fgs if fgs is not None else '실패'}</code> ← {_fg_src}<br>
Earnings 판정: {_earn_src}<br>
KRW: FRED DEXKOUS | WTI: FRED DCOILWTICO | DXY: yfinance DX-Y.NYB</div>""", unsafe_allow_html=True)
            # 데이터 신선도 — 핵심 시리즈만
            _key_series = ["DGS3MO","DGS2","DGS10","T10Y2Y","T10Y3M","VIXCLS","HY","UNRATE","FEDFUNDS","WTI","KRW","DXY","SOXX","SPX","GOLD","QQQ","WILSHIRE","GDP_NOMINAL"]
            _stale_rows = []
            for k in _key_series:
                if k in _fresh:
                    dt, src = _fresh[k]
                    _stale_rows.append(f"{k}: {dt} ({src})")
            st.caption("**데이터 최신일**: " + " · ".join(_stale_rows[:7]))
            if len(_stale_rows) > 7: st.caption("　" + " · ".join(_stale_rows[7:]))
            # 계절 판정 요약
            _ff6 = f"{ff6m_chg:+.2f}" if ff6m_chg is not None else "?"
            _un3 = f"{un3m_chg:+.2f}" if un3m_chg is not None else "?"
            st.caption(f"**계절**: {season_auto} ({season_conf}) | FF 6M={_ff6} | UNRATE 3M={_un3} | 점수={season_scores}")

    # ═══ TAB 1: 쌍발 엔진 ═══
    with tabs[1]:
        st.caption("📋 QQQ·SOXX 가격·드로다운·트리거 단계 + 계좌 연동 (state.json)")
        _t = C["text"]; _m = C["muted"]; _cd = C["card"]; _bd = C["border"]; _br = C["bright"]

        # 현재가 / 52주 고점 / DD / 단계 계산 (QQQ)
        _qs = yd.get("QQQ"); _ss = yd.get("SOXX")
        def _eng_calc(s, c1, c2, c3):
            if s is None or len(s) < 252: return None
            cur = float(s.iloc[-1])
            hi = float(s.iloc[-252:].max())
            dd_pct = (cur / hi - 1) * 100
            stg, mult = _dd_stage(dd_pct, c1, c2, c3)
            # 다음 트리거 가격 (현재 단계보다 한 단계 더 깊은 곳)
            dd_frac = dd_pct / 100
            if dd_frac > c1:    nxt_thr, nxt_name = c1, "경계장"
            elif dd_frac > c2:  nxt_thr, nxt_name = c2, "조정장"
            elif dd_frac > c3:  nxt_thr, nxt_name = c3, "폭락장"
            else:               nxt_thr, nxt_name = None, None
            nxt_price = hi * (1 + nxt_thr) if nxt_thr is not None else None
            nxt_dist_pct = ((nxt_price / cur) - 1) * 100 if nxt_price is not None else None
            return {"cur": cur, "hi": hi, "dd": dd_pct, "stg": stg, "mult": mult,
                    "nxt_name": nxt_name, "nxt_thr": nxt_thr, "nxt_price": nxt_price, "nxt_dist": nxt_dist_pct}
        _q_eng = _eng_calc(_qs, _dd_caution, _dd_correction, _dd_crash)
        _s_eng = _eng_calc(_ss, _soxl_dd1, _soxl_dd2, _soxl_dd3)

        def _stg_color(stg):
            return C["red"] if stg == "폭락장" else C["green"] if stg == "조정장" else C["orange"] if stg == "경계장" else _t

        # ── 1행: QQQ 4카드 ──
        st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_m};margin:6px 0 4px;font-weight:600'>🟦 QQQ 엔진</div>", unsafe_allow_html=True)
        q1, q2, q3, q4 = st.columns(4)
        if _q_eng:
            with q1: st.markdown(_mkcard("QQQ 현재가", f"${_q_eng['cur']:.2f}", f"52주 고점 ${_q_eng['hi']:.2f}", _br), unsafe_allow_html=True)
            with q2:
                _c = _stg_color(_q_eng['stg'])
                st.markdown(_mkcard("52주 DD", f"{_q_eng['dd']:.2f}%", f"{_q_eng['stg']} {_q_eng['mult']}", _c), unsafe_allow_html=True)
            with q3:
                if _q_eng['nxt_price'] is not None:
                    st.markdown(_mkcard(f"다음 트리거 ({_q_eng['nxt_name']})", f"${_q_eng['nxt_price']:.2f}", f"임계 {_q_eng['nxt_thr']*100:.0f}%", C["orange"]), unsafe_allow_html=True)
                else:
                    st.markdown(_mkcard("다음 트리거", "최종 단계", "더 내려갈 곳 없음", C["red"]), unsafe_allow_html=True)
            with q4:
                if _q_eng['nxt_dist'] is not None:
                    _dc = C["red"] if _q_eng['nxt_dist'] < -10 else C["orange"] if _q_eng['nxt_dist'] < -5 else C["green"]
                    st.markdown(_mkcard("진입까지 거리", f"{_q_eng['nxt_dist']:+.2f}%", "현재가 → 트리거", _dc), unsafe_allow_html=True)
                else:
                    st.markdown(_mkcard("진입까지 거리", "—", "", _m), unsafe_allow_html=True)
        else:
            st.info("QQQ 데이터 대기 중")

        # ── 2행: SOXX 4카드 ──
        st.markdown(f"<div style='height:8px'></div><div style='font-size:var(--mac-fs-md);color:{_m};margin:6px 0 4px;font-weight:600'>🟧 SOXX 엔진</div>", unsafe_allow_html=True)
        x1, x2, x3, x4 = st.columns(4)
        if _s_eng:
            with x1: st.markdown(_mkcard("SOXX 현재가", f"${_s_eng['cur']:.2f}", f"52주 고점 ${_s_eng['hi']:.2f}", _br), unsafe_allow_html=True)
            with x2:
                _c = _stg_color(_s_eng['stg'])
                st.markdown(_mkcard("52주 DD", f"{_s_eng['dd']:.2f}%", f"{_s_eng['stg']} {_s_eng['mult']}", _c), unsafe_allow_html=True)
            with x3:
                if _s_eng['nxt_price'] is not None:
                    st.markdown(_mkcard(f"다음 트리거 ({_s_eng['nxt_name']})", f"${_s_eng['nxt_price']:.2f}", f"임계 {_s_eng['nxt_thr']*100:.0f}%", C["orange"]), unsafe_allow_html=True)
                else:
                    st.markdown(_mkcard("다음 트리거", "최종 단계", "더 내려갈 곳 없음", C["red"]), unsafe_allow_html=True)
            with x4:
                if _s_eng['nxt_dist'] is not None:
                    _dc = C["red"] if _s_eng['nxt_dist'] < -10 else C["orange"] if _s_eng['nxt_dist'] < -5 else C["green"]
                    st.markdown(_mkcard("진입까지 거리", f"{_s_eng['nxt_dist']:+.2f}%", "현재가 → 트리거", _dc), unsafe_allow_html=True)
                else:
                    st.markdown(_mkcard("진입까지 거리", "—", "", _m), unsafe_allow_html=True)
        else:
            st.info("SOXX 데이터 대기 중")

        # ── 🚪 문지기 (VOO) — 파랄 때 1주 사놓고 쳐다보는 자리 ──
        st.markdown(f"<div style='height:14px'></div><div style='font-size:var(--mac-fs-md);color:{_m};margin:6px 0 4px;font-weight:600'>🚪 문지기 (VOO) <span style='font-weight:400;font-size:var(--mac-fs-sm)'>— 모든 게 파랄 때 1주만 사놓고 쳐다보는 자리</span></div>", unsafe_allow_html=True)
        _vs = yd.get("VOO")
        v1, v2, v3, v4 = st.columns(4)
        if _vs is not None and len(_vs) > 0:
            _v_cur = float(_vs.iloc[-1])
            _v_hi  = float(_vs.iloc[-252:].max()) if len(_vs) >= 252 else float(_vs.max())
            _v_dd  = (_v_cur / _v_hi - 1) * 100 if _v_hi > 0 else None
            # 1일 / 5일 / 30일 변화율
            def _chg(s, n):
                if s is None or len(s) <= n: return None
                try:
                    return (float(s.iloc[-1]) / float(s.iloc[-1 - n]) - 1) * 100
                except: return None
            _v_1d  = _chg(_vs, 1)
            _v_5d  = _chg(_vs, 5)
            _v_30d = _chg(_vs, 21)
            def _chg_color(x):
                if x is None: return _m
                return C["blue"] if x < 0 else C["green"]
            with v1:
                st.markdown(_mkcard("VOO 현재가", f"${_v_cur:.2f}", f"52주 고점 ${_v_hi:.2f}", _br), unsafe_allow_html=True)
            with v2:
                _ddc = C["blue"] if (_v_dd is not None and _v_dd < 0) else C["green"]
                st.markdown(_mkcard("52주 DD", f"{_v_dd:+.2f}%" if _v_dd is not None else "—", "고점 대비", _ddc), unsafe_allow_html=True)
            with v3:
                st.markdown(_mkcard("1일 변화", f"{_v_1d:+.2f}%" if _v_1d is not None else "—", "어제 대비", _chg_color(_v_1d)), unsafe_allow_html=True)
            with v4:
                _sub30 = f"30일 {_v_30d:+.2f}%" if _v_30d is not None else "—"
                st.markdown(_mkcard("5일 변화", f"{_v_5d:+.2f}%" if _v_5d is not None else "—", _sub30, _chg_color(_v_5d)), unsafe_allow_html=True)
        else:
            st.info("VOO 데이터 대기 중")

        # ── 3행: 계좌 연동 (state.json) ──
        st.markdown(f"<div style='height:14px'></div><div style='font-size:var(--mac-fs-md);color:{_m};margin:6px 0 4px;font-weight:600'>💼 계좌 평가 (state.json)</div>", unsafe_allow_html=True)
        if mk:
            # 3-1. 평가액 4카드 (총 / TQQQ / SOXL / 현금)
            a1, a2, a3, a4 = st.columns(4)
            def _money(v):
                if v is None: return "—"
                if abs(v) >= 1_000_000: return f"${v/1_000_000:.2f}M"
                if abs(v) >= 1_000:     return f"${v/1_000:.1f}K"
                return f"${v:,.0f}"
            with a1:
                if _mk_krw_usd is not None and _mk_krw_usd > 0:
                    st.markdown(_mkcard("총 자산", _money(_mk_total_val_adj), "TQQQ + SOXL + SGOV + KRW", _br), unsafe_allow_html=True)
                else:
                    st.markdown(_mkcard("총 자산", _money(_mk_total_val), "TQQQ + SOXL + SGOV", _br), unsafe_allow_html=True)
            with a2:
                _sub = f"{_mk_tqqq_shares:,}주 @ ${_mk_tqqq_price:.2f}" if (_mk_tqqq_shares and _mk_tqqq_price) else "—"
                st.markdown(_mkcard("TQQQ 평가액", _money(_mk_tqqq_eval), _sub, _t), unsafe_allow_html=True)
            with a3:
                _sub = f"{_mk_soxl_shares:,}주 @ ${_mk_soxl_price:.2f}" if (_mk_soxl_shares and _mk_soxl_price) else "—"
                st.markdown(_mkcard("SOXL 평가액", _money(_mk_soxl_eval), _sub, _t), unsafe_allow_html=True)
            with a4:
                # V3.5-hotfix3: SGOV + 원화 통합 표시 (환율 = yfinance KRW=X)
                _cp = f"{_mk_cash*100:.1f}%" if _mk_cash is not None else "—"
                if _mk_krw_usd is not None and _mk_krw_usd > 0:
                    _cash_main = _money(_mk_cash_total)
                    # 원화 표기: ₩26.0M / ₩780K
                    _krw = _mk_krw_val
                    if _krw >= 1_000_000: _krw_s = f"₩{_krw/1_000_000:.1f}M"
                    elif _krw >= 1_000:   _krw_s = f"₩{_krw/1_000:.0f}K"
                    else:                  _krw_s = f"₩{_krw:,.0f}"
                    _sub = f"SGOV {_money(_mk_sgov_val)} · {_krw_s} (≈{_money(_mk_krw_usd)}) · 비중 {_cp}"
                    st.markdown(_mkcard("현금", _cash_main, _sub, C["blue"]), unsafe_allow_html=True)
                else:
                    st.markdown(_mkcard("현금 (SGOV)", _money(_mk_sgov_val), f"비중 {_cp}", C["blue"]), unsafe_allow_html=True)

            # 3-2. Ratio + 손익 4카드 (TQQQ R / SOXL R / 누적투입 / 평가손익)
            st.markdown(f"<div style='height:6px'></div>", unsafe_allow_html=True)
            b1, b2, b3, b4 = st.columns(4)
            with b1:
                _v = f"{_mk_ratio:.3f}" if _mk_ratio is not None else "—"
                # V3.5-hotfix2: trigger/target은 외부 도구 활성 프리셋에서 동적으로 받음 (fallback V1.2)
                _trg = _mk_trigger if _mk_trigger is not None else 1.7
                _tgt = _mk_target  if _mk_target  is not None else 1.55
                _c = C["green"] if (_mk_ratio is not None and _mk_ratio < _tgt) else C["orange"] if (_mk_ratio is not None and _mk_ratio < _trg) else C["red"] if _mk_ratio is not None else _t
                _sub = f"트리거 {_trg:.2f} / 목표 {_tgt:.2f}" if _mk_ratio is not None else "—"
                if _ratio_is_live:
                    _sub += " · " + ("live" if _tq_live_src == "extended" else "daily")
                st.markdown(_mkcard("TQQQ Ratio", _v, _sub, _c), unsafe_allow_html=True)
            with b2:
                _v = f"{_mk_soxl_ratio:.3f}" if _mk_soxl_ratio is not None else "—"
                _c = C["green"] if (_mk_soxl_ratio is not None and _mk_soxl_ratio < 2.0) else _t
                _sub2 = "평가 / 투입"
                if _soxl_ratio_is_live:
                    _sub2 += " · " + ("live" if _sx_live_src == "extended" else "daily")
                st.markdown(_mkcard("SOXL Ratio", _v, _sub2, _c), unsafe_allow_html=True)
            with b3:
                _ci = (_mk_tqqq_cost or 0) + (_mk_soxl_cost or 0)
                st.markdown(_mkcard("누적 투입액", _money(_ci) if _ci > 0 else "—", "TQQQ + SOXL", _t), unsafe_allow_html=True)
            with b4:
                _eval_total = (_mk_tqqq_eval or 0) + (_mk_soxl_eval or 0)
                _ci2 = (_mk_tqqq_cost or 0) + (_mk_soxl_cost or 0)
                if _ci2 > 0:
                    _pnl = _eval_total - _ci2
                    _pnl_pct = (_pnl / _ci2) * 100
                    _pc = C["green"] if _pnl >= 0 else C["red"]
                    _sign = "+" if _pnl >= 0 else ""
                    st.markdown(_mkcard("평가 손익", f"{_sign}{_money(_pnl).lstrip('$')}".replace("+-","-").replace("$",""), f"{_sign}{_pnl_pct:.2f}%", _pc), unsafe_allow_html=True)
                else:
                    st.markdown(_mkcard("평가 손익", "—", "", _m), unsafe_allow_html=True)

            # 3-3. 매수 이력 / 다음 매수 4카드
            st.markdown(f"<div style='height:6px'></div>", unsafe_allow_html=True)
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.markdown(_mkcard("최근 점검일", _mk_last_check or "—", "엑셀 주간점검 마지막", _m), unsafe_allow_html=True)
            with c2:
                st.markdown(_mkcard("최근 매수일", _mk_last_buy or "—", "거래기록 마지막 row", _m), unsafe_allow_html=True)
            with c3:
                _v = f"{_mk_ytd_count}회" if _mk_ytd_count is not None else "—"
                _c = C["green"] if (_mk_ytd_count is not None and _mk_ytd_count >= 5) else _t
                st.markdown(_mkcard("YTD 매수 횟수", _v, "TQQQ + SOXL", _c), unsafe_allow_html=True)
            with c4:
                _v = _money(_mk_next_buy) if _mk_next_buy is not None else "—"
                _sub = "현재 단계 배율 적용" if _mk_next_buy is not None else "DCA 베이스 미설정"
                st.markdown(_mkcard("다음 매수 예정금", _v, _sub, C["orange"] if _mk_next_buy else _m), unsafe_allow_html=True)

            # 3-4. state.json 갱신 시각
            _upd = mk.get("updated", "—")
            if _upd != "—":
                try: _upd = _upd.split("T")[0] + " " + _upd.split("T")[1][:5]
                except: pass
            st.caption(f"💾 state.json 최종 갱신: {_upd}")

            # 트리거 임계값 표 (외부 도구에서 설정한 값) — 매수배율 DD + 재투입 DD
            st.markdown(f"<div style='height:10px'></div>", unsafe_allow_html=True)
            _th_html = f"""<div style='background:{_cd};border:1px solid {_bd};border-radius:8px;padding:12px 16px'>
                <div style='font-size:var(--mac-fs-md);color:{_m};font-weight:600;margin-bottom:6px'>⚙️ 매수배율 DD 임계 (주간 DCA 증폭)</div>
                <table style='width:100%;border-collapse:collapse;font-size:var(--mac-fs-md);margin-bottom:10px'>
                <tr style='border-bottom:1px solid {_bd};color:{_m}'>
                    <th style='text-align:left;padding:4px 8px'>종목</th>
                    <th style='text-align:right;padding:4px 8px'>경계장 ×1.5</th>
                    <th style='text-align:right;padding:4px 8px'>조정장 ×2.0</th>
                    <th style='text-align:right;padding:4px 8px'>폭락장 ×3.0</th></tr>
                <tr style='border-bottom:1px solid {_bd}'>
                    <td style='padding:4px 8px;color:{_t}'>QQQ</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["orange"]}'>{_dd_caution*100:.0f}%</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["green"]}'>{_dd_correction*100:.0f}%</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["red"]}'>{_dd_crash*100:.0f}%</td></tr>
                <tr>
                    <td style='padding:4px 8px;color:{_t}'>SOXX</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["orange"]}'>{_soxl_dd1*100:.0f}%</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["green"]}'>{_soxl_dd2*100:.0f}%</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["red"]}'>{_soxl_dd3*100:.0f}%</td></tr>
                </table>
                <div style='font-size:var(--mac-fs-md);color:{_m};font-weight:600;margin:6px 0'>💧 재투입 DD 임계 (SGOV → TQQQ 이체 속도)</div>
                <table style='width:100%;border-collapse:collapse;font-size:var(--mac-fs-md)'>
                <tr style='border-bottom:1px solid {_bd};color:{_m}'>
                    <th style='text-align:left;padding:4px 8px'>단계</th>
                    <th style='text-align:right;padding:4px 8px'>QQQ DD</th>
                    <th style='text-align:right;padding:4px 8px'>속도/주</th></tr>
                <tr style='border-bottom:1px solid {_bd}'>
                    <td style='padding:4px 8px;color:{_t}'>얕은</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["orange"]}'>{_reinv_dd_shallow*100:.0f}%</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["orange"]}'>{_reinv_spd_shallow*100:.0f}%</td></tr>
                <tr style='border-bottom:1px solid {_bd}'>
                    <td style='padding:4px 8px;color:{_t}'>중간</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["green"]}'>{_reinv_dd_mid*100:.0f}%</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["green"]}'>{_reinv_spd_mid*100:.0f}%</td></tr>
                <tr>
                    <td style='padding:4px 8px;color:{_t}'>깊은</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["red"]}'>{_reinv_dd_deep*100:.0f}%</td>
                    <td style='text-align:right;padding:4px 8px;color:{C["red"]}'>{_reinv_spd_deep*100:.0f}%</td></tr>
                </table></div>"""
            st.markdown(_th_html, unsafe_allow_html=True)
        else:
            st.warning("⚠️ state.json에 계좌 데이터 없음. 외부 도구에서 데이터를 채워야 연동된다.")

        # ── 4행: SOX/SPX 상대강도 (반도체 리딩 신호) ──
        if _sox_spx_s is not None and len(_sox_spx_s) > 60:
            st.markdown(f"<div style='height:14px'></div><div style='font-size:var(--mac-fs-md);color:{_m};margin:6px 0 4px;font-weight:600'>📊 SOX/SPX 상대강도 (반도체 리딩 신호)</div>", unsafe_allow_html=True)
            _rs_tail = _sox_spx_s.iloc[-252:]
            _fig_rs = go.Figure()
            _fig_rs.add_trace(go.Scatter(x=_rs_tail.index, y=_rs_tail.values, mode="lines",
                line=dict(color=C["orange"], width=2), name="SOX/SPX"))
            _lyt = _ly("SOX / SPX 1Y", 260)
            _fig_rs.update_layout(**_lyt)
            st.plotly_chart(_fig_rs, use_container_width=True, key="chart_rs")
            _rs_now = float(_rs_tail.iloc[-1]); _rs_avg = float(_rs_tail.mean())
            _rs_diff = (_rs_now / _rs_avg - 1) * 100
            _rs_msg = "반도체 강세 (위험선호 ON)" if _rs_diff > 2 else "반도체 약세 (위험회피)" if _rs_diff < -2 else "중립"
            _rs_c = C["green"] if _rs_diff > 2 else C["red"] if _rs_diff < -2 else _m
            st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{_rs_c};text-align:center;margin-top:-8px'>현재 1Y평균 대비 {_rs_diff:+.2f}% — {_rs_msg}</div>", unsafe_allow_html=True)

        # V3.6: 쌍발 엔진 export (다른 탭과 동일 패턴)
        st.markdown("---")
        _eh1, _eh2 = st.columns(2)
        with _eh1:
            _jbtn(export_engines, "engines", "📥 쌍발 엔진 JSON", "_eng")
        with _eh2:
            st.download_button("📥 쌍발 엔진 HTML",
                _export_html(export_engines, section="쌍발 엔진").encode("utf-8"),
                f"engines_{_ds}.html", "text/html", key="exp_eng_html")

    # ═══ TAB 2: 채권/금리 ═══
    with tabs[2]:
        easy_help(mode, HELP_TAB1)
        st.subheader(bsl("📈 채권이 형이다", mode))
        st.caption("형의 걱정은 동생의 미래다.")
        # 그래프 3종 모두 fd 딕셔너리 (DGS10/DGS2/DGS3MO=DTB3 매핑) 기반 코드 계산.
        # 메트릭 (3개월/2년/10년) 도 fd 동일 시리즈 → 그래프와 메트릭 일관성 보장.
        # 부호 통일: 모두 "장기물 - 단기물" 형태 → 양수 = 정상, 음수 = 역전 (3종 모두 동일 의미).
        sd_ = {}
        d10s = fd.get("DGS10"); d2s = fd.get("DGS2"); d3s = fd.get("DGS3MO")
        if d10s is not None and d2s is not None and len(d10s) > 0 and len(d2s) > 0:
            cm = d10s.index.intersection(d2s.index)
            if len(cm) > 0: sd_["10Y-2Y"] = d10s.loc[cm] - d2s.loc[cm]   # 10Y - 2Y
        if d10s is not None and d3s is not None and len(d10s) > 0 and len(d3s) > 0:
            cm = d10s.index.intersection(d3s.index)
            if len(cm) > 0: sd_["10Y-3M"] = d10s.loc[cm] - d3s.loc[cm]   # 10Y - 3M
        if d3s is not None and d2s is not None and len(d3s) > 0 and len(d2s) > 0:
            cm = d3s.index.intersection(d2s.index)
            if len(cm) > 0: sd_["2Y-3M"] = d2s.loc[cm] - d3s.loc[cm]     # 2Y - 3M (부호 통일)
        if sd_:
            cl = {"10Y-2Y": C["blue"], "10Y-3M": C["orange"], "2Y-3M": C["purple"]}
            # 그래프 위 ⓘ 툴팁 (스프레드 3종 미어캣 톤) — 각 _tip 을 별도 column 으로 격리해
            # st.markdown 한 호출 안에 nested HTML 이 markdown parser 에 의해 깨지는 문제 방지
            _spread_tip_keys = [s for s in ("10Y-2Y", "10Y-3M", "2Y-3M") if s in sd_]
            if _spread_tip_keys:
                st.markdown(
                    f"<div style='font-size:var(--mac-fs-sm);color:{C['muted']};margin:4px 0 2px'>"
                    f"📊 스프레드 3종 — 모두 <b>(장기물 − 단기물)</b>. "
                    f"<b style='color:{C['red']}'>0 밑 = 역전 = 위험</b>. "
                    f"각 라벨 옆 ⓘ 호버 — 미어캣 해설.</div>",
                    unsafe_allow_html=True,
                )
                _tip_cols = st.columns(len(_spread_tip_keys))
                for _ti, _spn in enumerate(_spread_tip_keys):
                    with _tip_cols[_ti]:
                        st.markdown(
                            f"<div style='font-size:var(--mac-fs-sm);font-weight:700;"
                            f"color:{cl[_spn]};text-align:center'>"
                            f"{_spn}{_tip(_SPREAD_HELP[_spn])}</div>",
                            unsafe_allow_html=True,
                        )
            _fig_bond = go.Figure()
            for n, s in sd_.items(): _fig_bond.add_trace(go.Scatter(x=s.index, y=s.values * 100, name=n, line=dict(color=cl.get(n, C["muted"]), width=1.5)))
            _fig_bond.add_hline(y=0, line_dash="dash", line_color=C["red"], opacity=0.5)
            _fig_bond.update_layout(**_ly("장단기금리차 (bp)", 350))
            st.plotly_chart(_fig_bond, use_container_width=True, key="chart_bond")
        gc = st.columns(4)
        gc[0].metric("3개월", f"{dgs3m:.2f}%" if dgs3m is not None else "—")
        gc[1].metric("2년", f"{dgs2:.2f}%" if dgs2 is not None else "—")
        gc[2].metric("10년", f"{dgs10:.2f}%" if dgs10 is not None else "—")
        gc[3].metric("FF금리", f"{ff:.2f}%" if ff is not None else "—")

        # ── 📊 금리곡선 상태 카드 (3 스프레드 부호 조합 → 6 상태 매칭) ──
        try:
            _t10y2y_now = float(sd_["10Y-2Y"].iloc[-1]) if "10Y-2Y" in sd_ else None
            _t10y3m_now = float(sd_["10Y-3M"].iloc[-1]) if "10Y-3M" in sd_ else None
            _t3m2y_now = float(sd_["2Y-3M"].iloc[-1]) if "2Y-3M" in sd_ else None
        except Exception:
            _t10y2y_now = _t10y3m_now = _t3m2y_now = None
        _cstate = _curve_state(_t10y2y_now, _t10y3m_now, _t3m2y_now)
        if _cstate is not None:
            _cs_label, _cs_struct, _cs_color_key, _cs_short, _cs_full = _cstate
            _cs_color = C.get(_cs_color_key, C["muted"])
            st.markdown(
                f"<div style='background:{C['card']};border:1px solid {C['border']};"
                f"border-left:3px solid {_cs_color};border-radius:8px;padding:12px 16px;margin-top:14px'>"
                f"<div style='font-size:var(--mac-fs-md);color:{C['muted']};font-weight:600;margin-bottom:4px'>"
                f"📊 금리곡선 상태{_tip(_cs_full)}</div>"
                f"<div style='font-size:var(--mac-fs-h3);color:{_cs_color};font-weight:700;margin:4px 0 2px'>"
                f"{_cs_label} <span style='color:{C['muted']};font-weight:500;font-size:var(--mac-fs-md)'>"
                f"({_cs_struct})</span></div>"
                f"<div style='font-size:var(--mac-fs-sm);color:{C['text']};line-height:1.5;margin-top:6px'>"
                f"{_cs_short}</div></div>",
                unsafe_allow_html=True,
            )

        # ── 심화 관찰 카드: 장기채/크레딧/인플레/채권변동성 (단독 관찰용, 스코어 미편입) ──
        st.markdown("---")
        st.markdown(f"<div style='font-size:var(--mac-fs-md);color:{C['muted']};margin:4px 0 10px;font-weight:600'>📊 심화 관찰 — 단독 지표 (스코어·사계절·프로토콜 미연동)</div>", unsafe_allow_html=True)

        def _bond_pct(s, v):
            try:
                if s is None or v is None: return None
                a = np.asarray(s.values, dtype=float); a = a[~np.isnan(a)]
                if len(a) == 0: return None
                return float((a < v).sum()) / len(a) * 100
            except: return None

        # 카드 A: 장기채 절대 수준 (DGS20, DGS30)
        _dgs20_s = fd.get("DGS20"); _dgs30_s = fd.get("DGS30")
        _dgs20_v = L(_dgs20_s) if _dgs20_s is not None else None
        _dgs30_v = L(_dgs30_s) if _dgs30_s is not None else None
        _fig_long = None
        _d20_lbl, _d20_col, _d20_key = j_dgs_abs(_dgs20_v)
        _d30_lbl, _d30_col, _d30_key = j_dgs_abs(_dgs30_v)
        _d20_cmt = _BOND_SENTINEL_COMMENTS.get(("dgs20", _d20_key), "") if _d20_key else ""
        _d30_cmt = _BOND_SENTINEL_COMMENTS.get(("dgs30", _d30_key), "") if _d30_key else ""
        st.markdown(f"<div style='font-size:var(--mac-fs-h3);color:{C['text']};font-weight:700;margin:6px 0 2px'>A. 장기채 절대 수준 (20Y · 30Y)</div>", unsafe_allow_html=True)
        st.markdown(
            f"<div style='color:{C['muted']};font-size:var(--mac-fs-sm);margin:2px 0 10px;line-height:1.5'>"
            f"<b style='color:{C['text']}'>DGS20</b>{_tip(_TIP_DGS20_LONG)} {_DESC_DGS20}<br>"
            f"<b style='color:{C['text']}'>DGS30</b>{_tip(_TIP_DGS30_LONG)} {_DESC_DGS30}"
            f"</div>", unsafe_allow_html=True)
        cA = st.columns([1, 1, 2])
        cA[0].metric("DGS20", f"{_dgs20_v:.2f}%" if _dgs20_v is not None else "—")
        cA[0].markdown(f"<div style='color:{_d20_col};font-size:var(--mac-fs-md);font-weight:700;margin-top:-6px'>{_d20_lbl}</div>", unsafe_allow_html=True)
        cA[1].metric("DGS30", f"{_dgs30_v:.2f}%" if _dgs30_v is not None else "—")
        cA[1].markdown(f"<div style='color:{_d30_col};font-size:var(--mac-fs-md);font-weight:700;margin-top:-6px'>{_d30_lbl}</div>", unsafe_allow_html=True)
        _pct20 = _bond_pct(_dgs20_s, _dgs20_v); _pct30 = _bond_pct(_dgs30_s, _dgs30_v)
        _pct_txt_a = []
        if _pct20 is not None: _pct_txt_a.append(f"20Y 분위 {_pct20:.0f}%")
        if _pct30 is not None: _pct_txt_a.append(f"30Y 분위 {_pct30:.0f}%")
        cA[2].markdown(f"<div style='color:{C['muted']};font-size:var(--mac-fs-sm);padding-top:18px;line-height:1.5'>임계: 4.0 / 4.5 / 5.0 %<br>5Y · {' · '.join(_pct_txt_a) if _pct_txt_a else '—'}</div>", unsafe_allow_html=True)
        # 거시 코멘트 (센티넬 슬롯이 채워져 있을 때만 표시)
        if _d20_cmt:
            st.markdown(f"<div style='background:{C['card']};border-left:3px solid {_d20_col};border-radius:4px;padding:8px 12px;margin:8px 0 4px;font-size:var(--mac-fs-sm);color:{C['text']};line-height:1.5'><b>DGS20</b> — {_d20_cmt}</div>", unsafe_allow_html=True)
        if _d30_cmt:
            st.markdown(f"<div style='background:{C['card']};border-left:3px solid {_d30_col};border-radius:4px;padding:8px 12px;margin:4px 0 8px;font-size:var(--mac-fs-sm);color:{C['text']};line-height:1.5'><b>DGS30</b> — {_d30_cmt}</div>", unsafe_allow_html=True)
        _has_long = (_dgs20_s is not None and len(_dgs20_s) > 0) or (_dgs30_s is not None and len(_dgs30_s) > 0)
        if _has_long:
            _fig_long = go.Figure()
            if _dgs20_s is not None and len(_dgs20_s) > 0:
                _fig_long.add_trace(go.Scatter(x=_dgs20_s.index, y=_dgs20_s.values, name="DGS20", line=dict(color=C["teal"], width=1.5)))
            if _dgs30_s is not None and len(_dgs30_s) > 0:
                _fig_long.add_trace(go.Scatter(x=_dgs30_s.index, y=_dgs30_s.values, name="DGS30", line=dict(color=C["orange"], width=2.0, dash="dash")))
            for _th, _col in [(4.0, C["red"]), (4.5, C["orange"]), (5.0, C["green"])]:
                _fig_long.add_hline(y=_th, line_dash="dot", line_color=_col, opacity=0.4,
                                    annotation_text=f"{_th:.1f}%", annotation_position="right")
            _fig_long.update_layout(**_ly("장기채 금리 5Y (%)", 280))
            st.plotly_chart(_fig_long, use_container_width=True, key="chart_long")
        else:
            st.caption("⚠️ DGS20/DGS30 시리즈 로딩 실패")

        # 카드 B: 크레딧 스프레드 (HY OAS, BAMLH0A0HYM2)
        _hy_s = fd.get("HY")
        _hy_v_bp = (hy * 100) if hy is not None else None
        _fig_hy = None
        st.markdown(f"<div style='font-size:var(--mac-fs-h3);color:{C['text']};font-weight:700;margin:14px 0 2px'>B. 크레딧 스프레드 (HY OAS)</div>", unsafe_allow_html=True)
        cB = st.columns([1, 1, 2])
        cB[0].metric("HY OAS", f"{_hy_v_bp:.0f}bp" if _hy_v_bp is not None else "—")
        if _hy_v_bp is not None:
            if _hy_v_bp >= 800: _hy_lbl, _hy_c = "신용경색", C["red"]
            elif _hy_v_bp >= 500: _hy_lbl, _hy_c = "위험 인식", C["orange"]
            elif _hy_v_bp >= 400: _hy_lbl, _hy_c = "경계", C["gold"]
            else: _hy_lbl, _hy_c = "평온", C["green"]
        else:
            _hy_lbl, _hy_c = "—", C["muted"]
        cB[1].markdown(f"<div style='color:{_hy_c};font-size:var(--mac-fs-md);font-weight:700;padding-top:20px'>{_hy_lbl}</div>", unsafe_allow_html=True)
        _hy_bp_s = (_hy_s * 100) if (_hy_s is not None and len(_hy_s) > 0) else None
        _hy_pct = _bond_pct(_hy_bp_s, _hy_v_bp) if _hy_bp_s is not None else None
        cB[2].markdown(f"<div style='color:{C['muted']};font-size:var(--mac-fs-sm);padding-top:18px'>임계: 400 / 500 / 800 bp · {('분위 '+f'{_hy_pct:.0f}%') if _hy_pct is not None else '—'}</div>", unsafe_allow_html=True)
        if _hy_s is not None and len(_hy_s) > 0:
            _fig_hy = go.Figure()
            _fig_hy.add_trace(go.Scatter(x=_hy_s.index, y=_hy_s.values * 100, name="HY OAS", line=dict(color=C["red"], width=1.5)))
            for _th, _col in [(400, C["gold"]), (500, C["orange"]), (800, C["red"])]:
                _fig_hy.add_hline(y=_th, line_dash="dash", line_color=_col, opacity=0.5,
                                  annotation_text=f"{_th}bp", annotation_position="right")
            _fig_hy.update_layout(**_ly("HY OAS 5Y (bp)", 280))
            st.plotly_chart(_fig_hy, use_container_width=True, key="chart_hy")
        else:
            st.caption("⚠️ HY OAS 시리즈 로딩 실패")

        # 카드 C: 인플레 기대 (T10YIE)
        _tie_s = fd.get("T10YIE")
        _tie_v = L(_tie_s) if _tie_s is not None else None
        _fig_tie = None
        _ti_lbl, _ti_c, _ti_key = j_t10yie(_tie_v)
        _ti_cmt = _BOND_SENTINEL_COMMENTS.get(("t10yie", _ti_key), "") if _ti_key else ""
        st.markdown(f"<div style='font-size:var(--mac-fs-h3);color:{C['text']};font-weight:700;margin:14px 0 2px'>C. 10년 인플레 기대 (T10YIE){_tip(_TIP_T10YIE_LONG)}</div>", unsafe_allow_html=True)
        st.markdown(f"<div style='color:{C['muted']};font-size:var(--mac-fs-sm);margin:2px 0 10px;line-height:1.5'>{_DESC_T10YIE}</div>", unsafe_allow_html=True)
        cC = st.columns([1, 1, 2])
        cC[0].metric("T10YIE", f"{_tie_v:.2f}%" if _tie_v is not None else "—")
        cC[1].markdown(f"<div style='color:{_ti_c};font-size:var(--mac-fs-md);font-weight:700;padding-top:20px'>{_ti_lbl}</div>", unsafe_allow_html=True)
        _tie_pct = _bond_pct(_tie_s, _tie_v) if _tie_s is not None else None
        cC[2].markdown(f"<div style='color:{C['muted']};font-size:var(--mac-fs-sm);padding-top:18px;line-height:1.5'>임계: 2.0 / 2.5 / 2.8 %<br>5Y · {('분위 '+f'{_tie_pct:.0f}%') if _tie_pct is not None else '—'}</div>", unsafe_allow_html=True)
        if _ti_cmt:
            st.markdown(f"<div style='background:{C['card']};border-left:3px solid {_ti_c};border-radius:4px;padding:8px 12px;margin:8px 0;font-size:var(--mac-fs-sm);color:{C['text']};line-height:1.5'>{_ti_cmt}</div>", unsafe_allow_html=True)
        if _tie_s is not None and len(_tie_s) > 0:
            _fig_tie = go.Figure()
            _fig_tie.add_trace(go.Scatter(x=_tie_s.index, y=_tie_s.values, name="T10YIE", line=dict(color=C["teal"], width=1.5)))
            for _th, _col in [(2.0, C["green"]), (2.5, C["gold"]), (2.8, C["red"])]:
                _fig_tie.add_hline(y=_th, line_dash="dash", line_color=_col, opacity=0.5,
                                   annotation_text=f"{_th:.1f}%", annotation_position="right")
            _fig_tie.update_layout(**_ly("10Y BEI 5Y (%)", 280))
            st.plotly_chart(_fig_tie, use_container_width=True, key="chart_tie")
        else:
            st.caption("⚠️ T10YIE 시리즈 로딩 실패")

        # 카드 D: 채권 변동성 (MOVE)
        _move_s = yd.get("MOVE")
        _move_v = L(_move_s) if _move_s is not None else None
        _move_src = "yfinance ^MOVE" if _move_s is not None and len(_move_s) > 0 else "실패"
        _fig_move = None
        _mv_lbl, _mv_c, _mv_key = j_move(_move_v)
        _mv_cmt = _BOND_SENTINEL_COMMENTS.get(("move", _mv_key), "") if _mv_key else ""
        st.markdown(f"<div style='font-size:var(--mac-fs-h3);color:{C['text']};font-weight:700;margin:14px 0 2px'>D. 채권 변동성 (MOVE){_tip(_TIP_MOVE_LONG)}</div>", unsafe_allow_html=True)
        st.markdown(f"<div style='color:{C['muted']};font-size:var(--mac-fs-sm);margin:2px 0 10px;line-height:1.5'>{_DESC_MOVE}</div>", unsafe_allow_html=True)
        cD = st.columns([1, 1, 2])
        cD[0].metric("MOVE", f"{_move_v:.1f}" if _move_v is not None else "—")
        cD[1].markdown(f"<div style='color:{_mv_c};font-size:var(--mac-fs-md);font-weight:700;padding-top:20px'>{_mv_lbl}</div>", unsafe_allow_html=True)
        _move_pct = _bond_pct(_move_s, _move_v) if _move_s is not None else None
        cD[2].markdown(f"<div style='color:{C['muted']};font-size:var(--mac-fs-sm);padding-top:18px;line-height:1.5'>임계: 80 / 100 / 120<br>5Y · {('분위 '+f'{_move_pct:.0f}%') if _move_pct is not None else '—'} · 소스: {_move_src}</div>", unsafe_allow_html=True)
        if _mv_cmt:
            st.markdown(f"<div style='background:{C['card']};border-left:3px solid {_mv_c};border-radius:4px;padding:8px 12px;margin:8px 0;font-size:var(--mac-fs-sm);color:{C['text']};line-height:1.5'>{_mv_cmt}</div>", unsafe_allow_html=True)
        if _move_s is not None and len(_move_s) > 0:
            _fig_move = go.Figure()
            _fig_move.add_trace(go.Scatter(x=_move_s.index, y=_move_s.values, name="MOVE", line=dict(color=C["orange"], width=1.5)))
            for _th, _col in [(80, C["green"]), (100, C["gold"]), (120, C["red"])]:
                _fig_move.add_hline(y=_th, line_dash="dash", line_color=_col, opacity=0.5,
                                    annotation_text=f"{_th}", annotation_position="right")
            _fig_move.update_layout(**_ly("MOVE 5Y", 280))
            st.plotly_chart(_fig_move, use_container_width=True, key="chart_move")
        else:
            st.caption("⚠️ MOVE 지수 로딩 실패 (yfinance ^MOVE / 대체 티커 모두 실패). CBOE 사이트 직접 확인.")

        st.markdown("---")
        _jbtn(export_bond, "bond", "📥 채권/금리 JSON", "_bond")
        st.download_button("📥 채권/금리 HTML", _export_html(export_bond, section="채권/금리", charts=[(_fig_bond, "장단기금리차")] if _fig_bond else None).encode("utf-8"), f"bond_{_ds}.html", "text/html", key="exp_bond_html")

    # ═══ TAB 2: 밸류에이션 ═══
    with tabs[3]:
        easy_help(mode, HELP_TAB2)
        st.subheader(bsl("💰 밸류에이션 — 중력은 무시할 수 없다", mode))
        st.markdown("> 터지기 전은 28이었다. 60, 70은 터진 뒤다.")
        vs = vd.get("source", "없음")
        if vs != "없음": st.success(f"데이터 소스: **{vs}**", icon="📡")
        vc = st.columns(4)
        # 이중 레인지 헬퍼
        _VDR_EMOJI = {"green": "🟢", "neutral": "⚪", "red": "🔴"}
        def _val_dual_sub(val, vr_key):
            vr = VAL_RANGES.get(vr_key)
            if vr is None or val is None: return ""
            fl = _val_dual_level(val, vr["full"]); rl = _val_dual_level(val, vr["recent"])
            if fl is None or rl is None: return ""
            fe = _VDR_EMOJI.get(fl, "⚪"); re = _VDR_EMOJI.get(rl, "⚪")
            # 코멘트
            _prefix = {"shiller_cape": "cape", "forward_per": "fpe", "trailing_per": "tpe"}
            _ck = f"{_prefix.get(vr_key, vr_key)}_{fl}_{rl}"
            _cmt = _VAL_DUAL_COMMENT.get(_ck, "")
            _cmt_html = f"<br><span style='font-size:var(--mac-fs-xs);color:{_t}'>{_cmt}</span>" if _cmt else ""
            return (f"<div style='font-size:var(--mac-fs-xs);color:{_m};line-height:1.6;margin-top:4px'>"
                    f"100년: {fe}  |  20년: {re}{_cmt_html}</div>")
        with vc[0]:
            st.metric("Trailing PE", f"{tpe:.1f}" if tpe is not None else "—")
            _vs = _val_dual_sub(tpe, "trailing_per")
            if _vs: st.markdown(_vs, unsafe_allow_html=True)
        with vc[1]:
            st.metric("Forward PE", f"{fpe:.1f}" if fpe is not None else "—")
            _vs = _val_dual_sub(fpe, "forward_per")
            if _vs: st.markdown(_vs, unsafe_allow_html=True)
        with vc[2]:
            st.metric("CAPE", f"{cape:.1f}" if cape is not None else "—")
            _vs = _val_dual_sub(cape, "shiller_cape")
            if _vs: st.markdown(_vs, unsafe_allow_html=True)
        vc[3].metric("배당수익률", f"{dy:.2f}%" if dy is not None else "—")

        st.markdown("---")
        _jbtn(export_valuation, "valuation", "📥 밸류에이션 JSON", "_val")
        st.download_button("📥 밸류에이션 HTML", _export_html(export_valuation, section="밸류에이션").encode("utf-8"), f"valuation_{_ds}.html", "text/html", key="exp_val_html")

    # ═══ TAB 3: 반도체 ═══
    with tabs[4]:
        easy_help(mode, HELP_TAB3)
        st.subheader(bsl("🏭 반도체가 선행한다", mode))
        st.caption("기술을 공부하지 말고 밤하늘의 달을 보라.")
        if sox_s is not None and spx_s is not None and len(sox_s) > 0 and len(spx_s) > 0:
            cm = sox_s.index.intersection(spx_s.index)
            if len(cm) > 0:
                ratio = sox_s.loc[cm] / spx_s.loc[cm]; ma = ratio.rolling(20).mean()
                _fig_semi = go.Figure()
                _fig_semi.add_trace(go.Scatter(x=ratio.index, y=ratio.values, name="일간", line=dict(color=C["muted"], width=0.5), opacity=0.4))
                _fig_semi.add_trace(go.Scatter(x=ma.index, y=ma.values, name="20일 MA", line=dict(color=C["green"], width=2)))
                _fig_semi.update_layout(**_ly("SOX/SPX", 300))
                st.plotly_chart(_fig_semi, use_container_width=True, key="chart_semi")
        pds = {"1M": 21, "3M": 63, "6M": 126, "1Y": 252}; pc = st.columns(len(pds))
        for i, (lb2, days) in enumerate(pds.items()):
            sc_ = chg(yd.get("SOXX"), days); spc_ = chg(yd.get("SPX"), days)
            rel = (sc_ - spc_) if (sc_ is not None and spc_ is not None) else None
            with pc[i]:
                _v = f"{rel:+.1f}%p" if rel is not None else "—"
                st.metric(lb2, _v, "아웃퍼폼" if (rel and rel > 0) else "언더퍼폼" if rel else "—")

        st.markdown("---")
        _jbtn(export_semi, "semi", "📥 반도체 JSON", "_semi")
        st.download_button("📥 반도체 HTML", _export_html(export_semi, section="반도체", charts=[(_fig_semi, "SOX/SPX 비율")] if _fig_semi else None).encode("utf-8"), f"semi_{_ds}.html", "text/html", key="exp_semi_html")

    # ═══ TAB 4: 계절 판단 ═══
    with tabs[5]:
        easy_help(mode, HELP_TAB4)
        st.subheader(bsl("🌡️ 계절 판단", mode))
        st.caption("날씨보단 계절이 중요하다. 관찰이다. 개입이 아니다.")

        # 상단: V8 계절 판정 + 2층 (ANFCI / CAPE_pct)
        # V8 결과 추출 (히스테리시스 + prefix + conf 적용)
        _v8_res = evaluate_v651_today(offset=0) or {}
        _v_l2 = _v8_res.get("v8_layer2") or {}
        _anfci = _v_l2.get("anfci"); _anfci_label = _v_l2.get("anfci_label", "데이터 없음")
        _cape_pct = _v_l2.get("cape_pct"); _cape_label = _v_l2.get("cape_pct_label", "데이터 없음")
        _recov_pct = _v_l2.get("recovery_pct"); _recov_label = _v_l2.get("recovery_label", "데이터 없음")
        _anfci_color = "#ff4444" if (_anfci is not None and _anfci >= 1.0) else \
                       ("#ffaa00" if (_anfci is not None and _anfci >= 0.5) else \
                       ("#fcc41f" if (_anfci is not None and _anfci >= 0.0) else C["muted"]))
        _cape_color = "#ff4444" if (_cape_pct is not None and _cape_pct >= 90) else \
                      ("#ffaa00" if (_cape_pct is not None and _cape_pct >= 75) else \
                      ("#fcc41f" if (_cape_pct is not None and _cape_pct >= 50) else C["muted"]))
        _recov_color = "#22c55e" if (_recov_pct is not None and _recov_pct <= -10) else \
                       ("#fcc41f" if (_recov_pct is not None and _recov_pct <= -5) else \
                       ("#ff4444" if (_recov_pct is not None and _recov_pct > 0) else C["muted"]))
        _anfci_str = f"{_anfci:+.2f}" if _anfci is not None else "n/a"
        _cape_str = f"{_cape_pct:.0f}" if _cape_pct is not None else "n/a"
        _recov_str = f"{_recov_pct:+.1f}%" if _recov_pct is not None else "n/a"
        _tip_anfci = ("시카고 연준 ANFCI (Adjusted National Financial Conditions Index).\n"
                      "주식·채권·환·신용 105개 지표를 합성해서 '돈 빌리기 얼마나 어려운가' 를 점수화한다.\n"
                      "0 = 평균.   양수 = 긴축적 (대출 까다롭고 회사채 스프레드 벌어진다).   음수 = 완화적.\n"
                      "1.0 넘으면 금융 위기 수준. -0.5 미만이면 돈잔치.\n"
                      "주간 갱신, 1971+.")
        _tip_cape = ("Shiller CAPE (10년 평균 이익 PE) 의 직근 20년 분포 백분위.\n"
                     "100 = 20년 중 가장 비싸다.   50 = 중간.   0 = 20년 최저.\n"
                     "75% 넘으면 역사적 고평가, 90% 넘으면 거품권.\n"
                     "현재 비싸냐 싸냐를 묻는 거지 '이게 곧 빠진다' 는 신호 아니다 — 비싼 게 더 비싸질 수도 있다.")
        _tip_recov = ("미국 신규 실업급여 청구건수 (Initial Claims) 4주 평균이 직근 13주 정점 대비 얼마나 빠졌는가.\n"
                      "🟢 -10%↓ = 회복 진행 (해고 줄어드는 중)\n"
                      "🟡 -5%↓ = 회복 시작\n"
                      "⚪ 0 부근 = 정점 부근\n"
                      "🔴 양수 = 신규 청구 상승 중 (해고 늘어난다)\n"
                      "1층이 '겨울' 판정해도 이게 🟢 면 '바닥은 지났다' 신호. 봄 임박 보조 지표.")

        b1, b2 = st.columns([1, 2])
        with b1:
            _sb2 = (season_auto.lstrip("초늦") if season_auto else None) or "—"
            sc_ = SC.get(_sb2, C["gold"]) if _sb2 in SC else C["gold"]
            s_disp2 = season_label(season_auto, mode) if season_auto else "—"
            _si2 = _tip(_tip_season(season_auto)) if season_auto else ""
            st.markdown(f"""<div style="background:{C['card']};border:2px solid {sc_};border-radius:12px;padding:24px;text-align:center">
                <div style="font-size:var(--mac-fs-md);color:{C['muted']}">자동 판정</div>
                <div style="font-size:var(--mac-fs-display);font-weight:700;color:{sc_};margin:10px 0">{s_disp2}{_si2}</div>
                <div style="font-size:var(--mac-fs-md);color:{C['text']}">확신도: {season_conf}</div>
                <div style="font-size:var(--mac-fs-sm);color:{_anfci_color};margin-top:10px;font-weight:600">금융여건 (ANFCI){_tip(_tip_anfci)} {_anfci_str} · {_anfci_label}</div>
                <div style="font-size:var(--mac-fs-sm);color:{_cape_color};margin-top:4px;font-weight:600">밸류 (CAPE %ile){_tip(_tip_cape)} {_cape_str} · {_cape_label}</div>
                <div style="font-size:var(--mac-fs-sm);color:{_recov_color};margin-top:4px;font-weight:600">회복 신호 (ICSA){_tip(_tip_recov)} {_recov_str} · {_recov_label}</div>
                </div>""", unsafe_allow_html=True)
        with b2:
            # V8 분모 가변 (봄/가을 11, 여름/겨울 9)
            _v8_evals = {sn: len(V8_SEASON_BOXES[sn]) for sn in ("봄","여름","가을","겨울")}
            _fig_season = go.Figure()
            for sn in ["봄", "여름", "가을", "겨울"]:
                _on = int(season_scores.get(sn, 0))
                _tot = _v8_evals[sn]
                _fig_season.add_trace(go.Bar(x=[sn], y=[_on], marker_color=SC[sn],
                              text=[f"{_on}/{_tot}"], textposition="outside", name=sn))
            _fig_season.update_layout(**_ly("", 250), showlegend=False); _fig_season.update_yaxes(range=[0, 12])
            st.plotly_chart(_fig_season, use_container_width=True, key="chart_season")

        # ── V3.10.0 역사 매칭 카드 (top 3 풀 디테일 + 추이 + 사이클 게이지 + 차원 매트릭스) ──
        _hm_era     = _hist_match.get("era")
        _hm_label   = _hist_match.get("label")
        _hm_score   = _hist_match.get("score", 0.0) or 0.0
        _hm_matches = _hist_match.get("matches") or []
        _hm_common  = _hist_match.get("common_dims") or []
        _bd2 = C["border"]; _cd2 = C["card"]; _t2 = C["text"]; _m2 = C["muted"]
        if not _hm_era:
            st.markdown(f"""<div class="maccard" style="background:{_cd2};border:1px solid {_bd2};border-radius:8px;
                padding:14px 18px;border-left:3px solid {C['muted']};margin-top:14px">
                <div style="font-size:var(--mac-fs-h3);color:{C['muted']};font-weight:700;margin-bottom:8px">역사적 유사국면: 매칭 없음 (최고 {_hm_score*100:.0f}%, 임계 {ERA_MATCH_THRESHOLD*100:.0f}%)</div>
                <div style="font-size:var(--mac-fs-md);color:{_t2};line-height:1.5">깔끔하게 매칭되는 역사적 선례가 없다.<br>무리하게 비유하지 않는다. 차원을 분리해서 봐라.</div>
                </div>""", unsafe_allow_html=True)
        else:
            # 추이 데이터 (1/7/30/90일)
            _hm_trends = _history_match_trend_multi(OBS_JSONL, days_list=(1, 7, 30, 90))
            _trend_parts = []
            for _d in (1, 7, 30, 90):
                _t = _hm_trends.get(_d)
                if _t is None: continue
                _short = (_t["era"].split("_")[0]) if _t.get("era") else "—"
                _arrow = "→" if _t["era"] == _hm_era else "≠"
                _trend_parts.append(f"{_d}일 전 {_arrow} {_short}")
            _trend_msg = ""
            _trend_alert = ""
            if _trend_parts:
                _today_short = _hm_era.split("_")[0]
                _trend_msg = "📊 추이: " + " · ".join(_trend_parts) + f" · 오늘 {_today_short}"
                if _hm_trends.get(30) and _hm_trends[30]["era"] != _hm_era:
                    _trend_alert = "⚠️ 30일 전 era 전환 — 사이클 단계 진행 중"
                elif _hm_trends.get(90) and _hm_trends[90]["era"] != _hm_era:
                    _trend_alert = "📈 90일 전 era 전환 — 분기 단위 변화 진행"

            # 매칭률 막대 (HTML)
            _bar_rows = []
            _medals = ["🥇", "🥈", "🥉"]
            for _i, _m in enumerate(_hm_matches[:3]):
                _pct = (_m.get("score", 0) or 0) * 100
                _label_short = _m.get("label", "—")
                _bar_w = max(2, int(_pct))
                _bar_color = sc_ if _i == 0 else (C["gold"] if _i == 1 else C["muted"])
                _bar_rows.append(
                    f"<div style='display:flex;align-items:center;gap:8px;margin:4px 0;font-size:var(--mac-fs-sm)'>"
                    f"<div style='min-width:24px'>{_medals[_i]}</div>"
                    f"<div style='min-width:200px;color:{_t2}'>{_label_short}</div>"
                    f"<div style='flex:1;background:{_bd2};border-radius:3px;height:10px;position:relative'>"
                    f"<div style='background:{_bar_color};height:100%;width:{_bar_w}%;border-radius:3px'></div></div>"
                    f"<div style='min-width:48px;text-align:right;color:{_t2};font-weight:600'>{_pct:.0f}%</div>"
                    f"</div>"
                )
            _bars_html = "".join(_bar_rows)

            # 공통 차원 박스
            _common_html = ""
            if _hm_common:
                _common_ko = ", ".join([_DIM_KO.get(_d, _d) for _d in _hm_common])
                _common_html = (f"<div style='margin-top:10px;padding:8px 10px;background:rgba(46,204,113,0.08);"
                                f"border-left:3px solid {C['green']};border-radius:4px;font-size:var(--mac-fs-sm)'>"
                                f"<span style='color:{C['green']};font-weight:700'>✅ 셋 다 공유하는 차원:</span> "
                                f"<span style='color:{_t2}'>{_common_ko}</span>"
                                f"<div style='font-size:var(--mac-fs-xs);color:{_m2};margin-top:2px'>"
                                f"→ 네가 보고 있는 건 이 차원들이 만드는 공통 패턴이다.</div></div>")

            # 헤더
            st.markdown(f"<div style='font-size:var(--mac-fs-h3);color:{C['bright']};font-weight:700;margin-top:14px'>🔍 역사적 유사국면 (top 3 거리 매칭)</div>", unsafe_allow_html=True)
            if _trend_msg:
                st.caption(_trend_msg)
            if _trend_alert:
                st.caption(_trend_alert)
            st.markdown(_bars_html, unsafe_allow_html=True)
            if _common_html:
                st.markdown(_common_html, unsafe_allow_html=True)

            # ─── 1위 풀 디테일 ───
            _m1 = _hm_matches[0]
            _m1_dur = _m1.get("historical_duration_days") or 0
            _m1_type = _m1.get("era_type", "period")
            _m1_consec = _era_consecutive_days(OBS_JSONL, _m1.get("era_id"))
            _m1_unmatched = _m1.get("unmatched_dims", []) or []
            _m1_cmt_h = (_m1.get("comment", "") or "").replace("\n", "<br>")
            _m1_qt = _m1.get("quote") or ""
            _m1_color = sc_

            # 사이클 진행도 게이지 (era_type 별 분기)
            if _m1_type == "moment":
                _cycle_html = (f"<div style='font-size:var(--mac-fs-sm);color:{_m2};margin-top:8px'>"
                               f"📍 정점/바닥형 — 단일 시점. 현재 1위 {_m1_consec}일째 · "
                               f"역사적으로 ~{_m1_dur}일 형성기 후 다음 단계 전환.</div>")
            elif _m1_type == "event":
                _pct = min(100, int(_m1_consec / _m1_dur * 100)) if _m1_dur else 0
                _cycle_html = (f"<div style='font-size:var(--mac-fs-sm);color:{_m2};margin-top:8px'>"
                               f"⚡ 이벤트형 — 단기 충격 ({_m1_dur}일). 현재 {_m1_consec}일째 · "
                               f"<span style='color:{_t2};font-weight:600'>{_pct}% 진행</span></div>")
            else:  # period
                _pct = min(100, int(_m1_consec / _m1_dur * 100)) if _m1_dur else 0
                _bar_filled = int(_pct / 5)
                _bar = "█" * _bar_filled + "░" * (20 - _bar_filled)
                _cycle_html = (f"<div style='font-size:var(--mac-fs-sm);color:{_m2};margin-top:8px;font-family:monospace'>"
                               f"🔄 기간형 — <span style='color:{_t2}'>{_bar}</span> "
                               f"<span style='color:{_t2};font-weight:600'>{_pct}%</span> · "
                               f"현재 {_m1_consec}일 / 역사 {_m1_dur}일</div>")

            # V3.11.0 DTW 진행도 (era 안정성 측정 — fastdtw 가용 시)
            _dtw_html = ""
            try:
                _dtw_long = _load_long_range_series(api_key)
                _dtw_curr = _build_raw_data_for_backfill(fd, yd)
                _dtw_res = measure_era_progress(_m1.get("era_id"), _dtw_long, _dtw_curr) if _DTW_AVAILABLE else None
                if _dtw_res and _dtw_res.get("progress_pct") is not None:
                    _pp = _dtw_res["progress_pct"]; _cd = _dtw_res.get("current_day_in_era") or 0
                    _td = _dtw_res.get("total_era_days") or 0; _conf = _dtw_res.get("confidence")
                    _ns = _dtw_res.get("series_used_count", 0)
                    _used = _dtw_res.get("series_used_names") or []
                    _miss = _dtw_res.get("series_missing_names") or []
                    _dbar = "█" * int(_pp/5) + "░" * (20 - int(_pp/5))
                    _conf_emoji = "🎯" if _conf == "high" else "📊"
                    _conf_ko = "높음" if _conf == "high" else "중간"
                    _era_obj = next((e for e in ERA_LIBRARY if e.get("id") == _m1.get("era_id")), {})
                    _hs = _era_obj.get("hist_start", "?"); _he = _era_obj.get("hist_end", "?")
                    _used_str = ", ".join(_used) if _used else "(없음)"
                    _miss_str = ", ".join(_miss) if _miss else "(없음)"
                    _miss_block = (
                        f"<div style='font-size:var(--mac-fs-xs);color:{_m2};margin-top:1px;font-family:monospace'>"
                        f"   └─ 부재: {_miss_str}</div>"
                    ) if _miss else ""
                    _dtw_html = (
                        f"<div style='margin-top:10px;padding:8px 12px;background:rgba(52,152,219,0.06);"
                        f"border-left:3px solid {C['blue']};border-radius:4px'>"
                        f"<div style='font-size:var(--mac-fs-md);color:{C['blue']};font-weight:700;margin-bottom:4px'>"
                        f"🎯 DTW 진행도: {_cd}일째 / {_td}일 ({_pp:.0f}%)</div>"
                        f"<div style='font-family:monospace;color:{_t2};font-size:var(--mac-fs-sm)'>{_dbar} {_pp:.0f}%</div>"
                        f"<div style='font-size:var(--mac-fs-xs);color:{_m2};margin-top:4px'>"
                        f"{_conf_emoji} 정확도: {_conf_ko} · {_ns}/8 시계열 사용</div>"
                        f"<div style='font-size:var(--mac-fs-xs);color:{_m2};margin-top:1px;font-family:monospace'>"
                        f"   ├─ 사용: {_used_str}</div>"
                        f"{_miss_block}"
                        f"<div style='font-size:var(--mac-fs-xs);color:{_m2};margin-top:4px'>"
                        f"역사 era: {_hs} → {_he}</div></div>"
                    )
                elif _dtw_res:
                    _reason = _dtw_res.get("reason") or "데이터 부족"
                    _used_names = _dtw_res.get("series_used_names") or []
                    _miss_names = _dtw_res.get("series_missing_names") or []
                    _used_str = (", ".join(_used_names)) if _used_names else "(없음)"
                    _miss_str = (", ".join(_miss_names)) if _miss_names else "(없음)"
                    _dtw_html = (
                        f"<div style='margin-top:10px;padding:6px 10px;background:rgba(128,128,128,0.05);"
                        f"border-left:3px solid {C['muted']};border-radius:4px'>"
                        f"<div style='font-size:var(--mac-fs-sm);color:{_m2}'>"
                        f"🎯 DTW 진행도: 측정 불가 <span style='font-size:var(--mac-fs-xs)'>({_reason})</span></div>"
                        f"<div style='font-size:var(--mac-fs-xs);color:{_m2};margin-top:1px;font-family:monospace'>"
                        f"   ├─ 가용: {_used_str}</div>"
                        f"<div style='font-size:var(--mac-fs-xs);color:{_m2};margin-top:1px;font-family:monospace'>"
                        f"   └─ 부재: {_miss_str}</div></div>"
                    )
                elif not _DTW_AVAILABLE:
                    _dtw_html = (
                        f"<div style='margin-top:10px;padding:6px 10px;background:rgba(128,128,128,0.05);"
                        f"border-left:3px solid {C['muted']};border-radius:4px'>"
                        f"<div style='font-size:var(--mac-fs-sm);color:{_m2}'>"
                        f"🎯 DTW 진행도: fastdtw 미설치 — install.bat 재실행</div></div>"
                    )
            except Exception as _dtwe:
                _dtw_html = ""

            _cycle_html = _cycle_html + _dtw_html

            _unmatched_html = ""
            if _m1_unmatched:
                _diff_ko = ", ".join([_DIM_KO.get(_d, _d) for _d in _m1_unmatched])
                _unmatched_html = (f"<div style='font-size:var(--mac-fs-sm);color:{_m2};margin-top:8px'>"
                                   f"<span style='color:{C['red']};font-weight:700'>❌ 다른 점:</span> {_diff_ko}"
                                   f"<div style='font-size:var(--mac-fs-xs);color:{_m2};margin-top:2px'>"
                                   f"이 차원들이 매칭률을 100% → {_m1.get('score',0)*100:.0f}% 로 깎았다.</div></div>")

            _qt_html = (f"<div style='font-size:var(--mac-fs-sm);color:{_m2};font-style:italic;margin-top:8px;"
                        f"padding-top:8px;border-top:1px solid {_bd2};line-height:1.4'>“{_m1_qt}”</div>") if _m1_qt else ""

            st.markdown(f"""<div class="maccard" style="background:{_cd2};border:1px solid {_bd2};border-radius:8px;
                padding:14px 18px;border-left:3px solid {_m1_color};margin-top:14px">
                <div style="font-size:var(--mac-fs-h3);color:{_m1_color};font-weight:700;margin-bottom:8px">
                🥇 1위: {_m1.get("label","—")} <span style='color:{_t2};font-weight:500'>({_m1.get("score",0)*100:.0f}% · {_m1.get("years_ago","?")}년 전)</span></div>
                <div style="font-size:var(--mac-fs-md);color:{_t2};line-height:1.5">{_m1_cmt_h}</div>
                {_qt_html}
                <div style='margin-top:10px;padding-top:8px;border-top:1px dashed {_bd2}'>
                    <div style='font-size:var(--mac-fs-sm);color:{_t2}'>
                        <span style='color:{C["bright"]};font-weight:700'>🔚 그 후:</span> {_m1.get("aftermath","—")}
                    </div>
                </div>
                {_cycle_html}{_unmatched_html}</div>""", unsafe_allow_html=True)

            # ─── 2위 / 3위 요약 ───
            for _idx, _m in enumerate(_hm_matches[1:3], start=2):
                _medal_i = _medals[_idx-1]
                _summary_color = C["gold"] if _idx == 2 else C["muted"]
                _m_dur = _m.get("historical_duration_days") or 0
                _m_first_line = (_m.get("comment", "") or "").split("\n")[0]
                _m_unmatched = _m.get("unmatched_dims", []) or []
                _diff_short = ""
                if _m_unmatched:
                    _diff_ko_short = ", ".join([_DIM_KO.get(_d, _d) for _d in _m_unmatched[:3]])
                    _diff_short = (f"<div style='font-size:var(--mac-fs-xs);color:{_m2};margin-top:4px'>"
                                   f"다른 점: {_diff_ko_short}</div>")
                st.markdown(f"""<div class="maccard" style="background:{_cd2};border:1px solid {_bd2};border-radius:8px;
                    padding:10px 14px;border-left:3px solid {_summary_color};margin-top:8px">
                    <div style="font-size:var(--mac-fs-md);color:{_summary_color};font-weight:700">
                    {_medal_i} {_idx}위: {_m.get("label","—")} <span style='color:{_t2};font-weight:500'>({_m.get("score",0)*100:.0f}% · {_m.get("years_ago","?")}년 전)</span></div>
                    <div style="font-size:var(--mac-fs-sm);color:{_m2};margin-top:4px">{_m_first_line}</div>
                    <div style='font-size:var(--mac-fs-sm);color:{_t2};margin-top:4px'>
                        <span style='color:{_m2}'>그 후:</span> {_m.get("aftermath","—")}
                    </div>{_diff_short}</div>""", unsafe_allow_html=True)

        st.markdown("---")
        st.caption("V8.0 40박스 — 봄 11 / 여름 9 / 가을 11 / 겨울 9. 가장 많이 켜진 계절이 지금 계절.")

        # 체크리스트 읽기 전용 표시 (V8 분모 가변)
        _g = C["green"]; _r = C["red"]; _m = C["muted"]; _cd = C["card"]; _bd = C["border"]; _br = C["bright"]
        c1, c2 = st.columns(2)
        season_icons = {"봄": "🌸", "여름": "☀️", "가을": "🍂", "겨울": "❄️"}
        season_subs = {"봄": SL.get("봄", "금융장세"), "여름": SL.get("여름", "실적장세"),
                       "가을": SL.get("가을", "역금융장세"), "겨울": SL.get("겨울", "역실적장세")}
        order = ["봄", "가을", "여름", "겨울"]  # 왼쪽: 봄+가을, 오른쪽: 여름+겨울
        for sn in order:
            items = season_checks.get(sn, [])
            sc_col = SC[sn]; icon = season_icons[sn]; sub = season_subs[sn]
            cnt = int(season_scores.get(sn, 0))
            tot = len(V8_SEASON_BOXES.get(sn, []))
            target_col = c1 if sn in ["봄", "가을"] else c2
            with target_col:
                st.markdown(f"**{icon} {sn}** ({cnt}/{tot}) — <span style='color:{sc_col}'>{sub}</span>", unsafe_allow_html=True)
                for label, val in items:
                    check = "✅" if val else "⬜"
                    color = _g if val else _m
                    _help = _SEASON_BOX_HELP.get((sn, label))
                    _help_i = _tip(_help) if _help else ""
                    st.markdown(f"<span style='color:{color};font-size:var(--mac-fs-md)'>{check} {label}{_help_i}</span>", unsafe_allow_html=True)
                st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        bc1, bc2 = st.columns(2)
        with bc1:
            if st.button("💾 계절 판단 저장"):
                sstate({"date": datetime.now().strftime("%Y-%m-%d"), "season": season_auto, "confidence": season_conf, "scores": season_scores, "mac_score": gs})
                st.success("state.json 저장 완료.")
        with bc2:
            _jbtn(export_season_d, "season", "📥 계절 판단 JSON", "_season")
            st.download_button("📥 계절 HTML", _export_html(export_season_d, section="계절 판단", charts=[(_fig_season, "계절 점수")] if _fig_season else None).encode("utf-8"), f"season_{_ds}.html", "text/html", key="exp_season_html")

        # ─── 📐 이 판정기에 대해 (설계 / 검증 / 신뢰도 / 한계) ───
        st.markdown("---")
        with st.expander("📐 이 판정기에 대해 — 설계 · 검증 · 신뢰도 · 한계"):
            st.markdown(f"""
<div style='font-size:var(--mac-fs-sm); color:{C['text']}; line-height:1.65'>

**설계 과정**
92개 거시 박스 후보 풀에서 ablation × grid search × 답지 채점 4단계 통과 후 최종 40박스. 봄 11 + 여름 9 + 가을 11 + 겨울 9. 모든 박스는 raw count, 가중치 없음. 가산/override/PA 가드 전면 폐기. 단일 임계가 단일 박스를 결정한다.

**검증**
1980-01 ~ 2024-12 일별 풀 백테스트 11,740 영업일. 5시점 광기 검증 (1999, 2007, 2021, 2024) 통과. 90일 라벨 안정성 평균 3.84 flip / 중간값 2 / P75 6 — 분기당 2회 라벨 변동.

**GT 80 채점 결과 (1980-2024 답지 80건, 2026-04-29 동률 룰 + 전이 규칙 개정 후)**

| 항목 | 값 |
|---|---|
| raw 정확률 | **75.75%** |
| confidence-weighted | **75.12%** |
| 정확 (1.0) | 54 / 80 (67.5%) |
| 인접 오답 (0.3) | 22 / 80 |
| 반대 오답 (0.0) | 4 / 80 |

| 계절 | 정확률 | 비고 |
|---|---|---|
| 여름 | 16/18 (88.9%) | 매우 높음 |
| 겨울 | 11/15 (73.3%) | 높음 |
| 가을 | 20/32 (62.5%) | 보통 (2층 ANFCI/CAPE 보조 시 80%+) |
| 봄 | 7/15 (46.7%) | 구조적 한계 (전이 규칙 + 회복 신호 축으로 보완) |

**봄 정확률이 낮은 이유**
봄 GT = 침체 바닥 회복기. 침체 바닥에서 동시점 거시 지표 (HY 5%+, 실업률 폭증, VIX 30+, 200dma 하회) 는 실제로 겨울 박스를 점등시킨다. 박스 카운트 시스템이 derivative (방향) 보다 level (수준) 에 가중되는 구조적 한계. 박스 추가 4가지 방안 모두 봄 +0~+2 / 다른 계절 회귀로 trade-off 한계 도달. **전이 규칙** (직전 30일 가을/겨울 → 현재 여름 + CAPE 20y %ile < 50 → 봄 override) 으로 얕은 침체 봄 일부 detection. 회복 신호 (ICSA) 2층 축이 "겨울이지만 바닥 지났다" 추가 보완.

**객관성 (외부 기관 지표 활용)**
ANFCI = 시카고 연준 105개 거시 합성 (1971+ 주간, 자체 정규화). CFNAI = 시카고 연준 (1967+ 월간). CAPE = Shiller 데이터셋 (1881+, multpl + Top10 가중). VIX = CBOE. HY OAS = ICE BofA. ICSA = 미 노동부 신규 실업급여 청구건수 (1967+ 주간). 자체 정규화 / 합성 점수 만들지 않는다 — 외부 공인 기관이 이미 정규화한 걸 그대로 쓴다.

**2층 — 부가 정보 (ANFCI · CAPE · 회복 신호)**
1층 박스가 "지금 상태"를 측정한다면 2층은 "위험과 방향"을 보여준다. **금융여건 (ANFCI)** = 자금줄 긴축도. **밸류 (CAPE %ile)** = 역사적 고/저평가 위치. **회복 신호 (ICSA)** = 신규 실업급여 청구 4주 평균이 직근 13주 정점 대비 얼마나 떨어졌는가 (음수일수록 회복 진행). 합산 안 한다 — 세 축 독립 표시. 1층이 "겨울" 판정해도 회복 신호 🟢 (정점 후 −10%↓) 면 "바닥 지났을 수 있다"고 사용자가 직접 해석한다. 봄 GT (침체 바닥 회복기) 시점은 박스 카운트 시스템 구조상 겨울로 보일 수밖에 없는데, 이 한계를 회복 신호 축으로 보완한다 — 시스템이 봄을 단정하는 게 아니라 재료를 주고 사용자가 판단한다.

**robust 한 점**
동률 시 사이클 진행 우선 (봄=여름→여름, 여름=가을→가을, 가을=겨울→겨울). 봄=겨울 동률은 겨울 우선 (침체 진행 중 — GT 검증 3건 모두 정답). 봄=가을 비인접 동률은 봄 우선 (회복 우세 — GT 1건). 전이 규칙 (방안 5): 직전 30일 best 가 가을/겨울 + 현재 여름 + CAPE 20y %ile < 50 → "봄" override (V자 반등 false 차단). 히스테리시스 margin 1.5 — 직전 1일 anchor + margin 미달 시 라벨 유지. 디스크 캐시 24h. 결측 박스는 분모에서 빠짐 (None 처리). fpe 영구 폐기 후 cape + tpe 만 사용 — backtest 일관성 보장.

**라벨 표기 (초/늦 prefix)**
4계절 × prefix(초/늦/없음) = 사이클 8단계 어휘. **best (단독 우세)** + **prefix (인접 강세)** 의 조합으로 사이클 단계를 단일 단어로 표현.

| 라벨 | 의미 | 발생 조건 |
|---|---|---|
| **초가을** | 가을 막 진입 | best=가을, 직전 (여름) score 인접 ≥ 33% |
| **늦여름** | 여름 끝물, 가을 임박 | best=여름, 직후 (가을) score 인접 ≥ 33% |
| **초여름** | 여름 막 진입 | best=여름, 직전 (봄) score 인접 ≥ 33% |
| **늦봄** | 봄 끝물, 여름 임박 | best=봄, 직후 (여름) score 인접 ≥ 33% |
| (그 외) | 단독 봄/여름/가을/겨울 | 인접 모두 < 33% |

**중요 — best 와 prefix 는 항상 일관:**
- "초가을" = best **가을** (시스템 결론은 가을). prefix 는 "여름이 막 끝났다" 보조 정보.
- "늦여름" = best **여름** (시스템 결론은 여름). prefix 는 "가을 임박" 보조 정보.
- 동률 (예: 여름=가을 4=4) → tiebreak 으로 가을 채택 → 라벨은 **"초가을"** 이지 "늦여름" 아님. **인지적 부조화 없음.**

**prefix 임계 (V69 비율 4/12 = 33% 그대로):**
- 봄/가을 (분모 11) → 인접 ≥ 4 (36%)
- 여름/겨울 (분모 9) → 인접 ≥ 3 (33%)

prefix 는 **사이클 진행 단계 보조 표현**이지 시스템 결론 (best) 자체가 아님. 박스 점수 + best 결정이 본질, prefix 는 단지 base 라벨 옆에 붙는 한국어 어휘.

**한계**
1995 이전 시점 = 박스 평가 가능 수 < 30 (전체 40 중) → 신뢰도 낮음. fpe historical 시계열 부재. prefix(초/늦) 룰은 V69 비율 그대로 적용 — 답지 검증 안 거침, 직관 보조용. F8 반사성 / B등급 (실적 / 부채/GDP / 배당수익률) 박스 보류. **봄 47% 한계는 박스 카운트 시스템의 구조적 floor** — 4가지 박스 보강 방안 (S_CROSS 게이트화, S_ICSA1 1층 승격, S_DIFF1 4지표 확산, S_DUAL 이중 임계) 시도 결과 모두 trade-off 발생 (봄 +N 시 다른 계절 -M). 회복 신호 2층 + 전이 규칙으로 보완.

**권하지 않는 사용**
단기 매매 시그널. 단일 박스 점등으로 전환 판단. 1995 이전 시점 backtest 결과 절대 신뢰. prefix(초/늦)를 시점 진단 핵심 기준으로 사용. 박스 점수가 절대 임계가 아니라 분포 통과 임계라는 점 잊으면 안 된다.

</div>""", unsafe_allow_html=True)

    # ═══ TAB 5: 관찰 기록 ═══
    with tabs[6]:
        easy_help(mode, HELP_TAB5)
        st.subheader(bsl("📋 관찰 기록", mode))
        od = st.date_input("관찰일", value=datetime.now().date(), key="od")
        # 자동 판정 결과를 기본값으로 (season_auto None-safe)
        season_list = ["봄", "여름", "가을", "겨울"]
        auto_idx = 0
        if season_auto:
            for i, s in enumerate(season_list):
                if s in season_auto: auto_idx = i; break
        os_ = st.selectbox("계절 (자동 판정 기반)", season_list, index=auto_idx, key="os")
        ob = st.text_area("근거", height=80, key="ob")
        gss = f"{gs:.0f}" if gs is not None else "?"
        _t2y = f"{t10y2y*100:.0f}" if t10y2y is not None else "?"
        _dxy = f"{dxy:.1f}" if dxy is not None else "?"
        _vix = f"{vix:.1f}" if vix is not None else "?"
        _hy = f"{hy*100:.0f}" if hy is not None else "?"
        _ff = f"{ff:.2f}" if ff is not None else "?"
        _krw = f"{krw:,.0f}" if krw is not None else "?"
        _wti = f"{wti:.1f}" if wti is not None else "?"
        rec = f"[{od}] {os_} ({ob or '—'}) | 거시:{gss}/100 | 2Y10Y:{_t2y}bp DXY:{_dxy} VIX:{_vix} HY:{_hy}bp FF:{_ff}% KRW:{_krw} WTI:{_wti}"
        with st.expander("미리보기"): st.code(rec)
        c1_, c2_ = st.columns(2)
        with c1_:
            if st.button("💾 저장"):
                try:
                    sobs(json.dumps({"date": str(od), "season": os_, "record": rec, "score_version": VERSION, "mac_score": gs}, ensure_ascii=False, indent=2), str(od))
                    st.success("저장.")
                except Exception as _save_err:
                    st.error(f"관측 저장 실패: {type(_save_err).__name__}: {_save_err}")
        with c2_: st.download_button("📥", rec.encode("utf-8"), f"obs_{od}.txt", "text/plain")
        st.markdown("---")
        if _obs_all:
            st.download_button("📥 관찰 기록 일괄 JSON", json.dumps(_obs_all, ensure_ascii=False, indent=2).encode("utf-8"), f"observations_{_ds}.json", "application/json", key="exp_obs_tab")
        for f in lobs()[:12]:
            try:
                d = json.loads(f.read_text("utf-8"))
                with st.expander(f"{d.get('date','?')} — {d.get('season','?')}"): st.code(d.get("record", ""))
            except: pass

    # ═══ TAB 7: 시계열 (V3.5 신규) ═══
    with tabs[7]:
        st.subheader(bsl("📈 시계열 — 시간축으로 본 프레임워크", mode))
        st.caption("3개월 전 거시가 몇이었고, 그때 클러스터 괴리도가 어땠고, 사계절이 뭐였는지. 위치보다 속도, 속도보다 가속도. 같은 점수라도 어디서 왔느냐가 다르다.")

        _hist_all = mac_history_load()
        if not _hist_all:
            st.info("아직 히스토리가 없다. 매일 한 번씩 앱을 켜면 자동으로 누적된다. 최소 7일은 쌓여야 의미가 생긴다.")
        else:
            _hist_n = len(_hist_all)
            # 기간 선택
            _per_map = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "All": None}
            _per_keys = list(_per_map.keys())
            _per_default = "3M" if _hist_n >= 30 else "All"
            _per_idx = _per_keys.index(_per_default)
            _per_sel = st.radio("기간", _per_keys, index=_per_idx, horizontal=True, key="ts_period")
            _per_days = _per_map[_per_sel]

            _today_d = _date.today()
            if _per_days is None:
                _hist = list(_hist_all)
            else:
                _hist = [h for h in _hist_all if (_today_d - _date.fromisoformat(h["date"])).days <= _per_days]

            if not _hist:
                st.warning(f"선택 기간({_per_sel})에 데이터가 없다. All로 바꿔봐라.")
            elif len(_hist) < 2:
                st.warning(f"선택 기간({_per_sel})에 데이터가 {len(_hist)}일치뿐이다. 추세를 그릴 수 없다. 누적 일수: {_hist_n}일.")
            else:
                _dates = [_date.fromisoformat(h["date"]) for h in _hist]
                _gss   = [h.get("score") for h in _hist]
                _mks   = [h.get("mk") for h in _hist]
                _divs  = [h.get("divergence") for h in _hist]
                _seas  = [h.get("season") for h in _hist]
                _CL_NAMES = ["채권/금리", "밸류에이션", "스트레스", "실물", "반도체"]
                _cl_series = {n: [] for n in _CL_NAMES}
                for h in _hist:
                    _cl = h.get("clusters") or {}
                    for n in _CL_NAMES:
                        _cl_series[n].append(_cl.get(n))

                # ── 사계절 배경 밴드 (연속 구간 묶기) ──
                _SEASON_FILL = {"봄": "rgba(76,175,80,0.10)", "여름": "rgba(255,193,7,0.10)",
                                "가을": "rgba(255,138,0,0.12)", "겨울": "rgba(33,150,243,0.10)"}
                def _season_base(s):
                    if not s: return None
                    for k in ["봄", "여름", "가을", "겨울"]:
                        if k in s: return k
                    return None
                _bands = []  # (start_date, end_date, season_base)
                _cur_s = None; _cur_start = None
                for d, s in zip(_dates, _seas):
                    sb = _season_base(s)
                    if sb != _cur_s:
                        if _cur_s is not None and _cur_start is not None:
                            _bands.append((_cur_start, d, _cur_s))
                        _cur_s = sb; _cur_start = d
                if _cur_s is not None and _cur_start is not None:
                    _bands.append((_cur_start, _dates[-1], _cur_s))

                # ── 메인 차트: 거시 + 미어캣 + 괴리도 ──
                _shapes = []
                for _bs, _be, _sn in _bands:
                    if _sn is None: continue
                    _shapes.append(dict(type="rect", xref="x", yref="paper",
                                        x0=_bs, x1=_be, y0=0, y1=1,
                                        fillcolor=_SEASON_FILL.get(_sn, "rgba(128,128,128,0.05)"),
                                        line=dict(width=0), layer="below"))
                _fig_ts = go.Figure()
                # V3.5 구 공식 데이터 — 회색 점선으로 병기 (기간 필터 적용)
                _hist_v35 = mac_history_v35_load()
                _dates_v35 = []; _gss_v35 = []; _mks_v35 = []
                if _hist_v35:
                    if _per_days is None:
                        _hist_v35_f = list(_hist_v35)
                    else:
                        _hist_v35_f = [h for h in _hist_v35
                                       if (_today_d - _date.fromisoformat(h["date"])).days <= _per_days]
                    if _hist_v35_f:
                        _dates_v35 = [_date.fromisoformat(h["date"]) for h in _hist_v35_f]
                        _gss_v35   = [h.get("score") for h in _hist_v35_f]
                        _mks_v35   = [h.get("mk") for h in _hist_v35_f]
                        _fig_ts.add_trace(go.Scatter(x=_dates_v35, y=_gss_v35, name="거시 (V3.5)",
                                                     mode="lines", line=dict(color="rgba(150,150,150,0.55)", width=1.3, dash="dot")))
                        if any(v is not None for v in _mks_v35):
                            _fig_ts.add_trace(go.Scatter(x=_dates_v35, y=_mks_v35, name="미어캣 (V3.5)",
                                                         mode="lines", line=dict(color="rgba(120,120,120,0.45)", width=1.1, dash="dot")))
                _fig_ts.add_trace(go.Scatter(x=_dates, y=_gss, name="거시", mode="lines+markers",
                                             line=dict(color=C["gold"], width=2.5), marker=dict(size=4)))
                if any(v is not None for v in _mks):
                    _fig_ts.add_trace(go.Scatter(x=_dates, y=_mks, name="미어캣", mode="lines+markers",
                                                 line=dict(color=C["blue"], width=2.5, dash="dot"), marker=dict(size=4)))
                if any(v is not None for v in _divs):
                    _fig_ts.add_trace(go.Scatter(x=_dates, y=_divs, name="클러스터 괴리도", mode="lines",
                                                 line=dict(color=C["muted"], width=1.5, dash="dash"), yaxis="y2"))
                # 공식 개편 경계선 (VERSION_STARTED 가 기간 범위 안에 있으면 표시)
                try:
                    _vstart = _date.fromisoformat(VERSION_STARTED)
                    _x_lo = min(_dates + _dates_v35) if _dates_v35 else _dates[0]
                    _x_hi = max(_dates + _dates_v35) if _dates_v35 else _dates[-1]
                    if _x_lo <= _vstart <= _x_hi:
                        _fig_ts.add_vline(x=_vstart, line=dict(color="rgba(200,80,80,0.55)", width=1.2, dash="dash"),
                                          annotation_text=f"공식 개편 (3.5 → {VERSION})", annotation_position="top",
                                          annotation_font_size=10, annotation_font_color="rgba(200,80,80,0.9)")
                except Exception:
                    pass
                # V3.10.1: era 전환점 marker overlay (top 1 era 변화 시점에 점선)
                try:
                    if _obs_df is not None and "history_era_top1" in _obs_df.columns:
                        _ets = _obs_df[["ts", "history_era_top1"]].dropna().sort_values("ts").reset_index(drop=True)
                        if len(_ets) > 1:
                            _ets["chg"] = _ets["history_era_top1"] != _ets["history_era_top1"].shift(1)
                            _changes = _ets[_ets["chg"]].iloc[1:]  # 첫 행은 default True 라 skip
                            for _, _row in _changes.iterrows():
                                _fig_ts.add_vline(
                                    x=_row["ts"],
                                    line=dict(color="rgba(150,80,150,0.45)", width=1, dash="dot"),
                                    annotation_text=f"≈ {str(_row['history_era_top1']).split('_')[0]}",
                                    annotation_position="top",
                                    annotation_font_size=9,
                                    annotation_font_color="rgba(150,80,150,0.85)",
                                )
                except Exception:
                    pass
                _ly_ts = _ly("거시 / 미어캣 + 사계절 밴드", 380)
                _ly_ts["shapes"] = _shapes
                _ly_ts["yaxis"] = dict(title="스코어 (0~100)", range=[0, 100], gridcolor=C["border"])
                _ly_ts["yaxis2"] = dict(title="괴리도", overlaying="y", side="right", showgrid=False, range=[0, 100])
                _ly_ts["legend"] = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                _fig_ts.update_layout(**_ly_ts)
                st.plotly_chart(_fig_ts, use_container_width=True, key="chart_ts")

                # ── 사계절 범례 ──
                _lg_html = "<div style='display:flex;gap:14px;font-size:var(--mac-fs-sm);color:" + C["muted"] + ";margin:-6px 0 12px'>"
                for k in ["봄", "여름", "가을", "겨울"]:
                    _lg_html += f"<span><span style='display:inline-block;width:14px;height:10px;background:{_SEASON_FILL[k]};border:1px solid {C['border']};vertical-align:middle;margin-right:4px'></span>{k}</span>"
                _lg_html += "</div>"
                st.markdown(_lg_html, unsafe_allow_html=True)

                # ── 5클러스터 차트: obs.jsonl 기반 (date 별 last ts) + history 폴백 union ──
                # mac_score_history 짧을 때 obs 의 cluster_* 누적이 훨씬 풍부 (방문마다 1행)
                _CL_OBS_KEYS = {"채권/금리": "cluster_bond", "밸류에이션": "cluster_val",
                                "스트레스": "cluster_stress", "실물": "cluster_real", "반도체": "cluster_semi"}
                _cl_dates_set = set(_dates)  # history 의 date 부터 시작
                _cl_obs_byday = {n: {} for n in _CL_NAMES}  # name → {date: value}
                # 1) history (V3.6) 의 cluster 값
                for h in _hist:
                    _cl_h = h.get("clusters") or {}
                    _d_h = _date.fromisoformat(h["date"]) if h.get("date") else None
                    if _d_h is None: continue
                    for n in _CL_NAMES:
                        v = _cl_h.get(n)
                        if v is not None:
                            _cl_obs_byday[n][_d_h] = v
                # 2) obs.jsonl 의 cluster_* (date별 마지막 ts)
                try:
                    _obs_cl_df = _hist_load_obs_df()
                    if _obs_cl_df is not None and not _obs_cl_df.empty:
                        _ts_col = pd.to_datetime(_obs_cl_df["ts"])
                        if hasattr(_ts_col.dt, "tz_localize") and getattr(_ts_col.dt, "tz", None) is not None:
                            _ts_col = _ts_col.dt.tz_localize(None)
                        _obs_clw = _obs_cl_df.assign(_ts=_ts_col, _date=_ts_col.dt.date)
                        # 기간 필터
                        if _per_days is not None:
                            _cut = _today_d - pd.Timedelta(days=_per_days).to_pytimedelta()
                            _obs_clw = _obs_clw[_obs_clw["_date"] >= _cut]
                        # date 별 마지막 ts 행
                        _obs_clw = _obs_clw.sort_values("_ts").groupby("_date").tail(1)
                        for _, row in _obs_clw.iterrows():
                            _d = row["_date"]; _cl_dates_set.add(_d)
                            for n, ck in _CL_OBS_KEYS.items():
                                if ck in _obs_clw.columns:
                                    v = row.get(ck)
                                    if v is not None and not (isinstance(v, float) and pd.isna(v)):
                                        # history 값이 있으면 history 우선 (이미 _cl_obs_byday 에 들어감)
                                        _cl_obs_byday[n].setdefault(_d, float(v))
                except Exception:
                    pass

                # 모든 date 에 대해 정렬된 시리즈 만들기
                _cl_dates_sorted = sorted(_cl_dates_set)
                _cl_series_full = {n: [_cl_obs_byday[n].get(d) for d in _cl_dates_sorted] for n in _CL_NAMES}

                if any(any(v is not None for v in _cl_series_full[n]) for n in _CL_NAMES):
                    _CL_COL = {"채권/금리": C["blue"], "밸류에이션": C["gold"], "스트레스": C["red"],
                               "실물": C["green"], "반도체": "#9C27B0"}
                    _fig_cl = go.Figure()
                    for n in _CL_NAMES:
                        if any(v is not None for v in _cl_series_full[n]):
                            _fig_cl.add_trace(go.Scatter(x=_cl_dates_sorted, y=_cl_series_full[n], name=n,
                                                         mode="lines+markers",
                                                         line=dict(color=_CL_COL.get(n, C["muted"]), width=2),
                                                         marker=dict(size=3)))
                    _ly_cl = _ly(f"5클러스터 점수 추이 ({len(_cl_dates_sorted)}일)", 320)
                    _ly_cl["shapes"] = _shapes
                    _ly_cl["yaxis"] = dict(title="클러스터 점수 (0~100)", range=[0, 100], gridcolor=C["border"])
                    _ly_cl["legend"] = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    _fig_cl.update_layout(**_ly_cl)
                    st.plotly_chart(_fig_cl, use_container_width=True, key="chart_cl")
                else:
                    st.caption("5클러스터 추이는 V3.5부터 누적된다. 며칠 더 기다려라.")

                # ── 요약 지표 ──
                _g_first = next((v for v in _gss if v is not None), None)
                _g_last  = next((v for v in reversed(_gss) if v is not None), None)
                _m_first = next((v for v in _mks if v is not None), None)
                _m_last  = next((v for v in reversed(_mks) if v is not None), None)
                sm = st.columns(4)
                with sm[0]: st.metric("기간", f"{_per_sel} ({len(_hist)}일)")
                with sm[1]:
                    if _g_first is not None and _g_last is not None:
                        st.metric("거시 변화", f"{_g_last:.0f}", f"{_g_last - _g_first:+.1f}")
                    else: st.metric("거시 변화", "—")
                with sm[2]:
                    if _m_first is not None and _m_last is not None:
                        st.metric("미어캣 변화", f"{_m_last:.0f}", f"{_m_last - _m_first:+.1f}")
                    else: st.metric("미어캣 변화", "—")
                with sm[3]: st.metric("총 누적 일수", f"{_hist_n}일")

                # ── CSV 다운로드 ──
                _csv_lines = ["date,mac,mk,divergence,season," + ",".join(_CL_NAMES)]
                for h in _hist:
                    _row = [h.get("date", ""),
                            f"{h.get('score'):.2f}" if h.get("score") is not None else "",
                            f"{h.get('mk'):.2f}" if h.get("mk") is not None else "",
                            f"{h.get('divergence'):.2f}" if h.get("divergence") is not None else "",
                            h.get("season") or ""]
                    _cl = h.get("clusters") or {}
                    for n in _CL_NAMES:
                        _v = _cl.get(n)
                        _row.append(f"{_v:.2f}" if _v is not None else "")
                    _csv_lines.append(",".join(_row))
                st.download_button("📥 시계열 CSV", ("\n".join(_csv_lines)).encode("utf-8-sig"),
                                   f"mac_timeseries_{_ds}.csv", "text/csv", key="exp_ts_csv")

        # ── V3.7 원본 + 관측 히스토리 ──────────────────────────────────
        st.markdown("---")
        st.markdown(bsl("#### 📊 원본 + 관측 히스토리 (V3.7)", mode))
        st.caption("원본 지표(FRED/yfinance)는 최초 1회 백필 후 방문마다 최근값 머지. 가공 지표(거시/미어캣/계절)는 앱 방문마다 스냅샷이 쌓인다. "
                   "내가 본 날만 점이 찍힌다 — 자주 봤으면 공식 차트와 비슷해지고, 드물게 봤으면 점점이 흩어진다.")

        # 재백필 버튼
        _bf_cols = st.columns([1, 1, 2])
        with _bf_cols[0]:
            if st.button("🔄 재백필 (실패분만)", key="hist_rebackfill_partial",
                         help="마커에 성공으로 기록되지 않은 시리즈만 다시 시도. 가볍다."):
                try:
                    _r = _hist_backfill_once(api_key)
                    _hist_load_raw_df.clear()
                    st.success(f"재백필 완료 — FRED {_r[0]}/{_r[0]+_r[1]}, YF {_r[2]}/{_r[2]+_r[3]}, {_r[4]:.1f}s")
                except Exception as _e: st.error(f"재백필 실패: {_e}")
        with _bf_cols[1]:
            if st.button("🧨 전체 강제 재백필", key="hist_rebackfill_full",
                         help="마커 무시, 모든 시리즈를 처음부터 다시 받는다. 수십 초 걸린다."):
                try:
                    _r = _hist_backfill_once(api_key, force_fred=RAW_FRED_IDS, force_yf=RAW_YF_TICKERS)
                    _hist_load_raw_df.clear()
                    st.success(f"전체 재백필 완료 — FRED {_r[0]}/{_r[0]+_r[1]}, YF {_r[2]}/{_r[2]+_r[3]}, {_r[4]:.1f}s")
                except Exception as _e: st.error(f"전체 재백필 실패: {_e}")

        _raw_df = _hist_load_raw_df()
        _obs_df = _hist_load_obs_df()

        # 원본(raw) + 관측 스냅샷(obs) 통합 탐색기
        _raw_cols = [c for c in _raw_df.columns if _raw_df[c].notna().any()] if (_raw_df is not None and not _raw_df.empty) else []
        _obs_cols = [c for c in _obs_df.columns if c not in ("ts","date","version","cluster_max","cluster_min","season","season_base","season_prefix","matrix_quadrant","matrix_key") and _obs_df[c].notna().any()] if (_obs_df is not None and not _obs_df.empty) else []

        if not _raw_cols and not _obs_cols:
            st.info("원본·관측 히스토리가 모두 비어 있다. 위의 재백필 버튼을 눌러라.")
        else:
            if True:
                # 한국명 + 쉬운모드 상세 설명 매핑
                _RAW_KO = {
                    # 금리/국채
                    "DGS2":   ("미국 2년 국채금리", "단기 국채 금리. 시장이 예상하는 연준 2년치 금리 경로를 반영한다."),
                    "DGS10":  ("미국 10년 국채금리", "장기 국채 금리. 성장·물가·기간프리미엄이 섞여 있는 세계 기준금리."),
                    "T10Y3M": ("10년-3개월 금리차(bp)", "장단기 금리차. 마이너스면 '역전' = 시장이 침체를 가격에 반영 중."),
                    "T10Y2Y": ("10년-2년 금리차(bp)", "가장 전통적인 장단기 금리차. 역전→정상화 구간이 고비."),
                    "FEDFUNDS": ("연방기금금리(FF)", "미국 기준금리. 모든 자산 가격의 할인율 뿌리."),
                    "T5YIE":  ("5년 기대 인플레이션", "TIPS-명목 차이로 산출. 시장이 향후 5년간 기대하는 물가 상승률."),
                    # 위험/스트레스
                    "VIXCLS": ("VIX 공포지수 (FRED)", "S&P500 옵션의 30일 내재변동성. 20↑=긴장, 30↑=공포."),
                    "^VIX":   ("VIX 공포지수 (yfinance)", "같은 VIX, yfinance 소스. FRED와 T+1 차이로 더 빠르다."),
                    "BAMLH0A0HYM2": ("하이일드 스프레드(HY)", "정크본드 금리-국채 금리. 벌어지면 신용시장이 비명을 지르는 중."),
                    "DRCCLACBS": ("상업은행 연체율", "미국 은행권 대출 연체율. 실물 스트레스 후행지표."),
                    # 고용/물가
                    "UNRATE": ("실업률(%)", "U-3 실업률. 4% 이하는 완전고용, 5% 돌파는 침체 신호."),
                    "PAYEMS": ("비농업 고용자수(천명)", "NFP. 매달 첫 금요일 발표. 전월 대비 증감이 진짜 지표."),
                    "JTSJOL": ("구인 건수(JOLTS)", "기업이 구인 광고를 낸 자리 수. 고용 수요의 선행지표."),
                    "UNEMPLOY": ("실업자 수(천명)", "U-3 실업자 절대값. JOLTS와 같은 단위라 격차(구인-실업자) 계산용."),
                    "CPIAUCSL": ("소비자물가(CPI)", "헤드라인 CPI. 전년 대비(YoY)가 연준이 보는 숫자."),
                    "CPILFESL": ("근원 CPI", "식품·에너지 제외. 연준이 진짜 보는 끈끈한 물가."),
                    "PCEPI":  ("소비자지출물가(PCE)", "연준의 공식 물가 지표. CPI보다 살짝 낮게 나오는 편."),
                    "PCEPILFE": ("근원 PCE", "식품·에너지 제외 PCE. 연준 2% 타깃의 실제 측정 대상."),
                    # 실물/심리
                    "GDP":    ("명목 GDP ($십억)", "미국 전체 GDP. 분기 발표. 추세 파악용."),
                    "UMCSENT": ("미시간 소비자심리", "소비자가 느끼는 경기 체감. 설문 기반 선행지표."),
                    "CFNAI":  ("시카고 연준 활동지수", "85개 월간 지표 가중평균. -0.7 이하=침체 확률 급상승."),
                    "DCOILWTICO": ("WTI 유가 (FRED)", "서부텍사스 중질유 현물. FRED 소스, 하루 지연."),
                    "CL=F":   ("WTI 유가 선물 (yfinance)", "WTI 근월물 선물. 실시간 가까운 가격."),
                    # 외환/원자재
                    "DEXKOUS": ("원/달러 (FRED)", "한국은행 고시 환율, 일일. FRED 소스."),
                    "KRW=X":  ("원/달러 (yfinance)", "실시간 KRW/USD. FRED보다 빠르다."),
                    "DX-Y.NYB": ("달러 인덱스(DXY)", "6개 주요통화 대비 달러 강도. 100 기준, 높으면 달러 강세."),
                    "GC=F":   ("금 선물", "COMEX 금 선물. 달러·실질금리의 반대편 자산."),
                    # 밸류에이션
                    "NCBEILQ027S": ("비금융기업 주식 부채($백만)", "Z.1 보고서. 버핏 지표(/GDP) 계산의 분자."),
                    "GFDEGDQ188S": ("연방정부 부채/GDP(%)", "미국 정부부채 비율. 재정 건전성 척도."),
                    # V3.10.5 SPY 배당수익률 시계열 (yfinance dividends + price 합성)
                    "DIVIDEND_YIELD": ("S&P500 배당수익률 (%)", "SPY trailing 12M dividends / SPY price × 100. 1993~ 일별. 1.5% 밑은 2000년뿐이었다."),
                    "SPY":    ("S&P500 ETF (SPY)", "SPDR S&P500. 배당 시계열 합성용 + 일별 가격."),
                    # 주식/ETF
                    "^GSPC":  ("S&P 500 지수", "미국 대형주 500. 시장의 기준선."),
                    "QQQ":    ("나스닥 100 ETF", "QQQ. 미국 대형 기술주 바스켓."),
                    "SOXX":   ("반도체 ETF(SOXX)", "iShares 반도체. NVDA/TSM/AVGO 등 30종."),
                    "TQQQ":   ("TQQQ (QQQ ×3)", "나스닥 100 일간 3배 레버리지. 우리 본 전장."),
                    "SOXL":   ("SOXL (SOXX ×3)", "반도체 일간 3배 레버리지. 쌍발 엔진의 두 번째."),
                    "XLE":    ("에너지 섹터 ETF", "XLE. 엑손·셰브런 등 대형 에너지주."),
                    "XLK":    ("기술 섹터 ETF", "XLK. 애플·MS·엔비디아 중심 기술주."),
                    "VOO":    ("S&P500 ETF (VOO)", "Vanguard S&P500. 미국 대형주 500. SPY·IVV와 동일 기초지수, 수수료 최저. '문지기' 용도."),
                    "SGOV":   ("초단기 국채 ETF (SGOV)", "iShares 0-3M Treasury Bond. 달러 파킹용 초안전 ETF. 배당으로 현금 수익률 반영."),
                    # V3.10.1 역사 매칭 시각화 pseudo keys
                    "__era_score_overlay":  ("📈 top 1/2/3 매칭률 추이",   "1위/2위/3위 era 매칭률을 한 차트에. 1·2위 격차가 좁아지면 전환기."),
                    "__era_timeline":       ("🎨 1위 era 변화 타임라인",  "어느 시점에 어느 era 가 1위였는지. 사이클 단계 진행 추적용."),
                    "__era_dim_overlay":    ("📊 차원별 매칭 추이 (10)",  "10개 차원 (계절·FF·밸류·신용·반도체 등) 매칭 여부 stacked 시계열."),
                    "__era_heatmap":        ("🔥 era 안정성 heatmap",     "1위 점유 era 의 매칭률 시간 분포. 색 길면 안정, 띄엄띄엄이면 노이즈."),
                    "__seasonal_overlay":   ("📅 월별 누적수익률 비교",   "최근 N년 1월~12월 누적 수익률 overlay. 올해가 평년 대비 어떤지."),
                    "__seasonal_heatmap":   ("🗓 월별 수익률 히트맵",     "최근 N년 월별 수익률 매트릭스. 어느 월이 강하고 약한지."),
                    # === 관측소 지표들 (가공, obs) ===
                    "mac_score":     ("거시 스코어", "거시 환경이 얼마나 사기 좋은가. 0~100, 높을수록 우호적."),
                    "meerkat_score": ("미어캣 스코어", "내 계좌 포지션이 얼마나 사기 좋은 상태인가. 현금비중·DD 등 반영."),
                    "mac_velocity":  ("거시 속도", "거시 스코어의 30일 변화량. 위치보다 속도, 속도보다 가속도."),
                    # === 클러스터 (가공, obs) ===
                    "cluster_bond":   ("클러스터: 채권/금리", "장단기금리차·FF·HY 등 채권시장 건강도. 0~100."),
                    "cluster_val":    ("클러스터: 밸류에이션", "Forward PE·CAPE·버핏 지표. 낮을수록 싸다."),
                    "cluster_stress": ("클러스터: 스트레스", "VIX·HY스프레드·연체율. 공포 신호 모음."),
                    "cluster_real":   ("클러스터: 실물", "실업률·GDP·CFNAI. 경기 펀더멘털."),
                    "cluster_semi":   ("클러스터: 반도체", "SOX/SPX·KRW·WTI. 한국 주식 본진."),
                    "cluster_divergence": ("클러스터 괴리도", "5개 클러스터 점수의 표준편차. 크면 시장이 쪼개져 있다는 신호."),
                    # === 계절 (가공, obs) ===
                    "season_confidence":   ("계절 확신도", "사계절 판정의 확신도. '판정 불가'~'매우 높음' 6단계."),
                    "season_score_spring": ("계절: 봄 점수", "'봄' 조건 9개 중 몇 개 충족. 0~9."),
                    "season_score_summer": ("계절: 여름 점수", "'여름' 조건 9개 중 몇 개 충족. 0~9."),
                    "season_score_autumn": ("계절: 가을 점수", "'가을' 조건 9개 중 몇 개 충족. 0~9."),
                    "season_score_winter": ("계절: 겨울 점수", "'겨울' 조건 9개 중 몇 개 충족. 0~9."),
                    "season_base":         ("계절 base", "사계절 자동 판정 (접두사 제외 base — 봄/여름/가을/겨울)."),
                    "season_prefix":       ("계절 접두사", "초/늦/없음 — 인접 계절 점수 ≥4 시 부착."),
                    "season_max_score":    ("계절 박스 총수", "박스 개수 메타 (5박스 시절 호환성). V3.8부터 9."),
                    # === V3.8.2 노동격차 + 공급충격 디커플링 ===
                    "labor_gap_K":     ("노동격차 (천명)", "JOLTS 구인 - UNEMPLOY 실업자. 양수=임금압력 / 음수=침체 진입."),
                    "labor_gap_label": ("노동격차 라벨", "과열 / 정상 / 균형 / 침체 진입."),
                    "wti_3m_pct":      ("WTI 3M (%)", "WTI 64거래일 변화율."),
                    "spx_3m_pct":      ("SPX 3M (%)", "SPX 64거래일 변화율."),
                    "shock_state":     ("공급충격 상태", "조용 / 선행 신호 / 박스 발동 직전 / 충격 활성 / 지정학 충격."),
                    "shock_box_on":    ("가을 #7 박스 ON", "WTI 3M > 15 AND SPX 3M < 0 충족 여부."),
                    # === 공포/밸류 (백필 불가 원본, obs) ===
                    "fear_greed":  ("Fear & Greed", "CNN 공포탐욕지수. 0(극공포)~100(극탐욕)."),
                    "forward_pe":  ("Forward PE", "향후 12개월 예상 EPS 기준 PER. 낮을수록 싸다(고 여겨진다)."),
                    "trailing_pe": ("Trailing PE", "지난 12개월 실적 기준 PER. 경기 변동에 민감."),
                    "cape":        ("CAPE (Shiller)", "10년 평균 EPS 기준 PER. 장기 밸류 버블 판정용."),
                    "dividend_yield": ("배당수익률 (%)", "S&P500 배당수익률. 낮을수록 비싸다 — 1.5% 밑은 2000년만 있었다."),
                    "qqq_dd_52w":  ("QQQ 52w DD(%)", "QQQ 최근 52주 고점 대비 하락%. 0에 가까우면 고점 부근."),
                    "soxx_dd_52w": ("SOXX 52w DD(%)", "SOXX 최근 52주 고점 대비 하락%. 반도체 조정 깊이."),
                    # === 가공 매크로 (raw 만으로는 표현 안 되는 카드 값) ===
                    "cfnai_ma3":     ("CFNAI MA3", "시카고 연준 활동지수 3M 평균. -0.7 이하 침체 룰."),
                    "ff6m_chg":      ("FF 6M 변화 (%p)", "연준 기준금리 6개월 변화량. 음수=인하 사이클."),
                    "un3m_chg":      ("실업률 3M 변화 (%p)", "실업률 3개월 변화. 0.5%p 넘으면 삼의 법칙 발동 근처."),
                    "real_rate":     ("실질금리 (%)", "FF 금리 - 기대 인플레. 파월이 진짜 보는 숫자."),
                    "buffett_ratio": ("버핏 지표 (%)", "시총/GDP. 200% 넘으면 거품 정점 영역."),
                    "sox_rel3":      ("SOX/SPX 3M 상대 (%p)", "반도체 vs 시장 3개월 상대수익률. 양수=아웃퍼폼."),
                    "xle_spy_3m":    ("XLE-SPY 3M (%p)", "에너지 vs 시장 3개월 상대. 양수=인플레 회귀."),
                    "xlk_spy_3m":    ("XLK-SPY 3M (%p)", "기술 vs 시장 3개월 상대. 양수=성장 회귀."),
                    # === F1 역전 해소 ===
                    "f1_2y10y_recovery_pct": ("F1: 2Y10Y 역전 해소율 (%)", "52주 최심점 대비 회복도. 100%면 정상화 완료."),
                    "f1_3m10y_recovery_pct": ("F1: 3M10Y 역전 해소율 (%)", "거시가 가장 신뢰하는 침체 신호의 해소 진행률."),
                    # === F2 인하 사이클 ===
                    "f2_cum_cut_bp": ("F2: 인하 사이클 누적 (bp)", "첫 인하 이후 누적 인하 폭 (음수)."),
                    "f2_months":     ("F2: 인하 후 경과 (개월)", "첫 인하 이후 경과 개월 수. 단계 분류용."),
                    # === F3 FF금리 위치 ===
                    "f3_ff_position": ("F3: FF금리 위치 (%ile)", "10년 분위수. 70+ 고점권 인하, 30- 저점권 인하."),
                    # === F5 가속도 모니터 (5종 ratio) ===
                    "f5_vix_ratio":     ("F5: VIX 가속도 ratio", "1M 변화 / 3M 평균 속도. >1.3 가속, <0.7 감속. (역발상)"),
                    "f5_hy_ratio":      ("F5: HY 가속도 ratio", "하이일드 스프레드 가속도. (역발상)"),
                    "f5_t10y2y_ratio":  ("F5: 2Y10Y 가속도 ratio", "장단기금리차 가속도."),
                    "f5_dxy_ratio":     ("F5: DXY 가속도 ratio", "달러 인덱스 가속도."),
                    "f5_soxspx_ratio":  ("F5: SOX/SPX 가속도 ratio", "반도체 상대강도 가속도."),
                    # === F6 Forward EPS ===
                    "f6_eps_chg_30d":   ("F6: Forward EPS 변화 (%)", "분석가 컨센서스 30일 변화 (누적 30일 미만이면 가용 max lookback). 가격을 따라가면 반사성."),
                    "f6_spx_chg_30d":   ("F6: SPX 변화 (%)", "S&P500 동기간 변화. EPS와 같이 보면 반사성 진단."),
                    "f6_lookback_days": ("F6: 실제 lookback (일)", "30일이면 본 정의, 미만이면 partial. 30일 채워질 때까지 임시값."),
                    # === Δ30D / ΔΔ (속도와 가속도) ===
                    "mac_delta":          ("거시 Δ30D", "거시 스코어 30일 변화량 (속도)."),
                    "mac_delta_delta":    ("거시 ΔΔ", "거시 스코어 2차 도함수 (가속도)."),
                    "cluster_bond_delta": ("채권/금리 Δ30D", "채권 클러스터 30일 변화."),
                    "cluster_bond_dd":    ("채권/금리 ΔΔ", "채권 클러스터 가속도."),
                    "cluster_val_delta":  ("밸류에이션 Δ30D", "밸류 클러스터 30일 변화."),
                    "cluster_val_dd":     ("밸류에이션 ΔΔ", "밸류 클러스터 가속도."),
                    "cluster_stress_delta": ("스트레스 Δ30D", "스트레스 클러스터 30일 변화."),
                    "cluster_stress_dd":    ("스트레스 ΔΔ", "스트레스 클러스터 가속도."),
                    "cluster_real_delta":   ("실물 Δ30D", "실물 클러스터 30일 변화."),
                    "cluster_real_dd":      ("실물 ΔΔ", "실물 클러스터 가속도."),
                    "cluster_semi_delta":   ("반도체 Δ30D", "반도체 클러스터 30일 변화."),
                    "cluster_semi_dd":      ("반도체 ΔΔ", "반도체 클러스터 가속도."),
                }
                def _fmt_raw(s):
                    ko = _RAW_KO.get(s)
                    if not ko: return s
                    if mode == "쉬운":
                        return f"{s} · {ko[0]} — {ko[1]}"
                    return f"{s} ({ko[0]})"

                # 카테고리 그룹 (원본 raw + 가공 obs)
                _RAW_GROUPS = {
                    # 가공 지표 (observations.jsonl)
                    "미어캣의 관측소 지표들": ["mac_score", "meerkat_score", "mac_velocity", "mac_delta", "mac_delta_delta"],
                    "클러스터": ["cluster_bond", "cluster_val", "cluster_stress", "cluster_real", "cluster_semi", "cluster_divergence"],
                    "클러스터 Δ/ΔΔ": ["cluster_bond_delta", "cluster_bond_dd", "cluster_val_delta", "cluster_val_dd",
                                       "cluster_stress_delta", "cluster_stress_dd", "cluster_real_delta", "cluster_real_dd",
                                       "cluster_semi_delta", "cluster_semi_dd"],
                    "계절": ["season_confidence", "season_score_spring", "season_score_summer", "season_score_autumn", "season_score_winter", "season_base", "season_prefix", "season_max_score"],
                    "공포/밸류 (방문 기록)": ["fear_greed", "forward_pe", "trailing_pe", "cape", "dividend_yield", "qqq_dd_52w", "soxx_dd_52w"],
                    "가공 매크로 지표": ["cfnai_ma3", "ff6m_chg", "un3m_chg", "real_rate", "buffett_ratio", "sox_rel3", "xle_spy_3m", "xlk_spy_3m",
                                          "labor_gap_K", "labor_gap_label", "wti_3m_pct", "spx_3m_pct", "shock_state", "shock_box_on"],
                    "추세/사이클 (F1·F2·F3)": ["f1_2y10y_recovery_pct", "f1_3m10y_recovery_pct",
                                              "f2_cum_cut_bp", "f2_months", "f3_ff_position"],
                    "가속도 모니터 (F5)": ["f5_vix_ratio", "f5_hy_ratio", "f5_t10y2y_ratio", "f5_dxy_ratio", "f5_soxspx_ratio"],
                    "반사성 (F6 Forward EPS)": ["f6_eps_chg_30d", "f6_spx_chg_30d", "f6_lookback_days"],
                    "역사 매칭 — 메타 (top 3 + aftermath)": [
                        "history_era_top1", "history_era_label", "history_score_top1",
                        "history_era_top2", "history_score_top2",
                        "history_era_top3", "history_score_top3",
                        "history_aftermath_top1", "history_years_ago_top1",
                        "history_era_type_top1", "history_duration_days_top1",
                        "history_unmatched_top1", "history_common_dims",
                    ],
                    "역사 매칭 — 차원별 일치율 (10차원)": [
                        "history_dim_match_season",
                        "history_dim_match_ff_pos",
                        "history_dim_match_ff_action",
                        "history_dim_match_inflation_trend",
                        "history_dim_match_valuation",
                        "history_dim_match_credit",
                        "history_dim_match_yield_curve",
                        "history_dim_match_semiconductor",
                        "history_dim_match_dollar",
                        "history_dim_match_external_shock",
                    ],
                    # 원본 지표 (raw.jsonl)
                    "금리/국채": ["DGS2", "DGS10", "T10Y3M", "T10Y2Y", "FEDFUNDS", "T5YIE"],
                    "위험/스트레스": ["VIXCLS", "BAMLH0A0HYM2", "DRCCLACBS", "^VIX"],
                    "고용/물가": ["UNRATE", "PAYEMS", "JTSJOL", "UNEMPLOY", "CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE"],
                    "실물/심리": ["GDP", "UMCSENT", "CFNAI", "DCOILWTICO", "CL=F"],
                    "외환/원자재": ["DEXKOUS", "DX-Y.NYB", "KRW=X", "GC=F"],
                    "밸류에이션": ["NCBEILQ027S", "GFDEGDQ188S", "DIVIDEND_YIELD"],
                    "주식/ETF": ["^GSPC", "QQQ", "VOO", "SPY", "SOXX", "TQQQ", "SOXL", "XLE", "XLK", "SGOV"],
                    # V3.10.1 역사 매칭 시각화 — pseudo-key (특수 renderer 분기)
                    "역사 매칭 — 시각화 (V3.10.1)": [
                        "__era_score_overlay",
                        "__era_timeline",
                        "__era_dim_overlay",
                        "__era_heatmap",
                    ],
                    # V3.11.1 달력 계절성 (TradingView Seasonal Chart 모방)
                    "달력 계절성": [
                        "__seasonal_overlay",
                        "__seasonal_heatmap",
                    ],
                }
                _GROUP_KO = {
                    "미어캣의 관측소 지표들": "거시·미어캣·속도·가속도",
                    "클러스터": "5개 클러스터 점수 + 괴리도",
                    "클러스터 Δ/ΔΔ": "클러스터별 30일 변화 + 2차 도함수",
                    "계절": "사계절 확신도 + 4계절 점수",
                    "공포/밸류 (방문 기록)": "F&G·Forward PE·CAPE·배당·52w DD 등 백필 불가 원본",
                    "가공 매크로 지표": "CFNAI·실질금리·버핏·실업률 변화 등 가공 카드 값",
                    "추세/사이클 (F1·F2·F3)": "역전 해소율 · 인하 사이클 · FF금리 위치",
                    "가속도 모니터 (F5)": "VIX·HY·2Y10Y·DXY·SOX/SPX 가속도 ratio",
                    "반사성 (F6 Forward EPS)": "분석가 컨센서스 30일 변화 + SPX 동기간",
                    "역사 매칭 — 메타 (top 3 + aftermath)": "1/2/3위 era id + 매칭률 + 그 후 + 시간거리 등",
                    "역사 매칭 — 차원별 일치율 (10차원)": "10개 차원 (계절/FF/밸류/신용 등) top 1 매칭 여부 (1/0)",
                    "금리/국채": "돈의 값 — 연준·국채",
                    "위험/스트레스": "공포·신용 긴장",
                    "고용/물가": "일자리와 물가",
                    "실물/심리": "진짜 경제 + 소비자 기분",
                    "외환/원자재": "달러·원자재",
                    "밸류에이션": "얼마나 비싼가",
                    "주식/ETF": "주식 본진",
                    "역사 매칭 — 시각화 (V3.10.1)": "top 1/2/3 추이 + 1위 타임라인 + 차원 매칭 overlay + 안정성 heatmap",
                    "달력 계절성": "월별 누적수익률 overlay + 월별 수익률 히트맵 (TradingView Seasonal 모방)",
                }
                _all_cols = set(_raw_cols) | set(_obs_cols)
                # 전체 그룹 노출 — 데이터 없는 그룹도 보여서 "앞으로 뭐가 쌓일 예정인지" 예고
                _grp_keys = list(_RAW_GROUPS.keys())
                def _grp_label(g):
                    _cnt = sum(1 for s in _RAW_GROUPS[g] if s in _all_cols)
                    _tot = len(_RAW_GROUPS[g])
                    _badge = f" ({_cnt}/{_tot})" if _cnt < _tot else ""
                    if mode == "쉬운":
                        return f"{g} — {_GROUP_KO.get(g,'')}{_badge}"
                    return f"{g}{_badge}"
                # 🔍 지표 검색 (영문 티커 / 한국명 / 설명 모두 매칭) — 카테고리 무관 전체 탐색
                _search_cols = st.columns([3, 1])
                with _search_cols[0]:
                    _qr = st.text_input(
                        "🔍 지표 검색",
                        key="hist_raw_search",
                        placeholder="예: 배당, CPI, 반도체, VIX, DGS10 …",
                        label_visibility="collapsed",
                    ).strip()
                with _search_cols[1]:
                    _gsel = st.selectbox("카테고리", _grp_keys, key="hist_raw_group",
                                         format_func=_grp_label, label_visibility="collapsed")

                if _qr:
                    # 검색 모드: 모든 그룹의 지표 중 티커/한국명/설명에 검색어 포함된 것만
                    _q_lower = _qr.lower()
                    _matched = []
                    for _gname, _items in _RAW_GROUPS.items():
                        for _s in _items:
                            if _s in _matched: continue
                            _ko = _RAW_KO.get(_s, ("", ""))
                            _text = f"{_s} {_ko[0]} {_ko[1]}".lower()
                            if _q_lower in _text:
                                _matched.append(_s)
                    _cands_all = _matched
                    st.caption(f"🔍 '{_qr}' 검색 결과: {len(_cands_all)}개 "
                               f"(카테고리 선택 무시). 지우면 카테고리 모드로 돌아온다.")
                else:
                    # 카테고리 모드: 선택된 그룹 항목만
                    _cands_all = _RAW_GROUPS.get(_gsel, [])

                # 후보: 그룹 전체 항목 (데이터 유무 무관) — 없는 건 선택해도 캡션으로 안내
                # pseudo-key (__era_*, __seasonal_*) 는 obs/raw 무관하게 즉시 활성화
                _cands = [s for s in _cands_all if s in _all_cols or s.startswith("__")]
                _cands_empty = [s for s in _cands_all if s not in _all_cols and not s.startswith("__")]
                if _cands_empty:
                    _empty_names = [(_RAW_KO.get(s, (s,""))[0] or s) for s in _cands_empty]
                    st.caption(f"⏳ 아직 데이터 없음 ({len(_cands_empty)}개): " + ", ".join(_empty_names) + " — 방문 누적되면 자동으로 활성화.")
                _ssel = st.multiselect("지표 선택 (복수)", _cands,
                                       default=_cands[:1] if _cands else [],
                                       format_func=_fmt_raw,
                                       key="hist_raw_series")
                # 쉬운모드: 선택한 지표마다 설명 한 줄 더
                if mode == "쉬운" and _ssel:
                    _desc_lines = []
                    for _s in _ssel:
                        _ko = _RAW_KO.get(_s)
                        if _ko: _desc_lines.append(f"• **{_ko[0]}** ({_s}) — {_ko[1]}")
                    if _desc_lines:
                        st.markdown("\n".join(_desc_lines))

                # 기간
                _rper_map = {"1Y": 365, "3Y": 365*3, "5Y": 365*5, "10Y": 365*10, "Max": None}
                _rper_sel = st.radio("기간", list(_rper_map.keys()), index=2, horizontal=True, key="hist_raw_period")
                _rper_d = _rper_map[_rper_sel]

                # 두 소스에서 시리즈 수집
                def _fetch_series(key):
                    """raw_df 또는 obs_df 에서 (index, values) 반환. 없으면 None."""
                    if _raw_df is not None and key in _raw_df.columns:
                        _yv = _raw_df[key].dropna()
                        if not _yv.empty: return (_yv.index, _yv.values, "raw")
                    if _obs_df is not None and key in _obs_df.columns:
                        _sub = _obs_df[["ts", key]].dropna()
                        if not _sub.empty:
                            _idx = pd.to_datetime(_sub["ts"]).dt.tz_localize(None) if getattr(_sub["ts"].dt, "tz", None) is not None else pd.to_datetime(_sub["ts"])
                            return (_idx, _sub[key].values, "obs")
                    return None

                # V3.10.1 / V3.11.1: 특수 renderer 분기 (era 시각화 + 달력 계절성)
                _ssel_specials = [s for s in (_ssel or []) if s.startswith("__")]
                _ssel_normal   = [s for s in (_ssel or []) if not s.startswith("__")]
                _ERA_RENDERERS = {
                    "__era_score_overlay":  _render_era_score_overlay,
                    "__era_timeline":       _render_era_timeline,
                    "__era_dim_overlay":    _render_era_dim_overlay,
                    "__era_heatmap":        _render_era_stability_heatmap,
                    "__seasonal_overlay":   _render_seasonal_overlay,
                    "__seasonal_heatmap":   _render_seasonal_heatmap,
                }
                for _spec in _ssel_specials:
                    _r = _ERA_RENDERERS.get(_spec)
                    if _r: _r(_obs_df)

                _series_bag = []
                _cut_ts = (pd.Timestamp.now() - pd.Timedelta(days=_rper_d)) if _rper_d is not None else None
                for _s in (_ssel_normal or []):
                    _got = _fetch_series(_s)
                    if _got is None: continue
                    _xs, _ys, _src = _got
                    if _cut_ts is not None:
                        _mask = _xs >= _cut_ts
                        try: _xs = _xs[_mask]; _ys = _ys[_mask]
                        except: pass
                    if len(_xs) == 0: continue
                    _series_bag.append((_s, _xs, _ys, _src))

                if not _series_bag and not _ssel_specials:
                    st.caption("선택한 지표·기간에 데이터가 없다.")
                elif not _series_bag and _ssel_specials:
                    pass  # special renderer 가 위에서 출력 완료
                else:
                    _fig_raw = go.Figure()
                    _rcols = [C["gold"], C["blue"], C["red"], C["green"], "#9C27B0", "#FF8A00", C["muted"]]
                    for _i, (_s, _xs, _ys, _src) in enumerate(_series_bag):
                        _ko = _RAW_KO.get(_s)
                        _nm = f"{_s} ({_ko[0]})" if _ko else _s
                        _mode_plot = "markers+lines" if _src == "obs" else "lines"
                        _mk = dict(size=6) if _src == "obs" else dict(size=3)
                        _fig_raw.add_trace(go.Scatter(
                            x=_xs, y=_ys, name=_nm, mode=_mode_plot,
                            line=dict(color=_rcols[_i % len(_rcols)], width=1.8),
                            marker=dict(size=_mk["size"], color=_rcols[_i % len(_rcols)])))
                    _ly_raw = _ly(f"히스토리 · {_gsel} · {_rper_sel}", 380)
                    _ly_raw["legend"] = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    _fig_raw.update_layout(**_ly_raw)
                    st.plotly_chart(_fig_raw, use_container_width=True, key=f"chart_raw_{_gsel}")
                    _n_pts = sum(len(b[1]) for b in _series_bag)
                    _n_obs = sum(1 for b in _series_bag if b[3] == "obs")
                    _n_raw = sum(1 for b in _series_bag if b[3] == "raw")
                    st.caption(f"시리즈 {len(_series_bag)}개 · 총 {_n_pts}점 · 원본 {_n_raw} / 관측 {_n_obs}")

        # ── 관측 스냅샷 (가공 지표) ──
        st.markdown(bsl("##### 📍 관측 스냅샷 (가공 지표 · 방문할 때만 기록)", mode))
        if _obs_df is None or _obs_df.empty:
            st.caption("아직 관측 스냅샷이 없다. 앱 방문 1회 = 1 행이 쌓인다.")
        else:
            _obs_metrics = {
                # 메인 스코어
                "거시 스코어 (거시 환경 점수)": "mac_score",
                "미어캣 스코어 (내 계좌 점수)": "meerkat_score",
                "거시 속도 (30일 변화율)": "mac_velocity",
                # 5 클러스터
                "클러스터: 채권/금리": "cluster_bond",
                "클러스터: 밸류에이션": "cluster_val",
                "클러스터: 스트레스": "cluster_stress",
                "클러스터: 실물": "cluster_real",
                "클러스터: 반도체": "cluster_semi",
                "클러스터 괴리도 (5개 표준편차)": "cluster_divergence",
                # 4 계절 (V3.8 9박스)
                "계절 확신도": "season_confidence",
                "계절 점수: 봄": "season_score_spring",
                "계절 점수: 여름": "season_score_summer",
                "계절 점수: 가을": "season_score_autumn",
                "계절 점수: 겨울": "season_score_winter",
                "계절 base (접두사 제외)": "season_base",
                "계절 접두사 (초/늦)": "season_prefix",
                "계절 박스 총수 (5박스/9박스 메타)": "season_max_score",
                # V3.8.2 노동격차 + 공급충격 디커플링
                "노동격차 (구인-실업자 K)": "labor_gap_K",
                "노동격차 라벨": "labor_gap_label",
                "WTI 3M (%)": "wti_3m_pct",
                "SPX 3M (%)": "spx_3m_pct",
                "공급충격 상태": "shock_state",
                "가을 #7 박스 ON": "shock_box_on",
                # 백필 불가 원본
                "Fear & Greed (CNN 공포탐욕지수)": "fear_greed",
                "Forward PE (예상 주가수익비율)": "forward_pe",
                "Trailing PE (실적 주가수익비율)": "trailing_pe",
                "CAPE (10년 평균 PER · 실러)": "cape",
                "배당수익률 (S&P500 Dividend Yield %)": "dividend_yield",
                # 52w DD
                "QQQ 52w DD (나스닥 고점 대비 하락%)": "qqq_dd_52w",
                "SOXX 52w DD (반도체 고점 대비 하락%)": "soxx_dd_52w",
                # 가공 매크로 지표
                "CFNAI MA3 (시카고 연준 활동지수)": "cfnai_ma3",
                "FF 6M 변화 (%p)": "ff6m_chg",
                "실업률 3M 변화 (%p)": "un3m_chg",
                "실질금리 (%)": "real_rate",
                "버핏 지표 (시총/GDP %)": "buffett_ratio",
                "SOX/SPX 3M 상대 (%p)": "sox_rel3",
                "XLE-SPY 3M (%p)": "xle_spy_3m",
                "XLK-SPY 3M (%p)": "xlk_spy_3m",
                # F1 역전 해소
                "F1: 2Y10Y 역전 해소율 (%)": "f1_2y10y_recovery_pct",
                "F1: 3M10Y 역전 해소율 (%)": "f1_3m10y_recovery_pct",
                # F2 인하 사이클
                "F2: 인하 사이클 누적 (bp)": "f2_cum_cut_bp",
                "F2: 인하 후 경과 (개월)": "f2_months",
                # F3 FF금리 위치
                "F3: FF금리 위치 (%ile)": "f3_ff_position",
                # F5 가속도 모니터
                "F5: VIX 가속도 ratio": "f5_vix_ratio",
                "F5: HY 가속도 ratio": "f5_hy_ratio",
                "F5: 2Y10Y 가속도 ratio": "f5_t10y2y_ratio",
                "F5: DXY 가속도 ratio": "f5_dxy_ratio",
                "F5: SOX/SPX 가속도 ratio": "f5_soxspx_ratio",
                # F6 Forward EPS
                "F6: Forward EPS 변화 (%)": "f6_eps_chg_30d",
                "F6: SPX 변화 (%)": "f6_spx_chg_30d",
                "F6: 실제 lookback (일)": "f6_lookback_days",
                # 거시 Δ/ΔΔ
                "거시 Δ30D (속도)": "mac_delta",
                "거시 ΔΔ (가속도)": "mac_delta_delta",
                # 클러스터별 Δ/ΔΔ
                "채권/금리 Δ30D": "cluster_bond_delta",
                "채권/금리 ΔΔ": "cluster_bond_dd",
                "밸류에이션 Δ30D": "cluster_val_delta",
                "밸류에이션 ΔΔ": "cluster_val_dd",
                "스트레스 Δ30D": "cluster_stress_delta",
                "스트레스 ΔΔ": "cluster_stress_dd",
                "실물 Δ30D": "cluster_real_delta",
                "실물 ΔΔ": "cluster_real_dd",
                "반도체 Δ30D": "cluster_semi_delta",
                "반도체 ΔΔ": "cluster_semi_dd",
            }
            _OBS_DESC = {
                "mac_score": "거시 환경이 얼마나 사기 좋은 환경인가. 0~100, 높을수록 우호적.",
                "meerkat_score": "내 계좌 포지션이 얼마나 사기 좋은 상태인가. 현금비중·DD 등 반영.",
                "mac_velocity": "거시 스코어의 30일 변화량. 위치보다 속도, 속도보다 가속도.",
                "cluster_bond": "채권/금리 클러스터 점수. 장단기금리차·FF·HY 등으로 구성. 0~100.",
                "cluster_val": "밸류에이션 클러스터. Forward PE·CAPE·버핏 지표 등. 낮을수록 싸다.",
                "cluster_stress": "스트레스 클러스터. VIX·HY스프레드·연체율 등 공포 신호 모음.",
                "cluster_real": "실물 클러스터. 실업률·GDP·CFNAI. 경기 펀더멘털 점수.",
                "cluster_semi": "반도체 클러스터. SOX/SPX·KRW·WTI. 한국 주식 본진.",
                "cluster_divergence": "5개 클러스터 점수의 표준편차. 크면 시장이 쪼개져 있다는 신호.",
                "season_confidence": "사계절 판정의 확신도. 6단계 라벨 ('판정 불가'~'매우 높음').",
                "season_base":       "사계절 자동 판정 base (접두사 제외 — 봄/여름/가을/겨울).",
                "season_prefix":     "사계절 접두사 (초/늦/없음). 인접 계절 점수 ≥4면 부착.",
                "season_max_score":  "박스 개수 메타 (5박스 시절 호환성). V3.8부터 9.",
                "season_score_spring": "'봄' 계절 조건 9개 중 몇 개가 충족됐는가. 0~9.",
                "season_score_summer": "'여름' 계절 조건 9개 중 몇 개 충족. 0~9.",
                "season_score_autumn": "'가을' 계절 조건 9개 중 몇 개 충족. 0~9.",
                "season_score_winter": "'겨울' 계절 조건 9개 중 몇 개 충족. 0~9.",
                "fear_greed": "CNN Fear & Greed Index. 0(극공포)~100(극탐욕).",
                "forward_pe": "향후 12개월 예상 EPS 기준 PER. 낮을수록 싸다(고 여겨진다).",
                "trailing_pe": "지난 12개월 실적 기준 PER. 경기 변동에 민감.",
                "cape": "Shiller CAPE. 10년 평균 EPS 기준 PER. 장기 밸류 버블 판정용.",
                "dividend_yield": "S&P500 배당수익률 %. 낮을수록 비싸다 — 1.5% 밑은 2000년뿐이었다.",
                "cfnai_ma3": "시카고 연준 활동지수 3M 평균. 0이 추세, -0.7 밑이면 침체 룰 발동.",
                "ff6m_chg": "연준 기준금리 6개월 변화 (%p). 음수=인하, 양수=인상 사이클.",
                "un3m_chg": "실업률 3개월 변화 (%p). 0.5%p 넘으면 삼의 법칙 발동 근처.",
                "real_rate": "실질금리 (FF - 기대인플레, %). 파월이 진짜 보는 긴축 강도.",
                "buffett_ratio": "버핏 지표 = 시총/GDP %. 200% 넘으면 거품 정점 영역.",
                "sox_rel3": "반도체 vs 시장 3개월 상대수익률 %p. 양수=반도체 아웃퍼폼.",
                "xle_spy_3m": "에너지 vs 시장 3개월 상대수익률 %p. 양수=인플레 회귀.",
                "xlk_spy_3m": "기술 vs 시장 3개월 상대수익률 %p. 양수=성장 회귀.",
                "f1_2y10y_recovery_pct": "2Y10Y 역전 52주 최심점 대비 회복도 %. 100% = 정상화 완료.",
                "f1_3m10y_recovery_pct": "3M10Y 역전 회복도 %. 거시가 가장 신뢰하는 침체 신호.",
                "f2_cum_cut_bp": "첫 인하 이후 누적 인하 폭 bp (음수). 사이클 진행도.",
                "f2_months": "첫 인하 이후 경과 개월. 0~3 초입, 3~9 중반, 9~15 후반, 15+ 장기화.",
                "f3_ff_position": "FF금리 10년 분위수. 70+ 고점권 인하, 30- 저점권 인하.",
                "f5_vix_ratio": "VIX 가속도 ratio. >1.3 가속, <0.7 감속. (역발상)",
                "f5_hy_ratio": "하이일드 스프레드 가속도. (역발상)",
                "f5_t10y2y_ratio": "장단기금리차 가속도.",
                "f5_dxy_ratio": "달러 인덱스 가속도.",
                "f5_soxspx_ratio": "반도체 상대강도 가속도.",
                "f6_eps_chg_30d": "Forward EPS 컨센서스 30일 변화 % (누적 30일 미만이면 가용 max lookback).",
                "f6_spx_chg_30d": "S&P500 동기간 변화 %. EPS와 같이 보면 반사성 진단.",
                "f6_lookback_days": "F6 계산에 실제로 사용된 lookback 일수. <30 이면 임시값 (partial).",
                "mac_delta": "거시 스코어 Δ30D = 최근 7일 평균 - 30일 전 7일 평균.",
                "mac_delta_delta": "거시 스코어 ΔΔ = 속도의 속도. 전환점 신호.",
                "cluster_bond_delta": "채권/금리 클러스터 30일 변화량.",
                "cluster_bond_dd": "채권/금리 클러스터 가속도 (2차 도함수).",
                "cluster_val_delta": "밸류에이션 클러스터 30일 변화량.",
                "cluster_val_dd": "밸류에이션 클러스터 가속도.",
                "cluster_stress_delta": "스트레스 클러스터 30일 변화량.",
                "cluster_stress_dd": "스트레스 클러스터 가속도.",
                "cluster_real_delta": "실물 클러스터 30일 변화량.",
                "cluster_real_dd": "실물 클러스터 가속도.",
                "cluster_semi_delta": "반도체 클러스터 30일 변화량.",
                "cluster_semi_dd": "반도체 클러스터 가속도.",
                "qqq_dd_52w": "QQQ가 최근 52주 고점 대비 얼마나 내려왔나. 0에 가까우면 고점 부근.",
                "soxx_dd_52w": "SOXX가 최근 52주 고점 대비 얼마나 내려왔나. 반도체 조정 깊이.",
            }
            # 전체 목록 노출 — 데이터 유무는 선택 후 안내
            _osel = st.selectbox("가공 지표 선택", list(_obs_metrics.keys()), key="hist_obs_metric")
            _ocol = _obs_metrics[_osel]
            if mode == "쉬운" and _ocol in _OBS_DESC:
                st.caption(f"💡 {_OBS_DESC[_ocol]}")
            if _ocol not in _obs_df.columns or not _obs_df[_ocol].notna().any():
                st.info(f"'{_osel}' 는 아직 관측 기록이 없다. 앱을 방문할수록 점이 찍힌다.")
                _avail = None
            else:
                _avail = {_osel: _ocol}
                _ocol = _avail[_osel]
                _osub = _obs_df[["ts", _ocol]].dropna()
                if _osub.empty:
                    st.caption("선택 지표에 값이 없다.")
                else:
                    _fig_obs = go.Figure()
                    _fig_obs.add_trace(go.Scatter(
                        x=_osub["ts"], y=_osub[_ocol], mode="markers+lines",
                        name=_osel, line=dict(color=C["gold"], width=1.2),
                        marker=dict(size=6, color=C["gold"])))
                    _ly_obs = _ly(f"관측 스냅샷 · {_osel} · 총 {len(_osub)}점", 300)
                    _fig_obs.update_layout(**_ly_obs)
                    st.plotly_chart(_fig_obs, use_container_width=True, key=f"chart_obs_{_osel}")
                    st.caption(f"총 관측 수: {len(_obs_df)}회 · 이 지표 유효: {len(_osub)}점 · 최초 {_obs_df['ts'].min().date()} ~ 최근 {_obs_df['ts'].max().date()}")

    # ═══ V3.12.0 시점 조회 탭 ═══
    with tabs[8]:
        st.subheader(bsl("🔮 역사적 시점 조회", mode))
        st.caption("임의 연/월 입력 → 그 시점 매크로 진단. 데이터 우선순위: 실측 → 백필 → 즉석계산.")
        from datetime import date as _qd_date
        _today_q = _qd_date.today()
        # 시점 조회 전용 long-range raw — 사이드바 '관찰 기간' 무시, yfinance max + FRED 1990~
        with st.spinner("장기 시계열 준비 중 (최초 1회 ~30초, 24h 캐싱)..."):
            _qraw = _build_long_range_raw(api_key)
        _q_starts = []
        for _s in (_qraw or {}).values():
            try:
                if _s is not None and len(_s) > 0:
                    _mi = _s.index.min()
                    if _mi is not None:
                        _q_starts.append(pd.Timestamp(_mi))
            except Exception: continue
        try:
            if OBS_JSONL.exists():
                with OBS_JSONL.open("r", encoding="utf-8") as _f:
                    for _ln in _f:
                        try:
                            _r = json.loads(_ln)
                            if _r.get("_backfill_marker"): continue
                            _d = (_r.get("date") or "").split()[0]
                            if _d:
                                _q_starts.append(pd.Timestamp(_d)); break
                        except Exception: continue
        except Exception: pass
        if _q_starts:
            _earliest = min(_q_starts)
            _q_min_year = int(_earliest.year)
            _q_min_month = int(_earliest.month)
        else:
            _q_min_year = _today_q.year
            _q_min_month = 1
        _q_max_year = _today_q.year
        _qcol1, _qcol2, _qcol3 = st.columns([1, 1, 1])
        with _qcol1:
            _q_years = list(range(_q_min_year, _q_max_year + 1))
            _qy = st.selectbox("연도", options=_q_years,
                                index=len(_q_years) - 1, key="query_year")
        with _qcol2:
            _mmin = _q_min_month if _qy == _q_min_year else 1
            _mmax = _today_q.month if _qy == _today_q.year else 12
            _q_months = list(range(_mmin, _mmax + 1))
            _qm = st.selectbox("월", options=_q_months,
                                index=len(_q_months) - 1, key="query_month")
        with _qcol3:
            st.write("")
            _qbtn = st.button("🔍 조회", use_container_width=True, key="query_btn")
        st.caption(f"가용 범위: {_q_min_year}-{_q_min_month:02d} ~ {_today_q.year}-{_today_q.month:02d} "
                   f"(월 단위, long-range raw + obs.jsonl 자동 산출)")
        if _qbtn:
            try: _obs_mt = float(OBS_JSONL.stat().st_mtime) if OBS_JSONL.exists() else 0.0
            except Exception: _obs_mt = 0.0
            _qres = _query_macro_month_cached_v6(
                _qy, _qm, api_key, round(_obs_mt, 0), _QUERY_SCHEMA_VERSION
            )
            st.markdown("---")
            _render_query_result(_qres)
            # ─── 다운로드 버튼 (다른 탭과 동일 패턴) ───
            try:
                _qsum = _qres.get("summary") or {}
                _q_export = {
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "version": VERSION,
                    "조회_연월": f"{_qres.get('year')}-{_qres.get('month'):02d}",
                    "영업일_수": _qres.get("n_business_days"),
                    "대표_계절": _qsum.get("dominant_season"),
                    "변동성": _qsum.get("volatility"),
                    "era_전환_횟수": _qsum.get("n_era_transitions"),
                    "계절_전환_횟수": _qsum.get("n_season_transitions"),
                    "계절_분포": _qsum.get("season_distribution"),
                    "4계절_평균_점수": _qsum.get("season_scores_mean"),
                    "4계절_표준편차": _qsum.get("season_scores_std"),
                    "역사_매칭_등장_분포": _qsum.get("top1_era_distribution"),
                    "역사_매칭_평균_매칭률": _qsum.get("top1_era_avg_score"),
                    "박스_평가_성공_일수": _qsum.get("box_valid_days"),
                    "박스_평가_실패_일수": _qsum.get("box_eval_failed_days"),
                    "박스_체크리스트": {
                        _se: [{"label": _l, "fire": _f, "valid": _v,
                                "pct": round((_f * 100 / _v) if _v else 0, 1)}
                               for _l, _f, _v in (_qsum.get("box_aggregated") or {}).get(_se, [])]
                        for _se in ("봄", "여름", "가을", "겨울")
                    },
                    "era_변화_timeline": _qres.get("timeline"),
                    "일별_평가": _qres.get("daily_results"),
                }
                st.markdown("---")
                _qe1, _qe2 = st.columns(2)
                with _qe1:
                    _jbtn(_q_export, f"query_{_qres.get('year')}_{_qres.get('month'):02d}",
                          "📥 시점 조회 JSON", "_query")
                with _qe2:
                    st.download_button(
                        "📥 시점 조회 HTML",
                        _export_html(_q_export, section="시점 조회").encode("utf-8"),
                        f"query_{_qres.get('year')}_{_qres.get('month'):02d}.html",
                        "text/html", key="exp_query_html",
                    )
            except Exception as _qee:
                st.caption(f"⚠️ 다운로드 빌드 실패: {type(_qee).__name__}: {_qee}")

    st.markdown("---"); st.markdown(f"> *{dq()}*")

    st.markdown(f"""
<div style='text-align:center; opacity:0.55; font-size:0.8em; padding:12px;'>
미어캣의 관측소 v{VERSION} · 작성: {__author__}
</div>
""", unsafe_allow_html=True)

if __name__ == "__main__": main()
