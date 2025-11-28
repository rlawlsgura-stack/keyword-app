# -*- coding: utf-8 -*-
"""
Keyword Risk Analyzer (v101 – Follow DB column order & values)
- Based on v97
- NEW: 드롭다운 목록을 JSON 파일로 영구 저장하여 재부팅 후에도 유지
- KEEP: 모든 기존 기능 유지 (조회 수 카운팅, 리포트, 최근 7일 초기화 등)
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Set, Optional
import pandas as pd
import streamlit as st

# --- Custom CSS for larger '텍스트 분석하기' button ---
import streamlit as st
st.markdown(
    """
<style>
div.stButton > button {
    height: 35px !important;
    padding: 12px 20px !important;
    font-size: 1rem !important;
}
</style>
""",
    unsafe_allow_html=True
)
# --- End Custom CSS ---
import streamlit.components.v1 as components
import html, io, re, zipfile, os, json
from xml.etree import ElementTree as ET
from pathlib import Path

st.markdown("""
<style>
div.streamlit-expander {
    margin-bottom: 0rem !important;
}
</style>
""", unsafe_allow_html=True)


from datetime import datetime, timedelta

# Streamlit cache alias (지원 버전 차이 대응)
try:
    _cache_data = st.cache_data
except AttributeError:  # Streamlit < 1.18
    _cache_data = st.cache

# Constants
MAX_HIGHLIGHT_HEIGHT = 600  # px
DEFAULT_HEADER_ROW = 1
CONFIG_FILE_NAME = "dropdown_config.json"  # 드롭다운 설정 저장 파일

st.markdown(
    """
    <style>
    mark {
        background: #ffd43b !important;
        background-color: #ffd43b !important;
        color: #000 !important;
    }
    /* Compact layout ONLY for highlight quick-select area */
    .quick-select-block div[data-testid="stHorizontalBlock"] {
        margin-bottom: 0 !important;
        padding-bottom: 0 !important;
    }
    .quick-select-block div[data-testid="column"] {
        margin-bottom: 0 !important;
        padding-bottom: 0 !important;
    }
    .quick-select-block [data-testid="stVerticalBlock"] {
        gap: 0 !important;
    }
    .quick-select-block div.stButton > button {
        padding-top: 0 !important;
        padding-bottom: 0 !important;
        min-height: 1.1rem !important;
    }
</style>
    """,
    unsafe_allow_html=True
)

# -----------------------------
# XLSX reader without openpyxl (first sheet only, basic types)
# -----------------------------
def _xlsx_list_worksheets(zf: zipfile.ZipFile) -> List[str]:
    paths = [p for p in zf.namelist() if p.startswith("xl/worksheets/") and p.endswith(".xml")]
    return sorted(paths) if paths else []

def _xlsx_load_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    try:
        with zf.open("xl/sharedStrings.xml") as fp:
            tree = ET.parse(fp)
        root = tree.getroot()
        ns = {"a": root.tag.split("}")[0].strip("{")}
        strings = []
        for si in root.findall("a:si", ns):
            parts = [t.text or "" for t in si.findall(".//a:t", ns)]
            strings.append("".join(parts))
        return strings
    except (KeyError, Exception):
        return []

def _xlsx_cell_value(cell, shared_strings: List[str]) -> str:
    t = cell.get("t")
    v = cell.find("./v")
    is_node = cell.find("./is")
    if t == "s":
        if v is not None and v.text is not None:
            try:
                idx = int(v.text)
                return shared_strings[idx] if 0 <= idx < len(shared_strings) else ""
            except Exception:
                return ""
        return ""
    if t == "inlineStr" and is_node is not None:
        parts = [n.text or "" for n in is_node.findall(".//t")]
        return "".join(parts)
    if v is None or v.text is None:
        return ""
    return v.text

def read_xlsx_without_openpyxl(file_bytes: bytes, header_row: Optional[int] = DEFAULT_HEADER_ROW) -> pd.DataFrame:
    """
    Parse first worksheet of an .xlsx file using zipfile + ElementTree.
    Limitations: styles/dates/formulas not evaluated; merged cells not handled.
    """
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        sheets = _xlsx_list_worksheets(zf)
        if not sheets:
            raise ValueError("XLSX 내부에서 시트를 찾을 수 없습니다.")
        target = sheets[0]
        shared = _xlsx_load_shared_strings(zf)
        with zf.open(target) as fp:
            tree = ET.parse(fp)
        root = tree.getroot()
        ns_uri = root.tag.split("}")[0].strip("{")
        ns = {"a": ns_uri}
        rows = []
        for row in root.findall(".//a:sheetData/a:row", ns):
            values = []
            cells = row.findall("./a:c", ns)
            for c in cells:
                values.append(_xlsx_cell_value(c, shared))
            rows.append(values)

    maxlen = max((len(r) for r in rows), default=0)
    norm = [r + [""] * (maxlen - len(r)) for r in rows]

    if header_row is not None and 1 <= header_row <= len(norm):
        header = [h.strip() for h in norm[header_row - 1]]
        data = norm[header_row:]
        df = pd.DataFrame(data, columns=header)
    else:
        df = pd.DataFrame(norm)
    df = df.fillna("").astype(str)
    return df

# -----------------------------
# Paths
# -----------------------------
def default_storage_path() -> Path:
    try:
        base = Path(__file__).parent
    except NameError:
        base = Path.cwd()
    return base / "keywords_db.csv"

def hits_log_path() -> Path:
    try:
        base = Path(__file__).parent
    except NameError:
        base = Path.cwd()
    return base / "keywords_hits_log.csv"

def get_config_path() -> Path:
    """드롭다운 설정 파일 경로"""
    try:
        base = Path(__file__).parent
    except NameError:
        base = Path.cwd()
    return base / CONFIG_FILE_NAME

# -----------------------------
# Constants & defaults
# -----------------------------
DB_COLS = [
    "kwd_no", "키워드명", "상품카테고리", "리스크 등급", "대체키워드",
    "리스크 등급별 세부 심의기준", "증빙자료유형",
    "키워드 조회 수(누적카운트)", "마지막 출력일자"
]

DISPLAY_F_COL = "리스크 등급별 세부 심의기준 (셀을 더블 클릭하여 전체 내용을 확인하세요)"
RAW_F_COL = "리스크 등급별 세부 심의기준"

# ---- Dynamic DB column order helpers ----
def get_db_cols():
    try:
        cols = list(st.session_state.get("db_cols_order") or [])
        if cols:
            return cols
    except Exception:
        pass
    return DB_COLS


def _rename_fcol(df):
    """UI 표시용으로만 RAW_F_COL → DISPLAY_F_COL 헤더를 변경합니다."""
    try:
        import pandas as _pd
        # Styler가 들어오면 원본 DataFrame을 가져와 컬럼만 바꾸고 다시 스타일 적용
        if hasattr(df, 'to_excel') and hasattr(df, 'style'):
            # DataFrame (가급적 이 분기로)
            return df.rename(columns={RAW_F_COL: DISPLAY_F_COL})
        # pandas Styler 처리
        if getattr(df, '__class__', None).__name__ == 'Styler':
            base = getattr(df, 'data', None)
            if base is not None and hasattr(base, 'rename'):
                return base.rename(columns={RAW_F_COL: DISPLAY_F_COL}).style
        # 그 외 객체도 columns 속성이 있으면 시도
        if hasattr(df, 'rename') and hasattr(df, 'columns'):
            return df.rename(columns={RAW_F_COL: DISPLAY_F_COL})
        return df
    except Exception:
        return df
DEFAULT_CATEGORIES = ["공통(전체)", "식품", "건강기능식품", "화장품", "공산품"]
CATEGORY_PREFIX = {"공통(전체)":"A", "식품":"F","건강기능식품":"G","화장품":"B","공산품":"I"}

RISK_OPTIONS = ["1등급(사용금지)","2등급(대체키워드사용)","3등급(조건부사용)","4등급(사용가능)"]

DEFAULT_DETAIL_CRITERIA = ["-","실증자료제출","시험성적서제출","기능입증자료제출","표시기준준수","전문의견서","문헌자료제출"]
DEFAULT_EVIDENCE_TYPES = ["-","인체적용시험결과서","기능성평가보고서","임상시험결과보고서","실험데이터요약서","제품성분분석표","문헌자료","시험성적서"]
DEFAULT_ALT_KEYWORDS = ["-","탄력","보습","진정","미백","주름개선","자외선차단","영양공급"]

RISK_COLORS = {
    "1등급(사용금지)": "#ff6b6b",
    "2등급(대체키워드사용)": "#ffa94d",
    "3등급(조건부사용)": "#ffd43b",
    "4등급(사용가능)": "#a9e34b",
}

# Regex patterns (compiled once for performance)
_delim_pattern = re.compile(r"[\/,;\|]")
_bracket_pairs = r"\(\)\[\]{}（）【】"
_kwd_no_pattern = re.compile(r"^([A-Z])(\d{3,})$")
_kwd_split_pattern = re.compile(r"^([A-Za-z]+)(\d+)$")
_custom_kwd_pattern = re.compile(r"^([A-Za-z]+)(\d{1,})$")

# -----------------------------
# Dropdown config persistence (JSON)
# -----------------------------
def load_dropdown_config() -> dict:
    """JSON 파일에서 드롭다운 설정 불러오기"""
    config_path = get_config_path()
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            st.warning(f"드롭다운 설정을 불러오는 중 오류 발생: {e}")
    
    # 파일이 없거나 오류 시 기본값 반환
    return {
        "opt_categories": DEFAULT_CATEGORIES.copy(),
        "opt_risks": RISK_OPTIONS.copy(),
        "opt_details": DEFAULT_DETAIL_CRITERIA.copy(),
        "opt_evidences": DEFAULT_EVIDENCE_TYPES.copy(),
        "opt_alt_terms": DEFAULT_ALT_KEYWORDS.copy()
    }

def save_dropdown_config():
    """현재 드롭다운 설정을 JSON 파일로 저장"""
    normalize_dropdown_lists()
    config_path = get_config_path()
    config = {
        "opt_categories": st.session_state.opt_categories,
        "opt_risks": st.session_state.opt_risks,
        "opt_details": st.session_state.opt_details,
        "opt_evidences": st.session_state.opt_evidences,
        "opt_alt_terms": st.session_state.opt_alt_terms
    }
    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"드롭다운 설정 저장 중 오류: {e}")


# -----------------------------
# Dropdown normalize helpers (ko sort + '-' last)
# -----------------------------
def _ko_sorted(seq):
    try:
        import locale
        locale.setlocale(locale.LC_COLLATE, 'ko_KR.UTF-8')
        key = locale.strxfrm
        return sorted(seq, key=key)
    except Exception:
        return sorted(seq)


def unique_values_from_db(col: str) -> list:
    """Return ko-sorted unique, non-empty values for a given column from the current DB."""
    try:
        df = st.session_state.kw_df
        if df is None or df.empty or col not in df.columns:
            return []
        vals = df[col].astype(str).str.strip()
        uniq = [v for v in vals.unique().tolist() if v]
        return _ko_sorted(uniq)
    except Exception:
        return []
def _dedup_keep_order(seq):
    seen = set()
    out = []
    for x in seq:
        x = (x or "").strip()
        if not x:
            continue
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def normalize_dropdown_lists():
    """Ensure details/evidences/alt_terms are ko-sorted and '-' placed last & present."""
    # 대상 목록: opt_details, opt_evidences, opt_alt_terms
    targets = ["opt_details", "opt_evidences", "opt_alt_terms"]
    for key in targets:
        lst = list(getattr(st.session_state, key, []) or [])
        # clean & dedup
        lst = _dedup_keep_order(lst)
        # remove '-' temporarily
        without_dash = [x for x in lst if x != "-"]
        # ko sort
        without_dash = _ko_sorted(without_dash)
        # append '-' at the end (guarantee presence)
        without_dash.append("-")
        setattr(st.session_state, key, without_dash)


def _dropdown_with_input_option(lst):
    """Return normalized list for selectbox: ko-sorted with '-' last, plus '(직접 입력)' at the end."""
    tmp = _dedup_keep_order(lst)
    tmp = [x for x in tmp if x != "-"]
    tmp = _ko_sorted(tmp)
    tmp.append("-")
    return tmp + ["(직접 입력)"]

# -----------------------------
# Persistence helpers (with encoding fallbacks)
# -----------------------------
def read_csv_with_fallback_bytes(raw: bytes) -> pd.DataFrame:
    last_err = None
    for enc in ["utf-8-sig", "utf-8", "cp949", "euc-kr", "latin1"]:
        try:
            return pd.read_csv(io.BytesIO(raw), dtype=str, encoding=enc).fillna("")
        except Exception as e:
            last_err = e
            continue
    raise last_err

def _ensure_counter_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "키워드 조회 수(누적카운트)" not in df.columns:
        df["키워드 조회 수(누적카운트)"] = "0"
    if "마지막 출력일자" not in df.columns:
        df["마지막 출력일자"] = ""
    if "키워드 등록일자" not in df.columns:
        df["키워드 등록일자"] = ""
    return df

def load_db(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            raw = path.read_bytes()
            df = read_csv_with_fallback_bytes(raw)
            for c in DB_COLS:
                if c not in df.columns:
                    df[c] = "0" if c == "키워드 조회 수(누적카운트)" else ""
            df = _ensure_counter_columns(df)
            return df
        except Exception as e:
            st.warning(f"저장된 DB를 읽는 중 오류가 발생했습니다: {e}")
    df = pd.DataFrame(columns=DB_COLS)
    df = _ensure_counter_columns(df)
    return df

def save_db(df: pd.DataFrame, path: Path) -> None:
    from pathlib import Path as _P


    try:
        _P(path).parent.mkdir(parents=True, exist_ok=True)
        sorted_df = sort_db_internal(df)
        sorted_df[get_db_cols()].to_csv(path, index=False, encoding="utf-8-sig")
    except Exception as e:
        st.error(f"DB 저장 중 오류: {e}")

def append_hits_log(kwd_list: List[str]) -> None:
    if not kwd_list:
        return
    log_p = hits_log_path()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [{"ts": ts, "kwd_no": k} for k in kwd_list if k]
    df = pd.DataFrame(rows, columns=["ts","kwd_no"])
    try:
        if log_p.exists():
            df.to_csv(log_p, mode="a", header=False, index=False, encoding="utf-8-sig")
        else:
            df.to_csv(log_p, mode="w", header=True, index=False, encoding="utf-8-sig")
    except Exception as e:
        st.warning(f"로그 저장 실패: {e}")
    # 조회 로그가 변경되면 캐시 무효화
    try:
        _cache_data.clear()
    except Exception:
        pass

@_cache_data(show_spinner=False)
def load_hits_log() -> pd.DataFrame:
    log_p = hits_log_path()
    if not log_p.exists():
        return pd.DataFrame(columns=["ts","kwd_no"])
    try:
        return pd.read_csv(log_p, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception:
        return pd.DataFrame(columns=["ts","kwd_no"])

def overwrite_hits_log(df: pd.DataFrame) -> None:
    """Overwrite the hits log CSV safely with utf-8-sig encoding."""
    log_p = hits_log_path()
    try:
        df.to_csv(log_p, mode="w", header=True, index=False, encoding="utf-8-sig")
    except Exception as e:
        st.error(f"조회 로그 저장 중 오류: {e}")
    # 전체 로그를 덮어썼으므로 캐시 무효화
    try:
        _cache_data.clear()
    except Exception:
        pass

# -----------------------------
# Sorting helpers
# -----------------------------

def _split_kwd_series(s):
    s = s.astype(str)
    m = s.str.extract(_kwd_split_pattern)
    pref = m[0].fillna("")
    num = m[1].fillna("0").astype(int)
    return pref, num

def sort_db_internal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "kwd_no" not in df.columns:
        return df
    pref, num = _split_kwd_series(df["kwd_no"])
    sorted_df = (
        df.assign(_pref=pref, _num=num)
        .sort_values(by=["상품카테고리", "_pref", "_num", "키워드명"], kind="mergesort")
        .drop(columns=["_pref", "_num"])
        .reset_index(drop=True)
    )
    return sorted_df

def sort_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "kwd_no" not in df.columns:
        return df
    pref, num = _split_kwd_series(df["kwd_no"])
    sorted_df = (
        df.assign(_pref=pref, _num=num)
        .sort_values(by=["_pref", "_num", "키워드명"], kind="mergesort")
        .drop(columns=["_pref", "_num"])
        .reset_index(drop=True)
    )
    return sorted_df

# -----------------------------
# Data model for matches
# -----------------------------
@dataclass
class Match2:
    term: str
    start: int
    end: int
    category: str = ""
    risk: str = ""
    detail: str = ""
    kwd_no: str | None = None

# -----------------------------
# Session init & numbering helpers
# -----------------------------
def init_state():
    if "storage_path" not in st.session_state:
        st.session_state.storage_path = str(default_storage_path())
    if "kw_df" not in st.session_state:
        st.session_state.kw_df = load_db(Path(st.session_state.storage_path))
        # Record DB column order as loaded
        try:
            st.session_state.db_cols_order = list(st.session_state.kw_df.columns)
        except Exception:
            st.session_state.db_cols_order = list(DB_COLS)
    
    # 드롭다운 설정을 JSON 파일에서 불러오기
    if "opt_categories" not in st.session_state:
        config = load_dropdown_config()
        st.session_state.opt_categories = config["opt_categories"]
        st.session_state.opt_risks = config["opt_risks"]
        st.session_state.opt_details = config["opt_details"]
        st.session_state.opt_evidences = config["opt_evidences"]
        st.session_state.opt_alt_terms = config["opt_alt_terms"]
        normalize_dropdown_lists()
    
    if "counters" not in st.session_state:
        st.session_state.counters = {}

def scan_existing_counters():
    for v in st.session_state.kw_df["kwd_no"].dropna().astype(str):
        m = _kwd_no_pattern.match(v.strip())
        if not m:
            continue
        pfx, num = m.group(1), int(m.group(2))
        st.session_state.counters[pfx] = max(st.session_state.counters.get(pfx, 0), num)

def get_prefix(category: str) -> str:
    if category in CATEGORY_PREFIX:
        return CATEGORY_PREFIX[category]
    first = (category[:1] or "U").upper()
    return first if re.match(r"[A-Z]", first) else "U"

def infer_prefix_from_existing(category: str) -> str:
    df = st.session_state.kw_df
    if df is not None and not df.empty:
        sub = df[df["상품카테고리"].astype(str).str.strip() == str(category).strip()]["kwd_no"].dropna().astype(str)
        if not sub.empty:
            pref = sub.str.extract(r"^([A-Z])(\d{3,})$")[0].dropna()
            if not pref.empty:
                return pref.value_counts().idxmax()
    return get_prefix(category)

def next_kwd_no(category: str) -> str:
    pfx = infer_prefix_from_existing(category)
    df = st.session_state.kw_df
    next_num = None
    if df is not None and not df.empty:
        sub = df[df["상품카테고리"].astype(str).str.strip() == str(category).strip()]["kwd_no"].astype(str)
        nums = sub.str.extract(r"^%s(\d{3,})$" % pfx)[0].dropna()
        if not nums.empty:
            next_num = int(nums.astype(int).max()) + 1
    if next_num is None:
        cur = st.session_state.counters.get(pfx, 0) + 1
        st.session_state.counters[pfx] = cur
        next_num = cur
    else:
        st.session_state.counters[pfx] = max(st.session_state.counters.get(pfx, 0), next_num)
    return f"{pfx}{next_num:03d}"

def normalize_upload(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = [str(c).strip().lower() for c in d.columns]
    mapping = {
        "kwd_no": "kwd_no",
        "keyword_no": "kwd_no",
        "키워드no": "kwd_no",
        "키워드 no": "kwd_no",
        "키워드명": "키워드명",
        "term": "키워드명",
        "상품카테고리": "상품카테고리",
        "category": "상품카테고리",
        "리스크 등급": "리스크 등급",
        "risk": "리스크 등급",
        "리스크 등급별 세부 심의기준": "리스크 등급별 세부 심의기준",
        "세부 심의기준": "리스크 등급별 세부 심의기준",
        "증빙자료유형": "증빙자료유형",
        "증빙자료": "증빙자료유형",
        "대체키워드": "대체키워드",
        "대체키워드명": "대체키워드"
    }
    rename_dict = {col: mapping[col] for col in d.columns if col in mapping}
    if rename_dict:
        d = d.rename(columns=rename_dict)
    for col in DB_COLS:
        if col not in d.columns:
            if col == "키워드 조회 수(누적카운트)":
                d[col] = "0"
            elif col == "마지막 출력일자":
                d[col] = ""
            else:
                d[col] = "" if col != "kwd_no" else None
    d = d[get_db_cols()]
    for c in d.columns:
        d[c] = d[c].astype(str).str.strip()
    return d

# -----------------------------
# Variant generation for partial matching
# -----------------------------
def generate_keyword_variants(term: str) -> list:
    if term is None:
        return []
    t = str(term).strip()
    if not t:
        return []
    variants = set()

    def add(v: str):
        v = (v or "").strip()
        if not v:
            return
        if re.search(r"[A-Za-z]", v):
            if len(v) < 3:
                return
        else:
            if len(v) < 2:
                return
        variants.add(v)

    add(t)
    for piece in _delim_pattern.split(t):
        add(piece)
    outside = re.sub(r"\s*[\(\[（【].*?[\)\]）】]\s*", " ", t).strip()
    add(outside)
    for inner in re.findall(r"[\(\[（【](.*?)[\)\]）】]", t):
        add(inner)
        for piece in _delim_pattern.split(inner):
            add(piece)

    def _leading_core(s: str):
        s = (s or "").strip()
        if not s:
            return None
        m_ko = re.match(r'^[가-힣]{3,}', s)
        if m_ko:
            return m_ko.group(0)[:3]
        m_en = re.match(r'^[A-Za-z]{4,}', s)
        if m_en:
            return m_en.group(0)[:4]
        return None
    for _v in list(variants):
        _lc = _leading_core(_v)
        if _lc:
            add(_lc)

    def _add_prefixes(s: str):
        s = (s or "").strip()
        if not s:
            return
        m_ko = re.match(r'^[가-힣]+', s)
        if m_ko:
            ko = m_ko.group(0)
            if len(ko) >= 2:
                add(ko[:2])
            if len(ko) >= 3:
                add(ko[:3])
        m_en = re.match(r'^[A-Za-z]+', s)
        if m_en:
            en = m_en.group(0)
            if len(en) >= 3:
                add(en[:3].lower())
            if len(en) >= 4:
                add(en[:4].lower())
    for _v in list(variants):
        _add_prefixes(_v)
    variants = {re.sub(r"\s+", " ", v) for v in variants}
    return list(variants)

# -----------------------------
# Matching & highlight
# -----------------------------
def find_matches(text: str, kw_df: pd.DataFrame, match_mode: str = "유사 키워드 포함", exact_relaxed: bool = False, **kwargs) -> List[Match2]:
    matches: List[Match2] = []
    base = text or ""
    lower = base.lower()

    df = kw_df.copy()
    for col in DB_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df[df["키워드명"].astype(str).str.strip() != ""]

    occupied_set = set()
    records = df.to_dict("records")

    for r in records:
        term = str(r["키워드명"]).strip()
        category = str(r.get("상품카테고리",""))
        risk = str(r.get("리스크 등급",""))
        detail = str(r.get("리스크 등급별 세부 심의기준",""))
        kwd_no = r.get("kwd_no") or None

        variants = generate_keyword_variants(term)
        if match_mode == "유사 키워드 포함" and exact_relaxed:
            term_no_br = re.sub(r"\s*[\(\[（【].*?[\)\]）】]\s*", " ", str(term)).strip()
            term_no_br = re.sub(r"\s+", " ", term_no_br)
            term_norm = re.sub(r"\s+", " ", str(term)).strip()
            for _cand in [term_no_br, term_norm]:
                if _cand and _cand not in variants:
                    variants.insert(0, _cand)

        if match_mode == "정확 일치":
            variants = [str(term)]
            if exact_relaxed:
                term_no_br = re.sub(r"\s*[\(\[（【].*?[\)\]）】]\s*", " ", str(term)).strip()
                term_no_br = re.sub(r"\s+", " ", term_no_br)
                term_norm = re.sub(r"\s+", " ", str(term)).strip()
                for _cand in [term_no_br, term_norm]:
                    if _cand and _cand not in variants:
                        variants.append(_cand)

        variants.sort(key=lambda s: len(s), reverse=True)

        for v in variants:
            v_lower = v.lower()
            start = 0
            while True:
                idx = lower.find(v_lower, start)
                if idx == -1:
                    break
                end = idx + len(v)

                for _i_m, _m in enumerate(list(matches)):
                    if getattr(_m, 'start', None) == idx and getattr(_m, 'end', 0) < end:
                        for _j in range(_m.start, _m.end):
                            occupied_set.discard(_j)
                        try:
                            matches.pop(_i_m)
                        except Exception:
                            pass

                range_occupied = any(i in occupied_set for i in range(idx, end))

                if not range_occupied:
                    matches.append(Match2(term=term, start=idx, end=end, category=category, risk=risk, detail=detail, kwd_no=kwd_no))
                    for i in range(idx, end):
                        occupied_set.add(i)
                else:
                    same_span_exists = any((m.start == idx and m.end == end) for m in matches)
                    already_same = any((m.start == idx and m.end == end and (m.kwd_no == kwd_no and m.category == category and m.risk == risk)) for m in matches)
                    if same_span_exists and not already_same:
                        matches.append(Match2(term=term, start=idx, end=end, category=category, risk=risk, detail=detail, kwd_no=kwd_no))

                start = end

    matches.sort(key=lambda m: (m.start, m.end))
    return matches



# -----------------------------
# NLP-enhanced matching wrapper (spacing-insensitive)
# -----------------------------
def _build_compact_index(src_text: str):
    """
    Build a whitespace-removed version of the text and an index map
    from compact index -> original index.
    """
    if not src_text:
        return "", []
    compact_chars = []
    index_map = []
    for i, ch in enumerate(src_text):
        if ch.isspace():
            continue
        compact_chars.append(ch)
        index_map.append(i)
    return "".join(compact_chars), index_map


def _spacing_insensitive_matches(
    text: str,
    kw_df: pd.DataFrame,
    match_mode: str = "유사 키워드 포함",
    exact_relaxed: bool = False,
) -> List[Match2]:
    """
    추가 NLP 로직 (1단계):
    - 키워드와 본문에서 **모든 공백을 제거한 상태**로도 한 번 더 매칭을 수행합니다.
    - 띄어쓰기 오류/변형으로 인해 놓치는 케이스를 줄이는 것이 목적입니다.
    - 기본 find_matches 결과에 **추가**로만 사용되며, 기존 로직을 대체하지 않습니다.
    """
    if not text or kw_df is None or kw_df.empty:
        return []

    # 현재는 "유사 키워드 포함" 모드에서만 동작하도록 제한 (기존 동작 영향 최소화)
    if match_mode != "유사 키워드 포함":
        return []

    base = text or ""
    compact_text, index_map = _build_compact_index(base)
    if not compact_text:
        return []

    # DB 컬럼 정합성 맞추기
    df = kw_df.copy()
    for col in DB_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df[df["키워드명"].astype(str).str.strip() != ""]
    records = df.to_dict("records")

    extra_matches: List[Match2] = []

    for r in records:
        term = str(r["키워드명"]).strip()
        if not term:
            continue
        category = str(r.get("상품카테고리", ""))
        risk = str(r.get("리스크 등급", ""))
        detail = str(r.get("리스크 등급별 세부 심의기준", ""))
        kwd_no = r.get("kwd_no") or None

        # 기존 변형 생성 로직 재사용
        variants = generate_keyword_variants(term)
        if not variants:
            variants = [term]

        for v in variants:
            v = str(v or "").strip()
            if not v:
                continue
            # 공백 제거 버전으로 비교
            v_compact = re.sub(r"\s+", "", v)
            if not v_compact:
                continue

            start_pos = 0
            while True:
                idx = compact_text.find(v_compact, start_pos)
                if idx < 0:
                    break
                end_idx_compact = idx + len(v_compact) - 1
                if end_idx_compact >= len(index_map):
                    break

                start_orig = index_map[idx]
                end_orig = index_map[end_idx_compact] + 1  # exclusive

                extra_matches.append(
                    Match2(
                        term=term,
                        start=start_orig,
                        end=end_orig,
                        category=category,
                        risk=risk,
                        detail=detail,
                        kwd_no=kwd_no,
                    )
                )
                start_pos = idx + 1

    # 중복/겹침은 상위 래퍼에서 정리
    extra_matches.sort(key=lambda m: (m.start, m.end))
    return extra_matches


def find_matches_nlp(
    text: str,
    kw_df: pd.DataFrame,
    match_mode: str = "유사 키워드 포함",
    exact_relaxed: bool = False,
    **kwargs,
) -> List[Match2]:
    """
    기존 find_matches에 **간단한 NLP 보정(띄어쓰기 무시 매칭)**을 얹은 래퍼입니다.

    - 1차: 기존 find_matches 로직 그대로 수행 (기존 동작 유지)
    - 2차: _spacing_insensitive_matches 로 추가 후보를 찾음
    - 3차: (term, start, end, kwd_no) 기준으로 중복 제거 후 병합

    향후 형태소 분석 / 오타 보정 로직을 이 함수 안에 단계적으로 추가할 수 있습니다.
    """
    base_matches = find_matches(
        text,
        kw_df,
        match_mode=match_mode,
        exact_relaxed=exact_relaxed,
        **kwargs,
    )

    try:
        extra_matches = _spacing_insensitive_matches(
            text,
            kw_df,
            match_mode=match_mode,
            exact_relaxed=exact_relaxed,
        )
    except Exception:
        extra_matches = []

    merged: List[Match2] = []
    # 먼저 기존 결과를 그대로 넣고
    for m in base_matches or []:
        merged.append(m)

    # 추가 결과를 병합하면서 중복 제거
    def _same(a: Match2, b: Match2) -> bool:
        return (
            a.start == b.start
            and a.end == b.end
            and str(a.term) == str(b.term)
            and (a.kwd_no or "") == (b.kwd_no or "")
        )

    for m in extra_matches:
        if any(_same(m, ex) for ex in merged):
            continue
        merged.append(m)

    merged.sort(key=lambda m: (m.start, m.end))
    return merged

def highlight_text(text: str, matches: List[Match2]) -> str:
    if not matches:
        return html.escape(text)

    def _risk_grade(r: str) -> int:
        r = (r or '').strip()
        if r.startswith('1'): return 1
        if r.startswith('2'): return 2
        if r.startswith('3'): return 3
        if r.startswith('4'): return 4
        return 0

    text_colors = {1:'#e03131', 2:'#1c7ed6', 3:'#1c7ed6', 4:'#2f9e44'}
    seg_info = {}
    for m in matches:
        seg = text[m.start:m.end]
        if not seg:
            continue
        info = seg_info.get(seg)
        if info is None:
            info = {'ids': [], 'first': m.start, 'risk_map': {}}
            seg_info[seg] = info
        if m.start < info['first']:
            info['first'] = m.start
        if m.kwd_no and m.kwd_no not in info['ids']:
            info['ids'].append(m.kwd_no)
        g = _risk_grade(m.risk)
        if m.kwd_no:
            info['risk_map'][m.kwd_no] = g

    if not seg_info:
        return html.escape(text)

    ordered = sorted(seg_info.items(), key=lambda kv: (kv[1]['first'], -len(kv[0])))
    working = text
    tokens = []
    for i, (seg, info) in enumerate(ordered):
        token = f"__HL_TOKEN_{i}__"
        tokens.append((token, seg, info['ids'], info['risk_map']))
        working = working.replace(seg, token)

    escaped = html.escape(working)

    for token, seg, ids, risk_map in tokens:
        frag = []
        for kid in ids:
            g = risk_map.get(kid, 0)
            color = text_colors.get(g, 'inherit')
            frag.append(f"<span class='kwdno-link' data-kwd='{html.escape(kid)}' style='color:{color}; text-decoration: underline; cursor:pointer'><b>{html.escape(kid)}</b></span>")
        ids_html = ", ".join(frag)

        base_style = "background:#ffd43b !important; background-color:#ffd43b !important; color:#000;"
        risk_set = {g for g in risk_map.values() if g}
        if len(risk_set) == 1:
            if risk_set == {1}:
                mark_style = f" style='{base_style} background:#ffd6d6 !important; background-color:#ffd6d6 !important;'"
            elif risk_set == {2}:
                mark_style = f" style='{base_style} background:#d0ebff !important; background-color:#d0ebff !important;'"
            elif risk_set == {3}:
                mark_style = f" style='{base_style} background:#d0ebff !important; background-color:#d0ebff !important;'"
            elif risk_set == {4}:
                mark_style = f" style='{base_style} background:#d3f9d8 !important; background-color:#d3f9d8 !important;'"
            else:
                mark_style = f" style='{base_style}'"
        else:
            mark_style = f" style='{base_style}'"

        leading_ws = seg[:len(seg) - len(seg.lstrip())]
        trailing_ws = seg[len(seg.rstrip()):]
        core = seg.strip()
        decorated = f"{leading_ws}<mark{mark_style}>【<b>{html.escape(core)}</b>】({ids_html})</mark>{trailing_ws}"
        escaped = escaped.replace(html.escape(token), decorated)

    return escaped

# -----------------------------
# Upload merge helpers (needed for v97)
# -----------------------------
def assign_missing_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for i, row in out.iterrows():
        if not str(row.get("kwd_no", "")).strip():
            cat = str(row.get("상품카테고리", "")).strip() or "공통(전체)"
            out.at[i, "kwd_no"] = next_kwd_no(cat)
    return out

def merge_or_overwrite(base: pd.DataFrame, incoming: pd.DataFrame, mode: str) -> Tuple[pd.DataFrame, int, int]:
    inc = incoming.copy()
    before = len(inc)
    if "kwd_no" in inc.columns and inc["kwd_no"].astype(str).str.strip().any():
        inc = inc.sort_values(by=["kwd_no"]).drop_duplicates(subset=["kwd_no"], keep="last")
    else:
        inc = inc.sort_values(by=["키워드명", "상품카테고리"]).drop_duplicates(subset=["키워드명", "상품카테고리"], keep="last")
    dedup_removed = before - len(inc)

    if mode == "overwrite":
        return inc.reset_index(drop=True), len(inc), dedup_removed

    if base.empty:
        return inc.reset_index(drop=True), len(inc), dedup_removed

    if inc["kwd_no"].astype(str).str.strip().any():
        new_ids: Set[str] = set(inc["kwd_no"].astype(str))
        base = base[~base["kwd_no"].astype(str).isin(new_ids)]

    merged = pd.concat([base, inc], ignore_index=True)
    return merged, len(inc), dedup_removed

# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Keyword Risk Analyzer", layout="wide")

# --- Compact button & row spacing ---
st.markdown("""
<style>
/* Reduce vertical gaps between rows of columns (affects button grids) */
div[data-testid="stHorizontalBlock"] {
    margin-bottom: 0rem;
}
/* Make buttons more compact (less vertical padding) */
div.stButton > button {
    padding-top: 0rem;
    padding-bottom: 0rem;
    min-height: 1.2rem;
}
</style>
""", unsafe_allow_html=True)
st.title("🔎 광고심의 자동화 솔루션(가칭)")

# --- Scroll margin for keyword anchor links ---
st.markdown("""
<style>
[id^="G"], [id^="F"], [id^="P"], [id^="K"] {
  scroll-margin-top: 100px;
}
</style>
""", unsafe_allow_html=True)

# --- Minimal wrap CSS (safe) ---
st.markdown(
    """
    <style>
    [data-testid="stDataFrame"] [role="gridcell"] {
        white-space: normal !important;
        word-break: break-word !important;
    }
    </style>
    """, unsafe_allow_html=True
)

st.markdown(
    f"""
<style>
.hl-box{{
  border:1px solid rgba(49,51,63,0.2);
  border-radius:8px;
  padding:12px;
  max-height:{MAX_HIGHLIGHT_HEIGHT}px;
  overflow-y:auto;
  overflow-x:auto;
  background:#fff;
  white-space:pre-wrap;
  line-height:1.5;
  box-sizing:border-box;
}}
.hl-box mark{{padding:0;border-radius:2px}}
[data-testid="stDataFrame"] div{{white-space:normal !important;}}
</style>
""",
    unsafe_allow_html=True,
)

init_state()
scan_existing_counters()

# Sidebar: 저장 경로 및 유틸 (초기화 UI는 제거됨)
st.sidebar.header("🗂 데이터 저장")
st.sidebar.caption("CSV로 저장/불러오기 (경로 문제 시 OneDrive 외부 경로 권장)")
st.sidebar.write("현재 경로:")
st.sidebar.code(st.session_state.storage_path, language="text")
new_path = st.sidebar.text_input("저장 경로 변경", value=st.session_state.storage_path)
c_sb1, c_sb2 = st.sidebar.columns(2)
if c_sb1.button("경로 적용", key="apply_path"):
    st.session_state.storage_path = new_path
    st.session_state.kw_df = load_db(Path(st.session_state.storage_path))
    scan_existing_counters()
    st.sidebar.success("경로 적용 및 DB 로드 완료")
if c_sb2.button("강제 저장", key="force_save"):
    save_db(st.session_state.kw_df, Path(st.session_state.storage_path))
    st.sidebar.success("저장 완료")



# --- 관리자 로그인 영역 (키워드 관리 탭 비공개용) ---
def render_admin_login():
    """사이드바에서 관리자 계정 로그인/로그아웃을 처리하고, st.session_state.is_admin 플래그를 유지합니다."""
    if "is_admin" not in st.session_state:
        st.session_state.is_admin = False

    admin_user = None
    admin_password = None

    # 1) Streamlit secrets에서 시도
    try:
        admin_user = st.secrets.get("ADMIN_USER", None)
        admin_password = st.secrets.get("ADMIN_PASSWORD", None)
    except Exception:
        # secrets 미설정 환경 등 예외는 무시
        pass

    # 2) 환경 변수에서 보조로 시도 (깃허브 공개 저장소에 비밀번호를 하드코딩하지 않기 위함)
    if admin_user is None:
        admin_user = os.environ.get("ADMIN_USER")
    if admin_password is None:
        admin_password = os.environ.get("ADMIN_PASSWORD")

    with st.sidebar.expander("🔐 관리자 로그인", expanded=False):
        st.caption("키워드 관리 탭은 관리자 전용입니다. 관리자만 비밀번호를 알고 있어야 합니다.")
        input_id = st.text_input("관리자 ID", key="admin_id")
        input_pw = st.text_input("관리자 비밀번호", type="password", key="admin_pw")

        c_login, c_logout = st.columns(2)
        with c_login:
            if st.button("로그인", key="admin_login"):
                ok = False
                if admin_password:
                    # ADMIN_USER가 설정되어 있으면 ID+PW 모두 검사, 아니면 PW만 검사
                    if admin_user:
                        ok = (input_id == admin_user and input_pw == admin_password)
                    else:
                        ok = (input_pw == admin_password)
                if ok:
                    st.session_state.is_admin = True
                    st.success("관리자 모드로 접속되었습니다.")
                else:
                    st.session_state.is_admin = False
                    st.error("관리자 인증에 실패했습니다. ID/비밀번호를 확인해주세요.")

        with c_logout:
            if st.button("로그아웃", key="admin_logout"):
                st.session_state.is_admin = False
                st.info("로그아웃 되었습니다.")

def is_admin() -> bool:
    """현재 세션이 관리자 모드인지 여부를 반환합니다."""
    return bool(st.session_state.get("is_admin", False))


render_admin_login()

tab1, tab3, tab2 = st.tabs(["분석하기", "리포트(주간 Top N)", "키워드 관리"])

with tab1:
    # ① 텍스트 입력
    st.subheader("① 텍스트 입력")
    sample = "이 문장에는 필러와 안티에이징이라는 키워드가 포함되어 있습니다."
    text = st.text_area("분석할 텍스트", value=sample, height=180)

    st.markdown("---")
    # ②) 분석 실행
    st.subheader("② 분석 실행")
    with st.expander("사전 필터 (선택)", expanded=True):
        cf1, cf2, cf3 = st.columns([1,1,1])
        with cf1:
            db_cats = unique_values_from_db("상품카테고리")
            pre_cats = st.multiselect("상품카테고리", options=db_cats, key="pre_cats")
        with cf2:
            db_risks = unique_values_from_db("리스크 등급")
            pre_risks = st.multiselect("리스크 등급", options=db_risks, key="pre_risks")
        with cf3:
            match_mode = st.radio("매칭 조건", ["유사 키워드 포함", "정확 일치"], index=1, key="match_mode")
            exact_relaxed = st.checkbox("(괄호 제거·공백 정규화·영문 대소문자 무시)", value=True, key="exact_relaxed")
    
    if st.button("텍스트 분석하기", type="primary", key="btn_analyze"):
        try:
            if st.session_state.kw_df.empty:
                st.info("키워드가 없습니다. 먼저 추가하세요.")
            else:
                matches = find_matches_nlp(
                    text,
                    st.session_state.kw_df,
                    match_mode=st.session_state.match_mode,
                    exact_relaxed=st.session_state.exact_relaxed
                )
                if st.session_state.get("pre_cats"):
                    matches = [m for m in matches if str(m.category).strip() in set(st.session_state.pre_cats)]
                if st.session_state.get("pre_risks"):
                    matches = [m for m in matches if str(m.risk).strip() in set(st.session_state.pre_risks)]

                if matches:
                    out = pd.DataFrame(
                        [
                            {
                                "kwd_no": m.kwd_no,
                                "키워드명": m.term,
                                "상품카테고리": m.category,
                                "리스크 등급": m.risk,
                                "리스크 등급별 세부 심의기준": m.detail,
                                "start": m.start,
                                "end": m.end,
                            }
                            for m in matches
                        ]
                    )
                    base_cols = ["kwd_no", "키워드명", "상품카테고리", "리스크 등급", "리스크 등급별 세부 심의기준"]
                    display_unique = out[base_cols].drop_duplicates(subset=["kwd_no"]).reset_index(drop=True)
                    display_unique = display_unique.sort_values(by="kwd_no", ascending=True).reset_index(drop=True)

                    # 누적 카운팅 + 마지막 출력일자 + 로그 기록
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    unique_kwds = display_unique["kwd_no"].dropna().astype(str).str.strip().tolist()
                    if unique_kwds:
                        df = st.session_state.kw_df
                        try:
                            df_indexed = df.set_index("kwd_no")
                            for kw in unique_kwds:
                                if kw in df_indexed.index:
                                    try:
                                        prev = int(str(df_indexed.at[kw, "키워드 조회 수(누적카운트)"]).strip() or "0")
                                    except Exception:
                                        prev = 0
                                    df_indexed.at[kw, "키워드 조회 수(누적카운트)"] = str(prev + 1)
                                    df_indexed.at[kw, "마지막 출력일자"] = now_str
                            st.session_state.kw_df = df_indexed.reset_index()
                        except Exception:
                            for kw in unique_kwds:
                                idx_list = st.session_state.kw_df.index[st.session_state.kw_df["kwd_no"] == kw].tolist()
                                if idx_list:
                                    idx = idx_list[0]
                                    try:
                                        prev = int(str(st.session_state.kw_df.at[idx, "키워드 조회 수(누적카운트)"]).strip() or "0")
                                    except Exception:
                                        prev = 0
                                    st.session_state.kw_df.at[idx, "키워드 조회 수(누적카운트)"] = str(prev + 1)
                                    st.session_state.kw_df.at[idx, "마지막 출력일자"] = now_str
                        save_db(st.session_state.kw_df, Path(st.session_state.storage_path))
                        append_hits_log(unique_kwds)

                    out_buf = io.BytesIO()
                    out.to_csv(out_buf, index=False, encoding="utf-8-sig")

                    st.session_state['analysis_df_full'] = out
                    st.session_state['analysis_df_display_unique'] = display_unique
                    st.session_state['analysis_highlight_html'] = highlight_text(text, matches)
                    st.session_state['analysis_count'] = len(display_unique)
                    st.session_state['analysis_text'] = text
                    st.session_state['analysis_csv_bytes'] = out_buf.getvalue()
                    st.session_state['analysis_show'] = True
                else:
                    st.session_state['analysis_show'] = False
                    st.session_state['analysis_df_full'] = None
                    st.session_state['analysis_df_display_unique'] = None
                    st.session_state['analysis_highlight_html'] = ''
                    st.session_state['analysis_count'] = 0
                    st.session_state['analysis_text'] = ''
                    st.session_state['analysis_csv_bytes'] = None
                    st.info("매칭된 키워드가 없습니다.")
        except Exception as e:
            st.error(f"오류 발생: {e}")

    _persist_df_full = st.session_state.get('analysis_df_full')
    _persist_df_unique = st.session_state.get('analysis_df_display_unique')
    _persist_highlight_html = st.session_state.get('analysis_highlight_html', '')
    _persist_count = st.session_state.get('analysis_count')
    _persist_csv = st.session_state.get('analysis_csv_bytes')

    if st.session_state.get('analysis_show') and _persist_df_full is not None:
        from streamlit import column_config as cc
        st.success(f"총 {_persist_count}건의 키워드가 발견되었습니다.")
        show_pos = st.checkbox("위치 인덱스(start/end) 보기", value=st.session_state.get("cb_pos", False), key="cb_pos")
        base_cols = ["kwd_no", "키워드명", "상품카테고리", "리스크 등급", "리스크 등급별 세부 심의기준"]
        column_cfg = {"리스크 등급별 세부 심의기준": cc.TextColumn()}

        # DB의 '모든 열'과 조인하여 전체 정보 표시
        try:
            merged_full = _persist_df_unique[["kwd_no"]].merge(
                st.session_state.kw_df[get_db_cols()], on="kwd_no", how="left"
            )
            merged_full = sort_for_display(merged_full)
        except Exception:
            merged_full = _persist_df_unique

        if show_pos:
            st.dataframe(_rename_fcol(_persist_df_full[base_cols + ["start", "end"]], width="stretch", column_config=column_cfg))
            st.markdown("**DB 전체 열 정보 (중복 제거)**")
            st.dataframe(_rename_fcol(merged_full), width="stretch")
        else:
            st.dataframe(_rename_fcol(merged_full), width="stretch")

        if _persist_csv is not None:
            st.download_button("분석 결과 CSV 다운로드", data=_persist_csv, file_name="analysis_results.csv", mime="text/csv", key="dl_analysis")

        
    st.subheader("하이라이트 미리보기")
    # 기존 하이라이트 HTML은 그대로 표시
    st.markdown(
        f"<div class='hl-box'>{_persist_highlight_html}</div>",
        unsafe_allow_html=True,
    )

    # 새 창/네비게이션 없이 값만 주입: 하이라이트 HTML에서 kwd_no 수집 후 버튼으로 제공
    def _normalize_kw_label(s: str) -> str:
        try:
            s = str(s)
        except Exception:
            return ""
        # Normalize common invisible spaces (NBSP, zero-width space) to regular space, then strip
        for ch in ("\u00A0", "\u200B"):
            s = s.replace(ch, " ")
        return s.strip()

    _kwd_candidates = []
    try:
        import re as _re_for_kwd
        _raw_kwd_candidates = _re_for_kwd.findall(r"data-kwd='([^']+)'", _persist_highlight_html or "")
        _cleaned_candidates = []
        for _c in _raw_kwd_candidates:
            _n = _normalize_kw_label(_c)
            if _n:
                _cleaned_candidates.append(_n)
        _kwd_candidates = list(dict.fromkeys(_cleaned_candidates))
    except Exception:
        _kwd_candidates = []


    # 하이라이트 빠른 선택: 토글 & 시각 표시(버튼 배경색 ON/OFF)
    if _kwd_candidates:
        # 👉 필터 입력창 내용(kwdno_filter_input_tab1)을 기준으로 선택 상태 계산
        _cur_filter = (st.session_state.get("kwdno_filter_input_tab1") or "").strip()
        _raw_selected_list = [p for p in _cur_filter.split(",") if p.strip()]
        selected_list = [_normalize_kw_label(p) for p in _raw_selected_list if _normalize_kw_label(p)]

        with st.expander("하이라이트 빠른 선택 (클릭하면 필터 입력창에 자동 입력)", expanded=False):
            st.markdown("<div class='quick-select-block'>", unsafe_allow_html=True)
            # 버튼 그리드 배치 (15열 고정)
            n = 15
            rows = [_kwd_candidates[i:i+n] for i in range(0, len(_kwd_candidates), n)]
            for row in rows:
                cols = st.columns(n)
                for i, k in enumerate(row):
                    if i >= len(cols):
                        break
                    with cols[i]:
                        # 필터 문자열에 포함되어 있으면 선택(불 ON)
                        is_selected = _normalize_kw_label(k) in selected_list
                        btn_type = "primary" if is_selected else "secondary"
                        if st.button(k, key=f"kwbtn_{k}", type=btn_type):
                            # 현재 필터 기준으로 목록 복사
                            parts = selected_list.copy()
                            if is_selected:
                                # 이미 선택 → 필터에서 제거 (불 OFF)
                                parts = [p for p in parts if p != k]
                            else:
                                # 선택 안 됨 → 필터에 추가 (불 ON)
                                if k not in parts:
                                    parts.append(k)
                            # 필터 입력창 값 갱신
                            st.session_state["kwdno_filter_input_tab1"] = ", ".join(parts)
                            # 이후 루프에서 바로 반영되도록 로컬 상태도 갱신
                            selected_list = parts
            st.markdown("</div>", unsafe_allow_html=True)

            # JS: clicking a highlighted kwd_no writes kwd to query param and reloads SAME window
            st.markdown("""
        <style>
        .sticky-kwd-expander {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            z-index: 999;
            background-color: white;
        }
        </style>
        """, unsafe_allow_html=True)

        st.markdown("""
            <script>
            // 하이라이트 클릭 시 kwd_no를 쿼리 파라미터로 주입
            document.addEventListener('click', function(e){
              const el = e.target.closest('.kwdno-link');
              if(!el) return;
              e.preventDefault();
              const kwd = el.getAttribute('data-kwd') || '';
              try {
                const url = new URL(window.location.href);
                url.searchParams.set('kwdno_click', kwd);
                window.location.href = url.toString();
              } catch (err) {
                console.warn('kwdno-click failed', err);
              }
            });

            // "하이라이트 빠른 선택" 익스팬더를 상단에 고정
            function markStickyKwdExpander() {
              try {
                const nodes = document.querySelectorAll('[data-testid="stExpander"]');
                nodes.forEach(function(el) {
                  const labelText = (el.innerText || "");
                  if (labelText.indexOf("하이라이트 빠른 선택") !== -1) {
                    el.classList.add("sticky-kwd-expander");
                  }
                });
              } catch (err) {
                console.warn('sticky kwd expander failed', err);
              }
            }
            window.addEventListener('load', markStickyKwdExpander);
            setTimeout(markStickyKwdExpander, 1500);
            </script>
            """, unsafe_allow_html=True)


        st.markdown("---")
        st.markdown("#### 🔍 kwd_no 필터")

        # Read kwd_no injected from highlight click (same-window reload)
        try:
            qp_val = None
            try:
                # Streamlit >= 1.30
                qp = st.query_params
                if isinstance(qp.get("kwdno_click"), list):
                    qp_val = (qp.get("kwdno_click") or [None])[0]
                else:
                    qp_val = qp.get("kwdno_click")
            except Exception:
                # Older Streamlit
                qp = st.experimental_get_query_params()
                qp_val = (qp.get("kwdno_click") or [None])[0]
            if qp_val:
                st.session_state["kwdno_filter_input_tab1"] = str(qp_val)
                # Clear the param to prevent sticky reload
                try:
                    st.query_params.clear()
                except Exception:
                    st.experimental_set_query_params()
        except Exception:
            pass

        def _reset_kwdno_tab1():
            st.session_state["kwdno_filter_input_tab1"] = ""

        kwd_filter_val = st.text_input(
            "kwd_no 입력 (쉼표로 구분하여 여러 개 입력 가능)",
            placeholder="예: G060,G105,I200,I201,I202,P024,P025,P027,F031,F131,F180",
            key="kwdno_filter_input_tab1",
        )

        col_f1, col_f2 = st.columns([1, 1])
        with col_f1:
            apply_kwd_filter_tab1 = st.button("필터 적용", key="btn_apply_kwd_filter_tab1")
        with col_f2:
            reset_kwd_filter_tab1 = st.button("초기화", key="btn_reset_kwd_filter_tab1", on_click=_reset_kwdno_tab1)

        _base_df = st.session_state.kw_df.copy()
        if apply_kwd_filter_tab1 and st.session_state.kwdno_filter_input_tab1.strip():
            _targets = [x.strip().upper() for x in st.session_state.kwdno_filter_input_tab1.split(",") if x.strip()]
            _view_df = _base_df[_base_df["kwd_no"].astype(str).str.upper().isin(_targets)]
            st.success(f"총 {len(_view_df)}건이 필터링되었습니다.")
        else:
            _view_df = _base_df

        st.caption(f"현재 표시된 항목: {len(_view_df)} / 전체: {len(_base_df)}")
        st.dataframe(_rename_fcol(sort_for_display(_view_df[get_db_cols()])), width="stretch")

        buf_bytes = io.BytesIO()
        st.session_state.kw_df[get_db_cols()].to_csv(buf_bytes, index=False, encoding="utf-8-sig")
        st.download_button("키워드 CSV 다운로드", data=buf_bytes.getvalue(), file_name="keywords_current.csv", mime="text/csv", key="dl_kw_current")

with tab2:
    if is_admin():
        st.subheader("키워드 관리 (편집/삭제/템플릿/업로드)")

        st.markdown("### 📂 키워드 업로드 (.csv / .xlsx)")
        col_u1, col_u2 = st.columns([2, 2])
        with col_u1:
            upload_mode = st.radio("업로드 모드", ["기존 유지 + 새로 추가", "완전 덮어쓰기"], horizontal=False, key="upload_mode")
        with col_u2:
            uploaded_file = st.file_uploader("키워드 템플릿 업로드", type=["csv", "xlsx"], key="uploader")

        if uploaded_file is not None:
            try:
                inc_df = None
                name = uploaded_file.name.lower()
                if name.endswith(".csv"):
                    raw = uploaded_file.read()
                    inc_df = read_csv_with_fallback_bytes(raw)
                elif name.endswith(".xlsx"):
                    raw = uploaded_file.read()
                    try:
                        inc_df = pd.read_excel(io.BytesIO(raw), dtype=str).fillna("")
                    except Exception:
                        inc_df = read_xlsx_without_openpyxl(raw, header_row=1)
                else:
                    st.error("지원하지 않는 파일 형식입니다. CSV 또는 XLSX를 업로드하세요.")
                    inc_df = None

                if inc_df is not None:
                    inc_df = normalize_upload(inc_df)
                    # 기존 유틸 함수 폴백 처리
                    merged = None
                    added_cnt = 0
                    dedup_removed = 0
                    try:
                        inc_df = assign_missing_ids(inc_df)
                        mode = "merge" if upload_mode == "기존 유지 + 새로 추가" else "overwrite"
                        merged, added_cnt, dedup_removed = merge_or_overwrite(st.session_state.kw_df, inc_df, mode)
                    except NameError:
                        merged = pd.concat([st.session_state.kw_df, inc_df], ignore_index=True)
                    st.session_state.kw_df = merged
                    save_db(st.session_state.kw_df, Path(st.session_state.storage_path))
                    st.success(f"업로드 완료 — 추가 {added_cnt}건, (내부 중복제거 {dedup_removed}건) 저장됨.")
            except Exception as e:
                st.error(f"업로드 처리 중 오류: {e}")
    
        st.subheader("키워드 입력/관리")

        c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
        with c1:
            term_in = st.text_input("키워드명 *", placeholder="예: 필러")
        with c2:
            db_cat_opts = unique_values_from_db("상품카테고리")
            cat_choice = st.selectbox("상품카테고리", db_cat_opts + ["(직접 입력)"])
        with c3:
            db_risk_opts = unique_values_from_db("리스크 등급")
            risk_choice = st.selectbox("리스크 등급", db_risk_opts)
        with c4:
            add_click = st.button("추가", type="primary", key="btn_add_row")

        c5, c6, c7 = st.columns([3, 3, 3])
        with c5:
            detail_choice = st.selectbox("리스크 등급별 세부 심의기준", _dropdown_with_input_option(st.session_state.opt_details))
        with c6:
            db_evid_opts = unique_values_from_db("증빙자료유형")
            evidence_choice = st.selectbox("증빙자료유형", _dropdown_with_input_option(db_evid_opts))
        with c7:
            alt_choice = st.selectbox("대체키워드", _dropdown_with_input_option(st.session_state.opt_alt_terms))

        new_cat = new_detail = new_evid = new_alt = ""
        new_kwd = ""
    
        if cat_choice == "(직접 입력)":
            cols_nc = st.columns([2,1])
            with cols_nc[0]:
                new_cat = st.text_input("새 카테고리 입력", key="new_cat_input")
            with cols_nc[1]:
                new_kwd = st.text_input("키워드NO (선택, 예: P001)", key="new_kwd_input")
    
        if detail_choice == "(직접 입력)":
            new_detail = st.text_input("새 세부 심의기준 입력", key="new_detail_input")
        if evidence_choice == "(직접 입력)":
            new_evid = st.text_input("새 증빙자료유형 입력", key="new_evid_input")
        if alt_choice == "(직접 입력)":
            new_alt = st.text_input("새 대체키워드 입력", key="new_alt_input")

        if add_click:
            if not term_in.strip():
                st.warning("키워드명은 필수입니다.")
            else:
                category = new_cat.strip() if cat_choice == "(직접 입력)" and new_cat.strip() else cat_choice
                detail = new_detail.strip() if detail_choice == "(직접 입력)" and new_detail.strip() else detail_choice
                evidence = new_evid.strip() if evidence_choice == "(직접 입력)" and new_evid.strip() else evidence_choice
                alt_term = new_alt.strip() if alt_choice == "(직접 입력)" and new_alt.strip() else alt_choice

                if category == "(직접 입력)" or detail == "(직접 입력)" or evidence == "(직접 입력)" or alt_term == "(직접 입력)":
                    st.warning("새 항목을 입력했으면 값을 채우거나 기존 목록에서 선택하세요.")
                else:
                    kwd = None
                    _kw = (st.session_state.get("new_kwd_input", "") or "").strip()
                    if _kw:
                        m = _custom_kwd_pattern.match(_kw)
                        if m:
                            pfx, num = m.group(1).upper(), m.group(2)
                            kwd = f"{pfx}{int(num):0{len(num)}d}"
                    if kwd is None:
                        kwd = next_kwd_no(category)
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    _existing_kwds = st.session_state.kw_df['kwd_no'].astype(str).str.upper().tolist()
                    if str(kwd).upper() in _existing_kwds:
                        st.warning(f"{kwd}은(는) 이미 존재하는 키워드 번호입니다. 다른 번호를 지정해 주세요.")
                        st.stop()

                    row = {
                        "kwd_no": kwd,
                        "키워드명": term_in.strip(),
                        "상품카테고리": category,
                        "리스크 등급": risk_choice,
                        "리스크 등급별 세부 심의기준": detail,
                        "증빙자료유형": evidence,
                        "대체키워드": alt_term,
                        "키워드 조회 수(누적카운트)": "0",
                        "마지막 출력일자": "",

                        "키워드 등록일자": now_str
                    }
                    st.session_state.kw_df = pd.concat([st.session_state.kw_df, pd.DataFrame([row])], ignore_index=True)
                    save_db(st.session_state.kw_df, Path(st.session_state.storage_path))
                    st.success(f"[{kwd}] '{term_in}' 추가 및 저장됨")

                    # 저장 성공 후에만 드롭다운 옵션에 신규 값 추가 및 JSON 저장
                    if cat_choice == "(직접 입력)" and new_cat.strip() and new_cat not in st.session_state.opt_categories:
                        st.session_state.opt_categories.append(new_cat.strip())
                        save_dropdown_config()
                    if detail_choice == "(직접 입력)" and new_detail.strip() and new_detail not in st.session_state.opt_details:
                        st.session_state.opt_details.append(new_detail.strip())
                        save_dropdown_config()
                    if evidence_choice == "(직접 입력)" and new_evid.strip() and new_evid not in st.session_state.opt_evidences:
                        st.session_state.opt_evidences.append(new_evid.strip())
                        save_dropdown_config()
                    if alt_choice == "(직접 입력)" and new_alt.strip() and new_alt not in st.session_state.opt_alt_terms:
                        st.session_state.opt_alt_terms.append(new_alt.strip())
                        save_dropdown_config()

        with st.expander("드롭다운 값 삭제 (관리자)"):
            colm1, colm2 = st.columns([2,3])

            with colm1:
                target_list = st.selectbox("대상 목록", [
                    "상품카테고리", "리스크 등급", "리스크 등급별 세부 심의기준", "증빙자료유형", "대체키워드"
                ], key="del_list")

            with colm2:
                options_map = {
                    "상품카테고리": st.session_state.opt_categories,
                    "리스크 등급": st.session_state.opt_risks,
                    "리스크 등급별 세부 심의기준": st.session_state.opt_details,
                    "증빙자료유형": st.session_state.opt_evidences,
                    "대체키워드": st.session_state.opt_alt_terms,
                }

                current = options_map.get(target_list, [])
                to_del = st.selectbox("삭제할 값 선택", current, key="del_value") if current else None

            if to_del is not None and st.button("삭제", key="btn_del_value"):
                st.session_state['__del_request__'] = (target_list, to_del)

            if st.session_state.get('__del_request__'):
                tgt, val = st.session_state['__del_request__']
                st.warning(f"{tgt}에서 '{val}' 값을 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.")

                cols_confirm = st.columns([1,1,6])

                if cols_confirm[0].button("예, 삭제", key="btn_del_confirm"):
                    lst = options_map[tgt]
                    try:
                        lst.remove(val)
                        save_dropdown_config()  # JSON 파일에 저장
                        st.success(f"삭제 완료: {tgt} → {val}")
                    except ValueError:
                        st.info("이미 삭제되었거나 존재하지 않습니다.")
                    st.session_state['__del_request__'] = None

                if cols_confirm[1].button("아니오", key="btn_del_cancel"):
                    st.session_state['__del_request__'] = None

        st.markdown("---")

        mode = st.radio("편집 모드 선택", ["드롭다운(제한 입력)", "자유 입력"], horizontal=True, key="edit_mode")
        base_df = sort_for_display(st.session_state.kw_df[get_db_cols()].copy())

        if mode == "드롭다운(제한 입력)":
            from streamlit import column_config as cc
            # Build union option lists including existing DB values
            db_cat_opts = unique_values_from_db("상품카테고리")
            db_risk_opts = unique_values_from_db("리스크 등급")
            db_detail_opts = unique_values_from_db(RAW_F_COL)
            db_alt_opts = unique_values_from_db("대체키워드")
            detail_opts = _dropdown_with_input_option(list(set((st.session_state.opt_details or []) + (db_detail_opts or []))))
            alt_opts = _dropdown_with_input_option(list(set((st.session_state.opt_alt_terms or []) + (db_alt_opts or []))))
            risk_opts = list(set((st.session_state.opt_risks or []) + (db_risk_opts or []))) or st.session_state.opt_risks
            cat_opts = list(set((st.session_state.opt_categories or []) + (db_cat_opts or []))) or st.session_state.opt_categories
            edited_df = st.data_editor(_rename_fcol(
                base_df),
                column_config={
                    "상품카테고리": cc.SelectboxColumn(options=cat_opts, required=False),
                    "리스크 등급": cc.SelectboxColumn(options=risk_opts, required=False),
                    DISPLAY_F_COL: cc.SelectboxColumn(options=detail_opts, required=False, label=DISPLAY_F_COL),
                    "증빙자료유형": cc.SelectboxColumn(options=st.session_state.opt_evidences, required=False),
                    "대체키워드": cc.SelectboxColumn(options=alt_opts, required=False),
                },
                width="stretch",
                num_rows="dynamic",
                key="editor_dropdown",
            )
        else:
            edited_df = st.data_editor(_rename_fcol(base_df), width="stretch", num_rows="dynamic", key="editor_free")

        csave, cdel, ctmpl = st.columns([1, 1, 2])
        with csave:
            if st.button("변경사항 저장", type="primary", key="btn_save_edits"):
                edited_df = edited_df.fillna("")
                # --- Map UI display column back to raw DB column before slicing ---
                try:
                    if DISPLAY_F_COL in edited_df.columns and RAW_F_COL not in edited_df.columns:
                        edited_df[RAW_F_COL] = edited_df[DISPLAY_F_COL]
                        try:
                            edited_df = edited_df.drop(columns=[DISPLAY_F_COL])
                        except Exception:
                            pass
                except Exception:
                    pass
                try:
                    st.session_state.kw_df = edited_df[get_db_cols()].copy()
                except KeyError:
                    missing = [c for c in get_db_cols() if c not in edited_df.columns]
                    for c in missing:
                        edited_df[c] = ""
                    st.session_state.kw_df = edited_df[get_db_cols()].copy()

                save_db(st.session_state.kw_df, Path(st.session_state.storage_path))
                st.success("수정 내용 저장 완료")

        with cdel:
            del_targets = st.multiselect(
                "삭제할 항목 선택 (kwd_no 기준)",
                options=st.session_state.kw_df["kwd_no"].tolist(),
                key="del_targets"
            )
            if st.button("선택 삭제", type="secondary", key="btn_delete_rows"):
                if not del_targets:
                    st.warning("삭제할 항목을 선택하세요.")
                else:
                    st.session_state.kw_df = st.session_state.kw_df[~st.session_state.kw_df["kwd_no"].isin(del_targets)].reset_index(drop=True)
                    save_db(st.session_state.kw_df, Path(st.session_state.storage_path))
                    st.success(f"{len(del_targets)}건 삭제 완료")

        with ctmpl:
            st.markdown("**업로드 템플릿 다운로드**")
            tmpl_csv_buf = io.BytesIO()
            pd.DataFrame(columns=DB_COLS).to_csv(tmpl_csv_buf, index=False, encoding="utf-8-sig")
            st.download_button("CSV 템플릿 다운로드", data=tmpl_csv_buf.getvalue(), file_name="keyword_template.csv", mime="text/csv", key="dl_tmpl_csv")
            try:
                tmpl_xlsx_buf = io.BytesIO()
                with pd.ExcelWriter(tmpl_xlsx_buf) as writer:
                    pd.DataFrame(columns=DB_COLS).to_excel(writer, sheet_name="keywords_template", index=False)
                st.download_button("엑셀 템플릿 다운로드", data=tmpl_xlsx_buf.getvalue(), file_name="keyword_template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_tmpl_xlsx")
            except Exception as e:
                st.info(f"엑셀 템플릿은 현재 환경에 openpyxl/xlsxwriter가 없어 CSV 템플릿으로 대체합니다. (세부: {e})")

        st.caption(f"현재 저장된 항목: {len(st.session_state.kw_df)}")
        st.dataframe(_rename_fcol(sort_for_display(st.session_state.kw_df[get_db_cols()])), width="stretch")


    else:
        st.subheader("키워드 관리 (관리자 전용)")
        st.info("이 탭은 관리자 전용입니다. 좌측 사이드바의 '관리자 로그인' 영역에서 인증 후 이용할 수 있습니다.")

with tab3:
    st.subheader("📈 리포트 — 최근 7일 Top N")

    # 리포트 본문
    log_df = load_hits_log()
    if log_df.empty:
        st.info("조회 로그가 아직 없습니다. 분석을 몇 번 실행하면 리포트가 생성됩니다.")
    else:
        col_r1, col_r2 = st.columns([1,1])
        with col_r1:
            top_n = st.number_input("Top N", min_value=1, max_value=100, value=10, step=1, key="report_topn")
        with col_r2:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=7)
            st.write(f"기간: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            _tmp = log_df.copy()
            _tmp["ts_dt"] = pd.to_datetime(_tmp["ts"], errors="coerce")
            mask = (_tmp["ts_dt"] >= start_dt) & (_tmp["ts_dt"] <= end_dt)
            week_df = _tmp.loc[mask]
            agg = week_df.groupby("kwd_no").size().reset_index(name="최근7일_조회수")
            agg = agg.sort_values("최근7일_조회수", ascending=False).head(top_n)

            report = agg.merge(st.session_state.kw_df[get_db_cols()], on="kwd_no", how="left")
            report = report.sort_values(["최근7일_조회수","kwd_no"], ascending=[False, True], kind="mergesort")

            # ---- Column order tweak: place '최근7일_조회수' right after '대체키워드' ----
            try:
                cols = list(report.columns)
                if "최근7일_조회수" in cols:
                    cols.remove("최근7일_조회수")
                    if "대체키워드" in cols:
                        idx = cols.index("대체키워드") + 1
                    elif "증빙자료유형" in cols:
                        idx = cols.index("증빙자료유형") + 1
                    else:
                        idx = len(cols)
                    cols.insert(idx, "최근7일_조회수")
                    report = report[cols]
            except Exception:
                pass
            # ----------------------------------------------------------------------

            # ---- Right align numeric columns ----
            align_cols = [c for c in ["최근7일_조회수", "키워드 조회 수(누적카운트)"] if c in report.columns]
            styled = report.style.set_properties(**{"text-align": "right"}, subset=align_cols)
            
            st.dataframe(_rename_fcol(styled), width="stretch")
            rep_buf = io.BytesIO()
            report.to_csv(rep_buf, index=False, encoding="utf-8-sig")
            st.download_button("리포트 CSV 다운로드 (최근 7일 Top N)", data=rep_buf.getvalue(), file_name="weekly_topN_report.csv", mime="text/csv", key="dl_week_report")
        except Exception as e:
            st.error(f"리포트 생성 중 오류: {e}")

    # ------------------------------
    # 🔁 조회 수 초기화 (CSV 다운로드 영역 '아래'로 이동)
    # ------------------------------
    st.markdown("---")
    st.markdown("### 🔁 조회 수 초기화")

    # 전체 초기화: DB의 누적카운트=0, 마지막 출력일자 공백 (로그는 유지 — 기존 동작 유지)
    col_z1, col_z2 = st.columns([1,1])
    with col_z1:
        confirm_reset_all = st.checkbox("전체 조회 수를 0으로 초기화하고 '마지막 출력일자'를 비우기", value=False, key="report_reset_ck")
        if st.button("전체 초기화 실행", disabled=not confirm_reset_all, key="report_reset_btn"):
            try:
                if "키워드 조회 수(누적카운트)" in st.session_state.kw_df.columns:
                    st.session_state.kw_df["키워드 조회 수(누적카운트)"] = "0"
                if "마지막 출력일자" in st.session_state.kw_df.columns:
                    st.session_state.kw_df["마지막 출력일자"] = ""
                save_db(st.session_state.kw_df, Path(st.session_state.storage_path))
                st.success("전체 초기화 완료 (로그 파일은 유지됩니다)")
            except Exception as e:
                st.error(f"초기화 실패: {e}")

    # 최근 7일 초기화: 로그에서 최근 7일 기록만 삭제 (DB 누적카운트는 건드리지 않음)
    with col_z2:
        confirm_reset_week = st.checkbox("최근 7일 조회수 초기화 (로그에서 최근 7일 기록 삭제)", value=False, key="report_reset7_ck")
        if st.button("최근 7일 초기화 실행", disabled=not confirm_reset_week, key="report_reset7_btn"):
            try:
                log_df2 = load_hits_log()
                if log_df2.empty:
                    st.info("삭제할 로그가 없습니다.")
                else:
                    end_dt2 = datetime.now()
                    start_dt2 = end_dt2 - timedelta(days=7)
                    log_df2["ts_dt"] = pd.to_datetime(log_df2["ts"], errors="coerce")
                    before_cnt = len(log_df2)
                    # keep records outside the last 7 days or with NaT (safety)
                    keep_mask = (log_df2["ts_dt"].isna()) | (log_df2["ts_dt"] < start_dt2) | (log_df2["ts_dt"] > end_dt2)
                    new_log = log_df2.loc[keep_mask, ["ts","kwd_no"]].reset_index(drop=True)
                    overwrite_hits_log(new_log)
                    removed = before_cnt - len(new_log)
                    st.success(f"최근 7일 로그 {removed}건 삭제 완료")
            except Exception as e:
                st.error(f"최근 7일 초기화 실패: {e}")