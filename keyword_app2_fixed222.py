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
    /* 기본 mark 스타일: 배경색은 지정하지 않고, 텍스트 색상만 지정 */
    mark {
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

# -----------------------------------------------------------------------------
# Supabase(Postgres) storage (optional)
# - If DB connection secrets are provided, the app will use Postgres as the
#   source of truth. Otherwise it falls back to local CSV as before.
# - This keeps existing UI/flow intact while enabling "업무 DB" mode on Streamlit Cloud.
# -----------------------------------------------------------------------------
from contextlib import contextmanager
from typing import Optional

def _get_secret(key: str, default=None):
    try:
        import streamlit as st  # type: ignore
        # Support both flat keys and [database] section in secrets.
        if key in st.secrets:
            return st.secrets.get(key, default)
        if "database" in st.secrets and key in st.secrets["database"]:
            return st.secrets["database"].get(key, default)
    except Exception:
        pass
    return default

def _pg_enabled() -> bool:
    return bool(_get_secret("DB_HOST") or _get_secret("host"))

def _pg_params() -> dict:
    # Accept either DB_* flat keys or common names under [database]
    host = _get_secret("DB_HOST") or _get_secret("host")
    port = _get_secret("DB_PORT") or _get_secret("port") or 5432
    dbname = _get_secret("DB_NAME") or _get_secret("dbname") or _get_secret("database") or "postgres"
    user = _get_secret("DB_USER") or _get_secret("user") or "postgres"
    password = _get_secret("DB_PASSWORD") or _get_secret("password")
    sslmode = _get_secret("DB_SSLMODE") or _get_secret("sslmode") or "require"
    return {
        "host": host,
        "port": int(port),
        "dbname": dbname,
        "user": user,
        "password": password,
        "sslmode": sslmode,
    }

@contextmanager
def _pg_conn():
    """Context manager returning a Postgres connection.
    Uses psycopg2 if available, otherwise psycopg (v3)."""
    params = _pg_params()
    try:
        import psycopg2  # type: ignore
        conn = psycopg2.connect(**params)
        try:
            yield conn
        finally:
            conn.close()
        return
    except ImportError:
        pass

    try:
        import psycopg  # type: ignore
        conn = psycopg.connect(**params)
        try:
            yield conn
        finally:
            conn.close()
        return
    except ImportError as e:
        raise RuntimeError(
            "Postgres mode is enabled but neither 'psycopg2' nor 'psycopg' is installed. "
            "Add 'psycopg2-binary' (recommended) to requirements.txt."
        ) from e

def _pg_fetch_keywords() -> pd.DataFrame:
    with _pg_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            select kwd_no, keyword_name, category, risk_grade, risk_detail,
                   evidence_type, alt_keyword, created_at, updated_at
            from public.keywords
            order by created_at desc
            """
        )
        rows = cur.fetchall()
        cols = ["kwd_no","키워드명","상품카테고리","리스크 등급","리스크 등급별 세부 심의기준","증빙자료유형","대체키워드","created_at","updated_at"]
        if not rows:
            df = pd.DataFrame(columns=cols)
        else:
            df = pd.DataFrame(rows, columns=cols)
        # Ensure legacy columns exist
        for c in DB_COLS:
            if c not in df.columns:
                df[c] = ""
        return df[cols + [c for c in DB_COLS if c not in cols]]

def _pg_sync_keywords(df: pd.DataFrame) -> None:
    """Persist the provided dataframe as the canonical state (legacy save_db semantics).
    This performs:
      - delete rows removed from df
      - upsert rows present in df
    """
    if df is None:
        return

    # Normalize dataframe columns to expected storage columns
    df2 = df.copy()
    # Map legacy column names to DB columns
    rename_map = {
        "키워드명": "keyword_name",
        "상품카테고리": "category",
        "리스크 등급": "risk_grade",
        "리스크 등급별 세부 심의기준": "risk_detail",
        "증빙자료유형": "evidence_type",
        "대체키워드": "alt_keyword",
    }
    for k in rename_map:
        if k not in df2.columns:
            df2[k] = ""
    df2 = df2.fillna("")

    with _pg_conn() as conn:
        cur = conn.cursor()

        # existing keys in DB
        cur.execute("select kwd_no from public.keywords")
        existing = {r[0] for r in cur.fetchall()}

        current = set(df2["kwd_no"].astype(str).tolist()) if "kwd_no" in df2.columns else set()
        to_delete = list(existing - current)
        if to_delete:
            cur.execute("delete from public.keywords where kwd_no = any(%s)", (to_delete,))

        # Upsert all current rows
        records = []
        for _, row in df2.iterrows():
            kwd_no = str(row.get("kwd_no","")).strip()
            if not kwd_no:
                continue
            records.append((
                kwd_no,
                str(row.get("키워드명","")).strip(),
                str(row.get("상품카테고리","")).strip(),
                str(row.get("리스크 등급","")).strip(),
                str(row.get("리스크 등급별 세부 심의기준","")).strip(),
                str(row.get("증빙자료유형","")).strip(),
                str(row.get("대체키워드","")).strip(),
            ))

        if records:
            cur.executemany(
                """
                insert into public.keywords
                  (kwd_no, keyword_name, category, risk_grade, risk_detail, evidence_type, alt_keyword)
                values (%s,%s,%s,%s,%s,%s,%s)
                on conflict (kwd_no) do update set
                  keyword_name = excluded.keyword_name,
                  category = excluded.category,
                  risk_grade = excluded.risk_grade,
                  risk_detail = excluded.risk_detail,
                  evidence_type = excluded.evidence_type,
                  alt_keyword = excluded.alt_keyword,
                  updated_at = now()
                """,
                records
            )

        conn.commit()

def _infer_prefix(category: str, df: 'Optional[pd.DataFrame]' = None) -> str:
    cat = str(category).strip()
    # Explicit mapping for known categories (PoC)
    explicit = {
        "USA(뷰티&디바이스)": "US",
    }
    if cat in explicit:
        return explicit[cat]

    if cat.startswith("USA"):
        return "US"

    rx = re.compile(r"^([A-Z]{1,3})(\d{2,})$")
    candidates = []

    if df is not None and not df.empty and "상품카테고리" in df.columns and "kwd_no" in df.columns:
        sub = df[df["상품카테고리"].astype(str) == cat]["kwd_no"].astype(str)
        for v in sub.head(200):
            m = rx.match(v.strip())
            if m:
                candidates.append(m.group(1))

    # If in Postgres mode, optionally look in DB for existing prefix patterns
    if not candidates and _pg_enabled():
        try:
            with _pg_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "select kwd_no from public.keywords where category = %s limit 200",
                    (cat,)
                )
                for (v,) in cur.fetchall():
                    m = rx.match(str(v).strip())
                    if m:
                        candidates.append(m.group(1))
        except Exception:
            pass

    if candidates:
        # Most common prefix
        return max(set(candidates), key=candidates.count)

    # Fallback
    return "U"

def _pg_next_kwd_no(prefix: str) -> str:
    with _pg_conn() as conn:
        cur = conn.cursor()
        cur.execute("select public.next_kwd_no(%s)", (prefix,))
        val = cur.fetchone()
        conn.commit()
        return str(val[0])

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

RISK_OPTIONS = ["1등급(사용금지)","2등급(대체키워드사용)","3등급(조건부사용)","4등급(사용가능)","5등급(테스트 등급)"]

DEFAULT_DETAIL_CRITERIA = ["-","실증자료제출","시험성적서제출","기능입증자료제출","표시기준준수","전문의견서","문헌자료제출"]
DEFAULT_EVIDENCE_TYPES = ["-","인체적용시험결과서","기능성평가보고서","임상시험결과보고서","실험데이터요약서","제품성분분석표","문헌자료","시험성적서"]
DEFAULT_ALT_KEYWORDS = ["-","탄력","보습","진정","미백","주름개선","자외선차단","영양공급"]

RISK_COLORS = {
    "1등급(사용금지)": "#ff6b6b",
    "2등급(대체키워드사용)": "#ffa94d",
    "3등급(조건부사용)": "#ffd43b",
    "4등급(사용가능)": "#a9e34b",
    "5등급(테스트 등급)": "#d8f5a2",
}

# Regex patterns (compiled once for performance)
_delim_pattern = re.compile(r"[\/,;\|]")
_bracket_pairs = r"\(\)\[\]{}（）【】"
# kwd_no는 접두사(영문 대문자 1자 이상) + 숫자(3자리 이상) 패턴을 기본으로 가정
# 예: A001, US000, USA0123
_kwd_no_pattern = re.compile(r"^([A-Z]+)(\d{3,})$")
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
    # If Supabase/Postgres secrets are configured, use Postgres as source of truth.
    if _pg_enabled():
        try:
            return _pg_fetch_keywords()
        except Exception as e:
            # Fail safe: show error and fall back to local CSV
            try:
                st.error(f"Postgres(DB) 로딩 실패로 로컬 CSV로 대체합니다: {e}")
            except Exception:
                pass
    # Legacy CSV fallback (original behavior)
    if path.exists():
        try:
            raw = path.read_bytes()
            df = pd.read_csv(io.BytesIO(raw), encoding="utf-8-sig")
        except Exception:
            df = pd.read_csv(path, encoding="utf-8-sig")
    else:
        df = pd.DataFrame(columns=DB_COLS)
    for c in DB_COLS:
        if c not in df.columns:
            df[c] = ""
    return df
def save_db(df: pd.DataFrame, path: Path) -> None:
    # If Supabase/Postgres secrets are configured, persist to Postgres.
    if _pg_enabled():
        try:
            _pg_sync_keywords(df)
            return
        except Exception as e:
            # Fail safe: show error and fall back to local CSV
            try:
                st.error(f"Postgres(DB) 저장 실패로 로컬 CSV로 대체합니다: {e}")
            except Exception:
                pass
    # Legacy CSV fallback (original behavior)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
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

    # 유사일치 조사/어미(접미사) 화이트리스트(사용자 추가분) 로드
    if "ko_suffix_whitelist_extra" not in st.session_state:
        st.session_state.ko_suffix_whitelist_extra = load_suffix_whitelist_config()
    if "suffix_suggestions" not in st.session_state:
        st.session_state.suffix_suggestions = {}

# 정규화(괄호 제거/공백 정규화/영문 대소문자 무시) 옵션과 함께 사용할
# 특수기호 무시(공백 치환) 리스트 로드

    # 특수기호 무시(공백 치환) 설정 로드
    if "ignored_special_symbols" not in st.session_state:
        st.session_state.ignored_special_symbols = load_ignored_special_symbols_config()

    # 키워드 번호 카운터 초기화
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
            # 기존 데이터에서 prefix(대문자 1자 이상) 분포를 기반으로 가장 흔한 prefix를 선택
            pref = sub.str.extract(r"^([A-Z]+)(\d{3,})$")[0].dropna()
            if not pref.empty:
                return pref.value_counts().idxmax()
    return get_prefix(category)

def _infer_number_width_from_existing(category: str, pfx: str) -> int:
    """해당 카테고리/접두사에서 사용 중인 숫자 자리수를 추정합니다.
    - 예: US000 → 3, ABC0001 → 4
    - 없으면 기본 3
    """
    try:
        df = st.session_state.kw_df
        if df is None or df.empty:
            return 3
        sub = df[df["상품카테고리"].astype(str).str.strip() == str(category).strip()]["kwd_no"].dropna().astype(str)
        if sub.empty:
            return 3
        nums = sub.str.extract(r"^%s(\d+)$" % re.escape(str(pfx)))[0].dropna().astype(str)
        if nums.empty:
            return 3
        return max(3, int(nums.str.len().max()))
    except Exception:
        return 3

def next_kwd_no(category: str) -> str:
    # Prefer DB-side atomic sequence when Postgres mode is enabled.
    if _pg_enabled():
        prefix = _infer_prefix(category, st.session_state.get("kw_df"))
        try:
            return _pg_next_kwd_no(prefix)
        except Exception as e:
            try:
                st.error(f"Postgres 발번 실패로 로컬 발번으로 대체합니다: {e}")
            except Exception:
                pass

    # Legacy local numbering fallback (original behavior, improved prefix parsing)
    df = st.session_state.get("kw_df", pd.DataFrame(columns=DB_COLS))
    rx = re.compile(r"^([A-Z]{1,3})(\d{2,})$")
    candidates = []
    if not df.empty and "상품카테고리" in df.columns and "kwd_no" in df.columns:
        sub = df[df["상품카테고리"].astype(str) == str(category).strip()]["kwd_no"].astype(str)
        for v in sub:
            m = rx.match(v.strip())
            if m:
                candidates.append((m.group(1), int(m.group(2)), len(m.group(2))))
    if candidates:
        # use most common prefix, then max number
        prefixes = [p for p,_,_ in candidates]
        prefix = max(set(prefixes), key=prefixes.count)
        max_num = max(n for p,n,_w in candidates if p==prefix)
        width = max(w for p,_n,w in candidates if p==prefix)
        return f"{prefix}{str(max_num+1).zfill(width)}"

    # If no existing in this category, infer prefix from category name
    prefix = _infer_prefix(category, df)
    return f"{prefix}{'001'}"
def preview_next_kwd_no(category: str) -> str:
    """next_kwd_no와 동일한 규칙으로 '다음 번호'를 미리 계산합니다.
    - st.session_state.counters를 변경하지 않습니다(미리보기 전용).
    """
    pfx = infer_prefix_from_existing(category)
    df = st.session_state.kw_df
    next_num = None
    width = _infer_number_width_from_existing(category, pfx)

    if df is not None and not df.empty:
        sub = df[df["상품카테고리"].astype(str).str.strip() == str(category).strip()]["kwd_no"].astype(str)
        nums = sub.str.extract(r"^%s(\d+)$" % re.escape(str(pfx)))[0].dropna()
        if not nums.empty:
            next_num = int(nums.astype(int).max()) + 1

    if next_num is None:
        # 실제 next_kwd_no는 counter를 증가시키지만, 여기서는 값을 '예측'만 합니다.
        cur = st.session_state.counters.get(pfx, 0) + 1
        next_num = cur

    return f"{pfx}{next_num:0{width}d}"

def category_kwdno_diagnostics(category: str, dropdown_options: list[str] | None = None) -> dict:
    """카테고리명 일치 여부와 DB 내 kwd_no 현황을 점검하기 위한 진단 정보를 반환합니다."""
    category_raw = "" if category is None else str(category)
    category_stripped = category_raw.strip()

    dropdown_options = dropdown_options or []
    exact_in_dropdown = category_raw in dropdown_options
    stripped_in_dropdown = category_stripped in [str(o).strip() for o in dropdown_options]

    # 비슷한 후보(공백 제거 후 비교, 대/소문자 무시)
    def _norm(s: str) -> str:
        return re.sub(r"\s+", "", str(s)).lower()

    norm_target = _norm(category_raw)
    near = []
    if norm_target:
        for o in dropdown_options:
            if _norm(o) == norm_target and o not in near:
                near.append(o)

    df = st.session_state.kw_df
    cat_df = None
    kwd_list = []
    prefix_counts = {}
    inferred_prefix = infer_prefix_from_existing(category_raw)
    prefix_source = "existing"  # default

    if category_raw in CATEGORY_PREFIX:
        prefix_source = "mapping"
    else:
        # 기존 데이터가 없으면 get_prefix로 떨어짐
        _tmp = df[df["상품카테고리"].astype(str).str.strip() == category_stripped] if (df is not None and not df.empty) else None
        if _tmp is None or _tmp.empty:
            prefix_source = "fallback"

    if df is not None and not df.empty and category_stripped:
        cat_df = df[df["상품카테고리"].astype(str).str.strip() == category_stripped].copy()
        if cat_df is not None and not cat_df.empty:
            kwd_list = cat_df["kwd_no"].dropna().astype(str).str.upper().tolist()
            # prefix 분포 (정규식에 맞는 것만)
            for k in kwd_list:
                mm = re.match(r"^([A-Z]+)(\d{3,})$", k)
                if mm:
                    prefix_counts[mm.group(1)] = prefix_counts.get(mm.group(1), 0) + 1

    return {
        "category_raw": category_raw,
        "category_stripped": category_stripped,
        "exact_in_dropdown": exact_in_dropdown,
        "stripped_in_dropdown": stripped_in_dropdown,
        "near_matches": near,
        "db_rows_in_category": 0 if cat_df is None else int(len(cat_df)),
        "kwd_no_samples": kwd_list[:20],
        "prefix_counts": prefix_counts,
        "inferred_prefix": inferred_prefix,
        "prefix_source": prefix_source,
        "preview_next_kwd_no": preview_next_kwd_no(category_raw) if category_stripped else None,
    }


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
    """Generate safe keyword variants for '유사 키워드 포함' mode.

    변경점(정밀도 개선):
    - (A) prefix(앞부분) 변형(예: 여드, 다이) 생성 로직 제거
    - 구두점/구분자 분리, 괄호 안/밖 텍스트 추출 등 '표기 변형'만 허용
    """
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
        # 너무 짧은 토큰은 오탐 위험이 높아 제외 (영문 3미만, 한글 2미만)
        if re.search(r"[A-Za-z]", v):
            if len(v) < 3:
                return
        else:
            if len(v) < 2:
                return
        variants.add(v)

    # 원문
    add(t)

    # 구분자(/ , ; |) 분리
    for piece in _delim_pattern.split(t):
        add(piece)

    # 괄호 밖 텍스트만 추출
    outside = re.sub(r"\s*[\(\[（【].*?[\)\]）】]\s*", " ", t).strip()
    add(outside)

    # (추가) 괄호 제거 결과를 한 번 더 정리해 '여드름(acne)' 같은 표기에서 '여드름'이 안정적으로 후보에 포함되도록 함
    outside_clean = re.sub(r"[^\w가-힣]+", " ", outside).strip()
    outside_clean = re.sub(r"\s+", " ", outside_clean)
    add(outside_clean)
    for piece in _delim_pattern.split(outside_clean):
        add(piece)

    # 괄호 안 텍스트도 후보로 추가
    for inner in re.findall(r"[\(\[（【](.*?)[\)\]）】]", t):
        add(inner)
        for piece in _delim_pattern.split(inner):
            add(piece)

    # 공백 정규화
    variants = {re.sub(r"\s+", " ", v) for v in variants}
    return list(variants)


# -----------------------------
# Matching & highlight
# -----------------------------
def _tokenize_with_spans(s: str):
    """Tokenize text into (token, start, end) spans.
    - 한글/영문/숫자 덩어리를 토큰으로 취급
    - 매칭은 토큰 단위(=단어 경계)로 수행하기 위함
    """
    if not s:
        return []
    token_re = re.compile(r"[A-Za-z0-9]+|[가-힣]+")
    return [(m.group(0), m.start(), m.end()) for m in token_re.finditer(s)]


def _tokenize_only(s: str):
    return [t for (t, _, _) in _tokenize_with_spans(s)]

# --- Korean postposition/ending whitelist for fuzzy matching ---
# Used to treat patterns like '여드름에는' as a fuzzy hit for keyword '여드름' (highlighting only the stem).
_KO_SUFFIX_WHITELIST = {
    "와","과","은","는","이","가","을","를","의","도","만","까지","부터","보다","마다",
    "에","에서","에게","께","한테","로","으로","로서","으로서","로써","으로써","처럼","같이",
    "나","이나","든","든지","라도","이나마",
    "라고","이라고","이라","이라도","이라면","이면","면","라는","이란","란","이라는","이라는","이야","야",
    "에는","에선","에서","에서의","에의","에만","에도","부터는","까지는",
}

# ---- Suffix whitelist persistence & UI helpers ----
_SUFFIX_CONFIG_PATH = Path(__file__).with_name("ko_suffix_whitelist.json")

def load_suffix_whitelist_config() -> list:
    """Load user-managed Korean suffix whitelist additions from JSON.
    Returns a list of suffix strings (may be empty).
    """
    try:
        if _SUFFIX_CONFIG_PATH.exists():
            data = json.loads(_SUFFIX_CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                items = data.get("extra_suffixes", [])
            else:
                items = data
            if isinstance(items, list):
                return [str(x).strip() for x in items if str(x).strip()]
    except Exception:
        pass
    return []

def save_suffix_whitelist_config(extra_suffixes: list) -> bool:
    """Persist user-managed suffix whitelist additions to JSON."""
    try:
        payload = {"extra_suffixes": [str(x).strip() for x in (extra_suffixes or []) if str(x).strip()]}
        _SUFFIX_CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False

def get_ko_suffix_whitelist() -> set:
    """Return merged whitelist = default + user additions from session_state."""
    wl = set(_KO_SUFFIX_WHITELIST)
    try:
        extra = st.session_state.get("ko_suffix_whitelist_extra", []) or []
        for x in extra:
            x = str(x).strip()
            if x:
                wl.add(x)
    except Exception:
        pass
    return wl


# --- English inflection helpers for fuzzy matching (verbs/adjectives/nouns) ---
def _normalize_english_token(token: str) -> str:
    """Lowercase & strip non-letter/hyphen characters for simple English comparisons."""
    return re.sub(r"[^a-zA-Z\-]", "", str(token)).lower()

def _english_inflection_common_prefix(a: str, b: str) -> int:
    """Return length of common prefix if a,b are considered inflectional variants (core rule set1+set2); otherwise 0.
    This is used only in '유사 키워드 포함' mode to highlight the shared stem part."""
    a_norm = _normalize_english_token(a)
    b_norm = _normalize_english_token(b)
    if not a_norm or not b_norm:
        return 0
    if a_norm == b_norm:
        # highlight full token when exactly equal
        return len(a_norm)

    def _is_variant(base: str, other: str) -> bool:
        # basic regular inflections: plural, past, progressive, comparative/superlative
        if len(base) < 3 or len(other) < 3:
            return False
        # direct suffixes for core rule sets 1+2
        for suf in ("s", "es", "ed", "ing", "er", "est"):
            if other == base + suf:
                return True
        # e-drop variants: base[:-1] + ed/ing/er/est when base endswith 'e'
        if base.endswith("e") and len(base) > 3:
            root = base[:-1]
            for suf in ("ed", "ing", "er", "est"):
                if other == root + suf:
                    return True
            # special: keep trailing 'e' and just add 'd' (e.g. 'even-tone' -> 'even-toned')
            if other == base + "d":
                return True
        return False

    eq = _is_variant(a_norm, b_norm) or _is_variant(b_norm, a_norm)
    if not eq:
        return 0

    # They are inflectional variants -> highlight longest common prefix as shared stem
    pref = 0
    for ch_a, ch_b in zip(a_norm, b_norm):
        if ch_a == ch_b:
            pref += 1
        else:
            break
    # avoid extremely short stems
    if pref < 3:
        return 0
    return pref



# --- Ignored special symbols whitelist (for relaxed normalization) ---
# When '(괄호 제거·공백 정규화·영문 대소문자 무시)' is enabled, we can additionally
# treat configured special symbols as whitespace (replace with space, length preserved).
_SPECIAL_SYMBOLS_CONFIG_PATH = Path(__file__).with_name("ignored_special_symbols.json")
_DEFAULT_IGNORED_SPECIAL_SYMBOLS = ["/", "&", ",", "*"]

def load_ignored_special_symbols_config() -> list:
    """Load user-managed ignored special symbols list from JSON.
    Returns a list of *single-character* symbols.
    """
    try:
        if _SPECIAL_SYMBOLS_CONFIG_PATH.exists():
            data = json.loads(_SPECIAL_SYMBOLS_CONFIG_PATH.read_text(encoding="utf-8"))
            items = None
            if isinstance(data, dict):
                items = data.get("symbols", [])
            elif isinstance(data, list):
                items = data
            if isinstance(items, list):
                out = []
                for x in items:
                    x = str(x)
                    if len(x) == 1 and x.strip() != "":
                        out.append(x)
                # dedup keep order
                seen = set()
                cleaned = []
                for ch in out:
                    if ch not in seen:
                        seen.add(ch)
                        cleaned.append(ch)
                return cleaned
    except Exception:
        pass
    return _DEFAULT_IGNORED_SPECIAL_SYMBOLS.copy()

def save_ignored_special_symbols_config(symbols: list) -> bool:
    """Persist ignored special symbols list to JSON."""
    try:
        cleaned = []
        seen = set()
        for x in (symbols or []):
            x = str(x)
            if len(x) != 1:
                continue
            if x.strip() == "":
                continue
            if x not in seen:
                seen.add(x)
                cleaned.append(x)
        payload = {"symbols": cleaned}
        _SPECIAL_SYMBOLS_CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False

def get_ignored_special_symbols() -> list:
    """Return current ignored special symbols list (single-character)."""
    try:
        syms = st.session_state.get("ignored_special_symbols", None)
        if isinstance(syms, list) and syms:
            return [str(x) for x in syms if isinstance(x, str) and len(x) == 1 and x.strip() != ""]
    except Exception:
        pass
    return _DEFAULT_IGNORED_SPECIAL_SYMBOLS.copy()

def _replace_ignored_symbols_with_space_keep_len(s: str, symbols: list) -> str:
    """Replace each configured symbol character with a single space.
    Length is preserved (important for start/end index mapping).
    """
    if not s:
        return s or ""
    if not symbols:
        return s
    # Build safe regex class for the given characters
    try:
        cls = "[" + "".join(re.escape(ch) for ch in symbols if isinstance(ch, str) and len(ch) == 1) + "]"
        if cls == "[]":
            return s
        return re.sub(cls, " ", s)
    except Exception:
        # Fallback: simple replace loop (still length-preserving)
        out = s
        for ch in symbols:
            if isinstance(ch, str) and len(ch) == 1:
                out = out.replace(ch, " ")
        return out

def _find_spans_by_compact_match(original: str, target: str, symbols: list) -> list:
    """Find spans in original where removing spaces and configured symbols yields target.
    Returns list of (start, end) spans (end is exclusive).
    This enables 'ignore special symbols' matching while preserving highlight indices.
    """
    if not original or not target:
        return []
    symset = set(ch for ch in (symbols or []) if isinstance(ch, str) and len(ch) == 1)
    n = len(original)
    tlen = len(target)
    spans = []
    for i in range(n):
        # quick skip: if char is ignorable, don't start here
        if original[i].isspace() or original[i] in symset:
            continue
        j = i
        built = []
        # Build compact until length reaches target length or end.
        while j < n and len(built) < tlen:
            ch = original[j]
            if ch.isspace() or ch in symset:
                j += 1
                continue
            built.append(ch)
            j += 1
        if len(built) != tlen:
            continue
        if "".join(built) == target:
            spans.append((i, j))
    return spans



def find_matches(text: str, kw_df: pd.DataFrame, match_mode: str = "유사 키워드 포함", exact_relaxed: bool = False, suffix_suggestions: dict = None, **kwargs) -> List[Match2]:
    """Find keyword matches in text.

    정밀도 개선(요청사항):
    - (A) prefix 변형 생성 제거: generate_keyword_variants에서 처리
    - (B) 단어 경계 매칭: 유사 모드에서 토큰 경계 기준으로만 검출
    - (C) 토큰 기반 매칭: 본문/키워드를 토큰화하고 토큰 시퀀스 비교로 검출

    * '정확 일치' 모드는 기존 substring 기반 동작을 유지합니다.
    """
    matches: List[Match2] = []
    base = text or ""
    # For relaxed normalization, optionally replace configured special symbols with spaces (length preserved)
    base_for_match = base
    if exact_relaxed:
        try:
            base_for_match = _replace_ignored_symbols_with_space_keep_len(base_for_match, get_ignored_special_symbols())
        except Exception:
            base_for_match = base
    lower = base_for_match.lower()

    df = kw_df.copy()
    for col in DB_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df[df["키워드명"].astype(str).str.strip() != ""]

    occupied_set = set()
    records = df.to_dict("records")

    # 토큰화(유사 모드에서 사용)
    token_spans = _tokenize_with_spans(base_for_match)
    tokens_lower = [t.lower() for (t, _, _) in token_spans]
    tokens_compact = [re.sub(r"\s+", "", t.lower()) for (t, _, _) in token_spans]  # 안전장치

    for r in records:
        term = str(r["키워드명"]).strip()
        category = str(r.get("상품카테고리", ""))
        risk = str(r.get("리스크 등급", ""))
        detail = str(r.get("리스크 등급별 세부 심의기준", ""))
        kwd_no = r.get("kwd_no") or None

        # 부분 문자열(최소 글자수) 모드: 키워드 내부 부분 문자열로 느슨하게 매칭
        if match_mode == "부분 문자열(최소 글자수)":
            try:
                min_len = int(kwargs.get("partial_min_len", 2))
            except Exception:
                min_len = 2
            if min_len < 2:
                min_len = 2
            term_norm = re.sub(r"\s+", "", str(term))
            if term_norm and len(term_norm) >= min_len:
                seen_spans = set()
                base_lower = base_for_match.lower()
                term_norm_lower = term_norm.lower()
                for L in range(min_len, len(term_norm_lower) + 1):
                    for i_sub in range(0, len(term_norm_lower) - L + 1):
                        sub = term_norm_lower[i_sub:i_sub+L]
                        start_idx = 0
                        while True:
                            idx2 = base_lower.find(sub, start_idx)
                            if idx2 == -1:
                                break
                            end2 = idx2 + L
                            span_key = (idx2, end2)
                            if span_key in seen_spans:
                                start_idx = idx2 + 1
                                continue
                            seen_spans.add(span_key)
                            range_occupied = any(i in occupied_set for i in range(idx2, end2))
                            if not range_occupied:
                                matches.append(
                                    Match2(
                                        term=term,
                                        start=idx2,
                                        end=end2,
                                        category=category,
                                        risk=risk,
                                        detail=detail,
                                        kwd_no=kwd_no,
                                    )
                                )
                                for i2 in range(idx2, end2):
                                    occupied_set.add(i2)
                            start_idx = idx2 + 1
            continue  # 다음 키워드로

        variants = generate_keyword_variants(term)

        # 기존 옵션: (괄호 제거·공백 정규화·영문 대소문자 무시)
        if match_mode == "유사 키워드 포함" and exact_relaxed:
            term_no_br = re.sub(r"\s*[\(\[（【].*?[\)\]）】]\s*", " ", str(term)).strip()
            term_no_br = re.sub(r"\s+", " ", term_no_br)
            term_norm = re.sub(r"\s+", " ", str(term)).strip()
            for _cand in [term_no_br, term_norm]:
                if _cand and _cand not in variants:
                    variants.insert(0, _cand)
                # Also add a version with ignored special symbols replaced by spaces (length preserved)
                if _cand and exact_relaxed:
                    try:
                        _cand2 = _replace_ignored_symbols_with_space_keep_len(_cand, get_ignored_special_symbols())
                        _cand2 = re.sub(r"\s+", " ", _cand2).strip()
                        if _cand2 and _cand2 not in variants:
                            variants.insert(0, _cand2)
                    except Exception:
                        pass

        if match_mode == "정확 일치":
            variants = [str(term)]
            if exact_relaxed:
                term_no_br = re.sub(r"\s*[\(\[（【].*?[\)\]）】]\s*", " ", str(term)).strip()
                term_no_br = re.sub(r"\s+", " ", term_no_br)
                term_norm = re.sub(r"\s+", " ", str(term)).strip()
                for _cand in [term_no_br, term_norm]:
                    if _cand and _cand not in variants:
                        variants.append(_cand)

        variants = [v for v in variants if v]
        variants.sort(key=lambda s: len(s), reverse=True)

        # ----------------------------
        # 유사 모드: 토큰 기반 + 단어 경계 + (안전한) 띄어쓰기 무시(토큰 결합) 매칭
        # ----------------------------
        if match_mode == "유사 키워드 포함":
            for v in variants:
                v = str(v).strip()
                if not v:
                    continue
                v_tokens = _tokenize_only(v)
                if not v_tokens:
                    continue
                v_tokens_lower = [x.lower() for x in v_tokens]
                v_compact = "".join(v_tokens_lower)

                n = len(v_tokens_lower)
                if n <= 0:
                    continue

                # 1) 정확한 토큰 시퀀스 일치
                starts = []
                for i in range(0, max(0, len(tokens_lower) - n + 1)):
                    if tokens_lower[i:i+n] == v_tokens_lower:
                        starts.append(i)

                # 2) 띄어쓰기 변형(붙여쓰기)을 위한 토큰 결합 일치
                #    - 예: '피부 보습' ↔ '피부보습'
                #    - window 토큰들을 붙인 문자열이 키워드 토큰 결합과 같으면 매칭
                #    - 단, 2토큰 이상일 때만 수행(1토큰은 1)에서 충분)
                if n >= 2:
                    for i in range(0, max(0, len(tokens_lower) - n + 1)):
                        if "".join(tokens_compact[i:i+n]) == v_compact:
                            starts.append(i)

                
                # If token-based match fails, allow limited Korean stem+suffix hits (whitelist),
                # e.g., '여드름에는' matching keyword '여드름' in fuzzy mode.
                # (3) 조사/어미(접미사) 화이트리스트 기반 매칭 + (4) 합성어(붙여쓰기) 내부 접미 매칭(제한적)
                # - 3) 예: '여드름에는' -> '여드름' + '에는'(whitelist)
                # - 4) 예: '미세스피큘' -> '스피큘'(token suffix)  ※ 유사일치에서만, 한글 키워드에 한해 제한적으로 허용
                suffix_hit_end = {}  # legacy: end override (kept for backward compatibility)
                span_override = {}   # {token_index: (start_orig, end_orig)} for highlight span overrides

                # 2-b) English inflection-aware token sequence match (core rule sets 1+2)
                #     - 단수/복수, 시제(-ed), 진행형(-ing), 비교급/최상급(-er/-est) 정도의 굴절만 허용
                #     - '정확 일치'가 아닌 '유사 키워드 포함' 모드에서만 사용
                if n >= 1:
                    window_len = max(0, len(tokens_lower) - n + 1)
                    for i2 in range(window_len):
                        # 정확 일치로 이미 검출된 window는 건너뜀
                        if tokens_lower[i2:i2+n] == v_tokens_lower:
                            continue
                        ok = True
                        local_override = None
                        # 각 토큰이 같거나(동일) 영어 굴절형(세트1+2) 관계인지 검사
                        for j2 in range(n):
                            src_tok = v_tokens_lower[j2]
                            tgt_tok = tokens_lower[i2 + j2]
                            if src_tok == tgt_tok:
                                continue
                            stem_len = _english_inflection_common_prefix(src_tok, tgt_tok)
                            if stem_len <= 0:
                                ok = False
                                break
                            # 공통 어간 부분까지만 하이라이트하도록 span_override에 기록
                            # (여러 토큰이 굴절형인 경우도 있을 수 있으나, 우선 마지막 것으로 덮어쓰기)
                            t_start = token_spans[i2 + j2][1]
                            t_end = t_start + stem_len
                            # window 전체 기준으로는, 첫 토큰 시작~공통 어간 끝까지 표시
                            local_override = (token_spans[i2][1], t_end)
                        if ok:
                            starts.append(i2)
                            if local_override is not None:
                                span_override[i2] = local_override


                is_korean_kw = (n == 1 and v_compact and any("가" <= ch <= "힣" for ch in v_compact))
                if is_korean_kw:
                    kw_len = len(v_compact)

                    # 3) 키워드 어간 + 조사/어미(접미사) 화이트리스트
                    for _i, _tok in enumerate(tokens_compact):
                        if _tok.startswith(v_compact) and len(_tok) > kw_len:
                            _suf = _tok[kw_len:]
                            if _suf in get_ko_suffix_whitelist():
                                starts.append(_i)
                                _s = token_spans[_i][1]
                                _e = token_spans[_i][1] + kw_len
                                span_override[_i] = (_s, _e)
                                suffix_hit_end[_i] = _e
                            else:
                                # 후보 접미사 제안(승인형): 유사일치에서 키워드 어간+접미사 형태로 보이지만
                                # 화이트리스트에 없어 매칭되지 않은 접미사를 수집합니다.
                                if isinstance(suffix_suggestions, dict):
                                    suffix_suggestions[_suf] = suffix_suggestions.get(_suf, 0) + 1

                    # 4) 합성어 내부 접미(끝부분) 매칭: token.endswith(keyword)
                    #    - 오탐 방지: (a) 한글 키워드 최소 길이, (b) 접두부 길이 제한, (c) 한글 토큰에 한함
                    if kw_len >= 2:
                        for _i, _tok in enumerate(tokens_compact):
                            if _tok.endswith(v_compact) and len(_tok) > kw_len:
                                prefix = _tok[:-kw_len]
                                # 접두부가 너무 길면 오탐 가능성이 커져 제한합니다.
                                if not (1 <= len(prefix) <= 6):
                                    continue
                                # 토큰이 순수 한글 덩어리일 때만 적용
                                if not all(("가" <= ch <= "힣") for ch in _tok):
                                    continue
                                # 하이라이트는 키워드 부분만(토큰 끝에서 kw_len)
                                _e = token_spans[_i][2]
                                _s = _e - kw_len
                                starts.append(_i)
                                span_override[_i] = (_s, _e)

                    # 5) 합성어 내부 접두(앞부분) 매칭: token.startswith(keyword)
                    #    - 오탐 방지: (a) 한글 키워드 최소 길이, (b) 접미부 길이 제한, (c) 한글 토큰에 한함
                    if kw_len >= 2:
                        for _i, _tok in enumerate(tokens_compact):
                            if _tok.startswith(v_compact) and len(_tok) > kw_len:
                                suffix = _tok[kw_len:]
                                # 접미부가 너무 길면 오탐 가능성이 커져 제한합니다.
                                if not (1 <= len(suffix) <= 6):
                                    continue
                                # 토큰이 순수 한글 덩어리일 때만 적용
                                if not all(("가" <= ch <= "힣") for ch in _tok):
                                    continue
                                # 하이라이트는 키워드 부분만(토큰 시작에서 kw_len)
                                _s = token_spans[_i][1]
                                _e = _s + kw_len
                                # 안전장치: 계산된 end가 토큰 끝을 넘지 않도록
                                if _e > token_spans[_i][2]:
                                    _e = token_spans[_i][2]
                                starts.append(_i)
                                span_override[_i] = (_s, _e)
# dedup starts
                if not starts:
                    continue
                starts = sorted(set(starts))

                for si in starts:
                    if 'span_override' in locals() and si in span_override:
                        start_orig, end_orig = span_override[si]
                    else:
                        start_orig = token_spans[si][1]
                        end_orig = suffix_hit_end.get(si, token_spans[si + n - 1][2])

                    # 기존 겹침/우선순위 로직 유지
                    for _i_m, _m in enumerate(list(matches)):
                        old_start = getattr(_m, 'start', None)
                        old_end = getattr(_m, 'end', 0)
                        if old_start is None:
                            continue
                        old_len = max(0, old_end - old_start)
                        new_len = max(0, end_orig - start_orig)
                        # 기존 로직: 같은 시작점에서 더 긴 매치가 있으면 교체 (대표 span 유지)
                        if old_start == start_orig and old_end < end_orig:
                            for _j in range(old_start, old_end):
                                occupied_set.discard(_j)
                            _m.start = start_orig
                            _m.end = end_orig
                            for _j in range(start_orig, end_orig):
                                occupied_set.add(_j)
                            continue
                        # 보완 로직: 포함 관계인 경우 더 넓은 구간을 대표 span으로 사용하고,
                        #           이후 매치들은 이 대표 span에 kwd_no를 누적해서 표시
                        if (start_orig <= old_start and end_orig >= old_end) or (old_start <= start_orig and old_end >= end_orig):
                            new_start = min(start_orig, old_start)
                            new_end = max(end_orig, old_end)
                            # 기존 occupied 구간 정리 후 대표 구간 재설정
                            for _j in range(old_start, old_end):
                                occupied_set.discard(_j)
                            _m.start = new_start
                            _m.end = new_end
                            for _j in range(new_start, new_end):
                                occupied_set.add(_j)
                            # 현재 후보도 대표 구간을 사용
                            start_orig, end_orig = new_start, new_end

                    range_occupied = any(i in occupied_set for i in range(start_orig, end_orig))

                    if not range_occupied:
                        matches.append(Match2(term=term, start=start_orig, end=end_orig, category=category, risk=risk, detail=detail, kwd_no=kwd_no))
                        for i2 in range(start_orig, end_orig):
                            occupied_set.add(i2)
                    else:
                        same_span_exists = any((m2.start == start_orig and m2.end == end_orig) for m2 in matches)
                        already_same = any((m2.start == start_orig and m2.end == end_orig and (m2.kwd_no == kwd_no and m2.category == category and m2.risk == risk)) for m2 in matches)
                        if same_span_exists and not already_same:
                            matches.append(Match2(term=term, start=start_orig, end=end_orig, category=category, risk=risk, detail=detail, kwd_no=kwd_no))
            continue  # 다음 키워드로

        # ----------------------------
        # 정확 일치 모드: 기존 substring find 유지
        # ----------------------------
        for v in variants:
            v_cmp = str(v)
            if exact_relaxed:
                try:
                    v_cmp = _replace_ignored_symbols_with_space_keep_len(v_cmp, get_ignored_special_symbols())
                except Exception:
                    v_cmp = str(v)
            v_lower = str(v_cmp).lower()

            # If relaxed normalization is enabled and ignored special symbols are configured,
            # allow matching across those symbols by treating them as whitespace/ignorable.
            # This fixes cases like '안티&에이징', '안티/에이징' matching '안티에이징'.
            if exact_relaxed:
                try:
                    syms = get_ignored_special_symbols()
                except Exception:
                    syms = []
                if syms:
                    target_compact = re.sub(r"\s+", "", v_lower)
                    try:
                        spans = _find_spans_by_compact_match(base.lower(), target_compact, syms)
                    except Exception:
                        spans = []
                    for (idx, end) in spans:
                        # prevent duplicate/overlap conflicts the same way as substring matches
                        if idx < 0 or end <= idx:
                            continue
                        # remove weaker existing match starting at same idx
                        for _i_m, _m in enumerate(list(matches)):
                            if getattr(_m, 'start', None) == idx and getattr(_m, 'end', 0) < end:
                                for _j in range(_m.start, _m.end):
                                    occupied_set.discard(_j)
                                try:
                                    matches.pop(_i_m)
                                except Exception:
                                    pass
                        if any((j in occupied_set) for j in range(idx, end)):
                            continue
                        for j in range(idx, end):
                            occupied_set.add(j)
                        matches.append(Match2(term=term, start=idx, end=end, category=category, risk=risk, detail=detail, kwd_no=kwd_no))
            start = 0
            while True:
                idx = lower.find(v_lower, start)
                if idx == -1:
                    break
                end = idx + len(v_cmp)

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
    """(Deprecated) 빈 결과를 반환합니다.

    기존 버전에서는 공백 제거(compact) 기반 substring 매칭을 추가로 수행했는데,
    이 방식이 '다이어트' ↔ '다 이유'처럼 우연한 글자 결합 오탐을 크게 유발했습니다.

    현재는 find_matches()에서 토큰 기반 + (안전한) 토큰 결합 매칭으로 대체되어
    별도의 공백 제거 substring 매칭을 수행하지 않습니다.
    """
    return []


def find_matches_nlp(
    text: str,
    kw_df: pd.DataFrame,
    match_mode: str = "유사 키워드 포함",
    exact_relaxed: bool = False,
    suffix_suggestions: dict = None,
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
            suffix_suggestions=suffix_suggestions,
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

def build_kwd_tooltip_attr(kwd_no: str) -> str:
    """kwd_no에 해당하는 키워드 리스트(kw_df)의 행(ROW) 전체 정보를 tooltip용 문자열로 변환합니다.
    HTML attribute로 넣기 위해 quote-safe escaping + 줄바꿈 엔티티(&#10;) 처리까지 수행합니다.
    """
    try:
        df = st.session_state.get("kw_df", None)
    except Exception:
        df = None

    if df is None or getattr(df, "empty", True):
        tip = f"kwd_no: {kwd_no}\n(키워드 리스트가 로드되지 않았습니다)"
        return html.escape(tip, quote=True).replace("\n", "&#10;")

    if "kwd_no" not in df.columns:
        tip = f"kwd_no: {kwd_no}\n(kw_df에 'kwd_no' 컬럼이 없습니다)"
        return html.escape(tip, quote=True).replace("\n", "&#10;")

    try:
        sub = df[df["kwd_no"].astype(str).str.strip() == str(kwd_no).strip()]
        if sub.empty:
            tip = f"kwd_no: {kwd_no}\n(해당 번호의 행을 찾지 못했습니다)"
            return html.escape(tip, quote=True).replace("\n", "&#10;")

        row = sub.iloc[-1]
        parts = []
        for col, val in row.items():
            # NaN/None 제외
            try:
                if pd.isna(val):
                    continue
            except Exception:
                pass
            sval = str(val).strip()
            if not sval:
                continue
            parts.append(f"{col}: {sval}")

        tip = "\n".join(parts) if parts else f"kwd_no: {kwd_no}"
        return html.escape(tip, quote=True).replace("\n", "&#10;")
    except Exception as e:
        tip = f"kwd_no: {kwd_no}\n(tooltip 생성 중 오류: {e})"
        return html.escape(tip, quote=True).replace("\n", "&#10;")



def build_kwd_tooltip_html(kwd_no: str) -> str:
    """kwd_no에 해당하는 키워드 리스트(kw_df)의 행(ROW)을 HTML tooltip로 생성합니다.

    중요:
    - 툴팁은 하이라이트 미리보기 영역 내부에서 `.kwdno-wrap:hover .kwd-tooltip` CSS로 표시됩니다.
    - 기존 구현은 `<span class='kwdno-wrap'>` 내부에 `<div>/<table>`을 삽입하여 (HTML 중첩 규칙 위반)
      브라우저가 DOM을 재배치하는 경우가 발생했고, 그때 hover 셀렉터가 깨져
      "일부만 툴팁이 뜨지 않는" 문제가 나타날 수 있었습니다.
    - 이를 방지하기 위해 툴팁 마크업을 *phrasing content*만으로 구성합니다.
      (span + br 기반의 라인 리스트; div/table 미사용)
    """
    try:
        df = st.session_state.get("kw_df", None)
    except Exception:
        df = None

    def _msg_box(msg: str) -> str:
        # span 기반(phrasing-only) tooltip
        return (
            "<span class='kwd-tooltip'>"
            f"<span class='kwd-tooltip-line' style='font-size:12px; white-space:pre-line;'>{html.escape(msg)}</span>"
            "</span>"
        )

    if df is None or getattr(df, "empty", True):
        return _msg_box(f"kwd_no: {kwd_no}\\n(키워드 리스트가 로드되지 않았습니다)")

    if "kwd_no" not in df.columns:
        return _msg_box(f"kwd_no: {kwd_no}\\n(kw_df에 'kwd_no' 컬럼이 없습니다)")

    try:
        sub = df[df["kwd_no"].astype(str).str.strip() == str(kwd_no).strip()]
        if sub.empty:
            return _msg_box(f"kwd_no: {kwd_no}\\n(해당 번호의 행을 찾지 못했습니다)")

        row = sub.iloc[-1]
        cols = list(df.columns)


        # 기본 툴팁에서 제외할 컬럼(요청사항)
        _exclude_cols = {
            '키워드 조회 수(누적카운트)',
            '마지막 출력일자',
            '키워드등록일자',
            '키워드등록인',
        }
        cols = [c for c in cols if str(c).strip() not in _exclude_cols]
        lines = []
        for c in cols:
            v = row.get(c, "")
            try:
                if pd.isna(v):
                    v = ""
            except Exception:
                pass
            # 각 라인은 span + <br> 형태로 구성(유효한 in-span 구조)
            lines.append(
                f"<span class='kwd-tt-row'>"
                f"<span class='kwd-tt-k'>{html.escape(str(c))}</span>"
                f"<span class='kwd-tt-v'>{html.escape(str(v))}</span>"
                f"</span>"
            )

        # 표처럼 보이도록 row들을 연속 배치 (phrasing-only)
        body = "".join(lines)
        return f"<span class='kwd-tooltip'>{body}</span>"
    except Exception as e:
        return _msg_box(f"kwd_no: {kwd_no}\\n(tooltip 생성 중 오류: {e})")


def highlight_text(text: str, matches: List[Match2]) -> str:
    """텍스트에 등급별 하이라이트를 적용하여 HTML 문자열로 반환합니다.

    핵심 개선:
    1) KWD_NO 표기는 기존처럼 괄호 포함 '(US036)' 형태를 유지하되,
       tooltip 매핑/조회는 항상 'US036' 같은 순수 토큰만 사용합니다.
       (괄호/공백/줄바꿈이 섞여도 정규화)
    2) 원문에 이미 '(US036 ...)' 같은 표기가 붙어있는 경우, 우리가 ids_html을 추가한 뒤
       원문에 남아있는 괄호 표기가 다시 렌더링되며 ')'만 다음 줄에 남는 문제가 생길 수 있어,
       하이라이트 구간 바로 뒤에 붙은 괄호표기(키워드번호 포함)를 "소비(건너뛰기)"합니다.
       → 원문엔 줄바꿈이 없어도 UI에서 ')'가 단독으로 내려가는 현상을 방지.
    """
    if not matches:
        return html.escape(text)

    def _risk_grade(r: str) -> int:
        r = (r or '').strip()
        if r.startswith('1'): return 1
        if r.startswith('2'): return 2
        if r.startswith('3'): return 3
        if r.startswith('4'): return 4
        if r.startswith('5'): return 5
        return 0

    text_colors = {1: '#e03131', 2: '#1c7ed6', 3: '#7048e8', 4: '#2f9e44'}

    _token_re = re.compile(r'([A-Za-z]{2})\s*([0-9]{3,})')

    def _extract_kwd_token(raw: str) -> str:
        s = "" if raw is None else str(raw)
        m = _token_re.search(s)
        if not m:
            return s.strip()
        return (m.group(1) + m.group(2)).upper()

    def _split_around_token(raw: str):
        s = "" if raw is None else str(raw)
        m = _token_re.search(s)
        if not m:
            return ("", s, "")
        head = s[:m.start()]
        token = (m.group(1) + m.group(2)).upper()
        tail = s[m.end():]
        return (head, token, tail)

    # (start, end) 구간별로 kwd_no / risk 정보 묶기
    spans = {}
    for m in matches:
        seg = text[m.start:m.end]
        if not seg:
            continue
        key = (m.start, m.end)
        info = spans.get(key)
        if info is None:
            info = {'seg': seg, 'ids_raw': [], 'risk_map': {}}
            spans[key] = info

        if m.kwd_no:
            info['ids_raw'].append(m.kwd_no)
            norm = _extract_kwd_token(m.kwd_no)
            info['risk_map'][norm] = _risk_grade(m.risk)

    if not spans:
        return html.escape(text)

    ordered = sorted(spans.items(), key=lambda kv: kv[0][0])

    result_parts = []
    cur = 0

    # 괄호 표기 소비용: 하이라이트 구간 직후의 "(...)"를 제거(건너뛰기)
    # - DOTALL로 줄바꿈 포함 가능
    # - 길이 제한을 두어 과도한 잡아먹기 방지
    paren_pat = re.compile(r'^[ \t\r\f\v\n]*\((.{0,200}?)\)', re.DOTALL)

    for (start, end), info in ordered:
        if start < cur:
            continue

        # 일반 텍스트 추가 (하이라이트 앞)
        if cur < start:
            result_parts.append(html.escape(text[cur:start]))

        seg = info['seg']
        ids_raw = info['ids_raw'] or []
        risk_map = info['risk_map'] or {}

        # kwd_no 링크 HTML 구성
        frag_parts = []
        for i, raw in enumerate(ids_raw):
            head, token, tail = _split_around_token(raw)
            norm = _extract_kwd_token(raw) or (token or str(raw).strip())

            g = risk_map.get(norm, 0)
            color = text_colors.get(g, 'inherit')
            tooltip_html = build_kwd_tooltip_html(norm)

            # 표기 유지: 괄호/구분자도 같은 wrap 안에 포함시켜 hover 영역이 끊기지 않게 합니다.
            # - 첫 번째 항목: '('를 head 쪽에 포함(이미 head에 '('가 있으면 중복 방지)
            # - 마지막 항목: tail에 ')'가 있으면 그대로 두고, 없으면 suffix로 ')' 추가
            prefix = ""
            if i == 0 and "(" not in head:
                prefix = "("
            sep = ", " if i > 0 else ""
            suffix = ""
            if i == len(ids_raw) - 1:
                if ")" not in tail:
                    suffix = ")"

            frag_content = (
                f"{html.escape(sep + prefix)}"
                f"{html.escape(head)}"
                f"<span class='kwdno-link' data-kwd='{html.escape(norm)}' "
                f"style='color:{color}; text-decoration:underline; cursor:pointer'>"
                f"<b>{html.escape(token or norm)}</b></span>"
                f"{html.escape(tail + suffix)}"
                f"{tooltip_html}"
            )
            frag_parts.append(f"<span class='kwdno-wrap'>{frag_content}</span>")

        ids_html = "".join(frag_parts)

        grades = {g for g in risk_map.values() if g}
        cls = "hl-mark hl-risk5" if grades == {5} else "hl-mark hl-riskN"

        leading_ws = seg[:len(seg) - len(seg.lstrip())]
        trailing_ws = seg[len(seg.rstrip()):]
        core = seg.strip()

        inner_html = f"【<b>{html.escape(core)}</b>】{ids_html}"
        decorated = f"{html.escape(leading_ws)}<span class='{cls}'>{inner_html}</span>{html.escape(trailing_ws)}"
        result_parts.append(decorated)

        # 원문 커서 이동: 기본은 end까지
        cur = end

        # 하이라이트 직후에 원문에 남아있는 "(US036 ...)" 같은 표기가 붙어있으면 소비(스킵)
        # → ')'가 다음 줄로 떨어져 보이는 현상 방지
        tail_text = text[cur:]
        mparen = paren_pat.match(tail_text)
        if mparen:
            inside = mparen.group(1) or ""
            if _token_re.search(inside):
                cur += mparen.end()

    # 남은 텍스트
    if cur < len(text):
        result_parts.append(html.escape(text[cur:]))

    return "".join(result_parts)
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
  overflow: visible;
  background:#fff;
  white-space:pre-wrap;
  line-height:1.5;
  box-sizing:border-box;
}}
.hl-box .hl-scroll{{
  max-height:{MAX_HIGHLIGHT_HEIGHT}px;
  min-height:21.0em; /* ~3 lines (line-height 1.5) */
  overflow-y:auto;
  overflow-x:auto;
}}
/* 기존 mark 기반 하이라이트는 유지하되, span.hl-mark 클래스를 새로 정의 */
.hl-box mark{{padding:0;border-radius:2px}}
.hl-box .hl-mark{{
  padding:0;
  border-radius:2px;
  color:#000;
}}
.hl-box .hl-risk5{{
  background:#d8f5a2 !important;
  background-color:#d8f5a2 !important;
}}
.hl-box .hl-riskN{{
  background:#ffd43b !important;
  background-color:#ffd43b !important;
}}
[data-testid="stDataFrame"] div{{white-space:normal !important;}}
/* kwd_no hover tooltip (table-style) */
.hl-box .kwdno-wrap{{
  position: relative;
  display: inline-block;
}}
.hl-box .kwd-tooltip{{
  display: none;
  position: absolute;
  left: 0;
  top: 1.6em;
  z-index: 9999;
  max-width: 92vw;
  min-width: 520px;
  width: max-content;
  max-height: 280px;
  overflow: auto;
  background: #f3f3f3;
  color: #111;
  border: 1px solid rgba(0,0,0,0.15);
  border-radius: 10px;
  box-shadow: 0 10px 28px rgba(0,0,0,0.18);
  padding: 8px 10px;
}}
.hl-box .kwd-tooltip-line{{
  display: block;
  font-size: 12px;
  line-height: 1.35;
  white-space: normal;
}}

.hl-box .kwd-tooltip{{
  /* 표처럼 보이도록 기본 폭/레이아웃을 조금 더 안정적으로 */
  min-width: 320px;
  row-gap: 2px;
}}
.hl-box .kwd-tt-row{{
  display: grid;
  grid-template-columns: max-content 1fr;
  column-gap: 10px;
  align-items: start;
  padding: 2px 0;
}}
.hl-box .kwd-tt-k{{
  font-weight: 700;
  white-space: nowrap;
}}
.hl-box .kwd-tt-v{{
  white-space: normal;
  overflow-wrap: anywhere;
}}
.hl-box .kwdno-wrap:hover .kwd-tooltip{{
  display: inline-grid;
}}
.hl-box table.kwd-tooltip-table{{
  border-collapse: collapse;
  font-size: 12px;
  line-height: 1.35;
}}
.hl-box table.kwd-tooltip-table th{{
  position: sticky;
  top: 0;
  background: #f6f7f9;
  font-weight: 600;
  white-space: nowrap;
}}
.hl-box table.kwd-tooltip-table th,
.hl-box table.kwd-tooltip-table td{{
  border: 1px solid rgba(0,0,0,0.08);
  padding: 4px 6px;
  vertical-align: top;
}}
.hl-box table.kwd-tooltip-table td{{
  white-space: normal;
  max-width: 320px;
}}

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
            match_mode = st.radio(
                "매칭 조건",
                ["정확 일치", "유사 키워드 포함", "부분 문자열(최소 글자수)"],
                index=0,
                key="match_mode",
            )
            # 매칭 조건별 말풍선 형태 설명 (마우스 오버 시 표시)
            st.markdown(
                """

<style>
.tooltip-mc { position: relative; display: inline-block; margin-right: 12px; cursor: help; }
.tooltip-mc:hover { z-index: 10000; }
.tooltip-mc .tooltip-mc-text {
    display: none;
    opacity: 0;
    pointer-events: none;
    width: 320px;
    background-color: #333;
    color: #fff;
    text-align: left;
    border-radius: 4px;
    padding: 8px 10px;
    position: absolute;
    z-index: 9999;
    bottom: 125%;
    left: 50%;
    transform: translateX(-50%);
    font-size: 11px;
    line-height: 1.4;
}
.tooltip-mc .tooltip-mc-text::after {
    content: "";
    position: absolute;
    top: 100%;
    left: 50%;
    margin-left: -5px;
    border-width: 5px;
    border-style: solid;
    border-color: #333 transparent transparent transparent;
}
.tooltip-mc:hover > .tooltip-mc-text { display: block; opacity: 1; pointer-events: auto; }
</style>

<div style="font-size: 12px; margin-top: 4px; margin-bottom: 4px;">
  <span style="opacity:0.8;">매칭 조건 도움말:</span>
  <span class="tooltip-mc">
    <b>정확 일치</b>
    <span class="tooltip-mc-text">
      DB에 등록된 문자열을 거의 그대로 썼을 때만 잡는, 가장 엄격한 모드<br>
      예) 키워드: Skin looked more even-toned<br>
      &nbsp;&nbsp;&nbsp;&nbsp;Skin looked more even-toned in 4 weeks → 매칭
    </span>
  </span>
  <span class="tooltip-mc">
    <b>유사 키워드 포함</b>
    <span class="tooltip-mc-text">
      정확 일치 케이스를 대부분 커버하면서, 실제 문장에서 자주 생기는 조사/어미, 합성어, 굴절형까지 더 넓게 잡는 모드<br>
      예) 키워드: 미세바늘<br>
      &nbsp;&nbsp;&nbsp;&nbsp;미세바늘시술은 전문의 상담 후 → 매칭
    </span>
  </span>
  <span class="tooltip-mc">
    <b>부분 문자열(최소 글자수)</b>
    <span class="tooltip-mc-text">
      키워드의 일부분만 N글자 이상 일치해도 매칭하는 탐색용 모드<br>
      (N은 사용자 지정: 2자 / 3자 / 4자 등)<br>
      예) 키워드: 안티에이징, N=2<br>
      &nbsp;&nbsp;&nbsp;&nbsp;에이징 케어 크림 → 매칭<br>
      예) 키워드: 안티에이징, N=4<br>
      &nbsp;&nbsp;&nbsp;&nbsp;에이 크림 → 미매칭
    </span>
  </span>
</div>
""",
                unsafe_allow_html=True,
            )
            # 부분 문자열 모드에서 최소 글자수 설정 (기본 2자 이상)
            if match_mode == "부분 문자열(최소 글자수)":
                st.number_input(
                    "부분 문자열 최소 글자수",
                    min_value=2,
                    max_value=20,
                    value=int(st.session_state.get("partial_min_len", 2) or 2),
                    step=1,
                    key="partial_min_len",
                )
            exact_relaxed = st.checkbox("(괄호 제거·공백 정규화·영문 대소문자 무시)", value=True, key="exact_relaxed")
            with st.expander("유사일치 조사/어미(접미사) 설정", expanded=False):
                st.caption("유사일치에서 '키워드+조사/어미' 결합 형태(예: 여드름에는, 미세바늘이라)를 인식하기 위한 설정입니다. 기본 리스트 + 사용자 추가분이 합쳐져 적용됩니다.")

                default_suffixes = sorted(set(_KO_SUFFIX_WHITELIST))
                extra_suffixes = st.session_state.get("ko_suffix_whitelist_extra", []) or []
                extra_suffixes = [str(x).strip() for x in extra_suffixes if str(x).strip()]

                st.markdown("**사용자 추가 접미사(편집 가능)**")
                raw = st.text_area(
                    "쉼표 또는 줄바꿈으로 구분해서 입력하세요",
                    value="\n".join(sorted(set(extra_suffixes))),
                    height=110,
                    key="ui_suffix_extra_editor",
                )

                c1, c2, c3 = st.columns([1,1,2])
                with c1:
                    if st.button("저장", key="btn_save_suffix_extra"):
                        items = []
                        for part in re.split(r"[\n,]+", raw or ""):
                            part = str(part).strip()
                            if part:
                                items.append(part)
                        # 중복 제거(순서 유지)
                        seen = set()
                        cleaned = []
                        for x in items:
                            if x not in seen and x not in _KO_SUFFIX_WHITELIST:
                                seen.add(x)
                                cleaned.append(x)
                        st.session_state.ko_suffix_whitelist_extra = cleaned
                        ok = save_suffix_whitelist_config(cleaned)
                        if ok:
                            st.success("접미사 리스트를 저장했습니다.")
                        else:
                            st.warning("저장에 실패했습니다. (쓰기 권한/경로를 확인해주세요)")

                with c2:
                    if st.button("초기화(추가분 제거)", key="btn_reset_suffix_extra"):
                        st.session_state.ko_suffix_whitelist_extra = []
                        ok = save_suffix_whitelist_config([])
                        if ok:
                            st.success("사용자 추가분을 초기화했습니다.")
                        else:
                            st.warning("저장에 실패했습니다. (쓰기 권한/경로를 확인해주세요)")

                with c3:
                    st.markdown("**기본 접미사(읽기 전용)**")
                    st.code(", ".join(default_suffixes), language="text")

                # ---- 후보 접미사 제안(승인형) ----
                suggestions = st.session_state.get("suffix_suggestions", {}) or {}
                if suggestions:
                    st.markdown("---")
                    st.markdown("**미검출 접미사 후보(승인형)**")
                    st.caption("최근 분석에서 '키워드+접미사' 형태로 보이지만 리스트에 없어 매칭되지 않은 접미사 후보입니다. 필요할 때만 추가하세요.")
                    # 자주 나온 것부터
                    for suf, cnt in sorted(suggestions.items(), key=lambda x: (-x[1], x[0])):
                        is_in_default = suf in _KO_SUFFIX_WHITELIST
                        is_in_extra = suf in set(st.session_state.get("ko_suffix_whitelist_extra", []) or [])
                        cols = st.columns([3,1,1])
                        cols[0].write(f"`{suf}`  (발견 {cnt}회)")
                        if is_in_default or is_in_extra:
                            cols[1].write("✅ 적용중")
                            cols[2].write("")
                        else:
                            if cols[1].button("추가", key=f"btn_add_suffix_{suf}"):
                                cur = st.session_state.get("ko_suffix_whitelist_extra", []) or []
                                cur = [str(x).strip() for x in cur if str(x).strip()]
                                if suf not in cur and suf not in _KO_SUFFIX_WHITELIST:
                                    cur.append(suf)
                                st.session_state.ko_suffix_whitelist_extra = cur
                                save_suffix_whitelist_config(cur)
                                cols[2].write("추가됨")
                else:
                    st.caption("미검출 접미사 후보가 아직 없습니다. (분석을 실행하면 후보가 수집됩니다.)")


            with st.expander("정규화: 무시할 특수기호 설정(공백으로 치환)", expanded=False):
                st.caption("정규화 옵션이 켜져 있을 때(괄호 제거·공백 정규화·영문 대소문자 무시) 적용됩니다. 입력한 특수기호는 *문자 1개 단위*로만 저장되며, 텍스트/키워드에서 해당 문자를 공백으로 바꿔 매칭합니다.")
                cur_syms = st.session_state.get("ignored_special_symbols", []) or []
                raw_syms = st.text_area(
                    "무시할 특수기호(한 줄에 하나 또는 쉼표로 구분)",
                    value="\n".join(cur_syms),
                    height=110,
                    key="ui_ignored_syms_editor",
                )

                c_sy1, c_sy2, c_sy3 = st.columns([1,1,2])
                with c_sy1:
                    if st.button("저장", key="btn_save_ignored_syms"):
                        items = []
                        for part in re.split(r"[\n,]+", raw_syms or ""):
                            part = str(part)
                            part = part.strip()
                            if not part:
                                continue
                            # 문자 1개 단위만 허용
                            if len(part) != 1:
                                st.warning(f"'{part}' 는 1글자 특수기호가 아니라서 제외했습니다.")
                                continue
                            items.append(part)
                        # 중복 제거(순서 유지)
                        seen = set()
                        cleaned = []
                        for ch in items:
                            if ch not in seen:
                                seen.add(ch)
                                cleaned.append(ch)
                        st.session_state.ignored_special_symbols = cleaned
                        ok = save_ignored_special_symbols_config(cleaned)
                        if ok:
                            st.success("특수기호 리스트를 저장했습니다.")
                        else:
                            st.warning("저장에 실패했습니다. (쓰기 권한/경로를 확인해주세요)")

                with c_sy2:
                    if st.button("기본값 복원", key="btn_reset_ignored_syms"):
                        st.session_state.ignored_special_symbols = _DEFAULT_IGNORED_SPECIAL_SYMBOLS.copy()
                        ok = save_ignored_special_symbols_config(st.session_state.ignored_special_symbols)
                        if ok:
                            st.success("기본값으로 복원했습니다.")
                        else:
                            st.warning("저장에 실패했습니다. (쓰기 권한/경로를 확인해주세요)")

                with c_sy3:
                    st.markdown("**현재 적용중 특수기호(읽기)**")
                    st.code(", ".join(st.session_state.get("ignored_special_symbols", []) or _DEFAULT_IGNORED_SPECIAL_SYMBOLS), language="text")


    
    if st.button("텍스트 분석하기", type="primary", key="btn_analyze"):
        try:
            # 항상 최신 키워드 DB 상태를 반영하기 위해 저장 경로에서 다시 로드
            try:
                st.session_state.kw_df = load_db(Path(st.session_state.storage_path))
            except Exception as _e:
                st.warning(f"키워드 DB를 다시 불러오는 중 오류가 발생했습니다: {_e}")
            if st.session_state.kw_df.empty:
                st.info("키워드가 없습니다. 먼저 추가하세요.")
            else:
                suffix_suggestions = {}
                matches = find_matches_nlp(
                    text,
                    st.session_state.kw_df,
                    match_mode=st.session_state.match_mode,
                    exact_relaxed=st.session_state.exact_relaxed,
                    suffix_suggestions=suffix_suggestions,
                    partial_min_len=int(st.session_state.get("partial_min_len", 2) or 2),
                )
                st.session_state.suffix_suggestions = suffix_suggestions
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
        f"<div class='hl-box'><div class='hl-scroll'>{_persist_highlight_html}</div></div>",
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

        st.markdown("---")
    _base_df = st.session_state.kw_df.copy()
    _view_df = _base_df

    # -----------------------------
    # 🧰 리스트 상단 필터 바 (옵션 A)
    # - 표 위의 필터 바에서 조건을 선택하면 표가 즉시 좁혀집니다.
    # - 아무 필터도 선택하지 않으면 기존과 동일하게 전체가 표시됩니다.
    # -----------------------------
    def _reset_list_filters_tab1():
        # 위젯 입력값 초기화
        st.session_state["list_filter_search_tab1"] = ""

        st.session_state["list_filter_cat_tab1"] = []
        st.session_state["list_filter_risk_tab1"] = []
        st.session_state["list_filter_proof_tab1"] = []
        # '적용된' 필터값도 함께 초기화 (적용 버튼 기반 UX)
        st.session_state["list_filter_applied_search_tab1"] = ""
        st.session_state["list_filter_applied_kwdno_tab1_removed"] = ""
        st.session_state["list_filter_applied_cat_tab1"] = []
        st.session_state["list_filter_applied_risk_tab1"] = []
        st.session_state["list_filter_applied_proof_tab1"] = []
        st.session_state["list_filter_has_applied_tab1"] = False

    def _apply_list_filter_edits_tab1(edited_df: pd.DataFrame, original_df: pd.DataFrame) -> None:
        """리스트 필터 하단 표 편집 내용을 st.session_state.kw_df 및 DB 파일에 반영합니다.

        - edited_df: st.data_editor 결과 (사용자가 편집한 값)
        - original_df: 편집 전 원본 표 (현재 필터 상태에서의 원래 값)
        """
        if edited_df is None or edited_df.empty:
            return
        if original_df is None or original_df.empty:
            return

        base_df = st.session_state.kw_df.copy()
        db_cols = get_db_cols()

        if "kwd_no" not in base_df.columns or "kwd_no" not in original_df.columns:
            st.warning("kwd_no 컬럼이 없어 편집 내용을 반영할 수 없습니다.")
            return

        # 공통으로 존재하는 컬럼만 업데이트
        cols_to_update = [c for c in db_cols if c in edited_df.columns and c in base_df.columns]
        if not cols_to_update:
            return

        # 인덱스를 기준으로 원본/수정본을 1:1 매핑
        edited_norm = edited_df[cols_to_update].reset_index(drop=True).copy()
        original_norm = original_df.reset_index(drop=True).copy()

        for c in cols_to_update:
            try:
                edited_norm[c] = edited_norm[c].astype(str).str.strip()
            except Exception:
                pass
        if "kwd_no" in original_norm.columns:
            try:
                original_norm["kwd_no"] = original_norm["kwd_no"].astype(str).str.strip()
            except Exception:
                pass

        # kwd_no가 변경된 항목(old -> new) 수집 (조회 로그 업데이트 등에 사용)
        kwd_change_map = {}
        if "kwd_no" in cols_to_update and "kwd_no" in original_norm.columns:
            for i in range(len(edited_norm)):
                try:
                    old_no = str(original_norm.loc[i, "kwd_no"])
                    new_no = str(edited_norm.loc[i, "kwd_no"])
                except Exception:
                    continue
                if old_no and new_no and old_no != new_no:
                    kwd_change_map[old_no] = new_no

        # base_df 업데이트: 원본 kwd_no를 키로 사용해 행 단위로 덮어쓰기
        for i in range(len(edited_norm)):
            if "kwd_no" not in original_norm.columns:
                break
            try:
                old_no = str(original_norm.loc[i, "kwd_no"])
            except Exception:
                continue
            if not old_no:
                continue

            row_mask = base_df["kwd_no"].astype(str) == old_no
            if not row_mask.any():
                continue

            for c in cols_to_update:
                base_df.loc[row_mask, c] = edited_norm.loc[i, c]

        st.session_state.kw_df = base_df
        save_db(st.session_state.kw_df, Path(st.session_state.storage_path))

        # 조회 로그에도 kwd_no 변경 사항을 반영 (리포트 연동 유지)
        if kwd_change_map:
            try:
                log_df = load_hits_log()
                if not log_df.empty and "kwd_no" in log_df.columns:
                    log_df["kwd_no"] = log_df["kwd_no"].astype(str)
                    for old_no, new_no in kwd_change_map.items():
                        log_df.loc[log_df["kwd_no"] == old_no, "kwd_no"] = new_no
                    overwrite_hits_log(log_df)
            except Exception as e:
                st.warning(f"조회 로그의 키워드 번호를 갱신하는 중 오류가 발생했습니다: {e}")


    st.markdown("#### 🧰 리스트 필터")

    # --------------------------------
    # 필터 적용 버튼 기반 UX
    # - 입력값을 바꿔도 즉시 표에 반영하지 않고,
    # - "필터 적용" 버튼을 눌렀을 때만 적용되도록 합니다.
    # --------------------------------
    
    # (Streamlit 경고 방지) key를 가진 위젯에는 default/value를 함께 주지 않습니다.
    # 대신 최초 1회만 session_state에 기본값을 세팅합니다.
    _init_defaults = {
        "list_filter_search_tab1": "",
        "list_filter_cat_tab1": [],
        "list_filter_risk_tab1": [],
        "list_filter_proof_tab1": [],
        "list_filter_edit_mode_tab1": False,
# 적용된 값(Apply 버튼 기준)
        "list_filter_applied_search_tab1": st.session_state.get("list_filter_applied_search_tab1", ""),
                "list_filter_applied_cat_tab1": st.session_state.get("list_filter_applied_cat_tab1", []),
        "list_filter_applied_risk_tab1": st.session_state.get("list_filter_applied_risk_tab1", []),
        "list_filter_applied_proof_tab1": st.session_state.get("list_filter_applied_proof_tab1", []),
}
    for _k, _v in _init_defaults.items():
        if _k not in st.session_state:
            st.session_state[_k] = _v

    with st.form("list_filter_form_tab1", clear_on_submit=False):
        f1, f2, f3, f4 = st.columns([3, 2, 2, 2])

        with f1:
            search_text = st.text_input(
                "키워드 검색",
                placeholder="키워드명 / 대체키워드 / kwd_no 검색",
                key="list_filter_search_tab1",
            )
        with f2:
            # 옵션 목록
            cat_opts = sorted([x for x in _base_df.get("상품카테고리", pd.Series([], dtype=str)).dropna().unique().tolist() if str(x).strip()])
            sel_cat = st.multiselect(
                "상품카테고리",
                options=cat_opts,
                key="list_filter_cat_tab1",
            )

        with f3:
            risk_opts = sorted([x for x in _base_df.get("리스크 등급", pd.Series([], dtype=str)).dropna().unique().tolist() if str(x).strip()])
            sel_risk = st.multiselect(
                "리스크 등급",
                options=risk_opts,
                key="list_filter_risk_tab1",
            )

        with f4:
            proof_opts = sorted([x for x in _base_df.get("증빙자료유형", pd.Series([], dtype=str)).dropna().unique().tolist() if str(x).strip()])
            sel_proof = st.multiselect(
                "증빙자료유형",
                options=proof_opts,
                key="list_filter_proof_tab1",
            )

        # 버튼 영역(요청: '필터 적용' 옆에 '필터 초기화' 고정 배치)
        b1, b2, _sp = st.columns([1, 1, 8])
        with b1:
            apply_clicked = st.form_submit_button(
                "필터 적용",
                key="btn_apply_list_filters_tab1",
                use_container_width=True,
            )
        with b2:
            # 같은 form 안에 두 버튼을 함께 배치해야 리런 시에도 위치가 틀어지지 않습니다.
            st.form_submit_button(
                "필터 초기화",
                key="btn_reset_list_filters_tab1",
                use_container_width=True,
                on_click=_reset_list_filters_tab1,
            )

    # '필터 적용' 시점에만 적용값을 저장
    if apply_clicked:
        st.session_state["list_filter_applied_search_tab1"] = st.session_state.get("list_filter_search_tab1", "")
        st.session_state["list_filter_applied_cat_tab1"] = st.session_state.get("list_filter_cat_tab1", [])
        st.session_state["list_filter_applied_risk_tab1"] = st.session_state.get("list_filter_risk_tab1", [])
        st.session_state["list_filter_applied_proof_tab1"] = st.session_state.get("list_filter_proof_tab1", [])
        st.session_state["list_filter_has_applied_tab1"] = True
        st.rerun()
    # Apply top filters (필터 적용 버튼을 눌렀을 때 저장된 값만 사용)
    _filtered_df = _view_df.copy()

    search_q = st.session_state.get("list_filter_applied_search_tab1", "")
    sel_cats = st.session_state.get("list_filter_applied_cat_tab1", [])
    sel_risks = st.session_state.get("list_filter_applied_risk_tab1", [])
    sel_evids = st.session_state.get("list_filter_applied_proof_tab1", [])
    # 키워드/대체키워드/kwd_no/리스크 등급별 세부 심의기준 검색
    if search_q and str(search_q).strip():
        # 쉼표로 여러 검색어를 입력하면 OR 조건으로 모두 포함 검색
        # 입력값에 괄호()가 포함되어도 검색되도록, 각 검색어의 앞뒤 괄호/공백을 제거하여 정규화
        raw_parts = [x for x in str(search_q).split(",") if str(x).strip()]
        q_list = []
        for _part in raw_parts:
            q = str(_part).strip()
            # (US087), US087), (US087 등도 모두 US087로 정규화
            q = re.sub(r"^[\s\(\)]+", "", q)
            q = re.sub(r"[\s\(\)]+$", "", q)
            if q:
                q_list.append(q)

        _kw = _filtered_df.get("키워드명", "").astype(str)
        _alt = _filtered_df.get("대체키워드", "").astype(str)
        _no = _filtered_df.get("kwd_no", "").astype(str)
        _detail = _filtered_df.get("리스크 등급별 세부 심의기준", "").astype(str)

        # 각 검색어별 mask를 만들고 OR로 합산
        mask_all = None
        for q in q_list:
            mask_q = (
                _kw.str.contains(q, case=False, na=False)
                | _alt.str.contains(q, case=False, na=False)
                | _no.str.contains(q, case=False, na=False)
                | _detail.str.contains(q, case=False, na=False)
            )
            mask_all = mask_q if mask_all is None else (mask_all | mask_q)

        if mask_all is not None:
            _filtered_df = _filtered_df[mask_all]

    # 멀티셀렉트 필터들
    if sel_cats:
        _filtered_df = _filtered_df[_filtered_df["상품카테고리"].astype(str).isin(sel_cats)]

    if sel_risks:
        _filtered_df = _filtered_df[_filtered_df["리스크 등급"].astype(str).isin(sel_risks)]

    if sel_evids:
        _filtered_df = _filtered_df[_filtered_df["증빙자료유형"].astype(str).isin(sel_evids)]
    # 리스트 보기/편집 모드 (관리자 전용 ON/OFF)
    edit_mode_effective = False
    mode_cols = st.columns([2, 8])
    with mode_cols[0]:
        if is_admin():
            st.checkbox(
                "리스트 편집 모드 (관리자 전용)",
                key="list_filter_edit_mode_tab1",
                help="ON: 아래 표에서 직접 값을 수정할 수 있습니다. OFF: 읽기 전용 뷰어 모드입니다.",
            )
            edit_mode_effective = bool(st.session_state.get("list_filter_edit_mode_tab1", False))
        else:
            # 관리자 로그인이 아니면 항상 뷰어 모드로 고정
            st.caption("리스트 편집 모드는 관리자 로그인 시에만 사용할 수 있습니다.")
            st.session_state["list_filter_edit_mode_tab1"] = False
            edit_mode_effective = False
    with mode_cols[1]:
        mode_label = "편집 모드(ON)" if edit_mode_effective else "뷰어 모드(읽기 전용)"
        st.caption(f"현재 표시된 항목: {len(_filtered_df)} / 전체: {len(_base_df)} · {mode_label}")

    _list_view_df = sort_for_display(_filtered_df[get_db_cols()])

    if edit_mode_effective and is_admin():
        st.info("편집 모드: 셀을 수정한 뒤 반드시 아래 '변경사항 저장' 버튼을 눌러야 DB에 반영됩니다.")
        edited_df = st.data_editor(
            _list_view_df,
            key="list_filter_editor_tab1",
            use_container_width=True,
        )
        save_clicked = st.button("변경사항 저장 (현재 필터된 행에만 적용)", key="btn_list_filter_save_tab1")
        if save_clicked:
            try:
                _apply_list_filter_edits_tab1(edited_df, _list_view_df)
                st.success("리스트 편집 내용이 저장되었습니다.")
            except Exception as e:
                st.error(f"리스트 편집 내용 저장 중 오류가 발생했습니다: {e}")
    else:
        st.dataframe(_rename_fcol(_list_view_df), width="stretch")

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