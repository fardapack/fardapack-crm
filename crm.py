# -*- coding: utf-8 -*-
"""
FardaPack Mini-CRM — Streamlit + SQLite (چندکاربره سبک) — نسخه با تاریخ شمسی، فرم‌های تاشونده و ویرایش در جدول
"""

import sqlite3
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Dict

import pandas as pd
import streamlit as st
import hashlib

# ========= تنظیمات صفحه + استایل سراسری (فونت Vazirmatn و RTL) =========
st.set_page_config(page_title="FardaPack Mini-CRM", page_icon="📇", layout="wide")

st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
    html, body, [data-testid="stAppViewContainer"] {
        direction: rtl;
        text-align: right !important;
        font-family: "Vazirmatn", sans-serif !important;
    }
    [data-testid="stDataFrame"] div[role="gridcell"],
    [data-testid="stDataFrame"] div[role="columnheader"] {
        text-align: right !important;
        direction: rtl !important;
        justify-content: flex-end !important;
        font-family: "Vazirmatn", sans-serif !important;
    }
    [data-testid="stTable"] table { direction: rtl; width: 100%; }
    [data-testid="stTable"] th, [data-testid="stTable"] td { text-align: right !important; }
    .stSelectbox label, .stTextInput label, .stTextArea label, .stTimeInput label { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True
)

# ========= تاریخ شمسی =========
try:
    from persiantools.jdatetime import JalaliDate
except Exception:
    JalaliDate = None

def _jalali_supported() -> bool:
    return JalaliDate is not None

def today_jalali_str() -> str:
    return JalaliDate.today().strftime("%Y/%m/%d") if _jalali_supported() else ""

def jalali_str_to_date(s: str) -> Optional[date]:
    if not s or not _jalali_supported():
        return None
    try:
        g = JalaliDate.strptime(s.strip(), "%Y/%m/%d").to_gregorian()
        return date(g.year, g.month, g.day)
    except Exception:
        return None

def date_to_jalali_str(d: date) -> str:
    if not d or not _jalali_supported():
        return ""
    return JalaliDate.fromgregorian(date=d).strftime("%Y/%m/%d")

# ========= تنظیمات تعطیلات (برای 10#) =========
# جمعه تعطیل است؛ علاوه بر آن این فهرست شمسی (YYYY/MM/DD) را می‌توانید تکمیل کنید:
HOLIDAYS_JALALI = {
    # نمونه‌ها:
    # "1403/01/01", "1403/01/12", "1403/03/14",
}
def is_holiday_gregorian(d: date) -> bool:
    # جمعه
    if d.weekday() == 4:  # Monday=0 .. Friday=4
        return True
    # تعطیلات خاص
    if _jalali_supported():
        js = date_to_jalali_str(d)
        if js and js in HOLIDAYS_JALALI:
            return True
    return False

# ========= پایگاه داده =========
DB_PATH = "crm.db"
CALL_STATUSES = ["ناموفق", "موفق", "خاموش", "رد تماس"]
TASK_STATUSES = ["در حال انجام", "پایان یافته"]
USER_STATUSES = ["بدون وضعیت", "در حال پیگیری", "پیش فاکتور", "مشتری شد"]  # 5#

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def sha256(txt: str) -> str:
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()

def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r[1] == col for r in rows)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # شرکت‌ها
    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            address TEXT,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER
        );
    """)
    # کاربران (رابط‌ها)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            phone TEXT,
            role TEXT,
            company_id INTEGER,
            note TEXT,
            status TEXT NOT NULL DEFAULT 'بدون وضعیت',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL
        );
    """)
    # تماس‌ها
    cur.execute("""
        CREATE TABLE IF NOT EXISTS calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            call_datetime TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('ناموفق','موفق','خاموش','رد تماس')),
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
    """)
    # پیگیری‌ها
    cur.execute("""
        CREATE TABLE IF NOT EXISTS followups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            details TEXT,
            due_date TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('در حال انجام','پایان یافته')) DEFAULT 'در حال انجام',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
    """)
    # کاربران ورود
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_sha256 TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin','agent')) DEFAULT 'agent',
            linked_user_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(linked_user_id) REFERENCES users(id) ON DELETE SET NULL
        );
    """)

    # مایگریشن افزودن ستون‌ها در صورت نبود
    for t in ("companies", "users", "calls", "followups"):
        if not _column_exists(conn, t, "created_by"):
            cur.execute(f"ALTER TABLE {t} ADD COLUMN created_by INTEGER;")
    if not _column_exists(conn, "users", "status"):
        cur.execute("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'بدون وضعیت';")

    # ایندکس‌ها
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_company ON users(company_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calls_user_datetime ON calls(user_id, call_datetime);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_followups_user_due ON followups(user_id, due_date);")

    # ادمین پیش‌فرض
    any_user = cur.execute("SELECT COUNT(*) FROM app_users;").fetchone()[0]
    if any_user == 0:
        cur.execute(
            "INSERT INTO app_users (username, password_sha256, role, linked_user_id) VALUES (?,?,?,NULL);",
            ("admin", sha256("admin123"), "admin"),
        )

    conn.commit()
    conn.close()

# ========= توابع ساخت/خوانش =========
def create_app_user(username: str, password: str, role: str, linked_user_id: Optional[int]):
    conn = get_conn()
    conn.execute(
        "INSERT INTO app_users (username, password_sha256, role, linked_user_id) VALUES (?,?,?,?);",
        (username.strip(), sha256(password), role, linked_user_id),
    )
    conn.commit()
    conn.close()

def auth_check(username: str, password: str) -> Optional[Dict]:
    conn = get_conn()
    row = conn.execute(
        "SELECT id, username, password_sha256, role, linked_user_id FROM app_users WHERE username=?;",
        (username.strip(),),
    ).fetchone()
    conn.close()
    if not row:
        return None
    uid, uname, pwh, role, linked_user_id = row
    if sha256(password) == pwh:
        return {"id": uid, "username": uname, "role": role, "linked_user_id": linked_user_id}
    return None

def list_companies(only_creator: Optional[int]) -> List[Tuple[int, str]]:
    conn = get_conn()
    if only_creator:
        rows = conn.execute(
            "SELECT id, name FROM companies WHERE created_by=? ORDER BY name COLLATE NOCASE;", (only_creator,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT id, name FROM companies ORDER BY name COLLATE NOCASE;").fetchall()
    conn.close()
    return rows

def list_users_basic(only_creator: Optional[int]) -> List[Tuple[int, str, Optional[int]]]:
    conn = get_conn()
    if only_creator:
        rows = conn.execute(
            "SELECT id, full_name, company_id FROM users WHERE created_by=? ORDER BY full_name COLLATE NOCASE;",
            (only_creator,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, full_name, company_id FROM users ORDER BY full_name COLLATE NOCASE;"
        ).fetchall()
    conn.close()
    return rows

def create_company(name: str, phone: str, address: str, note: str, creator_id: Optional[int]):
    conn = get_conn()
    conn.execute(
        "INSERT INTO companies (name, phone, address, note, created_by) VALUES (?,?,?,?,?);",
        (name.strip(), phone.strip(), address.strip(), note.strip(), creator_id),
    )
    conn.commit()
    conn.close()

def create_user(full_name: str, phone: str, role: str, company_id: Optional[int], note: str,
                status: str, creator_id: Optional[int]):
    conn = get_conn()
    conn.execute(
        "INSERT INTO users (full_name, phone, role, company_id, note, status, created_by) VALUES (?,?,?,?,?,?,?);",
        (full_name.strip(), phone.strip(), role.strip(), company_id, note.strip(), status, creator_id),
    )
    conn.commit()
    conn.close()

def create_call(user_id: int, call_dt: datetime, status: str, description: str, creator_id: Optional[int]):
    conn = get_conn()
    conn.execute(
        "INSERT INTO calls (user_id, call_datetime, status, description, created_by) VALUES (?,?,?,?,?);",
        (user_id, call_dt.isoformat(timespec="minutes"), status, description.strip(), creator_id),
    )
    conn.commit()
    conn.close()

def create_followup(user_id: int, title: str, details: str, due_date_val: date, status: str, creator_id: Optional[int]):
    conn = get_conn()
    conn.execute(
        "INSERT INTO followups (user_id, title, details, due_date, status, created_by) VALUES (?,?,?,?,?,?);",
        (user_id, title.strip(), details.strip(), due_date_val.isoformat(), status, creator_id),
    )
    conn.commit()
    conn.close()

def update_followup_status(task_id: int, new_status: str):
    conn = get_conn()
    conn.execute("UPDATE followups SET status=? WHERE id=?;", (new_status, task_id))
    conn.commit()
    conn.close()

def update_user_status(user_id: int, new_status: str):
    conn = get_conn()
    conn.execute("UPDATE users SET status=? WHERE id=?;", (new_status, user_id))
    conn.commit()
    conn.close()

# ========= DataFrame helpers =========
def df_calls_filtered(name_query: str, statuses: List[str], start: Optional[date], end: Optional[date],
                      only_user_id: Optional[int], only_creator: Optional[int]) -> pd.DataFrame:
    conn = get_conn()
    params, where = [], ["1=1"]
    if name_query:
        where.append("(u.full_name LIKE ? OR c.name LIKE ?)")
        q = f"%{name_query.strip()}%"; params += [q, q]
    if statuses:
        where.append("cl.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if start:
        where.append("date(cl.call_datetime) >= ?"); params.append(start.isoformat())
    if end:
        where.append("date(cl.call_datetime) <= ?"); params.append(end.isoformat())
    if only_user_id:
        where.append("u.id = ?"); params.append(only_user_id)
    if only_creator:
        where.append("cl.created_by = ?"); params.append(only_creator)

    sql = f"""
        SELECT cl.id AS call_id,
               u.id AS user_id,
               u.full_name AS نام_کاربر,
               COALESCE(c.name,'') AS شرکت,
               cl.call_datetime AS تاریخ_و_زمان,
               cl.status AS وضعیت,
               COALESCE(cl.description,'') AS توضیحات
        FROM calls cl
        JOIN users u ON u.id = cl.user_id
        LEFT JOIN companies c ON c.id = u.company_id
        WHERE {' AND '.join(where)}
        ORDER BY cl.call_datetime DESC, cl.id DESC;
    """
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df

def df_followups_filtered(name_query: str, statuses: List[str], start: Optional[date], end: Optional[date],
                          only_user_id: Optional[int], only_creator: Optional[int]) -> pd.DataFrame:
    conn = get_conn()
    params, where = [], ["1=1"]
    if name_query:
        where.append("(u.full_name LIKE ? OR c.name LIKE ?)")
        q = f"%{name_query.strip()}%"; params += [q, q]
    if statuses:
        where.append("f.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if start:
        where.append("date(f.due_date) >= ?"); params.append(start.isoformat())
    if end:
        where.append("date(f.due_date) <= ?"); params.append(end.isoformat())
    if only_user_id:
        where.append("u.id = ?"); params.append(only_user_id)
    if only_creator:
        where.append("f.created_by = ?"); params.append(only_creator)

    sql = f"""
        SELECT f.id AS task_id,
               u.id AS user_id,
               u.full_name AS نام_کاربر,
               COALESCE(c.name,'') AS شرکت,
               f.title AS عنوان,
               COALESCE(f.details,'') AS توضیحات,
               f.due_date AS تاریخ_پیگیری,
               f.status AS وضعیت
        FROM followups f
        JOIN users u ON u.id = f.user_id
        LEFT JOIN companies c ON c.id = u.company_id
        WHERE {' AND '.join(where)}
        ORDER BY f.due_date ASC, f.id DESC;
    """
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df

def df_users_advanced(name_q: str, created_from: Optional[date], created_to: Optional[date],
                      has_open_task: Optional[bool], last_call_from: Optional[date], last_call_to: Optional[date],
                      only_creator: Optional[int]) -> pd.DataFrame:
    """
    فهرست کاربران با فیلترهای:
    - نام (like)
    - بازه تاریخ ایجاد
    - آیا پیگیری باز دارد
    - بازه تاریخ آخرین تماس
    """
    conn = get_conn()
    params, where = [], []
    if name_q:
        where.append("u.full_name LIKE ?"); params.append(f"%{name_q.strip()}%")
    if created_from:
        where.append("date(u.created_at) >= ?"); params.append(created_from.isoformat())
    if created_to:
        where.append("date(u.created_at) <= ?"); params.append(created_to.isoformat())
    if only_creator:
        where.append("u.created_by = ?"); params.append(only_creator)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    base = f"""
      SELECT
        u.id AS ID,
        u.full_name AS نام,
        COALESCE(c.name,'') AS شرکت,
        COALESCE(u.phone,'') AS تلفن,
        COALESCE(u.role,'') AS سمت,
        COALESCE(u.status,'') AS وضعیت_کاربر,
        COALESCE(u.note,'') AS یادداشت,
        u.created_at AS تاریخ_ایجاد,
        (SELECT MAX(call_datetime) FROM calls cl WHERE cl.user_id=u.id) AS آخرین_تماس,
        EXISTS(SELECT 1 FROM followups f WHERE f.user_id=u.id AND f.status='در حال انجام') AS پیگیری_باز_دارد
      FROM users u
      LEFT JOIN companies c ON c.id=u.company_id
      {where_sql}
    """
    df = pd.read_sql_query(base, conn, params=params)

    # فیلتر «آیا پیگیری باز دارد»
    if has_open_task is not None:
        df = df[df["پیگیری_باز_دارد"] == (1 if has_open_task else 0)]

    # فیلتر بازه‌ی آخرین تماس
    if last_call_from:
        df = df[(df["آخرین_تماس"].notna()) & (pd.to_datetime(df["آخرین_تماس"]).dt.date >= last_call_from)]
    if last_call_to:
        df = df[(df["آخرین_تماس"].notna()) & (pd.to_datetime(df["آخرین_تماس"]).dt.date <= last_call_to)]

    conn.close()
    return df

# ========= رندر جدول (راست‌چین + ستون‌ها از راست) =========
def render_df(df: pd.DataFrame):
    if df is None or df.empty:
        st.info("داده‌ای یافت نشد."); return
    df_disp = df.copy()
    df_disp.insert(0, "ردیف", range(1, len(df_disp)+1))
    cols = df_disp.columns.tolist()
    id_cols = [c for c in cols if c in ["ID","call_id","task_id","user_id"]]
    other_cols = [c for c in cols if c not in id_cols and c != "ردیف"]
    ordered = ["ردیف"] + id_cols + other_cols
    df_disp = df_disp[ordered]
    df_disp = df_disp[df_disp.columns[::-1]]  # از راست شروع شود
    st.dataframe(df_disp, use_container_width=True)

# ========= Auth & UI =========
if "auth" not in st.session_state:
    st.session_state.auth = None

def current_user_id() -> Optional[int]:
    a = st.session_state.auth
    return a["id"] if a else None

def login_view():
    st.title("به FardaPack Mini-CRM ورود")
    if not _jalali_supported():
        st.info("برای تاریخ شمسی، در requirements.txt سطر «persiantools» را اضافه کنید و دوباره دیپلوی کنید.")
    with st.form("login_form"):
        u = st.text_input("نام کاربری")
        p = st.text_input("رمز عبور", type="password")
        ok = st.form_submit_button("ورود")
        if ok:
            info = auth_check(u, p)
            if info:
                st.session_state.auth = info
                st.rerun()
            else:
                st.error("نام کاربری یا رمز صحیح نیست.")

def role_label(r: str) -> str:
    return "مدیر" if r=="admin" else "کارشناس فروش"  # 6#

def header_userbox():
    a = st.session_state.auth
    if not a: return
    st.markdown(f"**کاربر:** {a['username']} — **نقش:** {role_label(a['role'])}")
    st.button("خروج", on_click=lambda: st.session_state.update({"auth": None}))

# ---------- داشبورد (7،8) ----------
def page_dashboard():
    st.subheader("داشبورد ساده")
    conn = get_conn()
    # تماس‌های امروز
    calls_today = conn.execute("SELECT COUNT(*) FROM calls WHERE date(call_datetime)=date('now');").fetchone()[0]
    # کاربران اضافه‌شده امروز
    users_today = conn.execute("SELECT COUNT(*) FROM users WHERE date(created_at)=date('now');").fetchone()[0]
    total_companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    conn.close()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("تماس‌های امروز", calls_today)
    c2.metric("کاربرانِ امروز", users_today)
    c3.metric("تعداد شرکت‌ها", total_companies)
    c4.metric("تعداد کاربران", total_users)
    st.info("از منوی کناری می‌توانید شرکت/کاربر بسازید، تماس ثبت کنید و پیگیری‌ها را مدیریت کنید.")

# ---------- شرکت‌ها ----------
def page_companies():
    a = st.session_state.auth
    only_creator = None if a["role"]=="admin" else a["id"]  # 7#
    st.subheader("ثبت و مدیریت شرکت‌ها")
    with st.expander("➕ افزودن شرکت", expanded=False):
        with st.form("company_form", clear_on_submit=True):
            name = st.text_input("نام شرکت *")
            phone = st.text_input("تلفن")
            address = st.text_area("آدرس")
            note = st.text_area("یادداشت")
            if st.form_submit_button("ثبت شرکت"):
                if not name.strip(): st.warning("نام شرکت اجباری است.")
                else:
                    create_company(name, phone, address, note, current_user_id())
                    st.success(f"شرکت «{name}» ثبت شد.")
    rows = list_companies(only_creator)
    if rows:
        df = pd.DataFrame(rows, columns=["ID", "نام شرکت"]).set_index("ID").reset_index()
        render_df(df)
    else:
        st.info("شرکتی ثبت نشده است.")

# ---------- کاربران (با فیلترهای 3#) ----------
def page_users():
    a = st.session_state.auth
    only_creator = None if a["role"]=="admin" else a["id"]
    st.subheader("ثبت و مدیریت کاربران (رابط‌ها)")

    companies = list_companies(only_creator)
    company_options = {"— بدون شرکت —": None}
    for cid, cname in companies: company_options[cname] = cid

    with st.expander("➕ افزودن کاربر (رابط)", expanded=False):
        with st.form("user_form", clear_on_submit=True):
            full_name = st.text_input("نام و نام‌خانوادگی *")
            phone = st.text_input("تلفن")
            role = st.text_input("سمت/نقش")
            company_name = st.selectbox("شرکت", list(company_options.keys()))
            user_status = st.selectbox("وضعیت کاربر", USER_STATUSES, index=0)  # 5#
            note = st.text_area("یادداشت")
            if st.form_submit_button("ثبت کاربر"):
                if not full_name.strip(): st.warning("نام کاربر اجباری است.")
                else:
                    create_user(full_name, phone, role, company_options[company_name], note, user_status, current_user_id())
                    st.success(f"کاربر «{full_name}» ثبت شد.")

    # فیلترها (3#)
    st.markdown("### فیلتر کاربران")
    f1, f2, f3, f4, f5 = st.columns([2,2,2,2,2])
    with f1: name_q = st.text_input("نام")
    with f2: created_from_j = st.text_input("از تاریخ ایجاد (شمسی)")
    with f3: created_to_j   = st.text_input("تا تاریخ ایجاد (شمسی)")
    with f4:
        opt = st.selectbox("پیگیری باز دارد؟", ["— مهم نیست —", "بله", "خیر"], index=0)
        has_open = None if opt=="— مهم نیست —" else (True if opt=="بله" else False)
    with f5: pass
    g1, g2 = st.columns([2,2])
    with g1: last_call_from_j = st.text_input("از تاریخ آخرین تماس (شمسی)")
    with g2: last_call_to_j   = st.text_input("تا تاریخ آخرین تماس (شمسی)")

    created_from = jalali_str_to_date(created_from_j) if created_from_j else None
    created_to   = jalali_str_to_date(created_to_j)   if created_to_j   else None
    last_call_from = jalali_str_to_date(last_call_from_j) if last_call_from_j else None
    last_call_to   = jalali_str_to_date(last_call_to_j)   if last_call_to_j   else None

    df = df_users_advanced(name_q, created_from, created_to, has_open,
                           last_call_from, last_call_to, only_creator)
    render_df(df)

    # پروفایل کاربر (4#)
    st.markdown("### پروفایل کاربر")
    user_map = list_users_basic(only_creator)
    if user_map:
        sel = st.selectbox("انتخاب کاربر", [f"{u[1]} (ID {u[0]})" for u in user_map])
        uid = int(sel.split("ID ")[1].rstrip(")"))
        render_user_profile(uid)
    else:
        st.info("هنوز کاربری ندارید.")

# ---------- تماس‌ها ----------
def page_calls():
    a = st.session_state.auth
    only_creator = None if a["role"]=="admin" else a["id"]
    st.subheader("ثبت تماس‌ها")
    # کاربرانی که این نقش اجازه دارد
    users = list_users_basic(only_creator)
    user_map = {f"{u[1]} (ID {u[0]})": u[0] for u in users}
    if not user_map:
        st.warning("ابتدا یک کاربر (رابط) بسازید."); return

    with st.expander("➕ افزودن تماس", expanded=False):
        with st.form("call_form", clear_on_submit=True):
            user_label = st.selectbox("کاربر *", list(user_map.keys()))
            j_date = st.text_input("تاریخ تماس (شمسی YYYY/MM/DD) *", value=today_jalali_str(), placeholder="مثلاً 1403/07/18")
            t = st.time_input("زمان تماس *", datetime.now().time().replace(second=0, microsecond=0))
            status = st.selectbox("وضعیت تماس *", CALL_STATUSES)
            desc = st.text_area("توضیحات")
            if st.form_submit_button("ثبت تماس"):
                d = jalali_str_to_date(j_date)
                if not d: st.warning("فرمت تاریخ صحیح نیست.")
                else:
                    call_dt = datetime.combine(d, t)
                    create_call(user_map[user_label], call_dt, status, desc, current_user_id())
                    st.success("تماس ثبت شد.")

    st.markdown("### فهرست تماس‌ها + فیلتر")
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1: name_q = st.text_input("جستجو در نام کاربر/نام شرکت")
    with c2: st_statuses = st.multiselect("فیلتر وضعیت", CALL_STATUSES, default=[])
    with c3: start_j = st.text_input("از تاریخ (شمسی)")
    with c4: end_j = st.text_input("تا تاریخ (شمسی)")
    start_date = jalali_str_to_date(start_j) if start_j else None
    end_date   = jalali_str_to_date(end_j) if end_j else None

    df = df_calls_filtered(name_q, st_statuses, start_date, end_date, None, only_creator)
    render_df(df)

# ---------- پیگیری‌ها (2# با ویرایش در جدول) ----------
def page_followups():
    a = st.session_state.auth
    only_creator = None if a["role"]=="admin" else a["id"]
    st.subheader("ثبت پیگیری‌ها")
    users = list_users_basic(only_creator)
    user_map = {f"{u[1]} (ID {u[0]})": u[0] for u in users}
    if not user_map:
        st.warning("ابتدا یک کاربر (رابط) بسازید."); return

    with st.expander("➕ افزودن پیگیری", expanded=False):
        with st.form("fu_form", clear_on_submit=True):
            user_label = st.selectbox("کاربر *", list(user_map.keys()))
            title = st.text_input("عنوان اقدام بعدی *", placeholder="مثلاً: ارسال پیش‌فاکتور")
            details = st.text_area("جزئیات")
            j_due = st.text_input("تاریخ پیگیری (شمسی YYYY/MM/DD) *", value=today_jalali_str())
            if st.form_submit_button("ثبت پیگیری"):
                d = jalali_str_to_date(j_due)
                if not d: st.warning("فرمت تاریخ صحیح نیست.")
                elif is_holiday_gregorian(d):   # 10#
                    st.error("تاریخ انتخابی تعطیل است. لطفاً روز کاری انتخاب کنید.")
                else:
                    create_followup(user_map[user_label], title, details, d, "در حال انجام", current_user_id())
                    st.success("پیگیری ثبت شد.")

    st.markdown("### فهرست پیگیری‌ها + فیلتر")
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1: name_q = st.text_input("جستجو در نام کاربر/نام شرکت", key="fu_q")
    with c2: st_statuses = st.multiselect("فیلتر وضعیت", TASK_STATUSES, default=[], key="fu_st")
    with c3: start_j = st.text_input("از تاریخ (شمسی)", key="fu_sd")
    with c4: end_j   = st.text_input("تا تاریخ (شمسی)", key="fu_ed")
    start_date = jalali_str_to_date(start_j) if start_j else None
    end_date   = jalali_str_to_date(end_j) if end_j else None

    df = df_followups_filtered(name_q, st_statuses, start_date, end_date, None, only_creator)

    if df.empty:
        st.info("داده‌ای یافت نشد."); return

    # ویرایش در جدول: فقط ستون «وضعیت» قابل‌ویرایش باشد
    df_edit = df.copy()
    df_edit = df_edit[df_edit.columns[::-1]]  # ظاهر از راست
    cfg = {
        "وضعیت": st.column_config.SelectboxColumn(
            "وضعیت", help="برای تغییر وضعیت کلیک کنید", options=TASK_STATUSES, width="small"
        )
    }
    edited = st.data_editor(df_edit, use_container_width=True, column_config=cfg, disabled=[
        c for c in df_edit.columns if c != "وضعیت"
    ])

    # تشخیص تغییرات وضعیت و اعمال در DB
    changed = edited[["task_id","وضعیت"]].merge(df_edit[["task_id","وضعیت"]], on="task_id", suffixes=("_new","_old"))
    changed = changed[changed["وضعیت_new"] != changed["وضعیت_old"]]
    if not changed.empty:
        for _, row in changed.iterrows():
            update_followup_status(int(row["task_id"]), str(row["وضعیت_new"]))
        st.success("وضعیت‌های تغییر یافته ذخیره شد. صفحه را رفرش کنید.")

# ---------- پروفایل کاربر (4#) ----------
def render_user_profile(user_id: int):
    conn = get_conn()
    info = conn.execute("""
        SELECT u.id, u.full_name, COALESCE(c.name,''), COALESCE(u.phone,''), COALESCE(u.role,''),
               COALESCE(u.status,''), COALESCE(u.note,''), u.created_at
        FROM users u
        LEFT JOIN companies c ON c.id=u.company_id
        WHERE u.id=?;
    """, (user_id,)).fetchone()
    if not info:
        conn.close(); st.warning("کاربر یافت نشد."); return
    tabs = st.tabs(["👤 اطلاعات", "📞 تماس‌ها", "📝 پیگیری‌ها", "👥 همکاران شرکت"])
    with tabs[0]:
        st.write("**نام:**", info[1])
        st.write("**شرکت:**", info[2])
        st.write("**تلفن:**", info[3])
        st.write("**سمت:**", info[4])
        st.write("**وضعیت کاربر:**", info[5])
        st.write("**یادداشت:**", info[6])
        st.write("**تاریخ ایجاد:**", info[7])

    with tabs[1]:
        df_calls = pd.read_sql_query("""
            SELECT id AS call_id, call_datetime AS تاریخ_و_زمان, status AS وضعیت, COALESCE(description,'') AS توضیحات
            FROM calls WHERE user_id=? ORDER BY call_datetime DESC;
        """, conn, params=(user_id,))
        render_df(df_calls)

    with tabs[2]:
        df_fu = pd.read_sql_query("""
            SELECT id AS task_id, title AS عنوان, COALESCE(details,'') AS توضیحات,
                   due_date AS تاریخ_پیگیری, status AS وضعیت
            FROM followups WHERE user_id=? ORDER BY due_date DESC;
        """, conn, params=(user_id,))
        render_df(df_fu)

    with tabs[3]:
        # همکاران: افرادی که در همان شرکت هستند
        df_peers = pd.read_sql_query("""
            SELECT u2.id AS ID, u2.full_name AS نام, COALESCE(u2.phone,'') AS تلفن, COALESCE(u2.role,'') AS سمت
            FROM users u
            JOIN users u2 ON u2.company_id = u.company_id AND u2.id != u.id
            WHERE u.id=?
            ORDER BY u2.full_name COLLATE NOCASE;
        """, conn, params=(user_id,))
        render_df(df_peers)
    conn.close()

# ---------- اجرای برنامه ----------
init_db()

if not st.session_state.auth:
    login_view()
else:
    with st.sidebar:
        st.header("FardaPack Mini-CRM")
        header_userbox()
        role = st.session_state.auth["role"]
        if role == "admin":
            page = st.radio("منو", ("داشبورد", "شرکت‌ها", "کاربران", "تماس‌ها", "پیگیری‌ها", "مدیریت دسترسی"), index=0)
        else:
            page = st.radio("منو", ("داشبورد", "شرکت‌ها", "کاربران", "تماس‌ها", "پیگیری‌ها"), index=0)

    if page == "داشبورد":         page_dashboard()
    elif page == "شرکت‌ها":       page_companies()
    elif page == "کاربران":       page_users()
    elif page == "تماس‌ها":       page_calls()
    elif page == "پیگیری‌ها":     page_followups()
    elif page == "مدیریت دسترسی": 
        if st.session_state.auth["role"]=="admin":
            # ساخت کاربر ورود
            all_users = list_users_basic(None)
            map_users = {"— بدون لینک —": None}
            for u in all_users: map_users[f"{u[1]} (ID {u[0]})"] = u[0]
            with st.expander("➕ ایجاد کاربر ورود", expanded=False):
                with st.form("new_app_user", clear_on_submit=True):
                    username = st.text_input("نام کاربری *")
                    password = st.text_input("رمز عبور *", type="password")
                    role_sel = st.selectbox("نقش *", ["agent", "admin"], index=0)
                    link_label = st.selectbox("لینک به کدام 'کاربر (رابط)'؟", list(map_users.keys()))
                    if st.form_submit_button("ایجاد کاربر ورود"):
                        if not username or not password: st.warning("نام کاربری و رمز عبور اجباری است.")
                        else:
                            try:
                                create_app_user(username, password, role_sel, map_users[link_label])
                                st.success("کاربر ایجاد شد.")
                            except sqlite3.IntegrityError:
                                st.error("این نام کاربری قبلاً وجود دارد.")
        else:
            st.info("این بخش فقط برای مدیر در دسترس است.")
