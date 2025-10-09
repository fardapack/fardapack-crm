# -*- coding: utf-8 -*-
"""
FardaPack Mini-CRM — Streamlit + SQLite
نسخه‌ی بدون پاپ‌آپ تاریخ (ورود دستی شمسی) + اصلاحات درخواستی:
- فونت Vazirmatn و سرتیتر‌های بولد
- باریک شدن ستون‌های ردیف/ID
- جداسازی نام و نام‌خانوادگی + فیلتر جداگانه
- فیلتر وضعیت کاربر
- کلیک روی نام کاربر برای باز شدن پروفایل (بالای صفحه)
- جلوگیری از شماره تلفن تکراری
- تعیین «کارشناس فروش» برای هر کاربر + امکان تغییر
- محدودیت دید: کارشناس فقط کاربرانِ خودش را می‌بیند؛ مدیر همه را
- ثبت «حوزه فعالیت»، «استان»، «سطح کاربر» (طلایی/نقره‌ای/برنز/هیچکدام) + ویرایش
- تعیین سطح برای شرکت‌ها
- داشبورد: تماس‌های 7 روز گذشته، تماس موفقِ امروز، پیگیری‌های عقب‌افتاده‌ی باز
"""

import sqlite3
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Dict

import pandas as pd
import streamlit as st
import hashlib

# ========= تنظیمات صفحه + استایل =========
st.set_page_config(page_title="FardaPack Mini-CRM", page_icon="📇", layout="wide")

st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
    /* فونت روی همه عناصر اعمال شود */
    html, body, * {
        font-family: "Vazirmatn", sans-serif !important;
    }
    /* راست‌چین عمومی */
    [data-testid="stAppViewContainer"] {
        direction: rtl;
        text-align: right !important;
    }
    /* بولد کردن سرتیترهای جدول‌ها */
    [data-testid="stDataFrame"] div[role="columnheader"],
    [data-testid="stTable"] th {
        font-weight: 700 !important;
    }
    /* تراز راست سلول‌ها */
    [data-testid="stDataFrame"] div[role="gridcell"] {
        text-align: right !important;
        justify-content: flex-end !important;
    }
    /* لیبل‌های ورودی کمی پررنگ‌تر */
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
    try:
        return JalaliDate.fromgregorian(date=d).strftime("%Y/%m/%d")
    except Exception:
        return ""

# ========= تعطیلات =========
HOLIDAYS_JALALI = set()  # مثلا: {"1403/01/01", "1403/01/12"}

def is_holiday_gregorian(d: date) -> bool:
    try:
        if d.weekday() == 4:  # جمعه
            return True
    except Exception:
        pass
    try:
        if _jalali_supported():
            js = date_to_jalali_str(d)
            if js and js in HOLIDAYS_JALALI:
                return True
    except Exception:
        pass
    return False

# ========= ثابت‌ها =========
DB_PATH = "crm.db"
CALL_STATUSES = ["ناموفق", "موفق", "خاموش", "رد تماس"]
TASK_STATUSES = ["در حال انجام", "پایان یافته"]
USER_STATUSES = ["بدون وضعیت", "در حال پیگیری", "پیش فاکتور", "مشتری شد"]
LEVELS = ["هیچکدام", "طلایی", "نقره‌ای", "برنز"]

# ========= DB =========
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
            level TEXT NOT NULL DEFAULT 'هیچکدام',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER
        );
    """)
    # کاربران (رابط‌ها)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT NOT NULL,
            phone TEXT UNIQUE,
            role TEXT,
            company_id INTEGER,
            note TEXT,
            status TEXT NOT NULL DEFAULT 'بدون وضعیت',
            domain TEXT,
            province TEXT,
            level TEXT NOT NULL DEFAULT 'هیچکدام',
            owner_id INTEGER,                -- کارشناس فروش مسئول (app_users.id)
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

    # مایگریشن ستون‌ها
    if not _column_exists(conn, "companies", "level"):
        cur.execute("ALTER TABLE companies ADD COLUMN level TEXT NOT NULL DEFAULT 'هیچکدام';")
    for t in ("companies", "users", "calls", "followups"):
        if not _column_exists(conn, t, "created_by"):
            cur.execute(f"ALTER TABLE {t} ADD COLUMN created_by INTEGER;")
    # users extra columns
    for col, default in [
        ("first_name", None), ("last_name", None), ("domain", None), ("province", None),
        ("level", "'هیچکدام'"), ("owner_id", None)
    ]:
        if not _column_exists(conn, "users", col):
            cur.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT" + (f" DEFAULT {default}" if default else "") + ";")
    # یکتا بودن phone را با خودِ اپ enforce می‌کنیم.

    # ایندکس‌ها
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_company ON users(company_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_owner ON users(owner_id);")
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

# ========= CRUD/Queries =========
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

def phone_exists(phone: str, ignore_user_id: Optional[int] = None) -> bool:
    """برای جلوگیری از شماره تکراری"""
    conn = get_conn()
    if ignore_user_id:
        row = conn.execute("SELECT id FROM users WHERE phone=? AND id<>?;", (phone.strip(), ignore_user_id)).fetchone()
    else:
        row = conn.execute("SELECT id FROM users WHERE phone=?;", (phone.strip(),)).fetchone()
    conn.close()
    return row is not None

def list_companies(only_owner_appuser: Optional[int]) -> List[Tuple[int, str]]:
    # شرکت‌ها مالک ندارد؛ محدودیت نمی‌گذاریم
    conn = get_conn()
    rows = conn.execute("SELECT id, name FROM companies ORDER BY name COLLATE NOCASE;").fetchall()
    conn.close()
    return rows

def list_sales_agents() -> List[Tuple[int, str]]:
    """لیست کاربران ورود با نقش agent برای نسبت دادن مالکیتِ کاربر"""
    conn = get_conn()
    rows = conn.execute("SELECT id, username FROM app_users WHERE role='agent' ORDER BY username;").fetchall()
    conn.close()
    return rows

def list_users_basic(only_owner_appuser: Optional[int]) -> List[Tuple[int, str, Optional[int]]]:
    conn = get_conn()
    if only_owner_appuser:
        rows = conn.execute(
            "SELECT id, full_name, company_id FROM users WHERE owner_id=? ORDER BY full_name COLLATE NOCASE;",
            (only_owner_appuser,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, full_name, company_id FROM users ORDER BY full_name COLLATE NOCASE;"
        ).fetchall()
    conn.close()
    return rows

def create_company(name: str, phone: str, address: str, note: str, level: str, creator_id: Optional[int]):
    conn = get_conn()
    conn.execute(
        "INSERT INTO companies (name, phone, address, note, level, created_by) VALUES (?,?,?,?,?,?);",
        (name.strip(), phone.strip(), address.strip(), note.strip(), level, creator_id),
    )
    conn.commit()
    conn.close()

def create_user(first_name: str, last_name: str, phone: str, job_role: str, company_id: Optional[int], note: str,
                status: str, domain: str, province: str, level: str, owner_id: Optional[int],
                creator_id: Optional[int]) -> Tuple[bool, str]:
    if phone and phone_exists(phone):
        return False, "شماره تماس تکراری است."
    full_name = f"{first_name.strip()} {last_name.strip()}".strip()
    conn = get_conn()
    conn.execute(
        """INSERT INTO users
           (first_name,last_name,full_name,phone,role,company_id,note,status,domain,province,level,owner_id,created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);""",
        (first_name.strip(), last_name.strip(), full_name, phone.strip(), job_role.strip(),
         company_id, note.strip(), status, domain.strip(), province.strip(), level, owner_id, creator_id),
    )
    conn.commit()
    conn.close()
    return True, "کاربر ثبت شد."

def update_user_owner(user_id: int, new_owner_id: Optional[int]):
    conn = get_conn()
    conn.execute("UPDATE users SET owner_id=? WHERE id=?;", (new_owner_id, user_id))
    conn.commit()
    conn.close()

def update_user_level(user_id: int, new_level: str):
    conn = get_conn()
    conn.execute("UPDATE users SET level=? WHERE id=?;", (new_level, user_id))
    conn.commit()
    conn.close()

def update_company_level(company_id: int, new_level: str):
    conn = get_conn()
    conn.execute("UPDATE companies SET level=? WHERE id=?;", (new_level, company_id))
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

# ========= DataFrame helpers =========
def df_calls_filtered(name_query: str, statuses: List[str], start: Optional[date], end: Optional[date],
                      only_user_id: Optional[int], only_owner_appuser: Optional[int]) -> pd.DataFrame:
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
    if only_owner_appuser:
        where.append("u.owner_id = ?"); params.append(only_owner_appuser)

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
                          only_user_id: Optional[int], only_owner_appuser: Optional[int]) -> pd.DataFrame:
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
    if only_owner_appuser:
        where.append("u.owner_id = ?"); params.append(only_owner_appuser)

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

def df_users_advanced(first_q: str, last_q: str,
                      created_from: Optional[date], created_to: Optional[date],
                      has_open_task: Optional[bool], last_call_from: Optional[date], last_call_to: Optional[date],
                      statuses: List[str],
                      only_owner_appuser: Optional[int]) -> pd.DataFrame:
    conn = get_conn()
    params, where = [], []
    if first_q:
        where.append("u.first_name LIKE ?"); params.append(f"%{first_q.strip()}%")
    if last_q:
        where.append("u.last_name LIKE ?"); params.append(f"%{last_q.strip()}%")
    if created_from:
        where.append("date(u.created_at) >= ?"); params.append(created_from.isoformat())
    if created_to:
        where.append("date(u.created_at) <= ?"); params.append(created_to.isoformat())
    if statuses:
        where.append("u.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if only_owner_appuser:
        where.append("u.owner_id = ?"); params.append(only_owner_appuser)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    base = f"""
      SELECT
        u.id AS ID,
        u.first_name AS نام,
        u.last_name AS نام_خانوادگی,
        u.full_name AS نام_کامل,
        COALESCE(c.name,'') AS شرکت,
        COALESCE(u.phone,'') AS تلفن,
        COALESCE(u.role,'') AS سمت,
        COALESCE(u.status,'') AS وضعیت_کاربر,
        COALESCE(u.level,'') AS سطح_کاربر,
        COALESCE(u.domain,'') AS حوزه_فعالیت,
        COALESCE(u.province,'') AS استان,
        COALESCE(u.note,'') AS یادداشت,
        u.created_at AS تاریخ_ایجاد,
        (SELECT MAX(call_datetime) FROM calls cl WHERE cl.user_id=u.id) AS آخرین_تماس,
        EXISTS(SELECT 1 FROM followups f WHERE f.user_id=u.id AND f.status='در حال انجام') AS پیگیری_باز_دارد
      FROM users u
      LEFT JOIN companies c ON c.id=u.company_id
      {where_sql}
      ORDER BY u.created_at DESC, u.id DESC
    """
    df = pd.read_sql_query(base, conn, params=params)

    if has_open_task is not None:
        df = df[df["پیگیری_باز_دارد"] == (1 if has_open_task else 0)]
    if last_call_from:
        df = df[(df["آخرین_تماس"].notna()) & (pd.to_datetime(df["آخرین_تماس"]).dt.date >= last_call_from)]
    if last_call_to:
        df = df[(df["آخرین_تماس"].notna()) & (pd.to_datetime(df["آخرین_تماس"]).dt.date <= last_call_to)]

    conn.close()
    return df

# ========= رندر جدول (با باریک‌سازی ستون‌ها) =========
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
    df_disp = df_disp[df_disp.columns[::-1]]
    col_cfg = {
        "ردیف": st.column_config.Column(width="small"),
    }
    for cid in ["ID","call_id","task_id","user_id"]:
        if cid in df_disp.columns:
            col_cfg[cid] = st.column_config.Column(width="small")
    st.dataframe(df_disp, use_container_width=True, column_config=col_cfg)

# ========= Auth & UI =========
if "auth" not in st.session_state:
    st.session_state.auth = None
if "view_user_id" not in st.session_state:
    st.session_state.view_user_id = None  # برای باز کردن پروفایل کاربر در بالا

def current_user_id() -> Optional[int]:
    a = st.session_state.auth
    return a["id"] if a else None

def is_admin() -> bool:
    return st.session_state.auth and st.session_state.auth["role"] == "admin"

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
    return "مدیر" if r=="admin" else "کارشناس فروش"

def header_userbox():
    a = st.session_state.auth
    if not a: return
    st.markdown(f"**کاربر:** {a['username']} — **نقش:** {role_label(a['role'])}")
    st.button("خروج", on_click=lambda: st.session_state.update({"auth": None, "view_user_id": None}))

# ---------- داشبورد ----------
def page_dashboard():
    st.subheader("داشبورد")
    conn = get_conn()
    # تماس‌های امروز
    calls_today = conn.execute("SELECT COUNT(*) FROM calls WHERE date(call_datetime)=date('now');").fetchone()[0]
    # تماس‌های موفق امروز
    calls_success_today = conn.execute(
        "SELECT COUNT(*) FROM calls WHERE date(call_datetime)=date('now') AND status='موفق';"
    ).fetchone()[0]
    # تماس‌های 7 روز گذشته
    last7 = conn.execute(
        "SELECT COUNT(*) FROM calls WHERE date(call_datetime) >= date('now','-7 day');"
    ).fetchone()[0]
    # پیگیری‌های عقب‌افتاده (باز و گذشته از امروز)
    today_iso = date.today().isoformat()
    overdue = conn.execute(
        "SELECT COUNT(*) FROM followups WHERE status='در حال انجام' AND date(due_date) < ?;", (today_iso,)
    ).fetchone()[0]
    # سایر
    total_companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    conn.close()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("تماس‌های امروز", calls_today)
    c2.metric("موفقِ امروز", calls_success_today)
    c3.metric("تماس‌های ۷ روز اخیر", last7)
    c4.metric("پیگیری‌های عقب‌افتاده", overdue)
    c5, c6 = st.columns(2)
    c5.metric("تعداد شرکت‌ها", total_companies)
    c6.metric("تعداد کاربران", total_users)

# ---------- شرکت‌ها ----------
def page_companies():
    st.subheader("ثبت و مدیریت شرکت‌ها")
    with st.expander("➕ افزودن شرکت", expanded=False):
        with st.form("company_form", clear_on_submit=True):
            name = st.text_input("نام شرکت *")
            phone = st.text_input("تلفن")
            address = st.text_area("آدرس")
            note = st.text_area("یادداشت")
            level = st.selectbox("سطح شرکت", LEVELS, index=0)
            if st.form_submit_button("ثبت شرکت"):
                if not name.strip(): st.warning("نام شرکت اجباری است.")
                else:
                    create_company(name, phone, address, note, level, current_user_id())
                    st.success(f"شرکت «{name}» ثبت شد.")

    # لیست شرکت‌ها + امکان تغییر سطح
    conn = get_conn()
    df = pd.read_sql_query("SELECT id AS ID, name AS نام, COALESCE(phone,'') AS تلفن, COALESCE(level,'') AS سطح FROM companies ORDER BY name COLLATE NOCASE;", conn)
    conn.close()
    render_df(df)
    if not df.empty and is_admin():
        st.markdown("**تغییر سطح شرکت**")
        with st.form("update_company_level"):
            company_id = st.selectbox("شرکت", [f"{r['نام']} (ID {int(r['ID'])})" for _, r in df.iterrows()])
            comp_id = int(company_id.split("ID ")[1].rstrip(")"))
            new_level = st.selectbox("سطح", LEVELS, index=0, key="comp_lvl")
            if st.form_submit_button("ذخیره سطح"):
                update_company_level(comp_id, new_level)
                st.success("ذخیره شد.")
                st.rerun()

# ---------- کاربران ----------
def page_users():
    st.subheader("ثبت و مدیریت کاربران (رابط‌ها)")

    only_owner = None if is_admin() else current_user_id()
    companies = list_companies(only_owner)
    company_options = {"— بدون شرکت —": None}
    for cid, cname in companies: company_options[cname] = cid

    agents = list_sales_agents()
    agent_map = {"— بدون کارشناس —": None}
    for aid, aname in agents: agent_map[aname] = aid

    with st.expander("➕ افزودن کاربر (رابط)", expanded=False):
        with st.form("user_form", clear_on_submit=True):
            col1, col2, col3 = st.columns([1,1,1])
            with col1: first_name = st.text_input("نام *")
            with col2: last_name  = st.text_input("نام خانوادگی *")
            with col3: phone = st.text_input("تلفن (یکتا) *")
            role = st.text_input("سمت/نقش")
            company_name = st.selectbox("شرکت", list(company_options.keys()))
            user_status = st.selectbox("وضعیت کاربر", USER_STATUSES, index=0)
            domain = st.text_input("حوزه فعالیت")
            province = st.text_input("استان")
            level = st.selectbox("سطح کاربر", LEVELS, index=0)
            owner_label = st.selectbox("کارشناس فروش", list(agent_map.keys()),
                                       index=0 if not is_admin() else 0)
            if not is_admin():
                # اگر کارشناس لاگین کرده، خودکار مالک شود
                owner_label = list(agent_map.keys())[0] if current_user_id() is None else st.session_state.auth["username"]
                # اگر نام کاربر ورود در لیست نبود، مقدار None می‌ماند.
                if st.session_state.auth["role"] == "agent":
                    # نگاشت نام به id
                    for aid, aname in agents:
                        if aname == st.session_state.auth["username"]:
                            agent_map[aname] = aid

            note = st.text_area("یادداشت")
            if st.form_submit_button("ثبت کاربر"):
                if not first_name.strip() or not last_name.strip() or not phone.strip():
                    st.warning("نام، نام‌خانوادگی و تلفن اجباری هستند.")
                else:
                    sel_owner = agent_map.get(owner_label) if is_admin() else current_user_id()
                    ok, msg = create_user(
                        first_name, last_name, phone, role, company_options[company_name],
                        note, user_status, domain, province, level, sel_owner, current_user_id()
                    )
                    if ok: st.success(msg)
                    else: st.error(msg)

    # ===== فیلتر کاربران =====
    st.markdown("### فیلتر کاربران")
    f1, f2, f3, f4 = st.columns([2,2,2,2])
    with f1: first_q = st.text_input("نام")
    with f2: last_q  = st.text_input("نام خانوادگی")
    with f3:
        created_from_j = st.text_input("از تاریخ ایجاد (شمسی) — مثلا 1403/01/01")
    with f4:
        created_to_j   = st.text_input("تا تاریخ ایجاد (شمسی)")

    g1, g2, g3 = st.columns([2,2,2])
    with g1:
        opt = st.selectbox("پیگیری باز دارد؟", ["— مهم نیست —", "بله", "خیر"], index=0)
        has_open = None if opt=="— مهم نیست —" else (True if opt=="بله" else False)
    with g2:
        last_call_from_j = st.text_input("از تاریخ آخرین تماس (شمسی)")
    with g3:
        last_call_to_j   = st.text_input("تا تاریخ آخرین تماس (شمسی)")

    h1 = st.multiselect("فیلتر وضعیت کاربر", USER_STATUSES, default=[])

    created_from = jalali_str_to_date(created_from_j) if created_from_j else None
    created_to   = jalali_str_to_date(created_to_j)   if created_to_j   else None
    last_call_from = jalali_str_to_date(last_call_from_j) if last_call_from_j else None
    last_call_to   = jalali_str_to_date(last_call_to_j)   if last_call_to_j   else None

    df = df_users_advanced(first_q, last_q, created_from, created_to,
                           has_open, last_call_from, last_call_to, h1,
                           only_owner)
    # نمای «نام» را برای کلیک ساده کنیم
    if not df.empty:
        # بالای صفحه اگر کاربری انتخاب شده، پروفایل را همین‌جا نشان بده
        if st.session_state.view_user_id:
            render_user_profile(st.session_state.view_user_id)
            st.divider()

        # جدول
        render_df(df)

        # لیست دکمه‌های نام کاربر برای باز کردن پروفایل (کلیک روی نام)
        st.markdown("**برای مشاهده پروفایل، روی نام کاربر کلیک کنید:**")
        for _, row in df.iterrows():
            uid = int(row["ID"])
            label = f"{row['نام']} {row['نام_خانوادگی']} (ID {uid})"
            if st.button(label, key=f"userbtn_{uid}"):
                st.session_state.view_user_id = uid
                st.rerun()
    else:
        st.info("کاربری یافت نشد.")

# ---------- تماس‌ها ----------
def page_calls():
    only_owner = None if is_admin() else current_user_id()
    st.subheader("ثبت تماس‌ها")
    users = list_users_basic(only_owner)
    user_map = {f"{u[1]} (ID {u[0]})": u[0] for u in users}
    if not user_map:
        st.warning("ابتدا یک کاربر (رابط) بسازید."); return

    with st.expander("➕ افزودن تماس", expanded=False):
        with st.form("call_form", clear_on_submit=True):
            user_label = st.selectbox("کاربر *", list(user_map.keys()))
            j_date = st.text_input("تاریخ تماس (شمسی YYYY/MM/DD) *", value=today_jalali_str())
            t = st.time_input("زمان تماس *", datetime.now().time().replace(second=0, microsecond=0))
            status = st.selectbox("وضعیت تماس *", CALL_STATUSES)
            desc = st.text_area("توضیحات")
            if st.form_submit_button("ثبت تماس"):
                d = jalali_str_to_date(j_date)
                if not d: st.warning("فرمت تاریخ صحیح نیست. نمونه: 1403/07/18")
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

    df = df_calls_filtered(name_q, st_statuses, start_date, end_date, None, only_owner)
    render_df(df)

# ---------- پیگیری‌ها ----------
def page_followups():
    only_owner = None if is_admin() else current_user_id()
    st.subheader("ثبت پیگیری‌ها")
    users = list_users_basic(only_owner)
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
                if not title.strip():
                    st.warning("عنوان پیگیری اجباری است.")
                else:
                    d = jalali_str_to_date(j_due)
                    if not d:
                        st.warning("فرمت تاریخ صحیح نیست. نمونه: 1403/07/18")
                    elif is_holiday_gregorian(d):
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

    df = df_followups_filtered(name_q, st_statuses, start_date, end_date, None, only_owner)
    if df.empty:
        st.info("داده‌ای یافت نشد."); return

    # ویرایش وضعیت داخل جدول
    df_edit = df.copy()[df.columns[::-1]]
    cfg = {
        "وضعیت": st.column_config.SelectboxColumn("وضعیت", options=TASK_STATUSES, width="small")
    }
    edited = st.data_editor(df_edit, use_container_width=True, column_config=cfg,
                            disabled=[c for c in df_edit.columns if c != "وضعیت"])

    changed = edited[["task_id","وضعیت"]].merge(
        df_edit[["task_id","وضعیت"]], on="task_id", suffixes=("_new","_old")
    )
    changed = changed[changed["وضعیت_new"] != changed["وضعیت_old"]]
    if not changed.empty:
        for _, row in changed.iterrows():
            update_followup_status(int(row["task_id"]), str(row["وضعیت_new"]))
        st.success("وضعیت‌ها ذخیره شد. برای به‌روزرسانی نما، صفحه را تازه کنید.")

# ---------- پروفایل کاربر ----------
def render_user_profile(user_id: int):
    conn = get_conn()
    info = conn.execute("""
        SELECT u.id, u.first_name, u.last_name, u.full_name,
               COALESCE(c.name,''), COALESCE(u.phone,''), COALESCE(u.role,''),
               COALESCE(u.status,''), COALESCE(u.level,''), COALESCE(u.domain,''), COALESCE(u.province,''),
               COALESCE(u.note,''), u.created_at, u.owner_id, u.company_id
        FROM users u
        LEFT JOIN companies c ON c.id=u.company_id
        WHERE u.id=?;
    """, (user_id,)).fetchone()
    if not info:
        conn.close(); st.warning("کاربر یافت نشد."); return

    tabs = st.tabs(["👤 اطلاعات", "📞 تماس‌ها", "📝 پیگیری‌ها", "⚙️ مدیریت"])
    with tabs[0]:
        st.write("**نام:**", info[1])
        st.write("**نام خانوادگی:**", info[2])
        st.write("**نام کامل:**", info[3])
        st.write("**شرکت:**", info[4])
        st.write("**تلفن:**", info[5])
        st.write("**سمت:**", info[6])
        st.write("**وضعیت کاربر:**", info[7])
        st.write("**سطح کاربر:**", info[8])
        st.write("**حوزه فعالیت:**", info[9])
        st.write("**استان:**", info[10])
        st.write("**یادداشت:**", info[11])
        st.write("**تاریخ ایجاد:**", info[12])

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
        # تغییر سطح و تغییر کارشناس فروش (فقط مدیر)
        if is_admin():
            agents = list_sales_agents()
            agent_label = {f"{name} (ID {aid})": aid for aid, name in agents}
            st.markdown("**تغییر کارشناس فروش**")
            with st.form(f"chg_owner_{user_id}"):
                sel = st.selectbox("کارشناس", list(agent_label.keys()))
                if st.form_submit_button("ذخیره کارشناس"):
                    update_user_owner(user_id, agent_label[sel])
                    st.success("ذخیره شد."); st.rerun()
        # تغییر سطح کاربر (مدیر و کارشناس هر دو می‌توانند)
        st.markdown("**تغییر سطح کاربر**")
        with st.form(f"chg_lvl_{user_id}"):
            new_level = st.selectbox("سطح کاربر", LEVELS, index=LEVELS.index(info[8]) if info[8] in LEVELS else 0)
            if st.form_submit_button("ذخیره سطح"):
                update_user_level(user_id, new_level)
                st.success("ذخیره شد."); st.rerun()

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
        if is_admin():
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
