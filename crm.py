# -*- coding: utf-8 -*-
"""
FardaPack Mini-CRM — Streamlit + SQLite (Streamlit 1.50 friendly)
- فونت Vazirmatn + RTL
- نشست پایدار با توکن (عدم خروج بعد از رفرش)
- تاریخ/ساعت شمسی در همه جدول‌ها
- ستون «کارشناس فروش» در همه جدول‌ها + فیلتر سراسری
- دیالوگ‌های پروفایل/ویرایش/ثبت تماس/پیگیری
- صفحات: داشبورد، شرکت‌ها، کاربران، تماس‌ها، پیگیری‌ها، مدیریت دسترسی (برای مدیر)
- 📥 ایمپورت اکسل مخاطبین در صفحه کاربران
- ✅ عملیات گروهی در صفحه کاربران (تغییر کارشناس فروشِ چندتایی)
"""

import sqlite3
from datetime import datetime, date, timedelta
from typing import Optional, List, Tuple, Dict

import pandas as pd
import streamlit as st
import hashlib
import uuid

# 👇 اضافه شد
import os, io, zipfile, time, shutil  # ← time و shutil افزوده شد

# ====================== صفحه و CSS ======================
st.set_page_config(page_title="FardaPack Mini-CRM", page_icon="📇", layout="wide")
st.markdown(
    """
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
      html, body, [data-testid="stAppViewContainer"]{
        direction: rtl; text-align: right !important;
        font-family: "Vazirmatn", sans-serif !important;
      }
      [data-testid="stSidebar"] * { font-family: "Vazirmatn", sans-serif !important; }
      /* جداول RTL + هدر بولد */
      [data-testid="stDataFrame"], [data-testid="stDataEditor"]{ direction: rtl !important; }
      [data-testid="stDataFrame"] div[role="columnheader"],
      [data-testid="stDataEditor"] div[role="columnheader"]{
        text-align: right !important; justify-content: flex-end !important; font-weight: 700 !important;
      }
      [data-testid="stDataFrame"] div[role="gridcell"],
      [data-testid="stDataEditor"] div[role="gridcell"]{
        text-align: right !important; justify-content: flex-end !important;
      }
    </style>
    """,
    unsafe_allow_html=True
)

# ====================== تاریخ شمسی ======================
try:
    from persiantools.jdatetime import JalaliDate, JalaliDateTime
except Exception:
    JalaliDate = None
    JalaliDateTime = None

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

def dt_to_jalali_str(dt_iso_or_none: Optional[str]) -> str:
    """yyyy-mm-dd[ hh:mm[:ss]] → 'YYYY/MM/DD HH:MM' شمسی"""
    if not dt_iso_or_none or not _jalali_supported():
        return dt_iso_or_none or ""
    try:
        if "T" in dt_iso_or_none:
            gdt = datetime.fromisoformat(dt_iso_or_none)
        else:
            try:
                gdt = datetime.strptime(dt_iso_or_none, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    gdt = datetime.strptime(dt_iso_or_none, "%Y-%m-%d %H:%M")
                except ValueError:
                    gdt = datetime.strptime(dt_iso_or_none, "%Y-%m-%d")
        jdt = JalaliDateTime.fromgregorian(datetime=gdt)
        return jdt.strftime("%Y/%m/%d %H:%M")
    except Exception:
        return dt_iso_or_none

# ====================== ثوابت و DB ======================
DB_PATH = "crm.db"
CALL_STATUSES = ["ناموفق", "موفق", "خاموش", "رد تماس"]
TASK_STATUSES = ["در حال انجام", "پایان یافته"]
USER_STATUSES = ["بدون وضعیت", "در حال پیگیری", "پیش فاکتور", "مشتری شد"]
COMPANY_STATUSES = ["بدون وضعیت", "در حال پیگیری", "پیش فاکتور", "مشتری شد"]
LEVELS = ["هیچکدام", "طلایی", "نقره‌ای", "برنز"]

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def sha256(txt: str) -> str:
    return hashlib.sha256((txt or "").encode("utf-8")).hexdigest()

def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r[1] == col for r in rows)

# ====================== ♻️ بازیابی دیتابیس از بکاپ (قبل از هر اتصال) ======================
def _integrity_ok(db_path: str) -> bool:
    try:
        con = sqlite3.connect(db_path)
        ok = con.execute("PRAGMA integrity_check;").fetchone()[0] == "ok"
        con.close()
        return ok
    except Exception:
        return False

with st.expander("♻️ بازیابی دیتابیس از بکاپ (.db یا .zip)"):
    up = st.file_uploader("فایل بکاپ را انتخاب کنید", type=["db","sqlite","zip"])
    col1, col2 = st.columns(2)
    with col1:
        st.caption("• فرمت‌های مجاز: .db / .sqlite یا فایل ZIP شامل فایل DB")
    with col2:
        restore_btn = st.button("بازیابی همین الان", use_container_width=True, type="primary")

    if restore_btn and up is not None:
        # اگر قبلاً اتصال باز شده، ببندیم
        if "conn" in st.session_state and st.session_state.conn:
            try:
                st.session_state.conn.close()
            except Exception:
                pass

        ts = time.strftime("%Y%m%d-%H%M%S")
        tmp_dir = f".restore_tmp_{ts}"
        os.makedirs(tmp_dir, exist_ok=True)

        candidate_db = None
        try:
            if up.name.lower().endswith(".zip"):
                zf = zipfile.ZipFile(io.BytesIO(up.read()))
                zf.extractall(tmp_dir)
                for root, _, files in os.walk(tmp_dir):
                    for fn in files:
                        if fn.lower().endswith((".db",".sqlite")):
                            candidate_db = os.path.join(root, fn)
                            break
                    if candidate_db:
                        break
            else:
                candidate_db = os.path.join(tmp_dir, "restore.db")
                with open(candidate_db, "wb") as f:
                    f.write(up.read())
        except Exception as e:
            st.error(f"خطا در خواندن فایل: {e}")

        if candidate_db and os.path.exists(candidate_db):
            if _integrity_ok(candidate_db):
                if os.path.exists(DB_PATH):
                    shutil.copyfile(DB_PATH, f"{DB_PATH}.bak.{ts}")
                shutil.copyfile(candidate_db, DB_PATH)
                st.success("✅ دیتابیس با موفقیت بازیابی شد. برنامه دوباره بارگذاری می‌شود…")
                st.toast("DB restored")
                st.rerun()
            else:
                st.error("فایل بکاپ معتبر نیست (PRAGMA integrity_check = NOT ok).")
        else:
            st.error("فایل DB داخل ZIP پیدا نشد یا قابل خواندن نبود.")

# ====================== init_db و بقیه ======================
def init_db():
    conn = get_conn(); cur = conn.cursor()

    # ---- companies ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            address TEXT,
            note TEXT,
            level TEXT NOT NULL DEFAULT 'هیچکدام',
            status TEXT NOT NULL DEFAULT 'بدون وضعیت',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER
        );
    """)
    if not _column_exists(conn, "companies", "status"):
        cur.execute("ALTER TABLE companies ADD COLUMN status TEXT NOT NULL DEFAULT 'بدون وضعیت';")
    if not _column_exists(conn, "companies", "level"):
        cur.execute("ALTER TABLE companies ADD COLUMN level TEXT NOT NULL DEFAULT 'هیچکدام';")

    # ---- users ----
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
            owner_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL
        );
    """)
    for col, default in [
        ("first_name", None), ("last_name", None), ("domain", None), ("province", None),
        ("level", "'هیچکدام'"), ("owner_id", None)
    ]:
        if not _column_exists(conn, "users", col):
            cur.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT" + (f" DEFAULT {default}" if default else "") + ";")

    # ---- calls ----
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

    # ---- followups ----
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

    # ---- app_users ----
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

    # ---- sessions ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            app_user_id INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT,
            FOREIGN KEY(app_user_id) REFERENCES app_users(id) ON DELETE CASCADE
        );
    """)

    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_company ON users(company_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_owner ON users(owner_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calls_user_datetime ON calls(user_id, call_datetime);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_followups_user_due ON followups(user_id, due_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(app_user_id);")

    # Seed admin
    if cur.execute("SELECT COUNT(*) FROM app_users;").fetchone()[0] == 0:
        cur.execute("INSERT INTO app_users (username, password_sha256, role) VALUES (?,?,?);",
                    ("admin", sha256("admin123"), "admin"))
    conn.commit(); conn.close()

# ====================== ابزار نشست پایدار ======================
def create_session(app_user_id: int, days_valid: int = 30) -> str:
    token = uuid.uuid4().hex
    expires = (datetime.utcnow() + timedelta(days=days_valid)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    conn.execute("INSERT INTO sessions (token, app_user_id, expires_at) VALUES (?,?,?);",
                 (token, app_user_id, expires))
    conn.commit(); conn.close()
    return token

def get_session_user(token: str):
    if not token:
        return None
    conn = get_conn()
    row = conn.execute("""
        SELECT au.id, au.username, au.role, au.linked_user_id
        FROM sessions s
        JOIN app_users au ON au.id = s.app_user_id
        WHERE s.token=? AND (s.expires_at IS NULL OR s.expires_at >= datetime('now'));
    """, (token,)).fetchone()
    conn.close()
    if not row:
        return None
    uid, uname, role, linked_user_id = row
    return {"id": uid, "username": uname, "role": role, "linked_user_id": linked_user_id}

def delete_session(token: str):
    if not token: return
    conn = get_conn()
    conn.execute("DELETE FROM sessions WHERE token=?;", (token,))
    conn.commit(); conn.close()

def set_url_token(token: str):
    try:
        qp = st.query_params
        qp["t"] = token
        st.query_params = qp
    except Exception:
        try:
            cur = st.experimental_get_query_params()
            cur["t"] = token
            st.experimental_set_query_params(**cur)
        except Exception:
            pass

def get_url_token() -> Optional[str]:
    try:
        qp = st.query_params
        return qp.get("t", [None])[0] if isinstance(qp.get("t", None), list) else qp.get("t", None)
    except Exception:
        try:
            cur = st.experimental_get_query_params()
            val = cur.get("t", [None])
            return val[0] if isinstance(val, list) else val
        except Exception:
            return None

def clear_url_token():
    try:
        qp = dict(st.query_params)
        if "t" in qp:
            del qp["t"]
        st.query_params = qp
    except Exception:
        try:
            cur = st.experimental_get_query_params()
            if "t" in cur:
                del cur["t"]
            st.experimental_set_query_params(**cur)
        except Exception:
            pass

# ====================== Auto-login از روی توکن URL ======================
def try_autologin_from_url_token():
    """
    اگر ?t=<token> در URL باشد و نشست فعال نباشد، کاربر را به‌طور خودکار لاگین می‌کند.
    - توکن از جدول sessions اعتبارسنجی می‌شود.
    - در صورت معتبر بودن، کلیدهای 'auth' و 'sess_token' در session_state ست می‌شوند.
    """
    # اگر از قبل لاگین هستیم، هیچ کاری نکن
    if st.session_state.get("auth"):
        return

    token = get_url_token()
    if not token:
        return

    info = get_session_user(token)
    if not info:
        return

    # ست‌کردن نشست
    st.session_state.auth = info
    st.session_state.sess_token = token

# ====================== CRUD و بقیه کدهای شما ======================
# (تمام همان توابع و صفحات که در کد فرستاده بودی؛ بدون تغییر)
# --- از اینجا به بعد همان کدی است که خودت فرستادی (df_* ، dialogها، صفحات و ... ) ---

# ... [تمام بخش‌های df_users_advanced, df_calls_by_filters, df_followups_by_filters,
# df_companies_advanced, dialog ها، صفحات dashboard/companies/users/calls/followups/access
# دقیقاً مثل پیام تو باقی می‌ماند؛ برای جلوگیری از طول پاسخ اینجا کوتاه شده، اما
# در فایل نهایی‌ات همان محتوا را نگه‌دار] ...

# ====================== اجرای برنامه ======================
# تلاش برای لاگین خودکار از URL
init_db()
try_autologin_from_url_token()

if "auth" not in st.session_state:
    st.session_state.auth = None
if "sess_token" not in st.session_state:
    st.session_state.sess_token = None

def current_user_id() -> Optional[int]:
    a = st.session_state.auth
    return a["id"] if a else None

def is_admin() -> bool:
    return bool(st.session_state.auth and st.session_state.auth["role"] == "admin")

def login_view():
    st.title("ورود به سیستم")
    with st.form("login"):
        u = st.text_input("نام کاربری")
        p = st.text_input("رمز عبور", type="password")
        remember = st.checkbox("مرا به خاطر بسپار", value=True)
        if st.form_submit_button("ورود"):
            info = auth_check(u, p)
            if info:
                st.session_state.auth = info
                if remember:
                    token = create_session(info["id"], days_valid=30)
                    st.session_state.sess_token = token
                    set_url_token(token)
                st.rerun()
            else:
                st.error("نام کاربری یا رمز صحیح نیست.")

def header_userbox():
    a = st.session_state.auth
    if not a:
        return
    st.markdown(f"**کاربر:** {a['username']} — **نقش:** {'مدیر' if a['role']=='admin' else 'کارشناس فروش'}")
    st.button("خروج", on_click=lambda: (delete_session(st.session_state.get("sess_token")), st.session_state.update({"auth": None, "sess_token": None}), clear_url_token(), st.rerun()))

if not st.session_state.auth:
    login_view()
else:
    with st.sidebar:
        st.markdown("**فردا پک**")
        header_userbox()
        role = st.session_state.auth["role"]
        page = st.radio(
            "منو",
            ("داشبورد","شرکت‌ها","کاربران","تماس‌ها","پیگیری‌ها") + (("مدیریت دسترسی",) if role == "admin" else tuple()),
            index=0
        )

    if page == "داشبورد":
        page_dashboard()
    elif page == "شرکت‌ها":
        page_companies()
    elif page == "کاربران":
        page_users()
    elif page == "تماس‌ها":
        page_calls()
    elif page == "پیگیری‌ها":
        page_followups()
    elif page == "مدیریت دسترسی":
        page_access()
