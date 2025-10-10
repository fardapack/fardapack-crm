# -*- coding: utf-8 -*-
"""
FardaPack Mini-CRM — Streamlit + SQLite (Streamlit 1.50 friendly)
- فونت Vazirmatn + راست‌چین همه‌ی جداول
- کاربران: ستون‌های اقدام درون جدول (نمایش/ویرایش/تماس/پیگیری) با CheckboxColumn
- شرکت‌ها: وضعیت + سطح + اکشن‌های ردیفی + فیلترها + دیالوگ پروفایل/ویرایش/ثبت تماس/پیگیری
- صفحات تماس‌ها و پیگیری‌ها در منو
"""

import sqlite3
from datetime import datetime, date
from typing import Optional, List, Tuple, Dict

import pandas as pd
import streamlit as st
import hashlib

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
    from persiantools.jdatetime import JalaliDate
except Exception:
    JalaliDate = None

def _jalali_supported() -> bool:
    return JalaliDate is not None

def today_jalali_str() -> str:
    return JalaliDate.today().strftime("%Y/%m/%d") if _jalali_supported() else ""

def jalali_str_to_date(s: str) -> Optional[date]:
    if not s or not _jalali_supported(): return None
    try:
        g = JalaliDate.strptime(s.strip(), "%Y/%m/%d").to_gregorian()
        return date(g.year, g.month, g.day)
    except Exception:
        return None

def date_to_jalali_str(d: date) -> str:
    if not d or not _jalali_supported(): return ""
    try:
        return JalaliDate.fromgregorian(date=d).strftime("%Y/%m/%d")
    except Exception:
        return ""

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
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()

def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r[1] == col for r in rows)

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

    # ایندکس‌ها
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_company ON users(company_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_owner ON users(owner_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calls_user_datetime ON calls(user_id, call_datetime);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_followups_user_due ON followups(user_id, due_date);")

    # کاربر مدیر اولیه
    if cur.execute("SELECT COUNT(*) FROM app_users;").fetchone()[0] == 0:
        cur.execute("INSERT INTO app_users (username, password_sha256, role) VALUES (?,?,?);",
                    ("admin", sha256("admin123"), "admin"))
    conn.commit(); conn.close()

# ====================== CRUD ======================
def list_companies(_: Optional[int]) -> List[Tuple[int, str]]:
    conn = get_conn()
    rows = conn.execute("SELECT id, name FROM companies ORDER BY name COLLATE NOCASE;").fetchall()
    conn.close(); return rows

def list_sales_accounts_including_admins() -> List[Tuple[int, str, str]]:
    conn = get_conn()
    rows = conn.execute("SELECT id, username, role FROM app_users WHERE role IN ('agent','admin') ORDER BY role DESC, username;").fetchall()
    conn.close(); return rows

def list_users_basic(only_owner_appuser: Optional[int]) -> List[Tuple[int, str, Optional[int]]]:
    conn = get_conn()
    if only_owner_appuser:
        rows = conn.execute("SELECT id, full_name, company_id FROM users WHERE owner_id=? ORDER BY full_name COLLATE NOCASE;",
                            (only_owner_appuser,)).fetchall()
    else:
        rows = conn.execute("SELECT id, full_name, company_id FROM users ORDER BY full_name COLLATE NOCASE;").fetchall()
    conn.close(); return rows

def phone_exists(phone: str, ignore_user_id: Optional[int] = None) -> bool:
    conn = get_conn()
    if ignore_user_id:
        row = conn.execute("SELECT 1 FROM users WHERE phone=? AND id<>?;", (phone.strip(), ignore_user_id)).fetchone()
    else:
        row = conn.execute("SELECT 1 FROM users WHERE phone=?;", (phone.strip(),)).fetchone()
    conn.close(); return row is not None

def create_company(name, phone, address, note, level, status, creator_id):
    conn = get_conn()
    conn.execute(
        "INSERT INTO companies (name, phone, address, note, level, status, created_by) VALUES (?,?,?,?,?,?,?);",
        (name.strip(), (phone or "").strip(), (address or "").strip(), (note or "").strip(), level, status, creator_id)
    )
    conn.commit(); conn.close()

def update_company(company_id: int, **fields):
    sets, params = [], []
    for k,v in fields.items(): sets.append(f"{k}=?"); params.append(v)
    if not sets: return True, "بدون تغییر"
    params.append(company_id)
    conn = get_conn()
    conn.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id=?;", params)
    conn.commit(); conn.close(); return True, "ذخیره شد."

def create_user(first_name, last_name, phone, job_role, company_id, note,
                status, domain, province, level, owner_id, creator_id) -> Tuple[bool, str]:
    if phone and phone_exists(phone): return False, "شماره تماس تکراری است."
    full_name = f"{(first_name or '').strip()} {(last_name or '').strip()}".strip()
    conn = get_conn()
    conn.execute("""INSERT INTO users
        (first_name,last_name,full_name,phone,role,company_id,note,status,domain,province,level,owner_id,created_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);""",
        ((first_name or "").strip(), (last_name or "").strip(), full_name, (phone or "").strip(), (job_role or "").strip(),
         company_id, (note or "").strip(), status, (domain or "").strip(), (province or "").strip(), level, owner_id, creator_id))
    conn.commit(); conn.close(); return True, "کاربر ثبت شد."

def update_user(user_id: int, **fields):
    if "phone" in fields and phone_exists(fields["phone"] or "", ignore_user_id=user_id):
        return False, "شماره تماس تکراری است."
    sets, params = [], []
    for k,v in fields.items():
        sets.append(f"{k}=?"); params.append(v)
    if not sets: return True, "بدون تغییر"
    params.append(user_id)
    conn = get_conn()
    conn.execute(f"UPDATE users SET {', '.join(sets)} WHERE id=?;", params)
    conn.commit(); conn.close(); return True, "ذخیره شد."

def update_followup_status(task_id: int, new_status: str):
    conn = get_conn(); conn.execute("UPDATE followups SET status=? WHERE id=?;", (new_status, task_id))
    conn.commit(); conn.close()

def create_call(user_id, call_dt: datetime, status, description, creator_id):
    conn = get_conn()
    conn.execute("INSERT INTO calls (user_id, call_datetime, status, description, created_by) VALUES (?,?,?,?,?);",
                 (user_id, call_dt.isoformat(timespec="minutes"), status, (description or "").strip(), creator_id))
    conn.commit(); conn.close()

def create_followup(user_id, title, details, due_date_val: date, status, creator_id):
    conn = get_conn()
    conn.execute("INSERT INTO followups (user_id, title, details, due_date, status, created_by) VALUES (?,?,?,?,?,?);",
                 (user_id, (title or "").strip(), (details or "").strip(), due_date_val.isoformat(), status, creator_id))
    conn.commit(); conn.close()

# ====================== DataFrames برای صفحات ======================
def df_users_advanced(first_q, last_q, created_from, created_to,
                      has_open_task, last_call_from, last_call_to,
                      statuses, only_owner_appuser):
    conn = get_conn(); params, where = [], []
    if first_q: where.append("u.first_name LIKE ?"); params.append(f"%{first_q.strip()}%")
    if last_q:  where.append("u.last_name  LIKE ?"); params.append(f"%{last_q.strip()}%")
    if created_from: where.append("date(u.created_at) >= ?"); params.append(created_from.isoformat())
    if created_to:   where.append("date(u.created_at) <= ?"); params.append(created_to.isoformat())
    if statuses: where.append("u.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if only_owner_appuser: where.append("u.owner_id=?"); params.append(only_owner_appuser)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    df = pd.read_sql_query(f"""
      SELECT
        u.id AS ID,
        u.first_name AS نام,
        u.last_name AS نام_خانوادگی,
        u.full_name AS نام_کامل,
        COALESCE(c.name,'') AS شرکت,
        COALESCE(u.phone,'') AS تلفن,
        COALESCE(u.status,'') AS وضعیت_کاربر,
        COALESCE(u.level,'') AS سطح_کاربر,
        COALESCE(u.domain,'') AS حوزه_فعالیت,
        COALESCE(u.province,'') AS استان,
        u.created_at AS تاریخ_ایجاد,
        (SELECT MAX(call_datetime) FROM calls cl WHERE cl.user_id=u.id) AS آخرین_تماس,
        EXISTS(SELECT 1 FROM followups f WHERE f.user_id=u.id AND f.status='در حال انجام') AS پیگیری_باز_دارد
      FROM users u
      LEFT JOIN companies c ON c.id=u.company_id
      {where_sql}
      ORDER BY u.created_at DESC, u.id DESC
    """, conn, params=params)

    if has_open_task is not None:
        df = df[df["پیگیری_باز_دارد"] == (1 if has_open_task else 0)]
    if last_call_from:
        df = df[(df["آخرین_تماس"].notna()) & (pd.to_datetime(df["آخرین_تماس"]).dt.date >= last_call_from)]
    if last_call_to:
        df = df[(df["آخرین_تماس"].notna()) & (pd.to_datetime(df["آخرین_تماس"]).dt.date <= last_call_to)]
    conn.close(); return df

def df_calls_by_filters(name_query, statuses, start, end, only_owner_appuser):
    conn = get_conn(); params, where = [], ["1=1"]
    if name_query:
        where.append("(u.full_name LIKE ? OR c.name LIKE ?)"); q=f"%{name_query.strip()}%"; params += [q,q]
    if statuses: where.append("cl.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if start: where.append("date(cl.call_datetime) >= ?"); params.append(start.isoformat())
    if end:   where.append("date(cl.call_datetime) <= ?"); params.append(end.isoformat())
    if only_owner_appuser: where.append("u.owner_id=?"); params.append(only_owner_appuser)
    df = pd.read_sql_query(f"""
        SELECT cl.id AS ID, u.full_name AS نام_کاربر, COALESCE(c.name,'') AS شرکت,
               cl.call_datetime AS تاریخ_و_زمان, cl.status AS وضعیت, COALESCE(cl.description,'') AS توضیحات
        FROM calls cl
        JOIN users u ON u.id=cl.user_id
        LEFT JOIN companies c ON c.id=u.company_id
        WHERE {' AND '.join(where)}
        ORDER BY cl.call_datetime DESC, cl.id DESC
    """, conn, params=params)
    conn.close(); return df

def df_followups_by_filters(name_query, statuses, start, end, only_owner_appuser):
    conn = get_conn(); params, where = [], ["1=1"]
    if name_query:
        where.append("(u.full_name LIKE ? OR c.name LIKE ?)"); q=f"%{name_query.strip()}%"; params += [q,q]
    if statuses: where.append("f.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if start: where.append("date(f.due_date) >= ?"); params.append(start.isoformat())
    if end:   where.append("date(f.due_date) <= ?"); params.append(end.isoformat())
    if only_owner_appuser: where.append("u.owner_id=?"); params.append(only_owner_appuser)
    df = pd.read_sql_query(f"""
        SELECT f.id AS ID, u.full_name AS نام_کاربر, COALESCE(c.name,'') AS شرکت,
               f.title AS عنوان, COALESCE(f.details,'') AS جزئیات, f.due_date AS تاریخ_پیگیری, f.status AS وضعیت
        FROM followups f
        JOIN users u ON u.id=f.user_id
        LEFT JOIN companies c ON c.id=u.company_id
        WHERE {' AND '.join(where)}
        ORDER BY f.due_date DESC, f.id DESC
    """, conn, params=params)
    conn.close(); return df

def df_companies_advanced(name_q, statuses, levels, created_from, created_to, has_open):
    conn = get_conn(); params, where = [], []
    if name_q: where.append("c.name LIKE ?"); params.append(f"%{name_q.strip()}%")
    if statuses: where.append("c.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if levels:   where.append("c.level IN (" + ",".join(["?"]*len(levels)) + ")");   params += levels
    if created_from: where.append("date(c.created_at) >= ?"); params.append(created_from.isoformat())
    if created_to:   where.append("date(c.created_at) <= ?"); params.append(created_to.isoformat())
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    df = pd.read_sql_query(f"""
        SELECT
          c.id AS ID,
          c.name AS نام_شرکت,
          COALESCE(c.phone,'') AS تلفن,
          COALESCE(c.status,'') AS وضعیت_شرکت,
          COALESCE(c.level,'') AS سطح_شرکت,
          c.created_at AS تاریخ_ایجاد,
          EXISTS(
            SELECT 1 FROM users u JOIN followups f ON f.user_id=u.id
            WHERE u.company_id=c.id AND f.status='در حال انجام'
          ) AS پیگیری_باز_دارد
        FROM companies c
        {where_sql}
        ORDER BY c.name COLLATE NOCASE;
    """, conn, params=params)
    if has_open is not None:
        df = df[df["پیگیری_باز_دارد"] == (1 if has_open else 0)]
    conn.close(); return df

# ====================== احراز هویت ======================
if "auth" not in st.session_state: st.session_state.auth = None

def current_user_id() -> Optional[int]:
    a = st.session_state.auth
    return a["id"] if a else None

def is_admin() -> bool:
    return bool(st.session_state.auth and st.session_state.auth["role"] == "admin")

def auth_check(username: str, password: str):
    conn = get_conn()
    row = conn.execute("SELECT id, username, password_sha256, role, linked_user_id FROM app_users WHERE username=?;",
                       (username.strip(),)).fetchone()
    conn.close()
    if not row: return None
    uid, uname, pwh, role, linked_user_id = row
    return {"id": uid, "username": uname, "role": role, "linked_user_id": linked_user_id} if sha256(password)==pwh else None

def login_view():
    st.title("ورود به سیستم")
    with st.form("login"):
        u = st.text_input("نام کاربری")
        p = st.text_input("رمز عبور", type="password")
        if st.form_submit_button("ورود"):
            info = auth_check(u, p)
            if info:
                st.session_state.auth = info
                st.rerun()
            else:
                st.error("نام کاربری یا رمز صحیح نیست.")

def role_label(r: str) -> str: return "مدیر" if r=="admin" else "کارشناس فروش"

def header_userbox():
    a = st.session_state.auth
    if not a: return
    st.markdown(f"**کاربر:** {a['username']} — **نقش:** {role_label(a['role'])}")
    st.button("خروج", on_click=lambda: st.session_state.update({"auth": None}))

# ====================== دیالوگ‌ها: کاربران ======================
@st.dialog("پروفایل کاربر")
def dlg_profile(user_id: int):
    conn = get_conn()
    u = conn.execute("""
      SELECT u.id, u.first_name, u.last_name, COALESCE(u.full_name,''), COALESCE(c.name,''), COALESCE(u.phone,''),
             COALESCE(u.role,''), COALESCE(u.status,''), COALESCE(u.level,''), COALESCE(u.domain,''), COALESCE(u.province,''),
             COALESCE(u.note,''), u.created_at, u.company_id
      FROM users u LEFT JOIN companies c ON c.id=u.company_id WHERE u.id=?;
    """, (user_id,)).fetchone()
    conn.close()
    if not u:
        st.warning("کاربر یافت نشد."); return

    info_tab, calls_tab, fu_tab, colleagues_tab = st.tabs(["اطلاعات کاربر","تماس‌ها","پیگیری‌ها","هم‌شرکتی‌ها"])
    with info_tab:
        st.markdown(f"**نام:** {u[1] or ''}")
        st.markdown(f"**نام خانوادگی:** {u[2] or ''}")
        st.markdown(f"**نام کامل:** {u[3] or ''}")
        st.markdown(f"**شرکت:** {u[4] or ''}")
        st.markdown(f"**تلفن:** {u[5] or ''}")
        st.markdown(f"**سمت:** {u[6] or ''}")
        st.markdown(f"**وضعیت:** {u[7] or ''}")
        st.markdown(f"**سطح:** {u[8] or ''}")
        st.markdown(f"**حوزه فعالیت:** {u[9] or ''}")
        st.markdown(f"**استان:** {u[10] or ''}")
        st.markdown(f"**یادداشت:** {u[11] or ''}")
        st.markdown(f"**تاریخ ایجاد:** {u[12] or ''}")

    with calls_tab
