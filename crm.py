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
    # افزودن ستون های جا مانده
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
        (name.strip(), phone.strip(), address.strip(), note.strip(), level, status, creator_id)
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
    full_name = f"{first_name.strip()} {last_name.strip()}".strip()
    conn = get_conn()
    conn.execute("""INSERT INTO users
        (first_name,last_name,full_name,phone,role,company_id,note,status,domain,province,level,owner_id,created_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);""",
        (first_name.strip(), last_name.strip(), full_name, phone.strip(), job_role.strip(),
         company_id, note.strip(), status, domain.strip(), province.strip(), level, owner_id, creator_id))
    conn.commit(); conn.close(); return True, "کاربر ثبت شد."

def update_user(user_id: int, **fields):
    if "phone" in fields and phone_exists(fields["phone"], ignore_user_id=user_id):
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
                 (user_id, call_dt.isoformat(timespec="minutes"), status, description.strip(), creator_id))
    conn.commit(); conn.close()

def create_followup(user_id, title, details, due_date_val: date, status, creator_id):
    conn = get_conn()
    conn.execute("INSERT INTO followups (user_id, title, details, due_date, status, created_by) VALUES (?,?,?,?,?,?);",
                 (user_id, title.strip(), details.strip(), due_date_val.isoformat(), status, creator_id))
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
            if info: st.session_state.auth = info; st.rerun()
            else: st.error("نام کاربری یا رمز صحیح نیست.")

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
    if not u: st.warning("کاربر یافت نشد."); return

    info_tab, calls_tab, fu_tab, colleagues_tab = st.tabs(["اطلاعات کاربر","تماس‌ها","پیگیری‌ها","هم‌شرکتی‌ها"])
    with info_tab:
        st.write("**نام:**", u[1]); st.write("**نام خانوادگی:**", u[2]); st.write("**نام کامل:**", u[3])
        st.write("**شرکت:**", u[4]); st.write("**تلفن:**", u[5]); st.write("**سمت:**", u[6])
        st.write("**وضعیت:**", u[7]); st.write("**سطح:**", u[8])
        st.write("**حوزه فعالیت:**", u[9]); st.write("**استان:**", u[10])
        st.write("**یادداشت:**", u[11]); st.write("**تاریخ ایجاد:**", u[12])

    with calls_tab:
        conn = get_conn()
        dfc = pd.read_sql_query("""
           SELECT id AS ID, call_datetime AS تاریخ_و_زمان, status AS وضعیت, COALESCE(description,'') AS توضیحات
           FROM calls WHERE user_id=? ORDER BY call_datetime DESC, id DESC;
        """, conn, params=(user_id,))
        conn.close()
        st.dataframe(dfc, use_container_width=True)

    with fu_tab:
        conn = get_conn()
        dff = pd.read_sql_query("""
           SELECT id AS ID, title AS عنوان, COALESCE(details,'') AS جزئیات, due_date AS تاریخ_پیگیری, status AS وضعیت
           FROM followups WHERE user_id=? ORDER BY due_date DESC, id DESC;
        """, conn, params=(user_id,))
        conn.close()
        st.dataframe(dff, use_container_width=True)

    with colleagues_tab:
        company_id = u[13]
        if not company_id: st.info("شرکت ثبت نشده است."); return
        conn = get_conn()
        dcol = pd.read_sql_query("""
            SELECT id AS ID, full_name AS نام_کامل, COALESCE(phone,'') AS تلفن, COALESCE(role,'') AS سمت
            FROM users WHERE company_id=? ORDER BY full_name;
        """, conn, params=(company_id,))
        conn.close()
        st.dataframe(dcol, use_container_width=True)

@st.dialog("ویرایش پروفایل")
def dlg_edit_user(user_id: int):
    conn = get_conn()
    row = conn.execute("""
        SELECT first_name,last_name,phone,role,company_id,note,status,domain,province,level,owner_id
        FROM users WHERE id=?;""", (user_id,)).fetchone()
    companies = list_companies(None)
    comp_map = {"— بدون شرکت —": None}; [comp_map.setdefault(n, i) for i,n in companies]  # type: ignore
    owners = list_sales_accounts_including_admins()
    owner_map = {"— بدون کارشناس —": None}; [owner_map.setdefault(f"{u} ({r})", i) for i,u,r in owners]  # type: ignore
    if not row: st.warning("کاربر یافت نشد."); return
    fn,ln,ph,rl,comp_id,note,stt,dom,prov,lvl,own = row
    with st.form(f"edit_user_{user_id}", clear_on_submit=False):
        c1,c2,c3 = st.columns(3)
        with c1: first_name = st.text_input("نام *", value=fn or "")
        with c2: last_name  = st.text_input("نام خانوادگی *", value=ln or "")
        with c3: phone      = st.text_input("تلفن *", value=ph or "")
        role = st.text_input("سمت", value=rl or "")
        comp_label = next((k for k,v in comp_map.items() if v == comp_id), "— بدون شرکت —")
        company_label = st.selectbox("شرکت", list(comp_map.keys()), index=list(comp_map.keys()).index(comp_label))
        note_v = st.text_area("یادداشت", value=note or "")
        s1,s2,s3 = st.columns(3)
        with s1: status_v = st.selectbox("وضعیت", USER_STATUSES, index=USER_STATUSES.index(stt) if stt in USER_STATUSES else 0)
        with s2: level_v  = st.selectbox("سطح", LEVELS, index=LEVELS.index(lvl) if lvl in LEVELS else 0)
        with s3:
            owner_label = next((k for k,v in owner_map.items() if v == own), "— بدون کارشناس —")
            owner_label = st.selectbox("کارشناس فروش (شامل مدیر)", list(owner_map.keys()),
                                       index=list(owner_map.keys()).index(owner_label))
        dom_v = st.text_input("حوزه فعالیت", value=dom or "")
        prov_v = st.text_input("استان", value=prov or "")
        if st.form_submit_button("ذخیره"):
            ok, msg = update_user(
                user_id,
                first_name=first_name, last_name=last_name, full_name=f"{first_name} {last_name}".strip(),
                phone=phone, role=role, company_id=comp_map[company_label], note=note_v,
                status=status_v, domain=dom_v, province=prov_v, level=level_v, owner_id=owner_map[owner_label]
            )
            st.success(msg) if ok else st.error(msg)
            st.rerun()

@st.dialog("ثبت تماس سریع")
def dlg_quick_call(user_id: int):
    with st.form(f"call_{user_id}", clear_on_submit=True):
        j_date = st.text_input("تاریخ تماس (شمسی YYYY/MM/DD) *", value=today_jalali_str())
        t = st.time_input("زمان تماس *", datetime.now().time().replace(second=0, microsecond=0))
        status = st.selectbox("وضعیت تماس *", CALL_STATUSES)
        desc = st.text_area("توضیحات")
        if st.form_submit_button("ثبت تماس"):
            d = jalali_str_to_date(j_date)
            if not d: st.warning("فرمت تاریخ صحیح نیست."); return
            create_call(user_id, datetime.combine(d,t), status, desc, current_user_id())
            st.success("تماس ثبت شد.")

@st.dialog("ثبت پیگیری سریع")
def dlg_quick_followup(user_id: int):
    with st.form(f"fu_{user_id}", clear_on_submit=True):
        title = st.text_input("عنوان اقدام بعدی *")
        details = st.text_area("جزئیات")
        j_due = st.text_input("تاریخ پیگیری (شمسی YYYY/MM/DD) *", value=today_jalali_str())
        if st.form_submit_button("ثبت"):
            if not title.strip(): st.warning("عنوان اجباری است."); return
            d = jalali_str_to_date(j_due)
            if not d: st.warning("فرمت تاریخ صحیح نیست."); return
            create_followup(user_id, title, details, d, "در حال انجام", current_user_id())
            st.success("پیگیری ثبت شد.")

# ====================== دیالوگ‌ها: شرکت‌ها ======================
@st.dialog("پروفایل شرکت")
def dlg_company_view(company_id: int):
    conn = get_conn()
    c = conn.execute("""
       SELECT id, name, COALESCE(phone,''), COALESCE(address,''), COALESCE(note,''),
              COALESCE(level,''), COALESCE(status,''), created_at
       FROM companies WHERE id=?;
    """, (company_id,)).fetchone()
    if not c:
        conn.close(); st.warning("شرکت یافت نشد."); return

    info_tab, users_tab, calls_tab, fu_tab = st.tabs(["اطلاعات شرکت","کاربران شرکت","تماس‌ها","پیگیری‌ها"])
    with info_tab:
        st.write("**نام شرکت:**", c[1])
        st.write("**تلفن:**", c[2])
        st.write("**آدرس:**", c[3])
        st.write("**یادداشت:**", c[4])
        st.write("**سطح:**", c[5])
        st.write("**وضعیت:**", c[6])
        st.write("**تاریخ ایجاد:**", c[7])

    with users_tab:
        dusers = pd.read_sql_query("""
          SELECT id AS ID, full_name AS نام_کامل, COALESCE(phone,'') AS تلفن, COALESCE(role,'') AS سمت
          FROM users WHERE company_id=? ORDER BY full_name;
        """, conn, params=(company_id,))
        st.dataframe(dusers, use_container_width=True)

    with calls_tab:
        dcalls = pd.read_sql_query("""
          SELECT cl.id AS ID, u.full_name AS نام_کاربر, cl.call_datetime AS تاریخ_و_زمان, cl.status AS وضعیت, COALESCE(cl.description,'') AS توضیحات
          FROM calls cl JOIN users u ON u.id=cl.user_id
          WHERE u.company_id=? ORDER BY cl.call_datetime DESC, cl.id DESC;
        """, conn, params=(company_id,))
        st.dataframe(dcalls, use_container_width=True)

    with fu_tab:
        dfu = pd.read_sql_query("""
          SELECT f.id AS ID, u.full_name AS نام_کاربر, f.title AS عنوان, COALESCE(f.details,'') AS جزئیات, f.due_date AS تاریخ_پیگیری, f.status AS وضعیت
          FROM followups f JOIN users u ON u.id=f.user_id
          WHERE u.company_id=? ORDER BY f.due_date DESC, f.id DESC;
        """, conn, params=(company_id,))
        st.dataframe(dfu, use_container_width=True)
    conn.close()

@st.dialog("ویرایش شرکت")
def dlg_company_edit(company_id: int):
    conn = get_conn()
    row = conn.execute("SELECT name, phone, address, note, level, status FROM companies WHERE id=?;", (company_id,)).fetchone()
    conn.close()
    if not row: st.warning("شرکت یافت نشد."); return
    name, phone, addr, note, level, status = row
    with st.form(f"edit_company_{company_id}", clear_on_submit=False):
        c1,c2 = st.columns(2)
        with c1: name_v  = st.text_input("نام شرکت *", value=name or "")
        with c2: phone_v = st.text_input("تلفن", value=phone or "")
        addr_v = st.text_area("آدرس", value=addr or "")
        note_v = st.text_area("یادداشت", value=note or "")
        c3,c4 = st.columns(2)
        with c3: level_v  = st.selectbox("سطح شرکت", LEVELS, index=LEVELS.index(level) if level in LEVELS else 0)
        with c4: status_v = st.selectbox("وضعیت شرکت", COMPANY_STATUSES, index=COMPANY_STATUSES.index(status) if status in COMPANY_STATUSES else 0)
        if st.form_submit_button("ذخیره"):
            ok, msg = update_company(company_id, name=name_v.strip(), phone=phone_v.strip(), address=addr_v.strip(),
                                     note=note_v.strip(), level=level_v, status=status_v)
            st.success(msg) if ok else st.error(msg)
            st.rerun()

@st.dialog("ثبت تماس برای شرکت")
def dlg_company_quick_call(company_id: int):
    # انتخاب یکی از کاربران این شرکت
    conn = get_conn()
    users = pd.read_sql_query("SELECT id, full_name FROM users WHERE company_id=? ORDER BY full_name;", conn, params=(company_id,))
    conn.close()
    if users.empty: st.info("برای این شرکت کاربری ثبت نشده است."); return
    options = {row["full_name"]: int(row["id"]) for _,row in users.iterrows()}
    with st.form(f"comp_call_{company_id}", clear_on_submit=True):
        user_label = st.selectbox("کاربر", list(options.keys()))
        j_date = st.text_input("تاریخ تماس (شمسی YYYY/MM/DD) *", value=today_jalali_str())
        t = st.time_input("زمان تماس *", datetime.now().time().replace(second=0, microsecond=0))
        status = st.selectbox("وضعیت تماس *", CALL_STATUSES)
        desc = st.text_area("توضیحات")
        if st.form_submit_button("ثبت تماس"):
            d = jalali_str_to_date(j_date)
            if not d: st.warning("فرمت تاریخ صحیح نیست."); return
            create_call(options[user_label], datetime.combine(d,t), status, desc, current_user_id())
            st.success("تماس ثبت شد.")

@st.dialog("ثبت پیگیری برای شرکت")
def dlg_company_quick_fu(company_id: int):
    conn = get_conn()
    users = pd.read_sql_query("SELECT id, full_name FROM users WHERE company_id=? ORDER BY full_name;", conn, params=(company_id,))
    conn.close()
    if users.empty: st.info("برای این شرکت کاربری ثبت نشده است."); return
    options = {row["full_name"]: int(row["id"]) for _,row in users.iterrows()}
    with st.form(f"comp_fu_{company_id}", clear_on_submit=True):
        user_label = st.selectbox("کاربر", list(options.keys()))
        title = st.text_input("عنوان *")
        details = st.text_area("جزئیات")
        j_due = st.text_input("تاریخ پیگیری (شمسی YYYY/MM/DD) *", value=today_jalali_str())
        if st.form_submit_button("ثبت"):
            if not title.strip(): st.warning("عنوان اجباری است."); return
            d = jalali_str_to_date(j_due)
            if not d: st.warning("فرمت تاریخ صحیح نیست."); return
            create_followup(options[user_label], title, details, d, "در حال انجام", current_user_id())
            st.success("پیگیری ثبت شد.")

# ====================== صفحات ======================
def page_dashboard():
    st.subheader("داشبورد")
    conn = get_conn()
    calls_today = conn.execute("SELECT COUNT(*) FROM calls WHERE date(call_datetime)=date('now');").fetchone()[0]
    calls_success_today = conn.execute("SELECT COUNT(*) FROM calls WHERE date(call_datetime)=date('now') AND status='موفق';").fetchone()[0]
    last7 = conn.execute("SELECT COUNT(*) FROM calls WHERE date(call_datetime) >= date('now','-7 day');").fetchone()[0]
    overdue = conn.execute("SELECT COUNT(*) FROM followups WHERE status='در حال انجام' AND date(due_date) < date('now');").fetchone()[0]
    total_companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    conn.close()
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("تماس‌های امروز", calls_today)
    c2.metric("موفقِ امروز", calls_success_today)
    c3.metric("تماس‌های ۷ روز اخیر", last7)
    c4.metric("پیگیری‌های عقب‌افتاده", overdue)
    c5,c6 = st.columns(2)
    c5.metric("تعداد شرکت‌ها", total_companies)
    c6.metric("تعداد کاربران", total_users)

def page_companies():
    st.subheader("ثبت و مدیریت شرکت‌ها")

    # --- افزودن شرکت ---
    with st.expander("➕ افزودن شرکت", expanded=False):
        with st.form("company_form", clear_on_submit=True):
            name = st.text_input("نام شرکت *"); phone = st.text_input("تلفن")
            address = st.text_area("آدرس"); note = st.text_area("یادداشت")
            c1,c2 = st.columns(2)
            level = c1.selectbox("سطح شرکت", LEVELS, index=0)
            status = c2.selectbox("وضعیت شرکت", COMPANY_STATUSES, index=0)
            if st.form_submit_button("ثبت شرکت"):
                if not name.strip(): st.warning("نام شرکت اجباری است.")
                else: create_company(name, phone, address, note, level, status, current_user_id()); st.success(f"شرکت «{name}» ثبت شد.")

    # --- فیلترها ---
    st.markdown("### فیلتر شرکت‌ها")
    f1,f2 = st.columns([2,1])
    q_name = f1.text_input("نام شرکت")
    f_status = f2.multiselect("وضعیت شرکت", COMPANY_STATUSES, default=[])
    g1,g2 = st.columns(2)
    f_level = g1.multiselect("سطح شرکت", LEVELS, default=[])
    from_j = g2.text_input("از تاریخ ایجاد (شمسی)")
    h1,h2 = st.columns(2)
    to_j = h1.text_input("تا تاریخ ایجاد (شمسی)")
    has_open_opt = h2.selectbox("پیگیری باز دارد؟", ["— مهم نیست —","بله","خیر"], index=0)

    created_from = jalali_str_to_date(from_j) if from_j else None
    created_to   = jalali_str_to_date(to_j) if to_j else None
    has_open = None if has_open_opt=="— مهم نیست —" else (True if has_open_opt=="بله" else False)

    dfc = df_companies_advanced(q_name, f_status, f_level, created_from, created_to, has_open)

    # --- ستون‌های اقدام داخل جدول ---
    if not dfc.empty:
        # اضافه کردن ستون اقدام
        base = dfc.copy()
        base["👁 نمایش"] = False
        base["✏ ویرایش"] = False
        base["📞 تماس"]  = False
        base["🗓️ پیگیری"] = False

        display_cols = ["نام_شرکت","تلفن","وضعیت_شرکت","سطح_شرکت","تاریخ_ایجاد","پیگیری_باز_دارد",
                        "👁 نمایش","✏ ویرایش","📞 تماس","🗓️ پیگیری"]
        colcfg = {
            "👁 نمایش":  st.column_config.CheckboxColumn("نمایش", help="نمایش پروفایل شرکت", width="small"),
            "✏ ویرایش": st.column_config.CheckboxColumn("ویرایش", help="ویرایش اطلاعات شرکت", width="small"),
            "📞 تماس":   st.column_config.CheckboxColumn("تماس", help="ثبت تماس برای یکی از کاربران شرکت", width="small"),
            "🗓️ پیگیری": st.column_config.CheckboxColumn("پیگیری", help="ثبت پیگیری برای یکی از کاربران شرکت", width="small"),
        }
        edited = st.data_editor(
            base, use_container_width=True, hide_index=True,
            column_order=display_cols, column_config=colcfg,
            disabled=["نام_شرکت","تلفن","وضعیت_شرکت","سطح_شرکت","تاریخ_ایجاد","پیگیری_باز_دارد"],
            key="companies_editor_widget"
        )

        # تشخیص کلیک جدید
        actions = ["👁 نمایش","✏ ویرایش","📞 تماس","🗓️ پیگیری"]
        def snapshot(df: pd.DataFrame) -> Dict[int, tuple]:
            snap = {}
            # چون hide_index=True، index اتوماتیک است؛ از ستون اول «ID» استفاده می‌کنیم اگر هست
            # پس ID را از dfc می‌گیریم و با ردیف‌ها منطبق می‌کنیم:
            # برای سادگی: map نام_شرکت -> ID (فرض: یکتا؛ در CRM واقعی بهتر از ID مخفی استفاده شود)
            return {int(dfc.loc[i, "ID"]): tuple(bool(df.loc[i, a]) for a in actions) for i in df.index}

        prev = st.session_state.get("companies_actions_prev", {})
        curr = snapshot(edited)

        for cid, states in curr.items():
            p = prev.get(cid, (False,False,False,False))
            if states[0] and not p[0]: dlg_company_view(cid)
            if states[1] and not p[1]: dlg_company_edit(cid)
            if states[2] and not p[2]: dlg_company_quick_call(cid)
            if states[3] and not p[3]: dlg_company_quick_fu(cid)
        st.session_state["companies_actions_prev"] = curr
    else:
        st.info("شرکتی یافت نشد.")

def page_users():
    st.subheader("ثبت و مدیریت کاربران (رابط‌ها)")
    only_owner = None if is_admin() else current_user_id()

    # افزودن
    companies = list_companies(only_owner)
    company_options = {"— بدون شرکت —": None}
    for cid, cname in companies: company_options[cname] = cid
    owners = list_sales_accounts_including_admins()
    owner_map = {"— بدون کارشناس —": None}
    for i,u,r in owners: owner_map[f"{u} ({r})"] = i

    with st.expander("➕ افزودن کاربر (رابط)", expanded=False):
        with st.form("user_form", clear_on_submit=True):
            c1,c2,c3 = st.columns(3)
            first_name = c1.text_input("نام *")
            last_name  = c2.text_input("نام خانوادگی *")
            phone      = c3.text_input("تلفن (یکتا) *")
            role = st.text_input("سمت/نقش")
            company_label = st.selectbox("شرکت", list(company_options.keys()))
            row1,row2,row3 = st.columns(3)
            user_status = row1.selectbox("وضعیت کاربر", USER_STATUSES, index=0)
            level = row2.selectbox("سطح کاربر", LEVELS, index=0)
            owner_label = row3.selectbox("کارشناس فروش (شامل مدیر)", list(owner_map.keys()), index=0)
            c4,c5 = st.columns(2)
            domain = c4.text_input("حوزه فعالیت")
            province = c5.text_input("استان")
            note = st.text_area("یادداشت")
            if st.form_submit_button("ثبت کاربر"):
                if not first_name.strip() or not last_name.strip() or not phone.strip():
                    st.warning("نام، نام‌خانوادگی و تلفن اجباری هستند.")
                else:
                    ok,msg = create_user(first_name,last_name,phone,role,company_options[company_label],note,
                                         user_status,domain,province,level,owner_map[owner_label],current_user_id())
                    st.success(msg) if ok else st.error(msg)

    # فیلترها
    st.markdown("### فیلتر کاربران")
    f1,f2,f3 = st.columns([1,1,1])
    first_q = f1.text_input("نام")
    last_q  = f2.text_input("نام خانوادگی")
    h_stat  = f3.multiselect("وضعیت کاربر", USER_STATUSES, default=[])
    g1,g2,g3 = st.columns([1,1,1])
    created_from_j = g1.text_input("از تاریخ ایجاد (شمسی)")
    created_to_j   = g2.text_input("تا تاریخ ایجاد (شمسی)")
    has_open_opt   = g3.selectbox("پیگیری باز دارد؟", ["— مهم نیست —", "بله", "خیر"], index=0)
    k1,k2 = st.columns([1,1])
    last_call_from_j = k1.text_input("از تاریخ آخرین تماس (شمسی)")
    last_call_to_j   = k2.text_input("تا تاریخ آخرین تماس (شمسی)")

    created_from = jalali_str_to_date(created_from_j) if created_from_j else None
    created_to   = jalali_str_to_date(created_to_j) if created_to_j else None
    last_call_from = jalali_str_to_date(last_call_from_j) if last_call_from_j else None
    last_call_to   = jalali_str_to_date(last_call_to_j) if last_call_to_j else None
    has_open = None if has_open_opt=="— مهم نیست —" else (True if has_open_opt=="بله" else False)

    df_all = df_users_advanced(first_q,last_q,created_from,created_to,has_open,
                               last_call_from,last_call_to,h_stat,only_owner)

    # نگاشت user_id امن
    conn = get_conn()
    id_map = pd.read_sql_query("SELECT id, full_name FROM users;", conn)
    conn.close()
    name_to_id = dict(zip(id_map["full_name"], id_map["id"]))
    df_all["user_id"] = df_all["نام_کامل"].map(name_to_id)

    # ترتیب ستون‌ها مطابق خواسته
    ordered = ["نام","نام_خانوادگی","شرکت","تلفن","وضعیت_کاربر",
               "سطح_کاربر","آخرین_تماس","حوزه_فعالیت","استان","پیگیری_باز_دارد"]
    ordered = [c for c in ordered if c in df_all.columns]

    # دیتافریم نمایشی با ستون‌های اقدام
    base = df_all[ordered + ["user_id"]].copy()
    base["👁 نمایش"] = False
    base["✏ ویرایش"] = False
    base["📞 تماس"]  = False
    base["🗓️ پیگیری"] = False
    base = base.set_index("user_id", drop=True)

    display_cols = ordered + ["👁 نمایش","✏ ویرایش","📞 تماس","🗓️ پیگیری"]
    colcfg = {
        "👁 نمایش":  st.column_config.CheckboxColumn("نمایش", help="نمایش پروفایل", width="small"),
        "✏ ویرایش": st.column_config.CheckboxColumn("ویرایش", help="ویرایش پروفایل", width="small"),
        "📞 تماس":   st.column_config.CheckboxColumn("تماس", help="ثبت تماس", width="small"),
        "🗓️ پیگیری": st.column_config.CheckboxColumn("پیگیری", help="ثبت پیگیری", width="small"),
    }

    edited = st.data_editor(
        base,
        use_container_width=True,
        hide_index=True,
        column_order=display_cols,
        column_config=colcfg,
        disabled=ordered,
        key="users_editor_widget"
    )

    # تشخیص «کلیک جدید»
    actions = ["👁 نمایش","✏ ویرایش","📞 تماس","🗓️ پیگیری"]
    def snapshot(df: pd.DataFrame) -> Dict[int, tuple]:
        return {int(uid): tuple(bool(row.get(a, False)) for a in actions)
                for uid, row in df.iterrows()}
    prev = st.session_state.get("users_actions_prev", {})
    curr = snapshot(edited)
    for uid, states in curr.items():
        p = prev.get(uid, (False, False, False, False))
        if states[0] and not p[0]: dlg_profile(uid)
        if states[1] and not p[1]: dlg_edit_user(uid)
        if states[2] and not p[2]: dlg_quick_call(uid)
        if states[3] and not p[3]: dlg_quick_followup(uid)
    st.session_state["users_actions_prev"] = curr

def page_calls():
    only_owner = None if is_admin() else current_user_id()
    st.subheader("ثبت تماس‌ها")
    # افزودن سریع تماس
    users = list_users_basic(only_owner)
    user_map = {f"{u[1]} (ID {u[0]})": u[0] for u in users}
    if users:
        with st.expander("➕ افزودن تماس", expanded=False):
            with st.form("call_form", clear_on_submit=True):
                user_label = st.selectbox("کاربر *", list(user_map.keys()))
                j_date = st.text_input("تاریخ تماس (شمسی YYYY/MM/DD) *", value=today_jalali_str())
                t = st.time_input("زمان تماس *", datetime.now().time().replace(second=0, microsecond=0))
                status = st.selectbox("وضعیت تماس *", CALL_STATUSES)
                desc = st.text_area("توضیحات")
                if st.form_submit_button("ثبت تماس"):
                    d = jalali_str_to_date(j_date)
                    if not d: st.warning("فرمت تاریخ صحیح نیست.")
                    else:
                        create_call(user_map[user_label], datetime.combine(d,t), status, desc, current_user_id())
                        st.success("تماس ثبت شد.")
    # فیلتر و نمایش
    c1,c2,c3,c4 = st.columns(4)
    name_q = c1.text_input("جستجو نام/شرکت")
    st_statuses = c2.multiselect("وضعیت", CALL_STATUSES, default=[])
    start_j = c3.text_input("از تاریخ (شمسی)")
    end_j   = c4.text_input("تا تاریخ (شمسی)")
    start_date = jalali_str_to_date(start_j) if start_j else None
    end_date   = jalali_str_to_date(end_j)   if end_j else None
    df = df_calls_by_filters(name_q, st_statuses, start_date, end_date, only_owner)
    st.dataframe(df, use_container_width=True)

def page_followups():
    only_owner = None if is_admin() else current_user_id()
    st.subheader("ثبت پیگیری‌ها")
    # افزودن سریع پیگیری
    users = list_users_basic(only_owner)
    user_map = {f"{u[1]} (ID {u[0]})": u[0] for u in users}
    if users:
        with st.expander("➕ افزودن پیگیری", expanded=False):
            with st.form("fu_form", clear_on_submit=True):
                user_label = st.selectbox("کاربر *", list(user_map.keys()))
                title = st.text_input("عنوان *")
                details = st.text_area("جزئیات")
                j_due = st.text_input("تاریخ پیگیری (شمسی YYYY/MM/DD) *", value=today_jalali_str())
                if st.form_submit_button("ثبت پیگیری"):
                    if not title.strip(): st.warning("عنوان اجباری است.")
                    else:
                        d = jalali_str_to_date(j_due)
                        if not d: st.warning("فرمت تاریخ صحیح نیست.")
                        else:
                            create_followup(user_map[user_label], title, details, d, "در حال انجام", current_user_id())
                            st.success("پیگیری ثبت شد.")
    # فیلتر و نمایش
    c1,c2,c3,c4 = st.columns(4)
    name_q = c1.text_input("جستجو نام/شرکت", key="fu_q")
    st_statuses = c2.multiselect("وضعیت", TASK_STATUSES, default=[], key="fu_st")
    start_j = c3.text_input("از تاریخ (شمسی)", key="fu_sd")
    end_j   = c4.text_input("تا تاریخ (شمسی)", key="fu_ed")
    start_date = jalali_str_to_date(start_j) if start_j else None
    end_date   = jalali_str_to_date(end_j)   if end_j else None
    df = df_followups_by_filters(name_q, st_statuses, start_date, end_date, only_owner)
    st.dataframe(df, use_container_width=True)

def page_access():
    if not is_admin(): st.info("این بخش فقط برای مدیر در دسترس است."); return
    all_users = list_users_basic(None)
    map_users = {"— بدون لینک —": None}
    for u in all_users: map_users[f"{u[1]} (ID {u[0]})"] = u[0]
    with st.expander("➕ ایجاد کاربر ورود", expanded=False):
        with st.form("new_app_user", clear_on_submit=True):
            username = st.text_input("نام کاربری *")
            password = st.text_input("رمز عبور *", type="password")
            role_sel = st.selectbox("نقش *", ["agent","admin"], index=0)
            link_label = st.selectbox("لینک به کدام 'کاربر (رابط)'؟", list(map_users.keys()))
            if st.form_submit_button("ایجاد"):
                if not username or not password: st.warning("نام کاربری و رمز عبور اجباری است.")
                else:
                    try:
                        conn = get_conn()
                        conn.execute("INSERT INTO app_users (username,password_sha256,role,linked_user_id) VALUES (?,?,?,?);",
                                     (username.strip(), sha256(password), role_sel, map_users[link_label]))
                        conn.commit(); conn.close(); st.success("کاربر ایجاد شد.")
                    except sqlite3.IntegrityError:
                        st.error("این نام کاربری قبلاً وجود دارد.")

# ====================== اجرا ======================
init_db()

if not st.session_state.auth:
    login_view()
else:
    with st.sidebar:
        st.markdown("**فردا پک**")
        header_userbox()
        role = st.session_state.auth["role"]
        page = st.radio(
            "منو",
            ("داشبورد","شرکت‌ها","کاربران","تماس‌ها","پیگیری‌ها") + (("مدیریت دسترسی",) if role=="admin" else tuple()),
            index=0
        )

    if page == "داشبورد":        page_dashboard()
    elif page == "شرکت‌ها":      page_companies()
    elif page == "کاربران":      page_users()
    elif page == "تماس‌ها":      page_calls()
    elif page == "پیگیری‌ها":    page_followups()
    elif page == "مدیریت دسترسی": page_access()
