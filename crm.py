# -*- coding: utf-8 -*-
"""
FardaPack Mini-CRM — Streamlit + SQLite (کاملاً ساده و چندکاربره سبک)
-------------------------------------------------------------------

✦ امکانات:
- ثبت کاربر (افراد/رابط‌ها)
- ثبت شرکت
- لینک‌کردن کاربر به شرکت (فیلد «شرکت» داخل فرم کاربر)
- ثبت تماس برای هر کاربر + فیلتر تاریخ/نام/وضعیت
  (وضعیت تماس: «ناموفق»، «موفق»، «خاموش»، «رد تماس»)
- ثبت پیگیری برای هر کاربر + فیلتر تاریخ/کاربر/وضعیت
  (وضعیت پیگیری: «در حال انجام»، «پایان یافته»)
- **چندکاربره‌ی سبک (آنلاین):** ورود ساده (لاگین) با نقش‌ها: «مدیر» و «بازاریاب».
  - بازاریاب فقط تماس‌ها و پیگیری‌های خودش را می‌بیند/می‌سازد.
  - مدیر همه‌چیز را می‌بیند و می‌تواند کاربرِ ورود (app user) بسازد.

✦ اجرا:
1) Python 3.9+ نصب باشد.
2) در پوشه‌ی پروژه:
   - Windows:
       python -m venv .venv
       .venv\Scripts\activate
   - macOS/Linux:
       python3 -m venv .venv
       source .venv/bin/activate
3) نصب وابستگی‌ها:
   pip install streamlit pandas
4) فایل را با نام crm.py ذخیره کنید و اجرا:
   streamlit run crm.py

✦ ورود پیش‌فرض (لطفاً فوراً عوض کنید):
- مدیر: کاربر «admin» با رمز «admin123». به صفحه «مدیریت دسترسی» بروید و کاربران را بسازید/رمز را عوض کنید.

✦ نکته‌ها:
- اولین اجرا یک فایل پایگاه‌داده‌ی «crm.db» کنار اسکریپت می‌سازد.
- برای پشتیبان‌گیری کافیست همین فایل «crm.db» را کپی کنید.
- این ابزار تک‌سروری و سبک است (SQLite + WAL). برای ۲–۳ کاربر همزمان کفایت می‌کند؛ برای تیم‌های بزرگ‌تر بعداً می‌توان به PostgreSQL مهاجرت کرد.
"""

import sqlite3
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Dict

import pandas as pd
import streamlit as st
import hashlib

# ---------------------------
#  تنظیمات اولیه رابط کاربری
# ---------------------------
st.set_page_config(page_title="FardaPack Mini-CRM", page_icon="📇", layout="wide")

# راست‌به‌چپ و فارسی‌سازی ساده
st.markdown(
    """
    <style>
    html, body, [class*="css"] { direction: rtl; text-align: right; }
    .rtl { direction: rtl; text-align: right; }
    .stSelectbox label, .stTextInput label, .stTextArea label, .stDateInput label, .stTimeInput label { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------
#  پایگاه داده
# ---------------------------
DB_PATH = "crm.db"

CALL_STATUSES = ["ناموفق", "موفق", "خاموش", "رد تماس"]
TASK_STATUSES = ["در حال انجام", "پایان یافته"]


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode=WAL;")  # بهبود همزمانی در SQLite
    return conn


def sha256(txt: str) -> str:
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # شرکت‌ها
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            address TEXT,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # کاربران (رابط‌ها)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            phone TEXT,
            role TEXT,
            company_id INTEGER,
            note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL
        );
        """
    )

    # تماس‌ها
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            call_datetime TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('ناموفق','موفق','خاموش','رد تماس')),
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """
    )

    # پیگیری‌ها (وظایف بعدی)
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS followups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            details TEXT,
            due_date TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('در حال انجام','پایان یافته')) DEFAULT 'در حال انجام',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """
    )

    # کاربران ورود به برنامه (app users)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS app_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_sha256 TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin','agent')) DEFAULT 'agent',
            linked_user_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(linked_user_id) REFERENCES users(id) ON DELETE SET NULL
        );
        """
    )

    # ایندکس‌های مفید
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_company ON users(company_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calls_user_datetime ON calls(user_id, call_datetime);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_followups_user_due ON followups(user_id, due_date);")

    # ایجاد ادمین پیش‌فرض اگر وجود ندارد
    has_any = cur.execute("SELECT COUNT(*) FROM app_users;").fetchone()[0]
    if has_any == 0:
        cur.execute(
            "INSERT INTO app_users (username, password_sha256, role, linked_user_id) VALUES (?,?,?,NULL);",
            ("admin", sha256("admin123"), "admin"),
        )

    conn.commit()
    conn.close()


# ---------------------------
#  توابع کمکی DB
# ---------------------------


def list_companies() -> List[Tuple[int, str]]:
    conn = get_conn()
    rows = conn.execute("SELECT id, name FROM companies ORDER BY name COLLATE NOCASE;").fetchall()
    conn.close()
    return rows


def list_users() -> List[Tuple[int, str, Optional[int]]]:
    conn = get_conn()
    rows = conn.execute("SELECT id, full_name, company_id FROM users ORDER BY full_name COLLATE NOCASE;").fetchall()
    conn.close()
    return rows


def create_company(name: str, phone: str = "", address: str = "", note: str = ""):
    conn = get_conn()
    conn.execute(
        "INSERT INTO companies (name, phone, address, note) VALUES (?, ?, ?, ?);",
        (name.strip(), phone.strip(), address.strip(), note.strip()),
    )
    conn.commit()
    conn.close()


def create_user(full_name: str, phone: str, role: str, company_id: Optional[int], note: str = ""):
    conn = get_conn()
    conn.execute(
        "INSERT INTO users (full_name, phone, role, company_id, note) VALUES (?, ?, ?, ?, ?);",
        (full_name.strip(), phone.strip(), role.strip(), company_id, note.strip()),
    )
    conn.commit()
    conn.close()


def create_call(user_id: int, call_dt: datetime, status: str, description: str = ""):
    conn = get_conn()
    conn.execute(
        "INSERT INTO calls (user_id, call_datetime, status, description) VALUES (?, ?, ?, ?);",
        (user_id, call_dt.isoformat(timespec="minutes"), status, description.strip()),
    )
    conn.commit()
    conn.close()


def create_followup(user_id: int, title: str, details: str, due_date_val: date, status: str):
    conn = get_conn()
    conn.execute(
        "INSERT INTO followups (user_id, title, details, due_date, status) VALUES (?, ?, ?, ?, ?);",
        (user_id, title.strip(), details.strip(), due_date_val.isoformat(), status),
    )
    conn.commit()
    conn.close()


# احراز هویت ساده

def sha_ok(input_pwd: str, stored_sha: str) -> bool:
    return sha256(input_pwd) == stored_sha


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
    if sha_ok(password, pwh):
        return {"id": uid, "username": uname, "role": role, "linked_user_id": linked_user_id}
    return None


def create_app_user(username: str, password: str, role: str, linked_user_id: Optional[int]):
    conn = get_conn()
    conn.execute(
        "INSERT INTO app_users (username, password_sha256, role, linked_user_id) VALUES (?,?,?,?);",
        (username.strip(), sha256(password), role, linked_user_id),
    )
    conn.commit()
    conn.close()


def df_calls_filtered(name_query: str, statuses: List[str], start: Optional[date], end: Optional[date], only_user_id: Optional[int]=None) -> pd.DataFrame:
    conn = get_conn()
    params = []
    where = ["1=1"]

    if name_query:
        where.append("(u.full_name LIKE ? OR c.name LIKE ?)")
        q = f"%{name_query.strip()}%"
        params.extend([q, q])

    if statuses:
        where.append("cl.status IN (" + ",".join(["?"] * len(statuses)) + ")")
        params.extend(statuses)

    if start:
        where.append("date(cl.call_datetime) >= ?")
        params.append(start.isoformat())
    if end:
        where.append("date(cl.call_datetime) <= ?")
        params.append(end.isoformat())

    if only_user_id:
        where.append("u.id = ?")
        params.append(only_user_id)

    sql = f"""
        SELECT cl.id AS call_id,
               u.full_name AS نام_کاربر,
               COALESCE(c.name, '') AS شرکت,
               cl.call_datetime AS تاریخ_و_زمان,
               cl.status AS وضعیت,
               COALESCE(cl.description, '') AS توضیحات
        FROM calls cl
        JOIN users u ON u.id = cl.user_id
        LEFT JOIN companies c ON c.id = u.company_id
        WHERE {' AND '.join(where)}
        ORDER BY cl.call_datetime DESC, cl.id DESC;
    """
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


def df_followups_filtered(name_query: str, statuses: List[str], start: Optional[date], end: Optional[date], only_user_id: Optional[int]=None) -> pd.DataFrame:
    conn = get_conn()
    params = []
    where = ["1=1"]

    if name_query:
        where.append("(u.full_name LIKE ? OR c.name LIKE ?)")
        q = f"%{name_query.strip()}%"
        params.extend([q, q])

    if statuses:
        where.append("f.status IN (" + ",".join(["?"] * len(statuses)) + ")")
        params.extend(statuses)

    if start:
        where.append("date(f.due_date) >= ?")
        params.append(start.isoformat())
    if end:
        where.append("date(f.due_date) <= ?")
        params.append(end.isoformat())

    if only_user_id:
        where.append("u.id = ?")
        params.append(only_user_id)

    sql = f"""
        SELECT f.id AS task_id,
               u.full_name AS نام_کاربر,
               COALESCE(c.name, '') AS شرکت,
               f.title AS عنوان,
               COALESCE(f.details, '') AS توضیحات,
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


def mark_followup_done(task_id: int):
    conn = get_conn()
    conn.execute("UPDATE followups SET status = 'پایان یافته' WHERE id = ?;", (task_id,))
    conn.commit()
    conn.close()


# ---------------------------
#  UI & Auth
# ---------------------------

if "auth" not in st.session_state:
    st.session_state.auth = None  # dict: {id, username, role, linked_user_id}


def login_view():
    st.title("ورود به FardaPack Mini-CRM")
    with st.form("login_form"):
        u = st.text_input("نام کاربری")
        p = st.text_input("رمز عبور", type="password")
        ok = st.form_submit_button("ورود")
        if ok:
            info = auth_check(u, p)
            if info:
                st.session_state.auth = info
                st.experimental_rerun()
            else:
                st.error("نام کاربری یا رمز صحیح نیست.")


def header_userbox():
    a = st.session_state.auth
    if not a:
        return
    st.markdown(f"**کاربر:** {a['username']} — **نقش:** {'مدیر' if a['role']=='admin' else 'بازاریاب'}")
    st.button("خروج", on_click=lambda: st.session_state.update({"auth": None}))


# --- صفحات ---

def page_dashboard():
    st.subheader("داشبورد ساده")

    conn = get_conn()
    total_companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    total_calls_7d = conn.execute("SELECT COUNT(*) FROM calls WHERE date(call_datetime) >= date('now','-7 day')").fetchone()[0]
    total_tasks_open = conn.execute("SELECT COUNT(*) FROM followups WHERE status = 'در حال انجام'").fetchone()[0]
    conn.close()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("تعداد شرکت‌ها", total_companies)
    c2.metric("تعداد کاربران", total_users)
    c3.metric("تماس‌های ۷ روز اخیر", total_calls_7d)
    c4.metric("پیگیری‌های باز", total_tasks_open)

    st.divider()
    st.markdown("### گام‌های پیشنهادی:")
    st.markdown("1) از منوی کناری، ابتدا **شرکت**‌ها را بسازید. سپس **کاربر** اضافه کنید و شرکت را برایش انتخاب کنید.")
    st.markdown("2) بعد به صفحه‌ی **تماس‌ها** بروید و برای هر کاربر تماس ثبت کنید.")
    st.markdown("3) در **پیگیری‌ها** اقدام بعدی را ثبت و پیگیری کنید.")


def page_companies():
    if st.session_state.auth["role"] != "admin":
        st.info("این بخش فقط برای مدیر در دسترس است.")
        return
    st.subheader("ثبت و مدیریت شرکت‌ها")
    with st.form("company_form", clear_on_submit=True):
        name = st.text_input("نام شرکت *")
        phone = st.text_input("تلفن")
        address = st.text_area("آدرس")
        note = st.text_area("یادداشت")
        submitted = st.form_submit_button("ثبت شرکت")
        if submitted:
            if not name.strip():
                st.warning("نام شرکت اجباری است.")
            else:
                create_company(name, phone, address, note)
                st.success(f"شرکت «{name}» ثبت شد.")

    st.divider()
    st.markdown("### فهرست شرکت‌ها")
    rows = list_companies()
    if rows:
        df = pd.DataFrame(rows, columns=["ID", "نام شرکت"]).set_index("ID")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("هنوز شرکتی ثبت نشده است.")


def page_users():
    if st.session_state.auth["role"] != "admin":
        st.info("این بخش فقط برای مدیر در دسترس است.")
        return
    st.subheader("ثبت و مدیریت کاربران (رابط‌ها)")

    companies = list_companies()
    company_options = {"— بدون شرکت —": None}
    for cid, cname in companies:
        company_options[cname] = cid

    with st.form("user_form", clear_on_submit=True):
        full_name = st.text_input("نام و نام‌خانوادگی *")
        phone = st.text_input("تلفن")
        role = st.text_input("سمت/نقش")
        company_name = st.selectbox("شرکت", list(company_options.keys()))
        note = st.text_area("یادداشت")
        submitted = st.form_submit_button("ثبت کاربر")
        if submitted:
            if not full_name.strip():
                st.warning("نام کاربر اجباری است.")
            else:
                create_user(full_name, phone, role, company_options[company_name], note)
                st.success(f"کاربر «{full_name}» ثبت شد.")

    st.expander("افزودن سریع شرکت جدید").markdown("از صفحه‌ی شرکت‌ها هم می‌توانید اضافه کنید؛ این فقط یادآوری است.")

    st.divider()
    st.markdown("### فهرست کاربران")
    rows = list_users()
    if rows:
        conn = get_conn()
        df = pd.read_sql_query(
            """
            SELECT u.id AS ID,
                   u.full_name AS نام,
                   COALESCE(c.name, '') AS شرکت,
                   COALESCE(u.phone,'') AS تلفن,
                   COALESCE(u.role,'') AS سمت,
                   COALESCE(u.note,'') AS یادداشت
            FROM users u
            LEFT JOIN companies c ON c.id = u.company_id
            ORDER BY u.full_name COLLATE NOCASE;
            """,
            conn,
        ).set_index("ID")
        conn.close()
        st.dataframe(df, use_container_width=True)
    else:
        st.info("هنوز کاربری ثبت نشده است.")


def page_calls():
    st.subheader("ثبت تماس‌ها")

    all_users = list_users()
    if not all_users:
        st.warning("ابتدا کاربر اضافه کنید (از منوی 'کاربران' توسط مدیر).")
        return

    a = st.session_state.auth
    if a["role"] == "admin":
        user_map = {f"{u[1]} (ID {u[0]})": u[0] for u in all_users}
    else:
        # برای بازاریاب فقط کاربر لینک‌شده خودش
        linked_id = a.get("linked_user_id")
        all_users = [u for u in all_users if u[0] == linked_id]
        user_map = {f"{u[1]} (ID {u[0]})": u[0] for u in all_users}
        if not user_map:
            st.error("مدیر هنوز شما را به یک 'کاربر (رابط)' لینک نکرده است.")
            return

    with st.form("call_form", clear_on_submit=True):
        user_label = st.selectbox("کاربر *", list(user_map.keys()))
        today = date.today()
        now_hm = datetime.now().time().replace(second=0, microsecond=0)
        d = st.date_input("تاریخ تماس *", today)
        t = st.time_input("زمان تماس *", now_hm)
        status = st.selectbox("وضعیت تماس *")
        desc = st.text_area("توضیحات")
        submitted = st.form_submit_button("ثبت تماس")
        if submitted:
            call_dt = datetime.combine(d, t)
            create_call(user_map[user_label], call_dt, status, desc)
            st.success("تماس ثبت شد.")

    st.divider()
    st.markdown("### فهرست تماس‌ها + فیلتر")
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1:
        name_q = st.text_input("جستجو در نام کاربر/نام شرکت")
    with c2:
      st_statuses = st.multiselect("فیلتر وضعیت", default=[])
    with c3:
        start_date = st.date_input("از تاریخ", value=None)
    with c4:
        end_date = st.date_input("تا تاریخ", value=None)

    only_uid = None if a["role"]=="admin" else a.get("linked_user_id")
    df = df_calls_filtered(name_q, st_statuses, start_date, end_date, only_user_id=only_uid)
    st.dataframe(df, use_container_width=True)

    if not df.empty:
        csv = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("دانلود CSV تماس‌ها", data=csv, file_name="calls.csv", mime="text/csv")


def page_followups():
    st.subheader("ثبت پیگیری‌ها")

    all_users = list_users()
    if not all_users:
        st.warning("ابتدا کاربر اضافه کنید (از منوی 'کاربران' توسط مدیر).")
        return

    a = st.session_state.auth
    if a["role"] == "admin":
        user_map = {f"{u[1]} (ID {u[0]})": u[0] for u in all_users}
    else:
        linked_id = a.get("linked_user_id")
        all_users = [u for u in all_users if u[0] == linked_id]
        user_map = {f"{u[1]} (ID {u[0]})": u[0] for u in all_users}
        if not user_map:
            st.error("مدیر هنوز شما را به یک 'کاربر (رابط)' لینک نکرده است.")
            return

    with st.form("fu_form", clear_on_submit=True):
        user_label = st.selectbox("کاربر *", list(user_map.keys()))
        title = st.text_input("عنوان اقدام بعدی *", placeholder="مثلاً: ارسال پیش‌فاکتور")
        details = st.text_area("جزئیات")
        due = st.date_input("تاریخ پیگیری *", value=date.today() + timedelta(days=1))
        status = st.selectbox("وضعیت" index=0)
        submitted = st.form_submit_button("ثبت پیگیری")
        if submitted:
            if not title.strip():
                st.warning("عنوان پیگیری اجباری است.")
            else:
                create_followup(user_map[user_label], title, details, due, status)
                st.success("پیگیری ثبت شد.")

    st.divider()
    st.markdown("### فهرست پیگیری‌ها + فیلتر")
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1:
        name_q = st.text_input("جستجو در نام کاربر/نام شرکت", key="fu_q")
    with c2:
        st_statuses = st.multiselect("فیلتر وضعیت" default=[], key="fu_st")
    with c3:
        start_date = st.date_input("از تاریخ", value=None, key="fu_sd")
    with c4:
        end_date = st.date_input("تا تاریخ", value=None, key="fu_ed")

    only_uid = None if a["role"]=="admin" else a.get("linked_user_id")
    df = df_followups_filtered(name_q, st_statuses, start_date, end_date, only_user_id=only_uid)
    st.dataframe(df, use_container_width=True)

    if not df.empty:
        csv = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("دانلود CSV پیگیری‌ها", data=csv, file_name="followups.csv", mime="text/csv")

    st.markdown("#### عملیات سریع")
    with st.form("done_form"):
        task_id = st.number_input("ID پیگیری برای اتمام", min_value=1, step=1)
        done_btn = st.form_submit_button("علامت‌گذاری به‌عنوان پایان‌یافته")
        if done_btn:
            try:
                mark_followup_done(int(task_id))
                st.success("به‌روزرسانی شد.")
            except Exception as e:
                st.error(f"خطا: {e}")


def page_access_admin():
    if st.session_state.auth["role"] != "admin":
        st.info("این بخش فقط برای مدیر در دسترس است.")
        return
    st.subheader("مدیریت دسترسی (کاربران ورود)")

    # فرم ساخت کاربر ورود
    all_users = list_users()
    map_users = {"— بدون لینک —": None}
    for u in all_users:
        map_users[f"{u[1]} (ID {u[0]})"] = u[0]

    with st.form("new_app_user", clear_on_submit=True):
        username = st.text_input("نام کاربری *")
        password = st.text_input("رمز عبور *", type="password")
        role = st.selectbox("نقش *", ["agent", "admin"], index=0)
        link_label = st.selectbox("لینک به کدام 'کاربر (رابط)'؟", list(map_users.keys()))
        submitted = st.form_submit_button("ایجاد کاربر ورود")
        if submitted:
            if not username or not password:
                st.warning("نام کاربری و رمز عبور اجباری است.")
            else:
                try:
                    create_app_user(username, password, role, map_users[link_label])
                    st.success("کاربر ایجاد شد.")
                except sqlite3.IntegrityError:
                    st.error("این نام کاربری قبلاً وجود دارد.")

    st.divider()
    st.markdown("### نکات:")
    st.markdown("- برای بازاریاب‌ها نقش **agent** انتخاب کنید و حتماً به یک 'کاربر (رابط)' لینک کنید.")
    st.markdown("- رمز پیش‌فرض admin را سریعاً تغییر دهید: یک کاربر **admin** جدید بسازید و قدیمی را حذف کنید (به‌صورت دستی در DB).")


# ---------------------------
#  اجرای برنامه
# ---------------------------
init_db()

if "auth" not in st.session_state or not st.session_state.auth:
    login_view()
else:
    with st.sidebar:
        st.header("FardaPack Mini-CRM")
        header_userbox()
        role = st.session_state.auth["role"]
        if role == "admin":
            page = st.radio(
                "منو",
                (
                    "داشبورد",
                    "شرکت‌ها",
                    "کاربران",
                    "تماس‌ها",
                    "پیگیری‌ها",
                    "مدیریت دسترسی",
                ),
                index=0,
            )
        else:
            page = st.radio(
                "منو",
                (
                    "داشبورد",
                    "تماس‌ها",
                    "پیگیری‌ها",
                ),
                index=0,
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
        page_access_admin()
