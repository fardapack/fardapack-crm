# -*- coding: utf-8 -*-
"""
FardaPack Mini-CRM — Streamlit + SQLite (چندکاربره سبک) — نسخه با تاریخ شمسی و فرم‌های تاشونده
--------------------------------------------------------------------------------------------

✦ امکانات:
- ثبت شرکت، کاربر (رابط)، تماس، پیگیری
- فیلتر تاریخ/نام/وضعیت + خروجی CSV
- ورود (لاگین) با نقش‌های «مدیر / بازاریاب»
  • بازاریاب فقط داده‌های لینک‌شده به خودش را می‌بیند
  • مدیر همه‌چیز را می‌بیند و کاربر ورود می‌سازد
- تاریخ‌های «ورودی/فیلتر» به صورت **شمسی** (YYYY/MM/DD) با تبدیل خودکار به میلادی
- فرم‌ها به صورت **expander** (با کلیک باز می‌شوند) و لیست‌ها به‌صورت پیش‌فرض نمایش داده می‌شوند

✦ اجرا محلی:
    pip install -r requirements.txt
    streamlit run crm.py

✦ نکات:
- پایگاه‌داده: crm.db کنار فایل ایجاد می‌شود (WAL برای همزمانی سبک)
- ورود پیش‌فرض (پس از اولین اجرا سریع عوض کنید): admin / admin123
- برای تاریخ شمسی باید در requirements.txt سطر زیر باشد: **persiantools**
"""

import sqlite3
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Dict

import pandas as pd
import streamlit as st
import hashlib

# تاریخ شمسی
try:
    from persiantools.jdatetime import JalaliDate
except Exception:
    JalaliDate = None  # اگر پکیج نصب نباشد، پیام راهنما می‌دهیم

# ---------------------------
#  تنظیمات اولیه UI
# ---------------------------
st.set_page_config(page_title="FardaPack Mini-CRM", page_icon="📇", layout="wide")
st.markdown(
    """
    <style>
    html, body, [class*="css"] { direction: rtl; text-align: right; }
    .stSelectbox label, .stTextInput label, .stTextArea label, .stDateInput label, .stTimeInput label { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------
#  کمکی: تاریخ شمسی <-> میلادی
# ---------------------------

def _jalali_supported() -> bool:
    return JalaliDate is not None


def today_jalali_str() -> str:
    if _jalali_supported():
        return JalaliDate.today().strftime("%Y/%m/%d")
    return ""


def jalali_str_to_date(s: str) -> Optional[date]:
    """ورودی: 'YYYY/MM/DD' => خروجی: datetime.date میلادی"""
    if not s:
        return None
    if not _jalali_supported():
        return None
    try:
        g = JalaliDate.strptime(s.strip(), "%Y/%m/%d").to_gregorian()
        return date(g.year, g.month, g.day)
    except Exception:
        return None


def date_to_jalali_str(d: date) -> str:
    if not d or not _jalali_supported():
        return ""
    j = JalaliDate.fromgregorian(date=d)
    return j.strftime("%Y/%m/%d")


# ---------------------------
#  پایگاه‌داده
# ---------------------------
DB_PATH = "crm.db"
CALL_STATUSES = ["ناموفق", "موفق", "خاموش", "رد تماس"]
TASK_STATUSES = ["در حال انجام", "پایان یافته"]

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode=WAL;")  # بهبود همزمانی
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
        """
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

    # پیگیری‌ها
    cur.execute(
        """
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

    # کاربران ورود (app_users)
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

# ---------------------------
#  توابع کمکی
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

def mark_followup_done(task_id: int):
    conn = get_conn()
    conn.execute("UPDATE followups SET status = 'پایان یافته' WHERE id = ?;", (task_id,))
    conn.commit()
    conn.close()

# جدول راست‌چین با ستون «ردیف» و جابجایی ID جلو

def render_df(df: pd.DataFrame):
    if df is None or df.empty:
        st.info("داده‌ای یافت نشد.")
        return
    df_disp = df.copy()
    df_disp.insert(0, "ردیف", range(1, len(df_disp)+1))
    cols = df_disp.columns.tolist()
    id_cols = [c for c in cols if c in ["ID","call_id","task_id"]]
    other_cols = [c for c in cols if c not in id_cols and c != "ردیف"]
    new_cols = ["ردیف"] + id_cols + other_cols
    df_disp = df_disp[new_cols]
    st.dataframe(df_disp, use_container_width=True)

# ---------------------------
#  UI + Auth
# ---------------------------
if "auth" not in st.session_state:
    st.session_state.auth = None


def login_view():
    st.title("به FardaPack Mini-CRM ورود")
    if not _jalali_supported():
        st.info("برای تاریخ شمسی، در requirements.txt سطر زیر را اضافه کنید و دوباره دیپلوی کنید: persiantools")
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


def header_userbox():
    a = st.session_state.auth
    if not a:
        return
    st.markdown(f"**کاربر:** {a['username']} — **نقش:** {'مدیر' if a['role']=='admin' else 'بازاریاب'}")
    st.button("خروج", on_click=lambda: st.session_state.update({"auth": None}))

# صفحات

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
    st.markdown("1) از منو: شرکت‌ها و کاربران را بسازید.")
    st.markdown("2) تماس‌ها را ثبت کنید؛ 3) پیگیری‌ها را مدیریت کنید.")


def page_companies():
    if st.session_state.auth["role"] != "admin":
        st.info("این بخش فقط برای مدیر در دسترس است.")
        return
    st.subheader("ثبت و مدیریت شرکت‌ها")

    with st.expander("➕ افزودن شرکت", expanded=False):
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
    rows = list_companies()
    if rows:
        df = pd.DataFrame(rows, columns=["ID", "نام شرکت"]).set_index("ID").reset_index()
        render_df(df)
    else:
        st.info("شرکتی ثبت نشده است.")


def page_users():
    if st.session_state.auth["role"] != "admin":
        st.info("این بخش فقط برای مدیر در دسترس است.")
        return
    st.subheader("ثبت و مدیریت کاربران (رابط‌ها)")

    companies = list_companies()
    company_options = {"— بدون شرکت —": None}
    for cid, cname in companies:
        company_options[cname] = cid

    with st.expander("➕ افزودن کاربر (رابط)", expanded=False):
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

    st.divider()
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
        )
        conn.close()
        render_df(df)
    else:
        st.info("کاربری ثبت نشده است.")


def _user_selection_map_for_role():
    all_users = list_users()
    a = st.session_state.auth
    if a["role"] == "admin":
        return {f"{u[1]} (ID {u[0]})": u[0] for u in all_users}
    else:
        linked_id = a.get("linked_user_id")
        all_users = [u for u in all_users if u[0] == linked_id]
        return {f"{u[1]} (ID {u[0]})": u[0] for u in all_users}


def page_calls():
    st.subheader("ثبت تماس‌ها")
    user_map = _user_selection_map_for_role()
    if not user_map:
        st.warning("ابتدا مدیر باید شما را به یک 'کاربر (رابط)' لینک کند.")
        return

    with st.expander("➕ افزودن تماس", expanded=False):
        with st.form("call_form", clear_on_submit=True):
            user_label = st.selectbox("کاربر *", list(user_map.keys()))
            j_date = st.text_input("تاریخ تماس (شمسی YYYY/MM/DD) *", value=today_jalali_str(), placeholder="مثلاً 1403/07/18")
            t = st.time_input("زمان تماس *", datetime.now().time().replace(second=0, microsecond=0))
            status = st.selectbox("وضعیت تماس *", CALL_STATUSES)
            desc = st.text_area("توضیحات")
            submitted = st.form_submit_button("ثبت تماس")
            if submitted:
                d = jalali_str_to_date(j_date)
                if not d:
                    st.warning("فرمت تاریخ صحیح نیست. نمونه: 1403/07/18")
                else:
                    call_dt = datetime.combine(d, t)
                    create_call(user_map[user_label], call_dt, status, desc)
                    st.success("تماس ثبت شد.")

    st.divider()
    st.markdown("### فهرست تماس‌ها + فیلتر")
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1:
        name_q = st.text_input("جستجو در نام کاربر/نام شرکت")
    with c2:
        st_statuses = st.multiselect("فیلتر وضعیت", CALL_STATUSES, default=[])
    with c3:
        start_j = st.text_input("از تاریخ (شمسی)", value="", placeholder="1403/01/01")
    with c4:
        end_j = st.text_input("تا تاریخ (شمسی)", value="", placeholder="1403/12/29")

    start_date = jalali_str_to_date(start_j) if start_j else None
    end_date = jalali_str_to_date(end_j) if end_j else None

    only_uid = None if st.session_state.auth["role"]=="admin" else st.session_state.auth.get("linked_user_id")
    df = df_calls_filtered(name_q, st_statuses, start_date, end_date, only_user_id=only_uid)
    render_df(df)


def page_followups():
    st.subheader("ثبت پیگیری‌ها")
    user_map = _user_selection_map_for_role()
    if not user_map:
        st.warning("ابتدا مدیر باید شما را به یک 'کاربر (رابط)' لینک کند.")
        return

    with st.expander("➕ افزودن پیگیری", expanded=False):
        with st.form("fu_form", clear_on_submit=True):
            user_label = st.selectbox("کاربر *", list(user_map.keys()))
            title = st.text_input("عنوان اقدام بعدی *", placeholder="مثلاً: ارسال پیش‌فاکتور")
            details = st.text_area("جزئیات")
            j_due = st.text_input("تاریخ پیگیری (شمسی YYYY/MM/DD) *", value=today_jalali_str(), placeholder="1403/07/18")
            status = st.selectbox("وضعیت", TASK_STATUSES, index=0)
            submitted = st.form_submit_button("ثبت پیگیری")
            if submitted:
                if not title.strip():
                    st.warning("عنوان پیگیری اجباری است.")
                else:
                    d = jalali_str_to_date(j_due)
                    if not d:
                        st.warning("فرمت تاریخ صحیح نیست. نمونه: 1403/07/18")
                    else:
                        create_followup(user_map[user_label], title, details, d, status)
                        st.success("پیگیری ثبت شد.")

    st.divider()
    st.markdown("### فهرست پیگیری‌ها + فیلتر")
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1:
        name_q = st.text_input("جستجو در نام کاربر/نام شرکت", key="fu_q")
    with c2:
        st_statuses = st.multiselect("فیلتر وضعیت", TASK_STATUSES, default=[], key="fu_st")
    with c3:
        start_j = st.text_input("از تاریخ (شمسی)", value="", key="fu_sd", placeholder="1403/01/01")
    with c4:
        end_j = st.text_input("تا تاریخ (شمسی)", value="", key="fu_ed", placeholder="1403/12/29")

    start_date = jalali_str_to_date(start_j) if start_j else None
    end_date = jalali_str_to_date(end_j) if end_j else None

    only_uid = None if st.session_state.auth["role"]=="admin" else st.session_state.auth.get("linked_user_id")
    df = df_followups_filtered(name_q, st_statuses, start_date, end_date, only_user_id=only_uid)
    render_df(df)

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
    all_users = list_users()
    map_users = {"— بدون لینک —": None}
    for u in all_users:
        map_users[f"{u[1]} (ID {u[0]})"] = u[0]

    with st.expander("➕ ایجاد کاربر ورود", expanded=False):
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

# ---------------------------
#  اجرای برنامه
# ---------------------------
init_db()

if not st.session_state.auth:
    login_view()
else:
    with st.sidebar:
        st.header("FardaPack Mini-CRM")
        header_userbox()
        role = st.session_state.auth["role"]
        if role == "admin":
            page = st.radio(
                "منو",
                ("داشبورد", "شرکت‌ها", "کاربران", "تماس‌ها", "پیگیری‌ها", "مدیریت دسترسی"),
                index=0,
            )
        else:
            page = st.radio(
                "منو",
                ("داشبورد", "تماس‌ها", "پیگیری‌ها"),
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
