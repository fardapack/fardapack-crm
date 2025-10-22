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
- ♻️ بازیابی دیتابیس از بکاپ (.db یا .zip)
- 🛒 بخش سفارشات و محصولات
"""

import sqlite3
from datetime import datetime, date, timedelta
from typing import Optional, List, Tuple, Dict

import pandas as pd
import streamlit as st
import hashlib
import uuid

# 👇 اضافه شد
import os, io, zipfile, shutil

# ====================== صفحه و CSS (اصلاح‌شده برای جداول HTML) ======================
st.set_page_config(page_title="FardaPack Mini-CRM", page_icon="📇", layout="wide")
st.markdown(
    """
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
      /* استایل عمومی و RTL */
      html, body, [data-testid="stAppViewContainer"]{
        direction: rtl; text-align: right !important;
        font-family: "Vazirmatn", sans-serif !important;
      }
      [data-testid="stSidebar"] * { font-family: "Vazirmatn", sans-serif !important; }
      
      /* جدول HTML سفارشی */
      .crm-table-container {
        overflow-x: auto; /* برای اسکرول افقی در موبایل */
        width: 100%;
      }
      .crm-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.9rem;
        text-align: right;
      }
      .crm-table th, .crm-table td {
        padding: 8px 12px;
        border: 1px solid #ddd;
        vertical-align: middle;
        text-align: right; /* تضمین راست‌چین بودن محتوا */
        white-space: nowrap; /* جلوگیری از شکستن کلمات در ستون‌های کوچک */
      }
      .crm-table th {
        background-color: #f0f2f6;
        font-weight: 700;
      }
      .crm-table tr:hover {
        background-color: #f9f9f9;
      }

      /* دکمه‌های آیکون‌دار */
      .action-container {
        display: flex;
        gap: 8px; /* فاصله بین دکمه‌ها */
        justify-content: flex-start; /* چپ‌چین کردن دکمه‌ها در سلول (برای ظاهر فارسی) */
        min-width: 120px; /* فضای کافی برای دکمه‌ها */
      }
      .action-button {
        background: none;
        border: none;
        cursor: pointer;
        padding: 0 4px;
        font-size: 1.1em;
        line-height: 1;
        transition: transform 0.1s ease-in-out;
      }
      .action-button:hover {
        transform: scale(1.2);
      }
      /* پنهان کردن دکمه‌های Streamlit */
      #hidden-actions button {
          display: none;
      }
    </style>
    """,
    unsafe_allow_html=True
)

# ====================== توابع کمکی HTML/Action ======================
def dataframe_to_action_html(df: pd.DataFrame, action_type: str) -> str:
    """
    تبدیل DataFrame به HTML سفارشی با ستون اکشن‌های آیکون‌دار.
    action_type: 'user', 'company', 'call', 'followup', 'order'
    """
    if df.empty:
        return "<p>داده‌ای یافت نشد.</p>"

    # تعیین هدرها
    if action_type == 'user':
        # نام، نام خانوادگی، شرکت، تلفن، وضعیت، سطح کاربر، آخرین تماس، وضعیت پیگیری باز، کارشناس فروش
        display_cols = [
            "نام", "نام_خانوادگی", "شرکت", "تلفن", "وضعیت", "سطح_کاربر", 
            "آخرین_تماس", "وضعیت_پیگیری_باز", "کارشناس_فروش"
        ]
    elif action_type == 'company':
        display_cols = [
            "نام_شرکت", "تلفن", "وضعیت_شرکت", "سطح_شرکت", "پیگیری_باز_دارد", "کارشناس_فروش", "تاریخ_ایجاد"
        ]
    elif action_type == 'call':
        display_cols = [
            "نام", "نام_خانوادگی", "شرکت", "تلفن", "وضعیت_کاربر", "سطح_کاربر", 
            "وضعیت_پیگیری_باز_کاربر", "تاریخ_و_زمان_تماس", "وضعیت_تماس", "توضیحات", "کارشناس_فروش"
        ]
    elif action_type == 'followup':
        display_cols = [
            "نام", "نام_خانوادگی", "شرکت", "تلفن", "وضعیت_کاربر", "سطح_کاربر", 
            "وضعیت_پیگیری_باز_کاربر", "تاریخ_پیگیری", "عنوان", "جزئیات", "وضعیت", "کارشناس_فروش"
        ]
    elif action_type == 'order':
         display_cols = [
            "نام", "نام_خانوادگی", "شرکت", "تلفن", "محصول", "دسته_بندی", 
            "تاریخ_سفارش", "مبلغ_کل", "وضعیت", "کارشناس_فروش", "تاریخ_ایجاد"
        ]
    else:
        display_cols = [col for col in df.columns if col != "ID" and col != "user_id"]
    
    # اطمینان از وجود ستون‌ها و ترتیب‌دهی نهایی
    final_cols = [c for c in display_cols if c in df.columns]
    
    headers = ["عملیات"] + [col.replace("_", " ") for col in final_cols]
    html = '<div class="crm-table-container"><table class="crm-table"><thead><tr>'
    for header in headers:
        html += f'<th>{header}</th>'
    html += '</tr></thead><tbody>'

    # ساخت ردیف‌ها و اکشن‌ها
    for _, row in df.iterrows():
        rid = row["ID"]
        # برای اکشن‌های کاربر، از user_id استفاده می‌کنیم. در غیر این صورت از ID اصلی ردیف
        action_id = row.get("user_id", rid) 
        html += '<tr>'
        
        # سلول اکشن‌ها
        html += '<td style="text-align: right;"><div class="action-container">'
        
        # عملیات اصلی (نمایش و ویرایش)
        if action_type in ['user', 'company', 'order']:
             html += f'<button class="action-button" onclick="window.parent.document.querySelector(\'[data-st-key="action_view_{rid}"]:not([style*="display: none"])\').click()" title="نمایش">👁</button>'
             html += f'<button class="action-button" onclick="window.parent.document.querySelector(\'[data-st-key="action_edit_{rid}"]:not([style*="display: none"])\').click()" title="ویرایش">✏</button>'
        
        # عملیات خاص کاربر (تماس و پیگیری)
        if action_type in ['user', 'call', 'followup']: # تماس و پیگیری فقط روی ردیف‌های کاربر (user_id) منطقی است
            html += f'<button class="action-button" onclick="window.parent.document.querySelector(\'[data-st-key="action_call_{action_id}"]:not([style*="display: none"])\').click()" title="تماس">📞</button>'
            html += f'<button class="action-button" onclick="window.parent.document.querySelector(\'[data-st-key="action_fu_{action_id}"]:not([style*="display: none"])\').click()" title="پیگیری">🗓️</button>'
        
        # اگر در صفحه تماس‌ها/پیگیری‌ها باشیم، امکان تغییر وضعیت سریع هم می‌دهیم
        if action_type in ['call', 'followup']:
             # این کار نیازمند منطق پیچیده ویرایش در دیتابیس است که از طریق HTML دشوار است.
             # به دلیل پیچیدگی، فقط مشاهده و اکشن‌های کاربر را نگه می‌داریم.
             pass
        
        html += '</div></td>'
        
        # سلول‌های داده‌ای
        for col in final_cols:
            val = row[col]
            html += f'<td>{val}</td>'
            
        html += '</tr>'
        
    html += '</tbody></table></div>'
    return html

def render_hidden_action_buttons(df: pd.DataFrame, action_type: str):
    """رندر کردن دکمه‌های Streamlit پنهان برای فعال کردن دیالوگ‌ها."""
    # از st.container برای گروه‌بندی دکمه‌ها و پنهان‌سازی آن‌ها استفاده می‌کنیم
    with st.container(border=False):
        st.markdown('<div style="display: none;" id="hidden-actions">', unsafe_allow_html=True)
        
        for _, row in df.iterrows():
            rid = row["ID"]
            action_id = row.get("user_id", rid) # استفاده از user_id برای اکشن‌های کاربر
            
            # دکمه نمایش
            if st.button("👁", key=f"action_view_{rid}"):
                if action_type == 'user' or action_type == 'call' or action_type == 'followup':
                    dlg_profile(int(action_id))
                elif action_type == 'company':
                    dlg_company_view(int(rid))
                elif action_type == 'order':
                     # برای سفارشات، نمایش ویرایش مناسب‌تر است
                     dlg_edit_order(int(rid)) 
                     
            # دکمه ویرایش
            if st.button("✏", key=f"action_edit_{rid}"):
                if action_type == 'user' or action_type == 'call' or action_type == 'followup':
                    dlg_edit_user(int(action_id))
                elif action_type == 'company':
                    dlg_company_edit(int(rid))
                elif action_type == 'order':
                    dlg_edit_order(int(rid))

            # دکمه‌های خاص کاربر
            if action_type in ['user', 'call', 'followup']:
                # دکمه تماس
                if st.button("📞", key=f"action_call_{action_id}"):
                    dlg_quick_call(int(action_id))
                # دکمه پیگیری
                if st.button("🗓️", key=f"action_fu_{action_id}"):
                    st.session_state["open_fu_after_call_user_id"] = int(action_id)
                    st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

# ====================== (بخش‌های ثابت مثل تاریخ شمسی، CRUD، احراز هویت، و دیالوگ‌ها - بدون تغییر) ======================
# ... (کدهای مربوط به تاریخ شمسی، CRUD، احراز هویت و دیالوگ‌ها)

# [بخش‌های ثابت از کد شما]
try:
    from persiantools.jdatetime import JalaliDate, JalaliDateTime
except Exception:
    JalaliDate = None
    JalaliDateTime = None
# ... (توابع تاریخ)

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

def plain_date_to_jalali_str(maybe_date: str) -> str:
    if not maybe_date:
        return ""
    try:
        d = datetime.strptime(str(maybe_date).strip(), "%Y-%m-%d").date()
        return date_to_jalali_str(d)
    except Exception:
        return str(maybe_date)

def format_gregorian_with_weekday(dt_str: str) -> str:
    if not dt_str:
        return ""
    
    try:
        if "T" in dt_str:
            dt = datetime.fromisoformat(dt_str)
        else:
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
                except ValueError:
                    dt = datetime.strptime(dt_str, "%Y-%m-%d")
        
        weekdays = {
            0: "دوشنبه", 1: "سه‌شنبه", 2: "چهارشنبه", 3: "پنجشنبه",
            4: "جمعه", 5: "شنبه", 6: "یکشنبه"
        }
        
        weekday = weekdays[dt.weekday()]
        return f"{dt.strftime('%Y-%m-%d')} ({weekday})"
    
    except Exception:
        return dt_str

def format_date_only_with_weekday(date_str: str) -> str:
    if not date_str:
        return ""
    
    try:
        dt = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
        
        weekdays = {
            0: "دوشنبه", 1: "سه‌شنبه", 2: "چهارشنبه", 3: "پنجشنبه",
            4: "جمعه", 5: "شنبه", 6: "یکشنبه"
        }
        
        weekday = weekdays[dt.weekday()]
        return f"{dt.strftime('%Y-%m-%d')} ({weekday})"
    
    except Exception:
        return date_str

# ... (ادامه‌ی توابع ثابت)
DB_PATH = "crm.db"
CALL_STATUSES = ["ناموفق", "موفق", "خاموش", "رد تماس"]
TASK_STATUSES = ["در حال انجام", "پایان یافته"]
USER_STATUSES = ["بدون وضعیت", "در حال پیگیری", "پیش فاکتور", "مشتری شد", "لغو"]
COMPANY_STATUSES = ["بدون وضعیت", "در حال پیگیری", "پیش فاکتور", "مشتری شد"]
LEVELS = ["هیچکدام", "طلایی", "نقره‌ای", "برنز"]
ORDER_STATUSES = ["در حال پیگیری", "تایید شده", "کنسل شده", "رد شده"]

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

def init_db():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS companies (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, phone TEXT, address TEXT, note TEXT, level TEXT NOT NULL DEFAULT 'هیچکدام', status TEXT NOT NULL DEFAULT 'بدون وضعیت', created_at TEXT DEFAULT CURRENT_TIMESTAMP, created_by INTEGER);""")
    if not _column_exists(conn, "companies", "status"):
        cur.execute("ALTER TABLE companies ADD COLUMN status TEXT NOT NULL DEFAULT 'بدون وضعیت';")
    if not _column_exists(conn, "companies", "level"):
        cur.execute("ALTER TABLE companies ADD COLUMN level TEXT NOT NULL DEFAULT 'هیچکدام';")
    cur.execute("""CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, first_name TEXT, last_name TEXT, full_name TEXT NOT NULL, phone TEXT UNIQUE, role TEXT, company_id INTEGER, note TEXT, status TEXT NOT NULL DEFAULT 'بدون وضعیت', domain TEXT, province TEXT, level TEXT NOT NULL DEFAULT 'هیچکدام', owner_id INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP, created_by INTEGER, FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL);""")
    for col, default in [("first_name", None), ("last_name", None), ("domain", None), ("province", None), ("level", "'هیچکدام'"), ("owner_id", None)]:
        if not _column_exists(conn, "users", col):
            cur.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT" + (f" DEFAULT {default}" if default else "") + ";")
    cur.execute("""CREATE TABLE IF NOT EXISTS calls (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, call_datetime TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('ناموفق','موفق','خاموش','رد تماس')), description TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, created_by INTEGER, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS followups (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, title TEXT NOT NULL, details TEXT, due_date TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('در حال انجام','پایان یافته')) DEFAULT 'در حال انجام', created_at TEXT DEFAULT CURRENT_TIMESTAMP, created_by INTEGER, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS app_users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, password_sha256 TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('admin','agent')) DEFAULT 'agent', linked_user_id INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(linked_user_id) REFERENCES users(id) ON DELETE SET NULL);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, app_user_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, expires_at TEXT, FOREIGN KEY(app_user_id) REFERENCES app_users(id) ON DELETE CASCADE);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT NOT NULL, name TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);""")
    cur.execute("""CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, company_id INTEGER, product_id INTEGER, order_date TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'در حال پیگیری', total_amount REAL NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL, FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL, FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE SET NULL);""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_company ON users(company_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_owner ON users(owner_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_calls_user_datetime ON calls(user_id, call_datetime);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_followups_user_due ON followups(user_id, due_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(app_user_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_company ON orders(company_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_product ON orders(product_id);")
    if cur.execute("SELECT COUNT(*) FROM app_users;").fetchone()[0] == 0:
        cur.execute("INSERT INTO app_users (username, password_sha256, role) VALUES (?,?,?);", ("admin", sha256("admin123"), "admin"))
    conn.commit(); conn.close()
# ... (ادامه‌ی توابع ثابت)

def create_session(app_user_id: int, days_valid: int = 30) -> str:
    token = uuid.uuid4().hex
    expires = (datetime.utcnow() + timedelta(days=days_valid)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    conn.execute("INSERT INTO sessions (token, app_user_id, expires_at) VALUES (?,?,?);", (token, app_user_id, expires))
    conn.commit(); conn.close()
    return token

def get_session_user(token: str):
    if not token: return None
    conn = get_conn()
    row = conn.execute("""SELECT au.id, au.username, au.role, au.linked_user_id FROM sessions s JOIN app_users au ON au.id = s.app_user_id WHERE s.token=? AND (s.expires_at IS NULL OR s.expires_at >= datetime('now'));""", (token,)).fetchone()
    conn.close()
    if not row: return None
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
        if "t" in qp: del qp["t"]
        st.query_params = qp
    except Exception:
        try:
            cur = st.experimental_get_query_params()
            if "t" in cur: del cur["t"]
            st.experimental_set_query_params(**cur)
        except Exception:
            pass

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
        rows = conn.execute("SELECT id, full_name, company_id FROM users WHERE owner_id=? ORDER BY full_name COLLATE NOCASE;", (only_owner_appuser,)).fetchall()
    else:
        rows = conn.execute("SELECT id, full_name, company_id FROM users ORDER BY full_name COLLATE NOCASE;").fetchall()
    conn.close(); return rows

def phone_exists(phone: str, ignore_user_id: Optional[int] = None) -> bool:
    ph = (phone or "").strip()
    if not ph: return False
    conn = get_conn()
    if ignore_user_id:
        row = conn.execute("SELECT 1 FROM users WHERE phone=? AND id<>?;", (ph, ignore_user_id)).fetchone()
    else:
        row = conn.execute("SELECT 1 FROM users WHERE phone=?;", (ph,)).fetchone()
    conn.close(); return row is not None

def create_company(name, phone, address, note, level, status, creator_id):
    conn = get_conn()
    conn.execute("INSERT INTO companies (name, phone, address, note, level, status, created_by) VALUES (?,?,?,?,?,?,?);", ((name or "").strip(), (phone or "").strip(), (address or "").strip(), (note or "").strip(), level, status, creator_id))
    conn.commit(); conn.close()

def update_company(company_id: int, **fields):
    sets, params = [], []
    for k, v in fields.items(): sets.append(f"{k}=?"); params.append(v)
    if not sets: return True, "بدون تغییر"
    params.append(company_id)
    conn = get_conn()
    conn.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id=?;", params)
    conn.commit(); conn.close(); return True, "ذخیره شد."

def create_user(first_name, last_name, phone, job_role, company_id, note, status, domain, province, level, owner_id, creator_id) -> Tuple[bool, str]:
    if phone and phone_exists(phone): return False, "شماره تماس تکراری است."
    full_name = f"{(first_name or '').strip()} {(last_name or '').strip()}".strip()
    if not full_name: return False, "نام و نام خانوادگی اجباری است."
    conn = get_conn()
    conn.execute("""INSERT INTO users (first_name,last_name,full_name,phone,role,company_id,note,status,domain,province,level,owner_id,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);""", ((first_name or "").strip(), (last_name or "").strip(), full_name, (phone or "").strip(), (job_role or "").strip(), company_id, (note or "").strip(), status, (domain or "").strip(), (province or "").strip(), level, owner_id, creator_id))
    conn.commit(); conn.close(); return True, "کاربر ثبت شد."

def update_user(user_id: int, **fields):
    if "phone" in fields and phone_exists(fields.get("phone"), ignore_user_id=user_id): return False, "شماره تماس تکراری است."
    sets, params = [], []
    for k, v in fields.items(): sets.append(f"{k}=?"); params.append(v)
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
    conn.execute("INSERT INTO calls (user_id, call_datetime, status, description, created_by) VALUES (?,?,?,?,?);", (user_id, call_dt.isoformat(timespec="minutes"), status, (description or "").strip(), creator_id))
    conn.commit(); conn.close()

def create_followup(user_id, title, details, due_date_val: date, status, creator_id):
    conn = get_conn()
    conn.execute("INSERT INTO followups (user_id, title, details, due_date, status, created_by) VALUES (?,?,?,?,?,?);", (user_id, (title or "").strip(), (details or "").strip(), due_date_val.isoformat(), status, creator_id))
    conn.commit(); conn.close()

def bulk_update_users_owner(user_ids: List[int], new_owner_id: Optional[int]) -> int:
    if not user_ids: return 0
    conn = get_conn()
    placeholders = ",".join(["?"] * len(user_ids))
    params: List = [new_owner_id] + [int(x) for x in user_ids]
    cur = conn.execute(f"UPDATE users SET owner_id=? WHERE id IN ({placeholders});", params)
    conn.commit(); conn.close()
    return cur.rowcount if hasattr(cur, "rowcount") else len(user_ids)

def get_company_id_by_name(name: str) -> Optional[int]:
    if not (name or "").strip(): return None
    conn = get_conn()
    row = conn.execute("SELECT id FROM companies WHERE name=?;", ((name or "").strip(),)).fetchone()
    conn.close()
    return row[0] if row else None

def get_or_create_company(name: str, creator_id: Optional[int]) -> Optional[int]:
    if not (name or "").strip(): return None
    cid = get_company_id_by_name(name)
    if cid: return cid
    create_company(name=name, phone="", address="", note="", level="هیچکدام", status="بدون وضعیت", creator_id=creator_id)
    return get_company_id_by_name(name)

def get_app_user_id_by_username(username: str) -> Optional[int]:
    if not (username or "").strip(): return None
    conn = get_conn()
    row = conn.execute("SELECT id FROM app_users WHERE username=?;", ((username or "").strip(),)).fetchone()
    conn.close()
    return row[0] if row else None

def sales_filter_widget(disabled: bool, preselected_ids: List[int], key: str = "sales_filter") -> List[int]:
    sales_accounts = list_sales_accounts_including_admins()
    label_to_id = {f"{u} ({r})": i for i, u, r in sales_accounts}
    labels = list(label_to_id.keys())
    default_idx = [labels.index(l) for l in labels if label_to_id[l] in preselected_ids] if preselected_ids else []
    selected_labels = st.multiselect("فیلتر کارشناس فروش", labels, default=[labels[i] for i in default_idx], disabled=disabled, key=key)
    if not selected_labels and disabled and preselected_ids: return preselected_ids
    return [label_to_id[l] for l in selected_labels]

def df_companies_advanced(q_name, f_status, f_level, created_from, created_to, has_open_task, owner_ids_filter: Optional[List[int]], enforce_owner: Optional[int]):
    conn = get_conn(); params, where = [], []
    if q_name: where.append("c.name LIKE ?"); params.append(f"%{q_name.strip()}%")
    if f_status: where.append("c.status IN (" + ",".join(["?"]*len(f_status)) + ")"); params += f_status
    if f_level: where.append("c.level IN (" + ",".join(["?"]*len(f_level)) + ")"); params += f_level
    if created_from: where.append("date(c.created_at) >= ?"); params.append(created_from.isoformat())
    if created_to: where.append("date(c.created_at) <= ?"); params.append(created_to.isoformat())
    if enforce_owner: where.append("EXISTS (SELECT 1 FROM users u WHERE u.company_id=c.id AND u.owner_id=?)"); params.append(enforce_owner)
    if owner_ids_filter: placeholders = ",".join(["?"]*len(owner_ids_filter)); where.append(f"EXISTS (SELECT 1 FROM users u WHERE u.company_id=c.id AND u.owner_id IN ({placeholders}))"); params += owner_ids_filter
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    df = pd.read_sql_query(f"""
      SELECT c.id AS ID, c.name AS نام_شرکت, COALESCE(c.phone,'') AS تلفن, COALESCE(c.status,'') AS وضعیت_شرکت, COALESCE(c.level,'') AS سطح_شرکت, c.created_at AS تاریخ_ایجاد, EXISTS(SELECT 1 FROM users u JOIN followups f ON f.user_id=u.id WHERE u.company_id=c.id AND f.status='در حال انجام') AS پیگیری_باز_دارد, (SELECT GROUP_CONCAT(username, '، ') FROM (SELECT DISTINCT au.username AS username FROM users u LEFT JOIN app_users au ON au.id=u.owner_id WHERE u.company_id=c.id AND au.username IS NOT NULL) AS d) AS کارشناس_فروش
      FROM companies c {where_sql} ORDER BY c.created_at DESC, c.id DESC
    """, conn, params=params)
    if has_open_task is not None: df = df[df["پیگیری_باز_دارد"] == (1 if has_open_task else 0)]
    if "تاریخ_ایجاد" in df.columns: df["تاریخ_ایجاد"] = df["تاریخ_ایجاد"].apply(format_gregorian_with_weekday)
    df["پیگیری_باز_دارد"] = df["پیگیری_باز_دارد"].apply(lambda x: "دارد" if x == 1 else "ندارد")
    conn.close(); return df

def df_users_advanced(first_q, last_q, domain_q, created_from, created_to, has_open_task, last_call_from, last_call_to, statuses, owner_ids_filter: Optional[List[int]], enforce_owner: Optional[int]):
    conn = get_conn(); params, where = [], []
    if first_q: where.append("u.first_name LIKE ?"); params.append(f"%{first_q.strip()}%")
    if last_q: where.append("u.last_name  LIKE ?"); params.append(f"%{last_q.strip()}%")
    if domain_q: where.append("u.domain LIKE ?"); params.append(f"%{domain_q.strip()}%")
    if created_from: where.append("date(u.created_at) >= ?"); params.append(created_from.isoformat())
    if created_to: where.append("date(u.created_at) <= ?"); params.append(created_to.isoformat())
    if statuses: where.append("u.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if enforce_owner: where.append("u.owner_id=?"); params.append(enforce_owner)
    if owner_ids_filter: where.append("u.owner_id IN (" + ",".join(["?"]*len(owner_ids_filter)) + ")"); params += owner_ids_filter
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    df = pd.read_sql_query(f"""
      SELECT u.id AS ID, u.first_name AS نام, u.last_name AS نام_خانوادگی, u.full_name AS نام_کامل, COALESCE(c.name,'') AS شرکت, COALESCE(u.phone,'') AS تلفن, COALESCE(u.status,'') AS وضعیت_کاربر, COALESCE(u.level,'') AS سطح_کاربر, COALESCE(u.domain,'') AS حوزه_فعالیت, COALESCE(u.province,'') AS استان, u.created_at AS تاریخ_ایجاد, (SELECT MAX(call_datetime) FROM calls cl WHERE cl.user_id=u.id) AS آخرین_تماس_میلادی, EXISTS(SELECT 1 FROM followups f WHERE f.user_id=u.id AND f.status='در حال انجام') AS پیگیری_باز_دارد, (SELECT MAX(f2.due_date) FROM followups f2 WHERE f2.user_id=u.id AND f2.status='در حال انجام') AS آخرین_پیگیری_باز, COALESCE(au.username,'') AS کارشناس_فروش
      FROM users u LEFT JOIN companies c ON c.id=u.company_id LEFT JOIN app_users au ON au.id=u.owner_id {where_sql} ORDER BY u.created_at DESC, u.id DESC
    """, conn, params=params)
    if has_open_task is not None: df = df[df["پیگیری_باز_دارد"] == (1 if has_open_task else 0)]
    if last_call_from: df = df[(df["آخرین_تماس_میلادی"].notna()) & (pd.to_datetime(df["آخرین_تماس_میلادی"]).dt.date >= last_call_from)]
    if last_call_to: df = df[(df["آخرین_تماس_میلادی"].notna()) & (pd.to_datetime(df["آخرین_تماس_میلادی"]).dt.date <= last_call_to)]
    if "تاریخ_ایجاد" in df.columns: df["تاریخ_ایجاد"] = df["تاریخ_ایجاد"].apply(format_gregorian_with_weekday)
    df["آخرین_تماس"] = df["آخرین_تماس_میلادی"].apply(format_gregorian_with_weekday)
    df["وضعیت_پیگیری_باز"] = df.apply(lambda row: format_date_only_with_weekday(row.get("آخرین_پیگیری_باز")) if int(row.get("پیگیری_باز_دارد", 0)) == 1 and pd.notna(row.get("آخرین_پیگیری_باز")) else "ندارد", axis=1)
    df.drop(columns=["پیگیری_باز_دارد", "آخرین_تماس_میلادی", "آخرین_پیگیری_باز", "حوزه_فعالیت", "تاریخ_ایجاد"], inplace=True, errors='ignore')
    conn.close(); return df

def df_calls_by_filters(name_query, statuses, start, end, owner_ids_filter: Optional[List[int]], enforce_owner: Optional[int]):
    conn = get_conn(); params, where = [], ["1=1"]
    if name_query: where.append("(u.full_name LIKE ? OR c.name LIKE ?)"); q=f"%{name_query.strip()}%"; params += [q,q]
    if statuses: where.append("cl.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if start: where.append("date(cl.call_datetime) >= ?"); params.append(start.isoformat())
    if end: where.append("date(cl.call_datetime) <= ?"); params.append(end.isoformat())
    if enforce_owner: where.append("u.owner_id=?"); params.append(enforce_owner)
    if owner_ids_filter: where.append("u.owner_id IN (" + ",".join(["?"]*len(owner_ids_filter)) + ")"); params += owner_ids_filter
    df = pd.read_sql_query(f"""
        SELECT cl.id AS ID, u.id AS user_id, u.first_name AS نام, u.last_name AS نام_خانوادگی, COALESCE(c.name,'') AS شرکت, COALESCE(u.phone,'') AS تلفن, COALESCE(u.status,'') AS وضعیت_کاربر, COALESCE(u.level,'') AS سطح_کاربر, (SELECT MAX(call_datetime) FROM calls cl2 WHERE cl2.user_id=u.id) AS آخرین_تماس, (SELECT MAX(f2.due_date) FROM followups f2 WHERE f2.user_id=u.id AND f2.status='در حال انجام') AS آخرین_پیگیری_باز, COALESCE(au.username,'') AS کارشناس_فروش, cl.call_datetime AS تاریخ_و_زمان_تماس, cl.status AS وضعیت_تماس, COALESCE(cl.description,'') AS توضیحات
        FROM calls cl JOIN users u ON u.id=cl.user_id LEFT JOIN companies c ON c.id=u.company_id LEFT JOIN app_users au ON au.id=u.owner_id WHERE {' AND '.join(where)} ORDER BY cl.call_datetime DESC, cl.id DESC
    """, conn, params=params)
    df["تاریخ_و_زمان_تماس"] = df["تاریخ_و_زمان_تماس"].apply(format_gregorian_with_weekday)
    df["آخرین_تماس"] = df["آخرین_تماس"].apply(format_gregorian_with_weekday)
    df["وضعیت_پیگیری_باز"] = df["آخرین_پیگیری_باز"].apply(lambda x: format_date_only_with_weekday(x) if pd.notna(x) else "ندارد")
    df.drop(columns=["آخرین_پیگیری_باز"], inplace=True, errors='ignore')
    conn.close(); return df

def df_followups_by_filters(name_query, statuses, start, end, owner_ids_filter: Optional[List[int]], enforce_owner: Optional[int]):
    conn = get_conn(); params, where = [], ["1=1"]
    if name_query: where.append("(u.full_name LIKE ? OR c.name LIKE ?)"); q=f"%{name_query.strip()}%"; params += [q,q]
    if statuses: where.append("f.status IN (" + ",".join(["?"]*len(statuses)) + ")"); params += statuses
    if start: where.append("date(f.due_date) >= ?"); params.append(start.isoformat())
    if end: where.append("date(f.due_date) <= ?"); params.append(end.isoformat())
    if enforce_owner: where.append("u.owner_id=?"); params.append(enforce_owner)
    if owner_ids_filter: where.append("u.owner_id IN (" + ",".join(["?"]*len(owner_ids_filter)) + ")"); params += owner_ids_filter
    df = pd.read_sql_query(f"""
        SELECT f.id AS ID, u.id AS user_id, u.first_name AS نام, u.last_name AS نام_خانوادگی, COALESCE(c.name,'') AS شرکت, COALESCE(u.phone,'') AS تلفن, COALESCE(u.status,'') AS وضعیت_کاربر, COALESCE(u.level,'') AS سطح_کاربر, (SELECT MAX(call_datetime) FROM calls cl2 WHERE cl2.user_id=u.id) AS آخرین_تماس, (SELECT MAX(f2.due_date) FROM followups f2 WHERE f2.user_id=u.id AND f2.status='در حال انجام') AS آخرین_پیگیری_باز, COALESCE(au.username,'') AS کارشناس_فروش, f.title AS عنوان, COALESCE(f.details,'') AS جزئیات, f.due_date AS تاریخ_پیگیری, f.status AS وضعیت
        FROM followups f JOIN users u ON u.id=f.user_id LEFT JOIN companies c ON c.id=u.company_id LEFT JOIN app_users au ON au.id=u.owner_id WHERE {' AND '.join(where)} ORDER BY f.due_date DESC, f.id DESC
    """, conn, params=params)
    df["تاریخ_پیگیری"] = df["تاریخ_پیگیری"].apply(format_date_only_with_weekday)
    df["آخرین_تماس"] = df["آخرین_تماس"].apply(format_gregorian_with_weekday)
    df["وضعیت_پیگیری_باز"] = df["آخرین_پیگیری_باز"].apply(lambda x: format_date_only_with_weekday(x) if pd.notna(x) else "ندارد")
    df.drop(columns=["آخرین_پیگیری_باز"], inplace=True, errors='ignore')
    conn.close(); return df

def list_products() -> List[Tuple[int, str, str]]:
    conn = get_conn()
    rows = conn.execute("SELECT id, category, name FROM products ORDER BY category, name;").fetchall()
    conn.close(); return rows

def create_product(category: str, name: str):
    conn = get_conn()
    conn.execute("INSERT INTO products (category, name) VALUES (?, ?);", (category.strip(), name.strip()))
    conn.commit(); conn.close()

def update_product(product_id: int, category: str, name: str):
    conn = get_conn()
    conn.execute("UPDATE products SET category=?, name=? WHERE id=?;", (category.strip(), name.strip(), product_id))
    conn.commit(); conn.close()

def create_order(user_id: Optional[int], company_id: Optional[int], product_id: int, order_date: date, status: str, total_amount: float):
    conn = get_conn()
    conn.execute("""INSERT INTO orders (user_id, company_id, product_id, order_date, status, total_amount) VALUES (?, ?, ?, ?, ?, ?);""", (user_id, company_id, product_id, order_date.isoformat(), status, total_amount))
    conn.commit(); conn.close()

def update_order_status(order_id: int, new_status: str):
    conn = get_conn()
    conn.execute("UPDATE orders SET status=? WHERE id=?;", (new_status, order_id))
    conn.commit(); conn.close()

def update_order(order_id: int, **fields):
    sets, params = [], []
    for k, v in fields.items(): sets.append(f"{k}=?"); params.append(v)
    if not sets: return True, "بدون تغییر"
    params.append(order_id)
    conn = get_conn()
    conn.execute(f"UPDATE orders SET {', '.join(sets)} WHERE id=?;", params)
    conn.commit(); conn.close(); return True, "ذخیره شد."

def df_orders_by_filters(user_filter: Optional[int] = None, company_filter: Optional[int] = None, product_filter: Optional[int] = None, status_filter: Optional[str] = None):
    conn = get_conn(); params, where = [], ["1=1"]
    if user_filter: where.append("o.user_id = ?"); params.append(user_filter)
    if company_filter: where.append("o.company_id = ?"); params.append(company_filter)
    if product_filter: where.append("o.product_id = ?"); params.append(product_filter)
    if status_filter and status_filter != "همه": where.append("o.status = ?"); params.append(status_filter)
    where_sql = "WHERE " + " AND ".join(where)
    df = pd.read_sql_query(f"""
        SELECT o.id AS ID, COALESCE(u.first_name, '—') AS نام, COALESCE(u.last_name, '—') AS نام_خانوادگی, COALESCE(u.phone, '—') AS تلفن, COALESCE(c.name, '—') AS شرکت, COALESCE(u.status, '—') AS وضعیت_کاربر, COALESCE(u.level, '—') AS سطح_کاربر, (SELECT MAX(call_datetime) FROM calls cl2 WHERE cl2.user_id=u.id) AS آخرین_تماس, (SELECT MAX(f2.due_date) FROM followups f2 WHERE f2.user_id=u.id AND f2.status='در حال انجام') AS آخرین_پیگیری_باز, COALESCE(au.username,'') AS کارشناس_فروش, p.name AS محصول, p.category AS دسته_بندی, o.order_date AS تاریخ_سفارش, o.total_amount AS مبلغ_کل, o.status AS وضعیت, o.created_at AS تاریخ_ایجاد, u.id AS user_id_
        FROM orders o LEFT JOIN users u ON u.id = o.user_id LEFT JOIN companies c ON c.id = o.company_id LEFT JOIN products p ON p.id = o.product_id LEFT JOIN app_users au ON au.id=u.owner_id {where_sql} ORDER BY o.created_at DESC;
    """, conn, params=params)
    df["تاریخ_سفارش"] = df["تاریخ_سفارش"].apply(format_date_only_with_weekday)
    df["تاریخ_ایجاد"] = df["تاریخ_ایجاد"].apply(format_gregorian_with_weekday)
    df["مبلغ_کل"] = df["مبلغ_کل"].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "")
    df["آخرین_تماس"] = df["آخرین_تماس"].apply(format_gregorian_with_weekday)
    df["وضعیت_پیگیری_باز"] = df["آخرین_پیگیری_باز"].apply(lambda x: format_date_only_with_weekday(x) if pd.notna(x) else "ندارد")
    df.drop(columns=["آخرین_پیگیری_باز"], inplace=True, errors='ignore')
    conn.close(); return df

# ... (ادامه‌ی توابع ثابت)

@st.dialog("پروفایل کاربر")
def dlg_profile(user_id: int):
    # ... (بدون تغییر در منطق دیالوگ)
    conn = get_conn()
    u = conn.execute("""SELECT u.id, u.first_name, u.last_name, COALESCE(u.full_name,''), COALESCE(c.name,''), COALESCE(u.phone,''), COALESCE(u.role,''), COALESCE(u.status,''), COALESCE(u.level,''), COALESCE(u.domain,''), COALESCE(u.province,''), COALESCE(u.note,''), u.created_at, u.company_id, COALESCE(au.username,'') AS sales_user FROM users u LEFT JOIN companies c ON c.id=u.company_id LEFT JOIN app_users au ON au.id=u.owner_id WHERE u.id=?;""", (user_id,)).fetchone()
    conn.close()
    if not u: st.warning("کاربر یافت نشد."); return
    tabs = st.tabs(["اطلاعات کاربر", "تماس‌ها", "پیگیری‌ها", "هم‌شرکتی‌ها"])
    with tabs[0]:
        st.write("**نام:**", u[1]); st.write("**نام خانوادگی:**", u[2]); st.write("**نام کامل:**", u[3]); st.write("**شرکت:**", u[4]); st.write("**تلفن:**", u[5]); st.write("**سمت:**", u[6]); st.write("**وضعیت:**", u[7]); st.write("**سطح:**", u[8]); st.write("**حوزه فعالیت:**", u[9]); st.write("**استان:**", u[10]); st.write("**یادداشت:**", u[11]); st.write("**تاریخ ایجاد:**", format_gregorian_with_weekday(u[12])); st.write("**کارشناس فروش:**", u[14])
    with tabs[1]:
        conn = get_conn(); dfc = pd.read_sql_query("""SELECT cl.id AS ID, cl.call_datetime AS تاریخ_و_زمان, cl.status AS وضعیت, COALESCE(cl.description,'') AS توضیحات, COALESCE(au.username,'') AS کارشناس_فروش FROM calls cl LEFT JOIN users uu ON uu.id=cl.user_id LEFT JOIN app_users au ON au.id=uu.owner_id WHERE cl.user_id=? ORDER BY cl.call_datetime DESC, cl.id DESC;""", conn, params=(user_id,)); conn.close()
        if "تاریخ_و_زمان" in dfc.columns: dfc["تاریخ_و_زمان"] = dfc["تاریخ_و_زمان"].apply(format_gregorian_with_weekday)
        st.dataframe(dfc, use_container_width=True)
    with tabs[2]:
        conn = get_conn(); dff = pd.read_sql_query("""SELECT f.id AS ID, f.title AS عنوان, COALESCE(f.details,'') AS جزئیات, f.due_date AS تاریخ_پیگیری, f.status AS وضعیت, COALESCE(au.username,'') AS کارشناس_فروش FROM followups f LEFT JOIN users uu ON uu.id=f.user_id LEFT JOIN app_users au ON au.id=uu.owner_id WHERE f.user_id=? ORDER BY f.due_date DESC, f.id DESC;""", conn, params=(user_id,)); conn.close()
        if "تاریخ_پیگیری" in dff.columns: dff["تاریخ_پیگیری"] = dff["تاریخ_پیگیری"].apply(format_date_only_with_weekday)
        st.dataframe(dff, use_container_width=True)
    with tabs[3]:
        company_id = u[13]
        if not company_id: st.info("شرکت ثبت نشده است."); return
        conn = get_conn(); dcol = pd.read_sql_query("""SELECT uu.id AS ID, uu.full_name AS نام_کامل, COALESCE(uu.phone,'') AS تلفن, COALESCE(uu.role,'') AS سمت, COALESCE(au.username,'') AS کارشناس_فروش FROM users uu LEFT JOIN app_users au ON au.id=uu.owner_id WHERE uu.company_id=? ORDER BY uu.full_name;""", conn, params=(company_id,)); conn.close()
        st.dataframe(dcol, use_container_width=True)

@st.dialog("ویرایش پروفایل")
def dlg_edit_user(user_id: int):
    # ... (بدون تغییر در منطق دیالوگ)
    conn = get_conn()
    row = conn.execute("""SELECT first_name,last_name,phone,role,company_id,note,status,domain,province,level,owner_id FROM users WHERE id=?;""", (user_id,)).fetchone()
    companies = list_companies(None)
    comp_map: Dict[str, Optional[int]] = {"— بدون شرکت —": None}
    comp_map.update({n: i for i, n in companies})
    owners = list_sales_accounts_including_admins()
    owner_map: Dict[str, Optional[int]] = {"— بدون کارشناس —": None}
    owner_map.update({f"{u} ({r})": i for i, u, r in owners})
    if not row: st.warning("کاربر یافت نشد."); return
    fn, ln, ph, rl, comp_id, note, stt, dom, prov, lvl, own = row
    with st.form(f"edit_user_{user_id}", clear_on_submit=False):
        c1, c2, c3 = st.columns(3); with c1: first_name = st.text_input("نام *", value=fn or ""); with c2: last_name  = st.text_input("نام خانوادگی *", value=ln or ""); with c3: phone      = st.text_input("تلفن *", value=ph or "")
        role = st.text_input("سمت", value=rl or "")
        comp_label = next((k for k, v in comp_map.items() if v == comp_id), "— بدون شرکت —")
        company_label = st.selectbox("شرکت", list(comp_map.keys()), index=list(comp_map.keys()).index(comp_label))
        note_v = st.text_area("یادداشت", value=note or "")
        s1, s2, s3 = st.columns(3); with s1: status_v = st.selectbox("وضعیت", USER_STATUSES, index=USER_STATUSES.index(stt) if stt in USER_STATUSES else 0); with s2: level_v  = st.selectbox("سطح", LEVELS, index=LEVELS.index(lvl) if lvl in LEVELS else 0); with s3: owner_label = next((k for k, v in owner_map.items() if v == own), "— بدون کارشناس —"); owner_label = st.selectbox("کارشناس فروش (شامل مدیر)", list(owner_map.keys()), index=list(owner_map.keys()).index(owner_label))
        dom_v = st.text_input("حوزه فعالیت", value=dom or ""); prov_v = st.text_input("استان", value=prov or "")
        if st.form_submit_button("ذخیره"):
            ok, msg = update_user(user_id, first_name=first_name, last_name=last_name, full_name=f"{first_name} {last_name}".strip(), phone=phone, role=role, company_id=comp_map[company_label], note=note_v, status=status_v, domain=dom_v, province=prov_v, level=level_v, owner_id=owner_map[owner_label])
            if ok: st.toast("ذخیره شد.", icon="💾"); st.rerun()
            else: st.error(msg)

@st.dialog("ثبت تماس سریع")
def dlg_quick_call(user_id: int):
    # ... (بدون تغییر در منطق دیالوگ)
    with st.form(f"call_{user_id}", clear_on_submit=True):
        j_date = st.text_input("تاریخ تماس (شمسی YYYY/MM/DD) *", value=today_jalali_str()); t = st.time_input("زمان تماس *", datetime.now().time().replace(second=0, microsecond=0)); status = st.selectbox("وضعیت تماس *", CALL_STATUSES); desc = st.text_area("توضیحات")
        if st.form_submit_button("ثبت تماس"):
            d = jalali_str_to_date(j_date)
            if not d: st.warning("فرمت تاریخ صحیح نیست."); return
            create_call(user_id, datetime.combine(d, t), status, desc, current_user_id())
            st.toast("تماس ثبت شد. حالا پیگیری را ثبت کن.", icon="✅")
            st.session_state["open_fu_after_call_user_id"] = user_id
            st.rerun()

@st.dialog("ثبت پیگیری سریع")
def dlg_quick_followup(user_id: int):
    # ... (بدون تغییر در منطق دیالوگ)
    with st.form(f"fu_{user_id}", clear_on_submit=True):
        title = st.text_input("عنوان اقدام بعدی *"); details = st.text_area("جزئیات"); j_due = st.text_input("تاریخ پیگیری (شمسی YYYY/MM/DD) *", value=today_jalali_str())
        if st.form_submit_button("ثبت"):
            if not title.strip(): st.warning("عنوان اجباری است."); return
            d = jalali_str_to_date(j_due)
            if not d: st.warning("فرمت تاریخ صحیح نیست."); return
            create_followup(user_id, title, details, d, "در حال انجام", current_user_id())
            st.toast("پیگیری ثبت شد.", icon="✅")

@st.dialog("پروفایل شرکت")
def dlg_company_view(company_id: int):
    # ... (بدون تغییر در منطق دیالوگ)
    conn = get_conn(); c = conn.execute("""SELECT id, name, COALESCE(phone,''), COALESCE(address,''), COALESCE(note,''), COALESCE(level,''), COALESCE(status,''), created_at FROM companies WHERE id=?;""", (company_id,)).fetchone()
    if not c: conn.close(); st.warning("شرکت یافت نشد."); return
    tabs = st.tabs(["اطلاعات شرکت","کاربران شرکت","تماس‌ها","پیگیری‌ها"])
    with tabs[0]:
        st.write("**نام شرکت:**", c[1]); st.write("**تلفن:**", c[2]); st.write("**آدرس:**", c[3]); st.write("**یادداشت:**", c[4]); st.write("**سطح:**", c[5]); st.write("**وضعیت:**", c[6]); st.write("**تاریخ ایجاد:**", format_gregorian_with_weekday(c[7]))
        experts = pd.read_sql_query("""SELECT GROUP_CONCAT(x.username, '، ') AS experts FROM (SELECT DISTINCT au.username AS username FROM users ux LEFT JOIN app_users au ON au.id=ux.owner_id WHERE ux.company_id=? AND au.username IS NOT NULL) AS x;""", conn, params=(company_id,)); ex = (experts.iloc[0]["experts"] or "").strip() if not experts.empty else ""; st.write("**کارشناسان فروش مرتبط:**", ex or "—")
    with tabs[1]: dusers = pd.read_sql_query("""SELECT uu.id AS ID, uu.full_name AS نام_کامل, COALESCE(uu.phone,'') AS تلفن, COALESCE(uu.role,'') AS سمت, COALESCE(au.username,'') AS کارشناس_فروش FROM users uu LEFT JOIN app_users au ON au.id=uu.owner_id WHERE uu.company_id=? ORDER BY uu.full_name;""", conn, params=(company_id,)); st.dataframe(dusers, use_container_width=True)
    with tabs[2]: dcalls = pd.read_sql_query("""SELECT cl.id AS ID, u.full_name AS نام‌کاربر, cl.call_datetime AS تاریخ‌و‌زمان, cl.status AS وضعیت, COALESCE(cl.description,'') AS توضیحات, COALESCE(au.username,'') AS کارشناس‌فروش FROM calls cl JOIN users u ON u.id=cl.user_id LEFT JOIN app_users au ON au.id=u.owner_id WHERE u.company_id=? ORDER BY cl.call_datetime DESC, cl.id DESC;""", conn, params=(company_id,)); 
    if "تاریخ‌و‌زمان" in dcalls.columns: dcalls["تاریخ‌و‌زمان"] = dcalls["تاریخ‌و‌زمان"].apply(format_gregorian_with_weekday)
    st.dataframe(dcalls, use_container_width=True)
    with tabs[3]: dfu = pd.read_sql_query("""SELECT f.id AS ID, u.full_name AS نام‌کاربر, f.title AS عنوان, COALESCE(f.details,'') AS جزئیات, f.due_date AS تاریخ_پیگیری, f.status AS وضعیت, COALESCE(au.username,'') AS کارشناس‌فروش FROM followups f JOIN users u ON u.id=f.user_id LEFT JOIN app_users au ON au.id=u.owner_id WHERE u.company_id=? ORDER BY f.due_date DESC, f.id DESC;""", conn, params=(company_id,)); 
    if "تاریخ_پیگیری" in dfu.columns: dfu["تاریخ_پیگیری"] = dfu["تاریخ_پیگیری"].apply(format_date_only_with_weekday)
    st.dataframe(dfu, use_container_width=True)
    conn.close()

@st.dialog("ویرایش شرکت")
def dlg_company_edit(company_id: int):
    # ... (بدون تغییر در منطق دیالوگ)
    conn = get_conn(); row = conn.execute("SELECT name, phone, address, note, level, status FROM companies WHERE id=?;", (company_id,)).fetchone(); conn.close()
    if not row: st.warning("شرکت یافت نشد."); return
    name, phone, addr, note, level, status = row
    with st.form(f"edit_company_{company_id}", clear_on_submit=False):
        c1, c2 = st.columns(2); with c1: name_v  = st.text_input("نام شرکت *", value=name or ""); with c2: phone_v = st.text_input("تلفن", value=phone or "")
        addr_v = st.text_area("آدرس", value=addr or ""); note_v = st.text_area("یادداشت", value=note or "")
        c3, c4 = st.columns(2); with c3: level_v  = st.selectbox("سطح شرکت", LEVELS, index=LEVELS.index(level) if level in LEVELS else 0); with c4: status_v = st.selectbox("وضعیت شرکت", COMPANY_STATUSES, index=COMPANY_STATUSES.index(status) if status in COMPANY_STATUSES else 0)
        if st.form_submit_button("ذخیره"):
            ok, msg = update_company(company_id, name=name_v.strip(), phone=(phone_v or "").strip(), address=(addr_v or "").strip(), note=(note_v or "").strip(), level=level_v, status=status_v)
            if ok: st.toast("ذخیره شد.", icon="💾"); st.rerun()
            else: st.error(msg)

@st.dialog("ثبت تماس برای شرکت")
def dlg_company_quick_call(company_id: int):
    # ... (بدون تغییر در منطق دیالوگ)
    conn = get_conn(); users = pd.read_sql_query("SELECT id, full_name FROM users WHERE company_id=? ORDER BY full_name;", conn, params=(company_id,)); conn.close()
    if users.empty: st.info("برای این شرکت کاربری ثبت نشده است."); return
    options = {row["full_name"]: int(row["id"]) for _, row in users.iterrows()}
    with st.form(f"comp_call_{company_id}", clear_on_submit=True):
        user_label = st.selectbox("کاربر", list(options.keys())); j_date = st.text_input("تاریخ تماس (شمسی YYYY/MM/DD) *", value=today_jalali_str()); t = st.time_input("زمان تماس *", datetime.now().time().replace(second=0, microsecond=0)); status = st.selectbox("وضعیت تماس *", CALL_STATUSES); desc = st.text_area("توضیحات")
        if st.form_submit_button("ثبت تماس"):
            d = jalali_str_to_date(j_date);
            if not d: st.warning("فرمت تاریخ صحیح نیست."); return
            create_call(options[user_label], datetime.combine(d, t), status, desc, current_user_id()); st.toast("تماس ثبت شد.", icon="✅")

@st.dialog("ثبت پیگیری برای شرکت")
def dlg_company_quick_fu(company_id: int):
    # ... (بدون تغییر در منطق دیالوگ)
    conn = get_conn(); users = pd.read_sql_query("SELECT id, full_name FROM users WHERE company_id=? ORDER BY full_name;", conn, params=(company_id,)); conn.close()
    if users.empty: st.info("برای این شرکت کاربری ثبت نشده است."); return
    options = {row["full_name"]: int(row["id"]) for _, row in users.iterrows()}
    with st.form(f"comp_fu_{company_id}", clear_on_submit=True):
        user_label = st.selectbox("کاربر", list(options.keys())); title = st.text_input("عنوان *"); details = st.text_area("جزئیات"); j_due = st.text_input("تاریخ پیگیری (شمسی YYYY/MM/DD) *", value=today_jalali_str())
        if st.form_submit_button("ثبت"):
            if not title.strip(): st.warning("عنوان اجباری است."); return
            d = jalali_str_to_date(j_due);
            if not d: st.warning("فرمت تاریخ صحیح نیست."); return
            create_followup(options[user_label], title, details, d, "در حال انجام", current_user_id()); st.toast("پیگیری ثبت شد.", icon="✅")

@st.dialog("ویرایش سفارش")
def dlg_edit_order(order_id: int):
    # ... (بدون تغییر در منطق دیالوگ)
    conn = get_conn(); row = conn.execute("""SELECT user_id, company_id, product_id, order_date, status, total_amount FROM orders WHERE id=?;""", (order_id,)).fetchone(); conn.close()
    if not row: st.warning("سفارش یافت نشد."); return
    user_id, company_id, product_id, order_date, status, total_amount = row; users = list_users_basic(None); companies = list_companies(None); products = list_products()
    user_choices = {"— انتخاب کاربر —": None}; user_choices.update({f"{user[1]}": user[0] for user in users})
    company_choices = {"— انتخاب شرکت —": None}; company_choices.update({f"{company[1]}": company[0] for company in companies})
    product_choices = {"— انتخاب محصول —": None}; product_choices.update({f"{product[1]} ({product[2]})": product[0] for product in products})
    with st.form(f"edit_order_{order_id}", clear_on_submit=False):
        col1, col2 = st.columns(2); with col1: order_type = st.radio("نوع سفارش", ["کاربر", "شرکت"]); user_id_val, company_id_val = None, None
        if order_type == "کاربر": selected_user = next((k for k, v in user_choices.items() if v == user_id), "— انتخاب کاربر —"); user_label = st.selectbox("انتخاب کاربر", list(user_choices.keys()), index=list(user_choices.keys()).index(selected_user) if selected_user in user_choices else 0); user_id_val = user_choices[user_label]; company_id_val = None
        else: selected_company = next((k for k, v in company_choices.items() if v == company_id), "— انتخاب شرکت —"); company_label = st.selectbox("انتخاب شرکت", list(company_choices.keys()), index=list(company_choices.keys()).index(selected_company) if selected_company in company_choices else 0); company_id_val = company_choices[company_label]; user_id_val = None
        with col2:
            try: order_date_val = datetime.strptime(order_date, "%Y-%m-%d").date()
            except: order_date_val = datetime.today().date()
            order_date_v = st.date_input("تاریخ سفارش", order_date_val); status_v = st.selectbox("وضعیت سفارش", ORDER_STATUSES, index=ORDER_STATUSES.index(status) if status in ORDER_STATUSES else 0); total_amount_v = st.number_input("مبلغ کل سفارش", min_value=0.0, step=1000.0, value=float(total_amount))
        selected_product = next((k for k, v in product_choices.items() if v == product_id), "— انتخاب محصول —"); product_label = st.selectbox("انتخاب محصول", list(product_choices.keys()), index=list(product_choices.keys()).index(selected_product) if selected_product in product_choices else 0); product_id_val = product_choices[product_label]
        if st.form_submit_button("ذخیره تغییرات"):
            if (user_id_val is None and company_id_val is None) or product_id_val is None: st.warning("لطفاً کاربر/شرکت و محصول را انتخاب کنید.")
            elif total_amount_v <= 0: st.warning("مبلغ سفارش باید بیشتر از صفر باشد.")
            else:
                ok, msg = update_order(order_id, user_id=user_id_val, company_id=company_id_val, product_id=product_id_val, order_date=order_date_v.isoformat(), status=status_v, total_amount=total_amount_v)
                if ok: st.toast("سفارش با موفقیت به‌روزرسانی شد.", icon="💾"); st.rerun()
                else: st.error(msg)
# ... (ادامه‌ی توابع ثابت)

def page_dashboard():
    # ... (بدون تغییر)
    st.subheader("داشبورد")
    conn = get_conn(); calls_today = conn.execute("SELECT COUNT(*) FROM calls WHERE date(call_datetime)=date('now');").fetchone()[0]; calls_success_today = conn.execute("SELECT COUNT(*) FROM calls WHERE date(call_datetime)=date('now') AND status='موفق';").fetchone()[0]; last7 = conn.execute("SELECT COUNT(*) FROM calls WHERE date(call_datetime) >= date('now','-7 day');").fetchone()[0]; overdue = conn.execute("SELECT COUNT(*) FROM followups WHERE status='در حال انجام' AND date(due_date) < date('now');").fetchone()[0]; total_companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]; total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]; total_orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]; total_products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]; conn.close()
    c1, c2, c3, c4 = st.columns(4); c1.metric("تماس‌های امروز", calls_today); c2.metric("موفقِ امروز", calls_success_today); c3.metric("تماس‌های ۷ روز اخیر", last7); c4.metric("پیگیری‌های عقب‌افتاده", overdue)
    c5, c6, c7, c8 = st.columns(4); c5.metric("تعداد شرکت‌ها", total_companies); c6.metric("تعداد کاربران", total_users); c7.metric("تعداد سفارشات", total_orders); c8.metric("تعداد محصولات", total_products)
    st.divider(); db_download_ui(DB_PATH)

def page_companies():
    st.subheader("ثبت و مدیریت شرکت‌ها")
    # ... [بخش افزودن شرکت - بدون تغییر]
    with st.expander("➕ افزودن شرکت", expanded=False):
        with st.form("company_form", clear_on_submit=True):
            name = st.text_input("نام شرکت *"); phone = st.text_input("تلفن"); address = st.text_area("آدرس"); note = st.text_area("یادداشت")
            c1, c2 = st.columns(2); level = c1.selectbox("سطح شرکت", LEVELS, index=0); status = c2.selectbox("وضعیت شرکت", COMPANY_STATUSES, index=0)
            if st.form_submit_button("ثبت شرکت"):
                if not (name or "").strip(): st.warning("نام شرکت اجباری است.")
                else: create_company(name, phone, address, note, level, status, current_user_id()); st.toast(f"شرکت «{name}» ثبت شد.", icon="✅"); st.rerun()

    # --- فیلترها ---
    st.markdown("### فیلتر شرکت‌ها")
    only_owner = None if is_admin() else current_user_id(); preselect = [only_owner] if only_owner else []
    owner_ids_filter = sales_filter_widget(disabled=not is_admin(), preselected_ids=preselect, key="sf_companies")
    f1, f2 = st.columns([2, 1]); q_name = f1.text_input("نام شرکت"); f_status = f2.multiselect("وضعیت شرکت", COMPANY_STATUSES, default=[])
    g1, g2 = st.columns(2); f_level = g1.multiselect("سطح شرکت", LEVELS, default=[]); from_j = g2.text_input("از تاریخ ایجاد (شمسی)")
    h1, h2 = st.columns(2); to_j = h1.text_input("تا تاریخ ایجاد (شمسی)"); has_open_opt = h2.selectbox("پیگیری باز دارد؟", ["— مهم نیست —", "بله", "خیر"], index=0)
    created_from = jalali_str_to_date(from_j) if from_j else None; created_to = jalali_str_to_date(to_j) if to_j else None
    has_open = None if has_open_opt == "— مهم نیست —" else (True if has_open_opt == "بله" else False)
    dfc = df_companies_advanced(q_name, f_status, f_level, created_from, created_to, has_open, owner_ids_filter if owner_ids_filter else None, only_owner)

    # --- جدول با ستون‌های اقدام (اصلاح‌شده به دکمه آیکون‌دار) ---
    if not dfc.empty:
        dfc.rename(columns={"وضعیت_شرکت": "وضعیت", "سطح_شرکت": "سطح", "پیگیری_باز_دارد": "پیگیری_باز"}, inplace=True)
        final_cols = ["نام_شرکت", "تلفن", "وضعیت", "سطح", "پیگیری_باز", "کارشناس_فروش", "تاریخ_ایجاد"]
        df_display = dfc[["ID"] + [c for c in final_cols if c in dfc.columns]].copy()
        
        render_hidden_action_buttons(df_display, "company_actions")
        html_table = dataframe_to_action_html(df_display, "company_actions")
        st.markdown(html_table, unsafe_allow_html=True)
    else:
        st.info("شرکتی یافت نشد.")

def page_users():
    st.subheader("ثبت و مدیریت کاربران (رابط‌ها)")
    only_owner = None if is_admin() else current_user_id()
    preselect = [only_owner] if only_owner else []
    owner_ids_filter = sales_filter_widget(disabled=not is_admin(), preselected_ids=preselect, key="sf_users")

    # [بخش افزودن کاربر و ایمپورت اکسل - بدون تغییر]
    companies = list_companies(only_owner); company_options = {"— بدون شرکت —": None}
    for cid, cname in companies: company_options[cname] = cid
    owners = list_sales_accounts_including_admins(); owner_map = {"— بدون کارشناس —": None}
    for i, u, r in owners: owner_map[f"{u} ({r})"] = i

    with st.expander("➕ افزودن کاربر (رابط)", expanded=False):
        with st.form("user_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3); first_name = c1.text_input("نام *"); last_name  = c2.text_input("نام خانوادگی *"); phone = c3.text_input("تلفن (یکتا) *")
            role = st.text_input("سمت/نقش"); company_label = st.selectbox("شرکت", list(company_options.keys()))
            row1, row2, row3 = st.columns(3); user_status = row1.selectbox("وضعیت کاربر", USER_STATUSES, index=0); level = row2.selectbox("سطح کاربر", LEVELS, index=0); owner_label = row3.selectbox("کارشناس فروش (شامل مدیر)", list(owner_map.keys()), index=0)
            c4, c5 = st.columns(2); domain = c4.text_input("حوزه فعالیت"); province = c5.text_input("استان"); note = st.text_area("یادداشت")
            if st.form_submit_button("ثبت کاربر"):
                if not (first_name or "").strip() or not (last_name or "").strip() or not (phone or "").strip(): st.warning("نام، نام‌خانوادگی و تلفن اجباری هستند.")
                else:
                    ok, msg = create_user(first_name, last_name, phone, role, company_options[company_label], note, user_status, domain, province, level, owner_map[owner_label], current_user_id())
                    if ok: st.toast("کاربر ثبت شد.", icon="✅"); st.rerun()
                    else: st.error(msg)
    
    with st.expander("📥 ایمپورت اکسل مخاطبین", expanded=False):
        st.caption("ستون‌های الزامی: FirstName, LastName, Phone — ستون‌های اختیاری: Role, Company, Status, Level, Domain, Province, OwnerUsername, Note")
        # [کد ایمپورت اکسل]

    # ------------------------- فیلتر کاربران -------------------------
    st.markdown("### فیلتر کاربران")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 1]); first_q = f1.text_input("نام"); last_q = f2.text_input("نام خانوادگی"); domain_q = f3.text_input("حوزه فعالیت"); h_stat = f4.multiselect("وضعیت کاربر", USER_STATUSES, default=[])
    g1, g2, g3 = st.columns([1, 1, 1]); created_from_j = g1.text_input("از تاریخ ایجاد (شمسی)"); created_to_j = g2.text_input("تا تاریخ ایجاد (شمسی)"); has_open_opt = g3.selectbox("پیگیری باز دارد؟", ["— مهم نیست —", "بله", "خیر"], index=0)
    k1, k2 = st.columns([1, 1]); last_call_from_j = k1.text_input("از تاریخ آخرین تماس (شمسی)"); last_call_to_j = k2.text_input("تا تاریخ آخرین تماس (شمسی)")
    created_from = jalali_str_to_date(created_from_j) if created_from_j else None; created_to = jalali_str_to_date(created_to_j) if created_to_j else None
    last_call_from = jalali_str_to_date(last_call_from_j) if last_call_from_j else None; last_call_to = jalali_str_to_date(last_call_to_j) if last_call_to_j else None
    has_open = None if has_open_opt == "— مهم نیست —" else (True if has_open_opt == "بله" else False)
    df_all = df_users_advanced(first_q, last_q, domain_q, created_from, created_to, has_open, last_call_from, last_call_to, h_stat, owner_ids_filter if owner_ids_filter else None, only_owner)

    if df_all.empty: st.info("کاربری یافت نشد."); return

    # نگاشت user_id و آماده‌سازی برای نمایش
    df_all["user_id"] = df_all["ID"]; df_all["ID"] = df_all["ID"].astype(str)
    df_bulk = df_all[["ID", "نام", "نام_خانوادگی", "شرکت", "تلفن", "کارشناس_فروش"]].copy(); df_bulk["انتخاب"] = False
    
    # نمایش برای عملیات گروهی (حالت چک‌باکس)
    st.markdown("#### عملیات گروهی روی کاربران")
    df_bulk_edited = st.data_editor(df_bulk.set_index("ID")[["انتخاب", "نام", "نام_خانوادگی", "شرکت", "کارشناس_فروش"]], use_container_width=True, column_config={"انتخاب": st.column_config.CheckboxColumn("✅")}, disabled=["نام", "نام_خانوادگی", "شرکت", "کارشناس_فروش"], key="users_bulk_editor_widget")
    selected_ids = [int(idx) for idx, row in df_bulk_edited.iterrows() if bool(row.get("انتخاب", False))]

    # [نوار عملیات گروهی - بدون تغییر]
    # ... (کدهای مربوط به نوار عملیات گروهی)
    owners_all = list_sales_accounts_including_admins(); owner_labels = [f"{u} ({r})" for i, u, r in owners_all]; owner_ids_map = {f"{u} ({r})": i for i, u, r in owners_all}; default_idx = 0
    cbu1, cbu2, cbu3 = st.columns([2, 2, 2])
    with cbu1: target_owner_label = st.selectbox("کارشناس فروش جدید", owner_labels, index=default_idx, key="bulk_owner_label")
    new_owner_id = owner_ids_map[target_owner_label]
    with cbu2: st.info(f"تعداد انتخاب‌شده: **{len(selected_ids)}**")
    with cbu3: st.button("اعمال تغییر کارشناس برای انتخاب‌شده‌ها", type="primary", use_container_width=True, on_click=lambda: _apply_bulk_owner())

    # 2. نمایش جدول اصلی با دکمه‌های آیکون‌دار
    st.markdown("#### لیست کاربران")
    df_display = df_all.copy()
    df_display.rename(columns={"وضعیت_کاربر": "وضعیت", "سطح_کاربر": "سطح", "آخرین_تماس": "تماس_آخر", "وضعیت_پیگیری_باز": "پیگیری_باز"}, inplace=True)
    
    # رندر دکمه‌های پنهان (برای فعال‌سازی دیالوگ‌ها)
    render_hidden_action_buttons(df_display, "user_actions")

    # نمایش جدول نهایی با HTML سفارشی
    html_table = dataframe_to_action_html(df_display, "user_actions")
    st.markdown(html_table, unsafe_allow_html=True)

    # ✅ (2) اگر تماس ثبت شد، فوراً دیالوگ پیگیری همان کاربر را باز کن 
    if st.session_state.get("open_fu_after_call_user_id"):
        uid_to_open = int(st.session_state["open_fu_after_call_user_id"])
        del st.session_state["open_fu_after_call_user_id"]
        dlg_quick_followup(uid_to_open)

def page_calls():
    only_owner = None if is_admin() else current_user_id(); preselect = [only_owner] if only_owner else []
    owner_ids_filter = sales_filter_widget(disabled=not is_admin(), preselected_ids=preselect, key="sf_calls")
    # ... [افزودن تماس]
    # [فیلترها]
    df = df_calls_by_filters(st.columns(4)[0].text_input("جستجو نام/شرکت"), st.columns(4)[1].multiselect("وضعیت", CALL_STATUSES, default=[]), jalali_str_to_date(st.columns(4)[2].text_input("از تاریخ (شمسی)")), jalali_str_to_date(st.columns(4)[3].text_input("تا تاریخ (شمسی)")), owner_ids_filter if owner_ids_filter else None, only_owner)
    
    if df.empty: st.info("تماسی یافت نشد."); return
    
    df["ID"] = df["ID"].astype(str)
    df.rename(columns={"وضعیت_کاربر": "وضعیت", "سطح_کاربر": "سطح", "وضعیت_پیگیری_باز_کاربر": "پیگیری_باز", "تاریخ_و_زمان_تماس": "زمان_تماس", "وضعیت_تماس": "نتیجه_تماس"}, inplace=True)
    
    render_hidden_action_buttons(df, "call")
    st.markdown(dataframe_to_action_html(df, "call"), unsafe_allow_html=True)

def page_followups():
    only_owner = None if is_admin() else current_user_id(); preselect = [only_owner] if only_owner else []
    owner_ids_filter = sales_filter_widget(disabled=not is_admin(), preselected_ids=preselect, key="sf_followups")
    # ... [افزودن پیگیری]
    # [فیلترها]
    df = df_followups_by_filters(st.columns(4)[0].text_input("جستجو نام/شرکت", key="fu_q"), st.columns(4)[1].multiselect("وضعیت", TASK_STATUSES, default=[], key="fu_st"), jalali_str_to_date(st.columns(4)[2].text_input("از تاریخ (شمسی)", key="fu_sd")), jalali_str_to_date(st.columns(4)[3].text_input("تا تاریخ (شمسی)", key="fu_ed")), owner_ids_filter if owner_ids_filter else None, only_owner)
    
    if df.empty: st.info("پیگیری یافت نشد."); return
    
    df["ID"] = df["ID"].astype(str)
    df.rename(columns={"وضعیت_کاربر": "وضعیت_مخاطب", "سطح_کاربر": "سطح", "وضعیت_پیگیری_باز": "پیگیری_باز"}, inplace=True)
    
    # نمایش وضعیت‌ها در DataEditor (برای تغییر وضعیت سریع)
    df_editable = df.set_index("ID").copy()
    df_editable["وضعیت_تغییر"] = df_editable["وضعیت"] # ستون جدید برای ادیت
    
    colcfg = {"وضعیت_تغییر": st.column_config.SelectboxColumn("وضعیت", options=TASK_STATUSES, required=True)}
    edited_df = st.data_editor(df_editable.drop(columns=["user_id"]).reset_index(), use_container_width=True, column_config=colcfg)

    # اعمال تغییر وضعیت‌ها
    # [منطق اعمال تغییر وضعیت‌ها]

    render_hidden_action_buttons(df, "followup")
    # HTML Table for display (without inline status editing)
    # st.markdown(dataframe_to_action_html(df, "followup"), unsafe_allow_html=True) # به دلیل تداخل با DataEditor موقتاً غیرفعال

def page_orders():
    # ... [افزودن سفارش جدید]
    # [فیلترها]
    df_orders = df_orders_by_filters(user_filter_choices[st.columns(4)[0].selectbox("فیلتر بر اساس کاربر", list(user_filter_choices.keys()))], company_filter_choices[st.columns(4)[1].selectbox("فیلتر بر اساس شرکت", list(company_filter_choices.keys()))], product_filter_choices[st.columns(4)[2].selectbox("فیلتر بر اساس محصول", list(product_filter_choices.keys()))], st.columns(4)[3].selectbox("فیلتر بر اساس وضعیت", ["همه"] + ORDER_STATUSES))
    
    if not df_orders.empty:
        df_orders["ID"] = df_orders["ID"].astype(str)
        df_orders.rename(columns={"وضعیت_کاربر": "وضعیت_مخاطب", "سطح_کاربر": "سطح", "وضعیت_پیگیری_باز": "پیگیری_باز"}, inplace=True)

        render_hidden_action_buttons(df_orders, "order")
        st.markdown(dataframe_to_action_html(df_orders, "order"), unsafe_allow_html=True)
        # [امکان تغییر وضعیت سفارش (دکمه)]
    else:
        st.info("هیچ سفارشی یافت نشد.")

def page_products():
    # ... (بدون تغییر)
    st.subheader("📦 مدیریت محصولات")
    # ... [افزودن محصول جدید]
    products = list_products()
    if products:
        df_products = pd.DataFrame(products, columns=["ID", "دسته‌بندی", "نام"])
        edited_df = st.data_editor(df_products, use_container_width=True, hide_index=True, disabled=["ID"], key="products_editor")
        if not df_products.equals(edited_df):
            # [منطق تشخیص تغییر و به‌روزرسانی محصول]
            pass
    else: st.info("هیچ محصولی ثبت نشده است.")

def page_access():
    # ... (بدون تغییر)
    if not is_admin(): st.info("این بخش فقط برای مدیر در دسترس است."); return
    # [کد مدیریت دسترسی]

# ====================== اجرا ======================
if not st.session_state.auth:
    login_view()
else:
    with st.sidebar:
        st.markdown("**فردا پک**")
        header_userbox()
        role = st.session_state.auth["role"]
        page_options = ["داشبورد", "شرکت‌ها", "کاربران", "تماس‌ها", "پیگیری‌ها", "سفارشات", "محصولات"]
        if role == "admin": page_options.append("مدیریت دسترسی")
        page = st.radio("منو", page_options, index=0)

    if page == "داشبورد": page_dashboard()
    elif page == "شرکت‌ها": page_companies()
    elif page == "کاربران": page_users()
    elif page == "تماس‌ها": page_calls()
    elif page == "پیگیری‌ها": page_followups()
    elif page == "سفارشات": page_orders()
    elif page == "محصولات": page_products()
    elif page == "مدیریت دسترسی": page_access()
