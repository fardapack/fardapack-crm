import json, os
import streamlit as st
import streamlit.components.v1 as components

# مسیر فولدر build کنار همین فایل
_build_dir = os.path.join(os.path.dirname(__file__), "frontend", "build")
_component = components.declare_component(
    "jalali_date", path=_build_dir
)

def jalali_date_input(label: str, key: str, value: str = "", format: str = "YYYY/MM/DD"):
    """
    یک ورودی تاریخ شمسی بسیار ساده.
    - value مثل '1403/07/18'
    - خروجی هم همان رشته (یا None اگر کاربر چیزی وارد نکرد)
    """
    return _component(label=label, key=key, default=value, fmt=format)
