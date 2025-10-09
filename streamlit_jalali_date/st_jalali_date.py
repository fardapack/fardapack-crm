# -*- coding: utf-8 -*-
import os
from pathlib import Path
import streamlit as st
import streamlit.components.v1 as components

# مسیر فولدر build
_build_dir = Path(__file__).parent / "frontend" / "build"
_component_func = components.declare_component(
    "jalali_date_input",
    path=str(_build_dir),
)

def jalali_date_input(label: str,
                      value: str | None = None,
                      key: str | None = None,
                      placeholder: str = "",
                      disabled: bool = False) -> str | None:
    """
    ورودی ساده‌ی تاریخ شمسی (فرمت: YYYY/MM/DD)
    - label: لیبل فیلد
    - value: مقدار اولیه (مثال '1403/07/18')
    - placeholder: متن راهنما
    - disabled: غیرفعال شدن فیلد
    خروجی: رشته‌ی تاریخ شمسی یا None
    """
    return _component_func(
        label=label,
        value=value or "",
        placeholder=placeholder,
        disabled=bool(disabled),
        default=value or "",
        key=key,
    )
