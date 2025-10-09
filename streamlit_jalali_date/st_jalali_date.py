import os
import streamlit as st
import streamlit.components.v1 as components

_COMPONENT_NAME = "streamlit_jalali_date"
_parent = os.path.dirname(os.path.abspath(__file__))
_build_dir = os.path.join(_parent, "frontend", "build")

if not os.path.exists(_build_dir):
    raise RuntimeError(
        "فولدر build برای کامپوننت پیدا نشد. باید در مسیر "
        "`streamlit_jalali_date/frontend/build` کپی شود."
    )

_component = components.declare_component(_COMPONENT_NAME, path=_build_dir)

def jalali_date_input(
    label: str,
    key: str = None,
    value: str = "",
    format: str = "YYYY/MM/DD",
    min: str = None,
    max: str = None,
    disabled: bool = False,
    placeholder: str = "",
):
    returned = _component(
        label=label,
        value=value or "",
        format=format,
        min=min,
        max=max,
        disabled=disabled,
        placeholder=placeholder,
        key=key,
        default=value or "",
    )
    return returned or ""
