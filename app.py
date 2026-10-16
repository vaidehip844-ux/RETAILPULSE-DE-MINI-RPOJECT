"""
RetailPulse - Real-Time Retail Analytics and Data Engineering Platform
Streamlit Executive Dashboard, Live Stream Operations, and Data Engineering Workbench
"""
import os
import sys
import time
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import requests

# Ensure project root is in python path
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config import (
    FLASK_HOST, FLASK_PORT, RAW_DATA_DIR, PROCESSED_DATA_DIR,
    ERRORS_DATA_DIR, REPORTS_DIR, LOGS_DIR, WOOCOMMERCE_WEBHOOK_SECRET
)
from database.connection import get_engine, get_db, get_db_type, check_db_status
from database.models import Order, OrderItem, Customer, Product, StagingSales, PipelineLog, DataQualityResult
from database.views import query_view
from streaming.broker import get_broker_status, broker_manager, publish_sales_event
from streaming.producer import (
    start_simulator_background, stop_simulator_background, get_simulator_status, SalesEventProducer
)
from streaming.consumer import (
    start_consumer_background, stop_consumer_background, get_consumer_status
)
from analytics.data_cube import DataCubeExplorer
from analytics.sql_analytics import SQLAnalytics
from processing.cleaning import DataCleaner
from processing.etl import etl_pipeline
from validation.quality_checker import data_quality_checker
from workflows.runner import trigger_dag_run, get_dag_status, get_dag_graph_structure

# Page Configuration
st.set_page_config(
    page_title="Retail Dashboard | RetailPulse",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling matching Executive Retail Analytics Theme
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;600;700;800;900&family=JetBrains+Mono:wght@500;600;700&display=swap');

    html, body, [class*="css"], .stApp {
        font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 16px !important;
        color: #0f172a !important;
        font-weight: 500 !important;
    }

    /* Overall Canvas */
    .stApp {
        background-color: #f1f5f9 !important;
        color: #0f172a !important;
    }

    /* Base Typography & High-Contrast Dark Text */
    p, span, div, label, li, td, th {
        color: #0f172a;
    }

    .stMarkdown, .stMarkdown p, .stMarkdown li, .stMarkdown span {
        color: #0f172a !important;
        font-size: 15.5px !important;
        line-height: 1.6 !important;
        font-weight: 500 !important;
    }

    .stMarkdown strong, .stMarkdown b, b, strong {
        color: #020617 !important;
        font-weight: 800 !important;
    }

    /* Captions - Dark Slate and Legible */
    .stCaption, [data-testid="stCaptionContainer"] p, small, caption {
        color: #1e293b !important;
        font-size: 14px !important;
        font-weight: 600 !important;
    }

    /* Headings */
    h1, [data-testid="stHeadingWithActionElements"] h1 {
        font-size: 30px !important;
        font-weight: 800 !important;
        color: #0f172a !important;
        letter-spacing: -0.02em !important;
    }
    h2, [data-testid="stHeadingWithActionElements"] h2 {
        font-size: 26px !important;
        font-weight: 800 !important;
        color: #0f172a !important;
    }
    h3, [data-testid="stHeadingWithActionElements"] h3 {
        font-size: 22px !important;
        font-weight: 800 !important;
        color: #0f172a !important;
    }
    h4, [data-testid="stHeadingWithActionElements"] h4 {
        font-size: 18px !important;
        font-weight: 800 !important;
        color: #0f172a !important;
    }
    h5, [data-testid="stHeadingWithActionElements"] h5 {
        font-size: 16px !important;
        font-weight: 700 !important;
        color: #0f172a !important;
    }

    /* Streamlit Metrics */
    [data-testid="stMetricLabel"] p {
        font-size: 15px !important;
        font-weight: 700 !important;
        color: #1e293b !important;
    }
    [data-testid="stMetricValue"] div {
        font-size: 28px !important;
        font-weight: 900 !important;
        color: #0f172a !important;
    }

    /* Interactive Dark Top Navbar */
    .top-navbar-wrapper {
        background-color: #0f172a !important;
        color: #ffffff !important;
        padding: 10px 24px !important;
        border-radius: 0 0 12px 12px !important;
        margin: -4rem -4rem 1.5rem -4rem !important;
        box-shadow: 0 4px 16px rgba(0,0,0,0.18) !important;
    }
    .top-navbar-wrapper [data-testid="stPopover"] > button {
        background-color: rgba(255, 255, 255, 0.12) !important;
        color: #ffffff !important;
        border: 1.5px solid rgba(255, 255, 255, 0.25) !important;
        border-radius: 8px !important;
        font-size: 14.5px !important;
        font-weight: 700 !important;
        padding: 6px 14px !important;
        height: 40px !important;
        margin-top: 2px !important;
        transition: all 0.2s ease !important;
    }
    .top-navbar-wrapper [data-testid="stPopover"] > button:hover {
        background-color: rgba(255, 255, 255, 0.22) !important;
        border-color: #38bdf8 !important;
        color: #ffffff !important;
    }
    .top-navbar-wrapper [data-testid="stPopover"] > button p {
        color: #ffffff !important;
        font-weight: 700 !important;
        font-size: 14.5px !important;
    }

    /* Popover Body Content */
    [data-testid="stPopoverBody"] {
        background-color: #ffffff !important;
        border: 1.5px solid #cbd5e1 !important;
        border-radius: 12px !important;
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.2) !important;
        padding: 18px !important;
        color: #0f172a !important;
        min-width: 360px !important;
        max-width: 440px !important;
    }
    [data-testid="stPopoverBody"] p,
    [data-testid="stPopoverBody"] span,
    [data-testid="stPopoverBody"] label,
    [data-testid="stPopoverBody"] div,
    [data-testid="stPopoverBody"] h4 {
        color: #0f172a !important;
        font-size: 14.5px !important;
    }
    [data-testid="stPopoverBody"] h4 {
        font-size: 18px !important;
        font-weight: 800 !important;
    }
    .user-avatar {
        background-color: #0d9488;
        color: #ffffff;
        font-weight: 800;
        font-size: 15px;
        width: 36px;
        height: 36px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    /* Vibrant Gradient Subheader Banner */
    .retail-hero-banner {
        background: linear-gradient(90deg, #0f4c5c 0%, #16697a 45%, #1f8a84 80%, #2ec4b6 100%);
        color: #ffffff;
        padding: 18px 26px;
        border-radius: 12px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 22px;
        box-shadow: 0 4px 16px rgba(15, 76, 92, 0.3);
    }
    .retail-hero-title {
        font-size: 24px;
        font-weight: 800;
        letter-spacing: -0.01em;
        color: #ffffff;
    }
    .retail-hero-filters {
        display: flex;
        gap: 12px;
    }
    .filter-pill-btn {
        background: #ffffff;
        color: #0f172a;
        border: 1px solid rgba(0,0,0,0.08);
        border-radius: 8px;
        padding: 8px 16px;
        font-size: 14px;
        font-weight: 700;
        display: flex;
        align-items: center;
        gap: 8px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.1);
    }

    /* Clean Card Titles */
    .content-card-title {
        font-size: 18px !important;
        font-weight: 800 !important;
        color: #0f172a !important;
        margin-bottom: 14px !important;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .content-card-title span {
        font-size: 18px !important;
        font-weight: 800 !important;
        color: #0f172a !important;
    }

    /* KPI Cards */
    .kpi-pill-white {
        background: #ffffff;
        border: 1.5px solid #cbd5e1;
        border-radius: 12px;
        padding: 16px 20px;
        display: flex;
        align-items: center;
        gap: 16px;
        margin-bottom: 14px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    .kpi-pill-white .kpi-label { font-size: 14px !important; font-weight: 700 !important; color: #1e293b !important; }
    .kpi-pill-white .kpi-val { font-size: 26px !important; font-weight: 900 !important; color: #0f172a !important; }

    .kpi-pill-blue {
        background: linear-gradient(135deg, #1d6fbd, #2563eb);
        color: #ffffff !important;
        border-radius: 12px;
        padding: 16px 20px;
        display: flex;
        align-items: center;
        gap: 16px;
        margin-bottom: 14px;
        box-shadow: 0 4px 14px rgba(37, 99, 235, 0.25);
    }
    .kpi-pill-blue .kpi-label { font-size: 14px !important; font-weight: 700 !important; color: #ffffff !important; opacity: 0.95; }
    .kpi-pill-blue .kpi-val { font-size: 26px !important; font-weight: 900 !important; color: #ffffff !important; }

    .kpi-pill-green {
        background: linear-gradient(135deg, #15803d, #22c55e);
        color: #ffffff !important;
        border-radius: 12px;
        padding: 16px 20px;
        display: flex;
        align-items: center;
        gap: 16px;
        margin-bottom: 14px;
        box-shadow: 0 4px 14px rgba(34, 197, 94, 0.25);
    }
    .kpi-pill-green .kpi-label { font-size: 14px !important; font-weight: 700 !important; color: #ffffff !important; opacity: 0.95; }
    .kpi-pill-green .kpi-val { font-size: 26px !important; font-weight: 900 !important; color: #ffffff !important; }

    .kpi-pill-teal {
        background: linear-gradient(135deg, #0f766e, #0284c7);
        color: #ffffff !important;
        border-radius: 12px;
        padding: 16px 20px;
        display: flex;
        align-items: center;
        gap: 16px;
        margin-bottom: 16px;
        box-shadow: 0 4px 14px rgba(13, 148, 136, 0.25);
    }
    .kpi-pill-teal .kpi-label { font-size: 14px !important; font-weight: 700 !important; color: #ffffff !important; opacity: 0.95; }
    .kpi-pill-teal .kpi-val { font-size: 26px !important; font-weight: 900 !important; color: #ffffff !important; }

    .kpi-icon-square {
        font-size: 26px !important;
        width: 48px !important;
        height: 48px !important;
        border-radius: 10px !important;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    /* Uniform Stream & Broker Metric Cards (Equal Height, Perfectly Aligned) */
    .stream-stat-card {
        background: #ffffff;
        border: 1.5px solid #cbd5e1;
        border-radius: 12px;
        padding: 12px 14px;
        display: flex;
        align-items: center;
        gap: 12px;
        height: 86px;
        min-height: 86px;
        box-sizing: border-box;
        box-shadow: 0 2px 6px rgba(0,0,0,0.03);
        margin-bottom: 10px;
        width: 100%;
        overflow: hidden;
    }
    .stream-stat-card:hover {
        border-color: #94a3b8;
        box-shadow: 0 4px 12px rgba(0,0,0,0.06);
    }
    .stream-stat-icon {
        width: 42px;
        height: 42px;
        min-width: 42px;
        max-width: 42px;
        border-radius: 10px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 20px;
        flex-shrink: 0;
    }
    .stream-stat-content {
        display: flex;
        flex-direction: column;
        justify-content: center;
        min-width: 0;
        flex: 1;
        overflow: hidden;
    }
    .stream-stat-label {
        font-size: 12px !important;
        font-weight: 700 !important;
        color: #475569 !important;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        margin-bottom: 2px;
    }
    .stream-stat-val {
        font-size: 19px !important;
        font-weight: 900 !important;
        color: #0f172a !important;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        display: flex;
        align-items: baseline;
        gap: 4px;
        line-height: 1.2;
    }
    .stream-stat-val .unit {
        font-size: 12px !important;
        font-weight: 600 !important;
        color: #64748b !important;
    }

    /* Key Performances Card */
    .key-performance-card {
        background: #ffffff;
        border: 1.5px solid #cbd5e1;
        border-radius: 12px;
        padding: 22px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
    }
    .kp-title {
        font-size: 18px !important;
        font-weight: 800 !important;
        color: #0f172a !important;
        margin-bottom: 16px;
    }
    .kp-section {
        margin-bottom: 16px;
    }
    .kp-label {
        font-size: 14px !important;
        font-weight: 700 !important;
        color: #1e293b !important;
        margin-bottom: 4px;
    }
    .kp-value {
        font-size: 32px !important;
        font-weight: 900 !important;
        color: #0f172a !important;
        letter-spacing: -0.02em;
    }

    /* Deep Navy Sidebar with Modern Nav */
    [data-testid="stSidebar"] {
        background: #0f172a !important;
        border-right: 1.5px solid rgba(255,255,255,0.12);
    }
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] div,
    [data-testid="stSidebar"] label {
        color: #f1f5f9 !important;
        font-size: 14.5px !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] > label {
        display: none !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] {
        display: flex !important;
        flex-direction: column !important;
        gap: 8px !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] > div:first-child,
    [data-testid="stSidebar"] [data-testid="stRadio"] label > div:first-child {
        display: none !important;
        visibility: hidden !important;
        width: 0 !important;
        height: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"],
    [data-testid="stSidebar"] [data-testid="stRadio"] label {
        display: flex !important;
        align-items: center !important;
        background: rgba(255, 255, 255, 0.06) !important;
        border: 1px solid rgba(255, 255, 255, 0.12) !important;
        border-radius: 8px !important;
        padding: 12px 16px !important;
        margin: 0 !important;
        cursor: pointer !important;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
        width: 100% !important;
        box-sizing: border-box !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
        background: rgba(255, 255, 255, 0.12) !important;
        border-color: rgba(56, 189, 248, 0.5) !important;
        transform: translateX(4px) !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label p {
        font-size: 15.5px !important;
        font-weight: 700 !important;
        color: #e2e8f0 !important;
        margin: 0 !important;
        padding: 0 !important;
        transition: color 0.2s ease !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label:hover p {
        color: #ffffff !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) {
        background: linear-gradient(90deg, #0d9488 0%, #0284c7 100%) !important;
        border: 1.5px solid #38bdf8 !important;
        border-left: 5px solid #67e8f9 !important;
        box-shadow: 0 4px 16px rgba(13, 148, 136, 0.45) !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) p {
        color: #ffffff !important;
        font-weight: 800 !important;
        font-size: 16px !important;
    }

    /* Tabs refinement */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #ffffff;
        padding: 6px;
        border-radius: 10px;
        border: 1.5px solid #cbd5e1;
        box-shadow: 0 2px 6px rgba(0,0,0,0.04);
    }
    .stTabs [data-baseweb="tab"] {
        height: 42px !important;
        padding: 0 18px !important;
        border-radius: 8px !important;
        color: #1e293b !important;
        font-weight: 700 !important;
        font-size: 14.5px !important;
        border: none;
    }
    .stTabs [data-baseweb="tab"] p {
        font-size: 14.5px !important;
        font-weight: 700 !important;
        color: #1e293b !important;
    }
    .stTabs [aria-selected="true"] {
        background: #0d9488 !important;
        color: #ffffff !important;
    }
    .stTabs [aria-selected="true"] p {
        color: #ffffff !important;
        font-weight: 800 !important;
    }

    /* Container Cards */
    [data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #ffffff !important;
        border: 1.5px solid #cbd5e1 !important;
        border-radius: 12px !important;
        padding: 20px 24px !important;
        box-shadow: 0 2px 10px rgba(0,0,0,0.04) !important;
        margin-bottom: 1.2rem !important;
    }

    /* Form Controls, Selectboxes & Dropdowns */
    div[data-baseweb="select"] {
        background-color: #ffffff !important;
        border-radius: 8px !important;
    }
    div[data-baseweb="select"] > div {
        background-color: #ffffff !important;
        color: #0f172a !important;
        border: 1.5px solid #cbd5e1 !important;
        border-radius: 8px !important;
        font-size: 15px !important;
        font-weight: 600 !important;
    }
    div[data-baseweb="select"] span {
        color: #0f172a !important;
        font-weight: 700 !important;
        font-size: 15px !important;
    }
    div[data-baseweb="select"] svg {
        fill: #0f172a !important;
    }

    /* Select Dropdown Popover and Menu Items */
    div[data-baseweb="popover"],
    div[data-baseweb="popover"] > div,
    ul[data-baseweb="menu"] {
        background-color: #ffffff !important;
        color: #0f172a !important;
        border: 1.5px solid #cbd5e1 !important;
        border-radius: 8px !important;
        box-shadow: 0 14px 32px rgba(0, 0, 0, 0.16) !important;
    }
    li[data-baseweb="menu-item"],
    div[data-baseweb="menu-item"] {
        background-color: #ffffff !important;
        color: #0f172a !important;
        font-size: 14.5px !important;
        font-weight: 600 !important;
        padding: 10px 16px !important;
    }
    li[data-baseweb="menu-item"]:hover,
    div[data-baseweb="menu-item"]:hover {
        background-color: #f1f5f9 !important;
        color: #0d9488 !important;
        font-weight: 700 !important;
    }

    /* Inputs */
    .stTextInput input, .stNumberInput input, .stDateInput input, .stTextArea textarea {
        background-color: #ffffff !important;
        color: #0f172a !important;
        font-size: 15px !important;
        font-weight: 600 !important;
        border: 1.5px solid #cbd5e1 !important;
        border-radius: 8px !important;
    }
    .stTextInput label, .stSelectbox label, .stMultiSelect label, .stDateInput label, .stFileUploader label, .stTextArea label, .stNumberInput label {
        color: #0f172a !important;
        font-weight: 800 !important;
        font-size: 15px !important;
    }

    /* Buttons */
    .stButton > button {
        font-size: 15px !important;
        font-weight: 700 !important;
        border-radius: 8px !important;
        padding: 8px 18px !important;
    }

    /* Dataframe and Tables */
    [data-testid="stDataFrame"] {
        background-color: #ffffff !important;
        border: 1.5px solid #cbd5e1 !important;
        border-radius: 8px !important;
        font-size: 14px !important;
    }
    [data-testid="stDataFrame"] div, [data-testid="stDataFrame"] span {
        color: #0f172a !important;
        font-weight: 500 !important;
    }

    /* Code Blocks and Pre Containers: High-Contrast Monospace with Bright White Text */
    [data-testid="stCode"], 
    [data-testid="stCode"] pre,
    [data-testid="stCode"] code,
    [data-testid="stCode"] span,
    [data-testid="stCode"] div,
    .stCodeBlock,
    .stCodeBlock pre,
    .stCodeBlock code,
    .stCodeBlock span,
    .stCodeBlock div,
    pre, 
    pre code,
    pre span,
    pre div {
        font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
        font-size: 14px !important;
        color: #f8fafc !important;
        background-color: #0f172a !important;
        line-height: 1.55 !important;
        font-weight: 600 !important;
    }

    [data-testid="stCode"], .stCodeBlock, pre {
        background-color: #0f172a !important;
        border: 1.5px solid #334155 !important;
        border-radius: 8px !important;
        padding: 12px 16px !important;
    }

    /* Clean Inline Code Badges */
    :not(pre) > code {
        background-color: #e2e8f0 !important;
        color: #0f172a !important;
        border: 1px solid #cbd5e1 !important;
        border-radius: 5px !important;
        padding: 2px 7px !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 13.5px !important;
        font-weight: 700 !important;
    }

    /* Exception & Traceback Containers: High-Contrast Light Red Theme (No Dark-on-Dark Black Boxes) */
    [data-testid="stException"],
    div[data-testid="stException"],
    .stException {
        background-color: #fef2f2 !important;
        border: 2px solid #ef4444 !important;
        border-radius: 10px !important;
        padding: 16px 20px !important;
        box-shadow: 0 4px 14px rgba(239, 68, 68, 0.12) !important;
    }
    [data-testid="stException"] *,
    div[data-testid="stException"] *,
    .stException * {
        color: #991b1b !important;
        background-color: transparent !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 14px !important;
        font-weight: 600 !important;
        line-height: 1.55 !important;
    }
    [data-testid="stException"] pre,
    div[data-testid="stException"] pre {
        background-color: #fee2e2 !important;
        border: 1px solid #fca5a5 !important;
        border-radius: 6px !important;
        padding: 12px 16px !important;
        color: #991b1b !important;
        overflow-x: auto !important;
    }
</style>
""", unsafe_allow_html=True)


def apply_reference_light_theme(fig):
    """Applies clean styling matching the executive retail analytics theme with dark, large, clear fonts."""
    fig.update_layout(
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        font=dict(family="Plus Jakarta Sans, sans-serif", color="#0f172a", size=14),
        margin=dict(l=10, r=10, t=25, b=20),
        legend=dict(
            bgcolor="rgba(255, 255, 255, 0.95)",
            bordercolor="#cbd5e1",
            borderwidth=1,
            font=dict(color="#0f172a", size=13)
        ),
        xaxis=dict(
            gridcolor="#f1f5f9",
            zerolinecolor="#cbd5e1",
            tickfont=dict(color="#0f172a", size=13)
        ),
        yaxis=dict(
            gridcolor="#f1f5f9",
            zerolinecolor="#cbd5e1",
            tickfont=dict(color="#0f172a", size=13)
        )
    )
    return fig


def check_flask_api_status():
    """Quick probe to verify if Flask REST API is listening."""
    try:
        res = requests.get(f"http://{FLASK_HOST}:{FLASK_PORT}/health", timeout=0.6)
        if res.status_code == 200:
            return {"online": True, "details": "HTTP 200 OK"}
        return {"online": False, "details": f"HTTP {res.status_code}"}
    except Exception:
        return {"online": False, "details": "Offline (Start: python -m api.app)"}


# User Accounts Database
USERS_DB = {
    "admin": {
        "password": "admin",
        "name": "Michael Anderson",
        "email": "m.anderson@retailpulse.io",
        "role": "Admin",
        "avatar": "M"
    },
    "analyst": {
        "password": "analyst",
        "name": "Elena Rostova",
        "email": "e.rostova@retailpulse.io",
        "role": "Analyst",
        "avatar": "E"
    },
    "guest": {
        "password": "guest",
        "name": "Guest Analyst",
        "email": "guest@retailpulse.io",
        "role": "Analyst",
        "avatar": "G"
    }
}


def get_current_user():
    """Returns the currently logged-in user profile dictionary."""
    if "current_user" not in st.session_state:
        st.session_state["logged_in"] = True
        st.session_state["current_user"] = dict(USERS_DB["admin"])
        st.session_state["user_role"] = USERS_DB["admin"]["role"]
    return st.session_state["current_user"]


def do_login(username_key="admin", password=None):
    """Authenticates and sets user session state."""
    u_key = (username_key or "").strip().lower()
    if u_key in USERS_DB:
        if password is not None and USERS_DB[u_key]["password"] != password:
            st.session_state["auth_error"] = "Invalid password. Try admin/admin or analyst/analyst."
            return False
        user_info = dict(USERS_DB[u_key])
        st.session_state["logged_in"] = True
        st.session_state["current_user"] = user_info
        st.session_state["user_role"] = user_info["role"]
        st.session_state["auth_error"] = None
        st.session_state["login_success_toast"] = f"Logged in as {user_info['name']} ({user_info['role']})"
        return True
    st.session_state["auth_error"] = f"Unknown user '{username_key}'. Try admin or analyst."
    return False


def do_logout():
    """Logs out user into Guest Analyst role."""
    st.session_state["logged_in"] = False
    st.session_state["current_user"] = {
        "name": "Guest User",
        "username": "guest",
        "email": "guest@retailpulse.io",
        "role": "Analyst",
        "avatar": "G"
    }
    st.session_state["user_role"] = "Analyst"
    st.session_state["auth_error"] = None
    st.session_state["login_success_toast"] = "Signed out to Guest Analyst mode."


def do_switch_role(new_role):
    """Updates active role permissions without widget collision."""
    st.session_state["user_role"] = new_role
    if "current_user" in st.session_state and isinstance(st.session_state["current_user"], dict):
        st.session_state["current_user"]["role"] = new_role


def trigger_system_test_alert():
    """Injects a real quarantined test alert into staging_sales for notification testing."""
    import uuid
    evt_id = f"TEST-ALERT-{uuid.uuid4().hex[:6].upper()}"
    try:
        with get_db() as session:
            test_stg = StagingSales(
                source="live_api",
                external_event_id=evt_id,
                raw_payload='{"test_alert": true, "reason": "Manually triggered test anomaly"}',
                processing_status="quarantined",
                error_message="Test Anomaly: Unit price < $0 caught by real-time validation filter."
            )
            session.add(test_stg)
            session.commit()
            st.session_state.setdefault("dismissed_notifs", set()).discard(f"stg_{test_stg.staging_id}")
    except Exception:
        st.session_state.setdefault("custom_alerts", []).append({
            "id": f"test_{uuid.uuid4().hex[:6]}",
            "title": f"Quarantined: {evt_id}",
            "message": "Test Anomaly: Unit price < $0 caught by real-time validation filter.",
            "severity": "CRITICAL",
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S")
        })


def get_system_notifications():
    """Aggregates real alerts from staging quarantine, DLQ broker, and data quality results."""
    st.session_state.setdefault("dismissed_notifs", set())
    notifications = []

    # 1. Custom test alerts
    for ca in st.session_state.get("custom_alerts", []):
        if ca["id"] not in st.session_state["dismissed_notifs"]:
            notifications.append(ca)

    # 2. Staging / Quarantined sales records
    try:
        with get_db() as session:
            failed_stg = session.query(StagingSales).filter(
                StagingSales.processing_status.in_(["failed", "quarantined"])
            ).order_by(StagingSales.received_at.desc()).limit(4).all()
            for s in failed_stg:
                nid = f"stg_{s.staging_id}"
                if nid not in st.session_state["dismissed_notifs"]:
                    notifications.append({
                        "id": nid,
                        "title": f"Quarantined: {s.external_event_id or 'Order'}",
                        "message": s.error_message or f"Validation failed on {s.source} ingestion.",
                        "severity": "CRITICAL",
                        "time": s.received_at.strftime("%H:%M:%S") if s.received_at else "Recent"
                    })
    except Exception:
        pass

    # 3. Dead-Letter Queue (DLQ) messages
    try:
        b_stat = get_broker_status()
        dlq_depth = b_stat.get("dlq_depth", 0)
        if dlq_depth > 0 and "dlq_alert" not in st.session_state["dismissed_notifs"]:
            notifications.append({
                "id": "dlq_alert",
                "title": "Dead-Letter Queue Alert",
                "message": f"{dlq_depth} unprocessable messages currently routed to DLQ.",
                "severity": "HIGH",
                "time": "Active"
            })
    except Exception:
        pass

    # 4. Data Quality constraint violations
    try:
        with get_db() as session:
            failed_dq = session.query(DataQualityResult).filter(
                DataQualityResult.success == False
            ).order_by(DataQualityResult.validated_at.desc()).limit(2).all()
            for dq in failed_dq:
                nid = f"dq_{dq.result_id}"
                if nid not in st.session_state["dismissed_notifs"]:
                    notifications.append({
                        "id": nid,
                        "title": "Data Quality Violation",
                        "message": f"{dq.expectation_name}: {dq.observed_value}",
                        "severity": "MEDIUM",
                        "time": dq.validated_at.strftime("%H:%M:%S") if dq.validated_at else "Recent"
                    })
    except Exception:
        pass

    # 5. Fallback system operational notice if all cleared or none found
    if not notifications and "sys_healthy" not in st.session_state["dismissed_notifs"]:
        notifications.append({
            "id": "sys_healthy",
            "title": "Platform Healthy",
            "message": "All database connections, pipelines, and streaming consumers are operational.",
            "severity": "INFO",
            "time": "Just now"
        })

    return notifications


def check_admin_permission(action_name="perform this operation"):
    """Validates if current role has admin privileges."""
    role = st.session_state.get("user_role", "Admin")
    if role != "Admin":
        st.warning(f"🔒 **Access Restricted**: The **{role}** role is read-only. Switch to **Admin** role in the sidebar or user menu to {action_name}.")
        return False
    return True


def render_top_navbar():
    """Renders interactive top header bar with working notifications tray and sign-in / profile popover."""
    user = get_current_user()
    is_logged_in = st.session_state.get("logged_in", True)
    notifs = get_system_notifications()
    unread_count = len(notifs)

    if "login_success_toast" in st.session_state:
        st.toast(st.session_state.pop("login_success_toast"), icon="✅")

    st.markdown('<div class="top-navbar-wrapper">', unsafe_allow_html=True)
    nb_col1, nb_col2, nb_col3, nb_col4 = st.columns([5.2, 2.3, 2.3, 2.2])

    with nb_col1:
        st.markdown("""
        <div style="display:flex; align-items:center; gap:12px; padding:6px 0;">
            <span style="font-size:24px;">💠</span>
            <span style="font-size:20px; font-weight:800; color:#ffffff; letter-spacing:-0.01em;">Retail Dashboard</span>
            <span style="font-size:12.5px; background:rgba(255,255,255,0.22); color:#ffffff; padding:3px 10px; border-radius:12px; font-weight:700; margin-left:6px;">Live Ingestion</span>
        </div>
        """, unsafe_allow_html=True)

    with nb_col2:
        st.markdown("""
        <div style="display:flex; align-items:center; gap:8px; padding-top:10px; justify-content:flex-end;">
            <span style="display:inline-block; width:10px; height:10px; border-radius:50%; background:#22c55e;"></span>
            <span style="font-size:14px; font-weight:700; color:#f1f5f9;">Stream: Online</span>
        </div>
        """, unsafe_allow_html=True)

    with nb_col3:
        notif_label = f"🔔 Alerts ({unread_count})" if unread_count > 0 else "🔔 Alerts (0)"
        with st.popover(notif_label, use_container_width=True):
            st.markdown("#### 🔔 System Notifications & Alerts")
            
            act_c1, act_c2 = st.columns(2)
            with act_c1:
                if st.button("🧹 Dismiss All", key="btn_clear_all_notifs", use_container_width=True):
                    for n in notifs:
                        st.session_state["dismissed_notifs"].add(n["id"])
                    st.rerun()
            with act_c2:
                if st.button("⚡ Test Alert", key="btn_trigger_test_alert", use_container_width=True, on_click=trigger_system_test_alert):
                    st.toast("⚡ Injected test quarantine anomaly into staging table!", icon="🔔")
                    st.rerun()

            st.markdown("---")
            if unread_count > 0:
                st.caption(f"{unread_count} active system alert{'s' if unread_count != 1 else ''}")
                for n in notifs:
                    sev_colors = {
                        "CRITICAL": ("#b91c1c", "#fee2e2", "#ef4444"),
                        "HIGH": ("#c2410c", "#ffedd5", "#f97316"),
                        "MEDIUM": ("#854d0e", "#fef9c3", "#eab308"),
                        "INFO": ("#0f766e", "#ccfbf1", "#0d9488")
                    }
                    text_color, bg_color, border_color = sev_colors.get(n["severity"], ("#475569", "#f1f5f9", "#cbd5e1"))

                    with st.container(border=True):
                        h_col, b_col = st.columns([8.2, 1.8])
                        with h_col:
                            st.markdown(f"""
                            <div style="display:flex; align-items:center; gap:8px; margin-bottom:6px;">
                                <span style="background:{bg_color}; color:{text_color}; border:1.5px solid {border_color}; font-size:11.5px; font-weight:800; padding:2px 8px; border-radius:4px;">{n['severity']}</span>
                                <span style="font-size:15.5px; font-weight:800; color:#0f172a;">{n['title']}</span>
                            </div>
                            <div style="font-size:14px; color:#0f172a; font-weight:600; line-height:1.5;">
                                {n['message']}
                            </div>
                            <div style="font-size:12.5px; color:#475569; font-weight:600; margin-top:6px;">🕒 {n['time']}</div>
                            """, unsafe_allow_html=True)
                        with b_col:
                            if st.button("✕", key=f"dismiss_{n['id']}", help="Dismiss alert"):
                                st.session_state["dismissed_notifs"].add(n["id"])
                                st.rerun()
            else:
                st.success("🎉 You're all caught up! No active alerts.")

            if st.session_state.get("dismissed_notifs"):
                if st.button("🔄 Restore Dismissed Alerts", key="btn_restore_notifs", use_container_width=True):
                    st.session_state["dismissed_notifs"] = set()
                    st.rerun()

    with nb_col4:
        if is_logged_in:
            profile_label = f"👤 {user['name'].split()[0]} ({user['role']})"
            with st.popover(profile_label, use_container_width=True):
                st.markdown("#### 👤 User Account & Profile")
                st.markdown(f"""
                <div style="display:flex; align-items:center; gap:12px; margin-bottom:14px; padding:14px; background:#f8fafc; border-radius:10px; border:1.5px solid #cbd5e1;">
                    <div class="user-avatar" style="width:44px; height:44px; font-size:17px;">{user['avatar']}</div>
                    <div>
                        <div style="font-weight:800; color:#0f172a; font-size:16px;">{user['name']}</div>
                        <div style="font-size:13.5px; color:#334155; font-weight:600;">{user['email']}</div>
                        <div style="font-size:13px; font-weight:800; color:{'#15803d' if user['role']=='Admin' else '#0284c7'}; margin-top:4px;">Role: {user['role']}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                st.markdown("**Switch Role Permissions**")
                def _on_top_role():
                    sel = st.session_state.get("_top_role_selector")
                    if sel:
                        do_switch_role(sel)

                st.selectbox(
                    "Active Role",
                    ["Admin", "Analyst"],
                    index=0 if user["role"] == "Admin" else 1,
                    key="_top_role_selector",
                    on_change=_on_top_role
                )

                st.markdown("---")
                st.button("🚪 Sign Out", key="btn_signout_top", use_container_width=True, on_click=do_logout)
        else:
            with st.popover("🔐 Sign In", use_container_width=True):
                st.markdown("#### 🔐 Sign In to RetailPulse")
                st.caption("Quick 1-Click Login:")
                q1, q2 = st.columns(2)
                with q1:
                    st.button("⚡ Admin", key="demo_admin_login", use_container_width=True, on_click=do_login, args=("admin",))
                with q2:
                    st.button("📊 Analyst", key="demo_analyst_login", use_container_width=True, on_click=do_login, args=("analyst",))

                st.markdown("---")
                with st.form("top_signin_form"):
                    login_user_in = st.text_input("Username", key="top_login_u", placeholder="admin or analyst")
                    login_pass_in = st.text_input("Password", type="password", key="top_login_p", placeholder="password")
                    if st.form_submit_button("Sign In", type="primary", use_container_width=True):
                        if do_login(login_user_in, login_pass_in):
                            st.rerun()
                        else:
                            st.error(st.session_state.get("auth_error", "Invalid credentials."))

    st.markdown('</div>', unsafe_allow_html=True)


def render_hero_banner(title="Retail Analytics & Reporting"):
    """Renders the vibrant teal/blue gradient sub-header."""
    st.markdown(f"""
    <div class="retail-hero-banner">
        <div class="retail-hero-title">{title}</div>
        <div class="retail-hero-filters">
            <div class="filter-pill-btn">
                <span>📊 Live Analytics</span>
                <span>▾</span>
            </div>
            <div class="filter-pill-btn">
                <span>{datetime.now(timezone.utc).strftime('%b %d, %Y')}</span>
                <span>📅</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_sidebar():
    """Renders persistent left navigation with exactly 3 primary options, user profile card, role switcher, and health indicators."""
    user = get_current_user()
    is_logged_in = st.session_state.get("logged_in", True)

    with st.sidebar:
        st.markdown("""
        <div style="padding: 12px 0 16px 0; display:flex; align-items:center; gap:10px;">
            <span style="font-size:24px;">💠</span>
            <span style="font-size:18px; font-weight:800; color:#ffffff; letter-spacing:-0.01em;">Retail Dashboard</span>
        </div>
        """, unsafe_allow_html=True)

        # Alerts summary badge in sidebar
        sb_notifs = get_system_notifications()
        sb_unread = len(sb_notifs)
        if sb_unread > 0:
            st.markdown(f"""
            <div style="background:rgba(239,68,68,0.18); border:1.5px solid rgba(239,68,68,0.45); border-radius:8px; padding:8px 12px; margin-bottom:14px; display:flex; align-items:center; justify-content:space-between;">
                <div style="display:flex; align-items:center; gap:8px;">
                    <span style="font-size:14px;">🔔</span>
                    <span style="font-size:13px; color:#fecaca; font-weight:800;">System Alerts</span>
                </div>
                <span style="background:#ef4444; color:#ffffff; font-size:11.5px; font-weight:800; padding:2px 8px; border-radius:10px;">{sb_unread}</span>
            </div>
            """, unsafe_allow_html=True)

        # EXACTLY THREE PRIMARY SIDEBAR NAVIGATION OPTIONS
        pages = [
            "🏠 Executive Dashboard",
            "⚡ Live Sales Stream",
            "📊 Sales Analytics"
        ]
        st.markdown("<div style='margin-bottom:8px; font-size:13px; font-weight:800; color:#f1f5f9; text-transform:uppercase; letter-spacing:0.06em;'>Navigation</div>", unsafe_allow_html=True)
        selected_page = st.radio("SIDEBAR_NAV_TABS", pages, index=0, label_visibility="collapsed")

        st.markdown("<div style='margin-top:20px; border-top:1px solid rgba(255,255,255,0.12); padding-top:16px;'></div>", unsafe_allow_html=True)
        st.markdown("<div style='margin-bottom:8px; font-size:13px; font-weight:800; color:#f1f5f9; text-transform:uppercase; letter-spacing:0.06em;'>Account & Security</div>", unsafe_allow_html=True)
        
        if is_logged_in:
            st.markdown(f"""
            <div style="background:rgba(255,255,255,0.08); border:1.5px solid rgba(255,255,255,0.15); border-radius:10px; padding:12px; margin-bottom:12px;">
                <div style="display:flex; align-items:center; gap:10px;">
                    <div class="user-avatar" style="width:36px; height:36px; font-size:15px;">{user['avatar']}</div>
                    <div style="overflow:hidden;">
                        <div style="color:#ffffff; font-weight:800; font-size:15px; white-space:nowrap; text-overflow:ellipsis; overflow:hidden;">{user['name']}</div>
                        <div style="color:#cbd5e1; font-size:13px; font-weight:600; white-space:nowrap; text-overflow:ellipsis; overflow:hidden;">{user['email']}</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            active_role = st.session_state.get("user_role", user.get("role", "Admin"))
            role_idx = 0 if active_role == "Admin" else 1

            def _on_sb_role():
                sel = st.session_state.get("_sb_role_selector")
                if sel:
                    do_switch_role(sel)

            selected_role = st.selectbox(
                "Active Role",
                ["Admin", "Analyst"],
                index=role_idx,
                key="_sb_role_selector",
                on_change=_on_sb_role
            )

            if selected_role == "Admin":
                st.caption("🟢 Full Write, Trigger & Execution Permissions")
            else:
                st.caption("🟡 Read-Only Mode (Analytics, Queries & Exports)")

            st.button("🚪 Sign Out", key="sidebar_signout_btn", use_container_width=True, on_click=do_logout)
        else:
            st.warning("👤 Signed Out: Read-Only Mode")
            st.markdown("<div style='font-size:13px; font-weight:800; color:#f1f5f9; text-transform:uppercase; margin-bottom:8px;'>Quick Demo Sign In</div>", unsafe_allow_html=True)
            q_col1, q_col2 = st.columns(2)
            with q_col1:
                st.button("⚡ Admin", key="sb_btn_login_admin", use_container_width=True, on_click=do_login, args=("admin",))
            with q_col2:
                st.button("📊 Analyst", key="sb_btn_login_analyst", use_container_width=True, on_click=do_login, args=("analyst",))

            with st.expander("🔑 Sign In with Credentials", expanded=False):
                with st.form("sb_creds_form"):
                    u_in = st.text_input("Username", key="sb_input_user", placeholder="admin")
                    p_in = st.text_input("Password", type="password", key="sb_input_pass", placeholder="admin")
                    if st.form_submit_button("Sign In", type="primary", use_container_width=True):
                        if do_login(u_in, p_in):
                            st.rerun()
                        else:
                            st.error(st.session_state.get("auth_error", "Login failed."))

        st.markdown("<div style='margin-top:16px; border-top:1px solid rgba(255,255,255,0.12); padding-top:14px;'></div>", unsafe_allow_html=True)
        st.markdown("<div style='margin-bottom:8px; font-size:13px; font-weight:800; color:#f1f5f9; text-transform:uppercase; letter-spacing:0.06em;'>Infrastructure Status</div>", unsafe_allow_html=True)

        db_stat = check_db_status()
        broker_stat = get_broker_status()
        flask_stat = check_flask_api_status()

        # Database Indicator (PostgreSQL Single DB)
        db_badge = "🟢" if db_stat.get("healthy") else "🔴"
        st.markdown(f"<div style='font-size:14px; color:#f8fafc; font-weight:600; margin-bottom:6px;'>{db_badge} <b>DB:</b> {db_stat.get('type', 'PostgreSQL')}</div>", unsafe_allow_html=True)

        # Broker Indicator
        broker_badge = "🟢" if broker_stat.get("is_rabbitmq_connected") else "🟡"
        st.markdown(f"<div style='font-size:14px; color:#f8fafc; font-weight:600; margin-bottom:6px;'>{broker_badge} <b>Broker:</b> {broker_stat.get('mode', 'Queue')}</div>", unsafe_allow_html=True)

        # Flask API Indicator
        flask_badge = "🟢" if flask_stat.get("online") else "⚪"
        st.markdown(f"<div style='font-size:14px; color:#f8fafc; font-weight:600; margin-bottom:10px;'>{flask_badge} <b>REST API:</b> {flask_stat.get('details')}</div>", unsafe_allow_html=True)

        st.markdown("<div style='margin-top:auto; padding-top:40px;'></div>", unsafe_allow_html=True)
        st.markdown("""
        <div style="font-size: 13px; color: #cbd5e1; font-weight: 600; text-align: center; border-top: 1px solid rgba(255,255,255,0.12); padding-top: 14px;">
            RetailPulse Data Engineering<br/>Practicals 1–15 Integrated
        </div>
        """, unsafe_allow_html=True)

    return selected_page


# ==============================================================================
# PAGE 1: EXECUTIVE DASHBOARD
# ==============================================================================
def page_executive_overview():
    render_top_navbar()
    render_hero_banner("Retail Analytics & Executive KPI Overview")

    # Fetch Base Views
    rev_data = query_view("view_daily_revenue", limit=90)
    repeat_data = query_view("view_repeat_customer_percentage", limit=1)
    growth_data = query_view("view_monthly_sales_growth", limit=12)
    regional_data = query_view("view_regional_sales_summary", limit=10)
    top_products_data = query_view("view_top_selling_products", limit=10)

    if not rev_data:
        st.warning("No sales records found in the database. Please initialize seed data or run the ETL pipeline.")
        return

    # Extract raw records for filtering
    engine = get_engine()
    with engine.connect() as conn:
        df_orders = pd.read_sql("""
            SELECT 
                o.order_id,
                o.customer_id,
                c.customer_name,
                c.region,
                p.category,
                p.product_name,
                oi.quantity,
                oi.line_total,
                o.source,
                o.status,
                o.order_timestamp
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products p ON oi.product_id = p.product_id
            WHERE LOWER(o.status) = 'completed'
        """, conn)

    df_orders["order_timestamp"] = pd.to_datetime(df_orders["order_timestamp"], errors="coerce", utc=True)

    # 1. Interactive Filters Card
    with st.container(border=True):
        st.markdown("<div class='content-card-title'><span>🔍 Multi-Dimensional Business Filters</span></div>", unsafe_allow_html=True)
        f_date, f_reg, f_cat, f_src = st.columns(4)
        
        with f_date:
            date_preset = st.selectbox("Date Range", ["All Time", "Last 7 Days", "Last 30 Days", "Last 90 Days"], index=0)
        with f_reg:
            avail_regions = ["All"] + sorted(df_orders["region"].dropna().unique().tolist())
            selected_region = st.selectbox("Region / Branch", avail_regions, index=0)
        with f_cat:
            avail_cats = ["All"] + sorted(df_orders["category"].dropna().unique().tolist())
            selected_cat = st.selectbox("Product Category", avail_cats, index=0)
        with f_src:
            avail_srcs = ["All"] + sorted(df_orders["source"].dropna().unique().tolist())
            selected_src = st.selectbox("Data Source Channel", avail_srcs, index=0)

    # Apply Filters
    filtered_df = df_orders.copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    if date_preset == "Last 7 Days":
        cutoff = now_utc - pd.Timedelta(days=7)
        filtered_df = filtered_df[filtered_df["order_timestamp"] >= cutoff]
    elif date_preset == "Last 30 Days":
        cutoff = now_utc - pd.Timedelta(days=30)
        filtered_df = filtered_df[filtered_df["order_timestamp"] >= cutoff]
    elif date_preset == "Last 90 Days":
        cutoff = now_utc - pd.Timedelta(days=90)
        filtered_df = filtered_df[filtered_df["order_timestamp"] >= cutoff]

    if selected_region != "All":
        filtered_df = filtered_df[filtered_df["region"] == selected_region]
    if selected_cat != "All":
        filtered_df = filtered_df[filtered_df["category"] == selected_cat]
    if selected_src != "All":
        filtered_df = filtered_df[filtered_df["source"] == selected_src]

    # Calculate Filtered Metrics
    total_rev = filtered_df["line_total"].sum() if not filtered_df.empty else 0.0
    total_orders = filtered_df["order_id"].nunique() if not filtered_df.empty else 0
    total_items = filtered_df["quantity"].sum() if not filtered_df.empty else 0
    aov = round(total_rev / max(total_orders, 1), 2)
    repeat_pct = repeat_data[0].get("repeat_customer_percentage", 0.0) if repeat_data else 0.0
    latest_growth = growth_data[-1].get("mom_growth_pct", 0.0) if growth_data else 0.0

    # 2. Rule-Based Anomaly & Health Alert Banners
    anomalies_found = []
    if (filtered_df["line_total"] < 0).any():
        anomalies_found.append("Negative revenue transaction detected.")
    if total_orders > 0 and aov < 5.0:
        anomalies_found.append("Abnormally low Average Order Value (< $5.00) detected.")
    
    if anomalies_found:
        st.warning(f"⚠️ **Rule-Based Sales Anomaly Detected**: {' | '.join(anomalies_found)}")
    else:
        st.success("✅ **Rule-Based Health**: All transaction totals, prices, and volumes conform to valid business thresholds.")

    # 3. Main Layout: Charts Grid (Left 8.5 Cols) vs KPI Pill Stack (Right 3.5 Cols)
    main_col, right_col = st.columns([8.5, 3.5])

    with main_col:
        # Area Trend Chart
        with st.container(border=True):
            st.markdown(f"""
            <div class="content-card-title">
                <span>Total Sales Revenue Trend</span>
                <span style="font-size:14px; font-weight:700; color:#0f172a; background:#f1f5f9; border:1.5px solid #cbd5e1; padding:4px 12px; border-radius:6px;">{date_preset}</span>
            </div>
            """, unsafe_allow_html=True)

            if not filtered_df.empty:
                daily_trend = filtered_df.copy()
                daily_trend["day_str"] = daily_trend["order_timestamp"].dt.strftime("%Y-%m-%d")
                daily_grp = daily_trend.groupby("day_str")["line_total"].sum().reset_index()
                daily_grp = daily_grp.sort_values("day_str")

                fig_area = px.area(
                    daily_grp, x="day_str", y="line_total",
                    labels={"day_str": "Date", "line_total": "Sales Revenue ($)"}
                )
                fig_area.update_traces(
                    line=dict(color="#1d6fbd", width=2.5),
                    fillcolor="rgba(186, 215, 240, 0.45)"
                )
                apply_reference_light_theme(fig_area)
                fig_area.update_layout(height=280)
                st.plotly_chart(fig_area, use_container_width=True)
            else:
                st.info("No orders found for the selected filter parameters.")

        # Bottom Row: Category Share & Regional Revenue
        b_col1, b_col2 = st.columns(2)
        with b_col1:
            with st.container(border=True):
                st.markdown("<div class='content-card-title'><span>Sales by Product Category</span></div>", unsafe_allow_html=True)
                if not filtered_df.empty:
                    cat_grp = filtered_df.groupby("category")["line_total"].sum().reset_index()
                    fig_pie = px.pie(
                        cat_grp, names="category", values="line_total",
                        color_discrete_sequence=["#1d6fbd", "#38bdf8", "#5cb85c", "#f59e0b", "#8b5cf6"]
                    )
                    fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                    apply_reference_light_theme(fig_pie)
                    fig_pie.update_layout(height=260, showlegend=True)
                    st.plotly_chart(fig_pie, use_container_width=True)
                else:
                    st.info("No category data.")

        with b_col2:
            with st.container(border=True):
                st.markdown("<div class='content-card-title'><span>Gross Profit by Region / Branch</span></div>", unsafe_allow_html=True)
                if not filtered_df.empty:
                    reg_grp = filtered_df.groupby("region")["line_total"].sum().reset_index()
                    colors = ["#1d6fbd" if i % 2 == 0 else "#0d9488" for i in range(len(reg_grp))]
                    fig_bar = go.Figure(data=[
                        go.Bar(
                            x=reg_grp["region"],
                            y=reg_grp["line_total"],
                            marker_color=colors
                        )
                    ])
                    apply_reference_light_theme(fig_bar)
                    fig_bar.update_layout(height=260, yaxis_title="Revenue ($)", xaxis_title="Region")
                    st.plotly_chart(fig_bar, use_container_width=True)
                else:
                    st.info("No regional data.")

    with right_col:
        # Stacked Colored KPI Cards
        st.markdown(f"""
        <div class="kpi-pill-white">
            <div class="kpi-icon-square" style="color:#0284c7; background:#e0f2fe;">📊</div>
            <div>
                <div class="kpi-label">Filtered Orders</div>
                <div class="kpi-val">{total_orders:,}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="kpi-pill-blue">
            <div class="kpi-icon-square" style="background:rgba(255,255,255,0.2);">💵</div>
            <div>
                <div class="kpi-label">Net Sales Revenue</div>
                <div class="kpi-val">${total_rev:,.2f}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="kpi-pill-green">
            <div class="kpi-icon-square" style="background:rgba(255,255,255,0.2);">🪙</div>
            <div>
                <div class="kpi-label">Estimated Gross Profit (35%)</div>
                <div class="kpi-val">${total_rev*0.35:,.2f}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="kpi-pill-teal">
            <div class="kpi-icon-square" style="background:rgba(255,255,255,0.2);">🧺</div>
            <div>
                <div class="kpi-label">Average Order Value (AOV)</div>
                <div class="kpi-val">${aov:,.2f}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="key-performance-card">
            <div class="kp-title">Key Performance Indicators</div>
            <div class="kp-section">
                <div class="kp-label">Repeat Customer Rate</div>
                <div class="kp-value">{repeat_pct}%</div>
            </div>
            <div class="kp-section" style="margin-bottom:0;">
                <div class="kp-label">Monthly Growth (MoM)</div>
                <div class="kp-value" style="color:{'#16a34a' if latest_growth >= 0 else '#dc2626'};">{'+' if latest_growth > 0 else ''}{latest_growth}%</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # 4. Product Rankings and Customer Analytics Row
    r_col1, r_col2 = st.columns(2)
    with r_col1:
        with st.container(border=True):
            st.markdown("<div class='content-card-title'><span>🏆 Top 5 Best-Selling Products</span></div>", unsafe_allow_html=True)
            if top_products_data:
                df_top = pd.DataFrame(top_products_data).head(5)
                st.dataframe(
                    df_top.rename(columns={
                        "product_name": "Product",
                        "category": "Category",
                        "total_units_sold": "Units Sold",
                        "total_revenue": "Revenue ($)"
                    }),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No top product data available.")

    with r_col2:
        with st.container(border=True):
            st.markdown("<div class='content-card-title'><span>👥 Customer Value Analytics (RFM Ranking)</span></div>", unsafe_allow_html=True)
            try:
                rfm_df = SQLAnalytics.run_category_rfm_subquery().head(5)
                st.dataframe(
                    rfm_df.rename(columns={
                        "customer_name": "Customer Name",
                        "region": "Region",
                        "total_orders": "Orders",
                        "total_spent": "Total Spent ($)",
                        "customer_value_rank": "Rank"
                    })[["Rank", "Customer Name", "Region", "Orders", "Total Spent ($)"]],
                    use_container_width=True,
                    hide_index=True
                )
            except Exception as e:
                st.info(f"Customer analytics: {e}")

    # 5. EXPANDABLE DATA CUBE EXPLORER (CUBE, ROLLUP & GROUPING SETS)
    st.write("")
    with st.expander("🎲 Multidimensional Data Cube Explorer (CUBE, ROLLUP & GROUPING SETS Queries)", expanded=False):
        st.markdown("""
        **Syllabus Practical 10**: Executes genuine multidimensional aggregation over relational sales records.
        Demonstrates `GROUP BY CUBE`, `GROUP BY ROLLUP`, and `GROUP BY GROUPING SETS` with `GROUPING()` flags distinguishing subtotals and grand totals.
        """)

        cube_mode = st.radio(
            "Select OLAP Aggregation Operation",
            [
                "1. GROUP BY CUBE (region, category)",
                "2. GROUP BY ROLLUP (sales_year, sales_month, category)",
                "3. GROUP BY GROUPING SETS ((region), (category), (source), ())"
            ],
            index=0
        )

        try:
            if "CUBE" in cube_mode:
                cube_df = DataCubeExplorer.run_cube_query()
                st.markdown("##### 📌 CUBE: All Combinations of Region & Category + Subtotals & Grand Total")
                st.dataframe(cube_df, use_container_width=True, hide_index=True)
                st.download_button("📥 Download CUBE Results as CSV", cube_df.to_csv(index=False), "sales_data_cube.csv", "text/csv")
            elif "ROLLUP" in cube_mode:
                rollup_df = DataCubeExplorer.run_rollup_query()
                st.markdown("##### 📌 ROLLUP: Hierarchical Time Aggregations (Year ➔ Month ➔ Category)")
                st.dataframe(rollup_df, use_container_width=True, hide_index=True)
                st.download_button("📥 Download ROLLUP Results as CSV", rollup_df.to_csv(index=False), "sales_data_rollup.csv", "text/csv")
            else:
                gsets_df = DataCubeExplorer.run_grouping_sets_query()
                st.markdown("##### 📌 GROUPING SETS: Independent Marginal Totals across Dimensions")
                st.dataframe(gsets_df, use_container_width=True, hide_index=True)
                st.download_button("📥 Download GROUPING SETS Results as CSV", gsets_df.to_csv(index=False), "sales_grouping_sets.csv", "text/csv")
        except Exception as e:
            st.error(f"Error computing Data Cube: {e}")


# ==============================================================================
# PAGE 2: LIVE SALES STREAM
# ==============================================================================
def page_live_sales_monitor():
    render_top_navbar()
    render_hero_banner("Live Ingestion & Stream Operations")

    sim_stat = get_simulator_status()
    cons_stat = get_consumer_status()
    broker_stat = get_broker_status()
    flask_stat = check_flask_api_status()

    # 1. Operational Controls Container
    with st.container(border=True):
        st.markdown("<div class='content-card-title'><span>⚙️ Real-Time Stream Simulator & Broker Controls</span></div>", unsafe_allow_html=True)
        
        c_sim, c_cons, c_params, c_auto = st.columns([3, 3, 3.5, 2.5])

        with c_sim:
            st.markdown(f"**Simulator:** `{'ACTIVE 🟢' if sim_stat['is_running'] else 'IDLE ⚪'}`")
            st.caption(f"Produced: {sim_stat['events_produced']} synthetic events")
            if not sim_stat["is_running"]:
                if st.button("▶ Start Generator", type="primary", use_container_width=True):
                    if check_admin_permission("start the event generator"):
                        interval_val = st.session_state.get("sim_interval", 2.0)
                        total_val = st.session_state.get("sim_total", 200)
                        err_val = st.session_state.get("sim_inject_errs", True)
                        start_simulator_background(interval=interval_val, total=total_val, inject_errors=err_val)
                        st.rerun()
            else:
                if st.button("⏹ Stop Generator", use_container_width=True):
                    stop_simulator_background()
                    st.rerun()

        with c_cons:
            st.markdown(f"**Consumer:** `{'ACTIVE 🟢' if cons_stat['is_running'] else 'IDLE ⚪'}`")
            st.caption(f"Consumed: {cons_stat['events_consumed']} streamed messages")
            if not cons_stat["is_running"]:
                if st.button("▶ Start Consumer", type="secondary", use_container_width=True):
                    if check_admin_permission("start the streaming consumer"):
                        start_consumer_background(poll_interval=0.5)
                        st.rerun()
            else:
                if st.button("⏹ Stop Consumer", use_container_width=True):
                    stop_consumer_background()
                    st.rerun()

        with c_params:
            st.slider("Interval (sec)", 0.5, 5.0, 2.0, 0.5, key="sim_interval")
            st.number_input("Max Events", 10, 2000, 200, 50, key="sim_total")
            st.checkbox("Inject Deliberate Duplicates & Anomalies", value=True, key="sim_inject_errs")

        with c_auto:
            auto_refresh = st.checkbox("Auto-Refresh (2s)", value=False)
            if st.button("🔄 Refresh Stream", use_container_width=True):
                st.rerun()
            st.caption(f"Broker: {broker_stat.get('mode')}")

    # 2. Broker Monitoring KPI Strip
    st.write("")
    m1, m2, m3, m4, m5 = st.columns(5)
    
    # Query staging duplicate detection count
    engine = get_engine()
    with engine.connect() as conn:
        dups_count = pd.read_sql("SELECT COUNT(*) as c FROM staging_sales WHERE processing_status = 'processed' AND raw_payload LIKE '%simulated%'", conn).iloc[0]["c"]
        dlq_count = pd.read_sql("SELECT COUNT(*) as c FROM staging_sales WHERE processing_status in ('failed', 'quarantined')", conn).iloc[0]["c"]

    with m1:
        st.markdown(f"""
        <div class="stream-stat-card">
            <div class="stream-stat-icon" style="background:#e0f2fe; color:#0369a1;">📥</div>
            <div class="stream-stat-content">
                <div class="stream-stat-label">Queue Depth</div>
                <div class="stream-stat-val">{broker_stat.get('queue_depth', 0)} <span class="unit">msgs</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with m2:
        st.markdown(f"""
        <div class="stream-stat-card">
            <div class="stream-stat-icon" style="background:#fee2e2; color:#b91c1c;">⚠️</div>
            <div class="stream-stat-content">
                <div class="stream-stat-label">Dead-Letter Queue</div>
                <div class="stream-stat-val" style="color:#b91c1c;">{broker_stat.get('dlq_depth', 0) + int(dlq_count)} <span class="unit">msgs</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with m3:
        st.markdown(f"""
        <div class="stream-stat-card">
            <div class="stream-stat-icon" style="background:#dcfce7; color:#15803d;">🛡️</div>
            <div class="stream-stat-content">
                <div class="stream-stat-label">Duplicates Caught</div>
                <div class="stream-stat-val">{dups_count:,} <span class="unit">events</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with m4:
        st.markdown(f"""
        <div class="stream-stat-card">
            <div class="stream-stat-icon" style="background:#fef3c7; color:#b45309;">⏱️</div>
            <div class="stream-stat-content">
                <div class="stream-stat-label">Processing Latency</div>
                <div class="stream-stat-val">&lt; 15 <span class="unit">ms</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with m5:
        flask_color = "#15803d" if flask_stat.get("online") else "#64748b"
        flask_bg = "#dcfce7" if flask_stat.get("online") else "#f1f5f9"
        st.markdown(f"""
        <div class="stream-stat-card">
            <div class="stream-stat-icon" style="background:{flask_bg}; color:{flask_color};">⚡</div>
            <div class="stream-stat-content">
                <div class="stream-stat-label">REST API (Port 5000)</div>
                <div class="stream-stat-val" style="color:{flask_color}; font-size:16px;">{'ONLINE' if flask_stat.get('online') else 'OFFLINE'}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # 3. Channel Breakdown Cards
    with engine.connect() as conn:
        source_counts = pd.read_sql(
            "SELECT source, COUNT(*) as count, ROUND(SUM(total_amount), 2) as revenue FROM orders GROUP BY source",
            conn
        )

    s1, s2, s3, s4 = st.columns(4)
    with s1:
        c = source_counts[source_counts["source"] == "live_api"]["count"].sum() if not source_counts.empty else 0
        st.markdown(f"""
        <div class="stream-stat-card">
            <div class="stream-stat-icon" style="background:#dcfce7; color:#166534;">⚡</div>
            <div class="stream-stat-content">
                <div class="stream-stat-label">Live Partner REST API</div>
                <div class="stream-stat-val">{c:,} <span class="unit">orders</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with s2:
        c = source_counts[source_counts["source"] == "woocommerce_webhook"]["count"].sum() if not source_counts.empty else 0
        st.markdown(f"""
        <div class="stream-stat-card">
            <div class="stream-stat-icon" style="background:#f3e8ff; color:#6b21a8;">🛍️</div>
            <div class="stream-stat-content">
                <div class="stream-stat-label">WooCommerce Webhook</div>
                <div class="stream-stat-val">{c:,} <span class="unit">orders</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with s3:
        c = source_counts[source_counts["source"] == "simulated"]["count"].sum() if not source_counts.empty else 0
        st.markdown(f"""
        <div class="stream-stat-card">
            <div class="stream-stat-icon" style="background:#e0f2fe; color:#0369a1;">📡</div>
            <div class="stream-stat-content">
                <div class="stream-stat-label">Simulated Stream Events</div>
                <div class="stream-stat-val">{c:,} <span class="unit">orders</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with s4:
        c = source_counts[source_counts["source"] == "csv_batch"]["count"].sum() if not source_counts.empty else 0
        st.markdown(f"""
        <div class="stream-stat-card">
            <div class="stream-stat-icon" style="background:#fef3c7; color:#b45309;">📁</div>
            <div class="stream-stat-content">
                <div class="stream-stat-label">CSV Batch Ingestion</div>
                <div class="stream-stat-val">{c:,} <span class="unit">orders</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # 4. Live Ingestion Feed with Source Badging
    st.write("")
    with st.container(border=True):
        st.markdown("<div class='content-card-title'><span>⚡ Real-Time Transactions Feed (Live Ingestion Log)</span></div>", unsafe_allow_html=True)
        
        recent_orders = []
        with get_db() as session:
            orders = session.query(Order).order_by(Order.order_timestamp.desc()).limit(40).all()
            for o in orders:
                recent_orders.append({
                    "Order ID": o.order_id,
                    "Customer": o.customer_id,
                    "Channel Source": o.source,
                    "Order Type": "Real Live Order" if o.source in ["live_api", "woocommerce_webhook", "csv_batch"] else "Generated Event",
                    "Timestamp": o.order_timestamp.strftime("%Y-%m-%d %H:%M:%S") if o.order_timestamp else "",
                    "Status": (o.status or "").upper(),
                    "Amount ($)": f"${o.total_amount:,.2f}" if o.total_amount is not None else "$0.00"
                })

        if recent_orders:
            st.dataframe(pd.DataFrame(recent_orders), use_container_width=True, hide_index=True)
        else:
            st.info("No incoming orders recorded yet.")

    if auto_refresh:
        time.sleep(2)
        st.rerun()


# ==============================================================================
# PAGE 3: SALES ANALYTICS (9 SECONDARY TABS)
# ==============================================================================
def page_sales_analytics():
    render_top_navbar()
    render_hero_banner("Sales Analytics & Data Engineering Workbench")

    # 4 SECONDARY TABS
    tab_labels = [
        "📁 Data Explorer",
        "⚙️ ETL Pipeline",
        "✅ Data Quality",
        "🔌 API Tools"
    ]
    tabs = st.tabs(tab_labels)

    # --------------------------------------------------------------------------
    # TAB 1: DATA EXPLORER (EDA & CLEANING - PRACTICALS 3 & 4)
    # --------------------------------------------------------------------------
    with tabs[0]:
        st.markdown("### 📁 Exploratory Data Analysis & Automated Cleaning Pipeline")
        st.caption("Syllabus Practicals 3 & 4: Profiling, missing value imputation, duplicate removal, and IQR outlier capping.")

        col_src, col_dl = st.columns([7, 3])
        with col_src:
            data_source_mode = st.radio(
                "Choose Data Source",
                ["sample_retail_sales.csv (1,500 Production Records)", "test_retail_batch.csv (Edge Cases: Duplicates, Missing, Outliers)", "Upload Custom CSV"],
                horizontal=True,
                index=0
            )

        test_path = RAW_DATA_DIR / "test_retail_batch.csv"
        sample_path = RAW_DATA_DIR / "sample_retail_sales.csv"

        if data_source_mode == "Upload Custom CSV":
            csv_upload = st.file_uploader("Upload CSV Dataset for Profiling & Cleaning", type=["csv"], key="eda_csv_upload")
            if csv_upload is not None:
                raw_df = pd.read_csv(csv_upload)
                dataset_title = csv_upload.name
            elif sample_path.exists():
                raw_df = pd.read_csv(sample_path)
                dataset_title = f"Default: {sample_path.name}"
            else:
                st.info("Please upload a CSV file.")
                return
        elif "test_retail_batch" in data_source_mode:
            raw_df = pd.read_csv(test_path) if test_path.exists() else pd.read_csv(sample_path)
            dataset_title = "test_retail_batch.csv (Edge-Case Test Suite)"
        else:
            raw_df = pd.read_csv(sample_path)
            dataset_title = "sample_retail_sales.csv (Complete Sales History)"

        with col_dl:
            st.download_button(
                "📥 Download Current Test CSV",
                raw_df.to_csv(index=False),
                file_name=f"test_dataset_{int(time.time())}.csv",
                mime="text/csv",
                use_container_width=True
            )

        with st.container(border=True):
            st.markdown(f"**Loaded Dataset:** `{dataset_title}` ({len(raw_df):,} rows, {len(raw_df.columns)} columns)")
            st.dataframe(raw_df.head(6), use_container_width=True, hide_index=True)

        # Profile Dataset
        profile = DataCleaner.profile_dataset(raw_df)
        
        # 4 Metric Cards
        p1, p2, p3, p4 = st.columns(4)
        p1.metric("Total Rows", f"{profile['total_rows']:,}")
        p2.metric("Total Columns", f"{profile['total_columns']}")
        p3.metric("Duplicate Rows", f"{profile['duplicate_count']:,}")
        total_missing = sum(profile['missing_counts'].values())
        p4.metric("Missing Cell Values", f"{total_missing:,}")

        c_left, c_right = st.columns(2)
        with c_left:
            with st.container(border=True):
                st.markdown("##### 🔍 Missing Values Analysis")
                miss_df = pd.DataFrame({
                    "Column": list(profile["missing_counts"].keys()),
                    "Missing Count": list(profile["missing_counts"].values()),
                    "Missing %": [profile["missing_percentages"].get(col, 0.0) for col in profile["missing_counts"]]
                })
                st.dataframe(miss_df[miss_df["Missing Count"] > 0] if (miss_df["Missing Count"] > 0).any() else miss_df, use_container_width=True, hide_index=True)

        with c_right:
            with st.container(border=True):
                st.markdown("##### 📊 Numeric Statistical Profiling & Outliers (IQR)")
                num_records = []
                for col, stats in profile["numeric_summary"].items():
                    num_records.append({
                        "Column": col,
                        "Mean": stats["mean"],
                        "Std": stats["std"],
                        "Min": stats["min"],
                        "Median": stats["median"],
                        "Max": stats["max"],
                        "IQR": stats["iqr"],
                        "Outliers": stats["outliers_count"]
                    })
                if num_records:
                    st.dataframe(pd.DataFrame(num_records), use_container_width=True, hide_index=True)
                else:
                    st.info("No numeric columns found.")

    # --------------------------------------------------------------------------
    # TAB 2: ETL PIPELINE (LINEAGE, RUNS & QUARANTINE - PRACTICAL 7)
    # --------------------------------------------------------------------------
    with tabs[1]:
        st.markdown("### ⚙️ End-to-End Idempotent ETL Pipeline")
        st.caption("Syllabus Practical 7: Extract, Transform, Validate, and Idempotent Load with Quarantining & Lineage.")

        # Lineage Visual
        with st.container(border=True):
            st.markdown("<div class='content-card-title'><span>🌐 Data Lineage & Ingestion Flow</span></div>", unsafe_allow_html=True)
            st.markdown("""
            ```mermaid
            flowchart LR
                A["Sources (CSV, Live API, Webhook, Stream)"] --> B["Staging & Deduplication Table"]
                B --> C["DataCleaner (IQR Capping & Imputation)"]
                C --> D["Great Expectations Validation"]
                D --> E[("PostgreSQL 3NF Schema")]
                E --> F[("5 SQL Analytical Business Views")]
                D -.->|Failed Records| Q[("Quarantine Storage")]
            ```
            """)

        # Execute Batch ETL
        with st.container(border=True):
            st.markdown("<div class='content-card-title'><span>⚡ Trigger Batch ETL Execution</span></div>", unsafe_allow_html=True)
            avail_batch_files = [f.name for f in RAW_DATA_DIR.glob("*.csv")]
            selected_batch_name = st.selectbox("Select Target Batch File", avail_batch_files, index=0)
            etl_file = RAW_DATA_DIR / selected_batch_name
            st.markdown(f"**Target Batch File:** `{etl_file}` ({etl_file.stat().st_size / 1024:.1f} KB)" if etl_file.exists() else "No sample file found.")

            if st.button("▶ Execute Full Batch ETL Pipeline", type="primary"):
                if check_admin_permission("run the batch ETL pipeline"):
                    with st.spinner(f"Extracting, cleaning, validating, and idempotently loading {selected_batch_name}..."):
                        res = etl_pipeline.run_batch_etl(etl_file, dataset_name=selected_batch_name)
                        st.success(f"ETL Execution {res['status']}: Processed {res['total_extracted']} records in {res['duration_seconds']}s! Inserted: {res['records_inserted']}, Skipped/Duplicates: {res['records_skipped']}.")

        # Execution History & Run Comparison
        with st.container(border=True):
            st.markdown("<div class='content-card-title'><span>📊 Pipeline Execution History & Run Comparison</span></div>", unsafe_allow_html=True)
            with get_db() as session:
                logs = session.query(PipelineLog).order_by(PipelineLog.started_at.desc()).limit(15).all()
                log_data = []
                for l in logs:
                    dur = round((l.completed_at - l.started_at).total_seconds(), 2) if (l.completed_at and l.started_at) else 0.0
                    log_data.append({
                        "Log ID": l.log_id,
                        "Pipeline": l.pipeline_name,
                        "Task": l.task_name,
                        "Status": l.status,
                        "Records Processed": l.records_processed,
                        "Duration (s)": dur,
                        "Started At": l.started_at.strftime("%Y-%m-%d %H:%M:%S") if l.started_at else ""
                    })

            if log_data:
                df_logs = pd.DataFrame(log_data)
                st.dataframe(df_logs, use_container_width=True, hide_index=True)
                fig_runs = px.bar(df_logs, x="Started At", y="Records Processed", color="Status", title="Records Processed Across Recent Pipeline Runs")
                apply_reference_light_theme(fig_runs)
                st.plotly_chart(fig_runs, use_container_width=True)
            else:
                st.info("No pipeline execution logs recorded yet.")

        # Quarantine Inspector
        with st.expander("🚨 Quarantine Inspector (data/errors/)", expanded=False):
            err_files = list(ERRORS_DATA_DIR.glob("*.json")) + list(ERRORS_DATA_DIR.glob("*.csv"))
            if err_files:
                st.markdown(f"Found **{len(err_files)} quarantined error files**.")
                sel_err = st.selectbox("Inspect Quarantined File", [f.name for f in err_files])
                target_f = ERRORS_DATA_DIR / sel_err
                if target_f.suffix == ".json":
                    st.json(json.loads(target_f.read_text(encoding="utf-8")))
                else:
                    st.dataframe(pd.read_csv(target_f), use_container_width=True)
            else:
                st.info("No quarantined error files found.")

    # --------------------------------------------------------------------------
    # TAB 3: DATA QUALITY (GREAT EXPECTATIONS - PRACTICAL 14)
    # --------------------------------------------------------------------------
    with tabs[2]:
        st.markdown("### ✅ Data Quality & Observability Suite")
        st.caption("Syllabus Practical 14: Automated Great Expectations suite, persistence to database, and HTML report downloads.")

        sample_csv = RAW_DATA_DIR / "sample_retail_sales.csv"
        df_dq = pd.read_csv(sample_csv) if sample_csv.exists() else pd.DataFrame()

        if st.button("🧪 Execute Great Expectations Test Suite", type="primary"):
            with st.spinner("Evaluating constraints..."):
                dq_res = data_quality_checker.run_suite(df_dq, dataset_name="retail_sales_feed")
                
                q1, q2, q3, q4 = st.columns(4)
                q1.metric("Total Expectations", dq_res["total_expectations"])
                q2.metric("Passed Assertions", dq_res["passed_expectations"])
                q3.metric("Failed Assertions", dq_res["failed_expectations"])
                q4.metric("Health Score", f"{dq_res['success_rate_pct']}%")

                st.markdown("##### 📋 Detailed Assertion Results")
                results_table = []
                for exp in dq_res["results"]:
                    results_table.append({
                        "Expectation": exp["expectation"],
                        "Status": "PASS 🟢" if exp["success"] else "FAIL 🔴",
                        "Observed Value": exp["observed_value"],
                        "Severity": exp["severity"]
                    })
                st.dataframe(pd.DataFrame(results_table), use_container_width=True, hide_index=True)

                # HTML Report Download
                rep_path = Path(dq_res["report_path"])
                if rep_path.exists():
                    st.download_button(
                        "📥 Download Great Expectations HTML Report",
                        rep_path.read_text(encoding="utf-8"),
                        rep_path.name,
                        "text/html"
                    )

        # Historical Quality Runs Table
        st.write("")
        with st.container(border=True):
            st.markdown("<div class='content-card-title'><span>📈 Historical Validation Logs (data_quality_results)</span></div>", unsafe_allow_html=True)
            with get_db() as session:
                hist = session.query(DataQualityResult).order_by(DataQualityResult.validated_at.desc()).limit(20).all()
                hist_data = []
                for h in hist:
                    hist_data.append({
                        "Dataset": h.dataset_name,
                        "Expectation": h.expectation_name,
                        "Success": "PASS 🟢" if h.success else "FAIL 🔴",
                        "Observed": h.observed_value,
                        "Validated At": h.validated_at.strftime("%Y-%m-%d %H:%M:%S") if h.validated_at else ""
                    })
            if hist_data:
                st.dataframe(pd.DataFrame(hist_data), use_container_width=True, hide_index=True)
            else:
                st.info("No data quality history recorded yet.")

    # --------------------------------------------------------------------------
    # TAB 4: API TOOLS (REST API & WEBHOOK - PRACTICAL 1)
    # --------------------------------------------------------------------------
    with tabs[3]:
        st.markdown("### 🔌 REST API & Webhook Testing Workbench")
        st.caption("Syllabus Practical 1: Flask endpoints, Pydantic schema validation, and HMAC-SHA256 signature verification.")

        api_endpoint = st.selectbox(
            "Select API Endpoint",
            [
                "GET /health (Platform Health Diagnostic)",
                "GET /api/metrics (Executive Business KPIs)",
                "GET /api/orders (Paginated Order Retrieval)",
                "POST /api/orders (Ingest Sales Order)",
                "POST /webhooks/woocommerce (WooCommerce Webhook)"
            ]
        )

        sample_order_payload = json.dumps({
            "order_id": f"ORD-API-{int(time.time())}",
            "customer_id": "CUST-002",
            "source": "live_api",
            "status": "completed",
            "items": [
                {
                    "product_id": "PROD-001",
                    "quantity": 2,
                    "unit_price": 299.99
                }
            ]
        }, indent=2)

        if "GET" in api_endpoint:
            url_path = api_endpoint.split()[1].split("(")[0].strip()
            target_url = f"http://{FLASK_HOST}:{FLASK_PORT}{url_path}"
            st.markdown(f"**Target URL:** `{target_url}`")
            if st.button("🚀 Send GET Request", type="primary"):
                try:
                    res = requests.get(target_url, timeout=2.0)
                    st.success(f"Status: HTTP {res.status_code}")
                    st.json(res.json())
                except Exception as e:
                    st.error(f"Request failed: {e}. Ensure Flask API is running (`python -m api.app`).")
        else:
            url_path = api_endpoint.split()[1].split("(")[0].strip()
            target_url = f"http://{FLASK_HOST}:{FLASK_PORT}{url_path}"
            payload_text = st.text_area("Request JSON Body", value=sample_order_payload, height=180)
            if st.button("🚀 Send POST Request", type="primary"):
                try:
                    headers = {"Content-Type": "application/json"}
                    if "woocommerce" in url_path:
                        headers["X-RetailPulse-Test"] = "true"
                    res = requests.post(target_url, data=payload_text, headers=headers, timeout=2.0)
                    st.success(f"Status: HTTP {res.status_code}")
                    st.json(res.json())
                except Exception as e:
                    st.error(f"Request failed: {e}. Ensure Flask API is running (`python -m api.app`).")


# ==============================================================================
# MAIN ROUTER
# ==============================================================================
def main():
    selected = render_sidebar()

    if "Live Sales" in selected:
        page_live_sales_monitor()
    elif "Sales Analytics" in selected:
        page_sales_analytics()
    else:
        page_executive_overview()


if __name__ == "__main__":
    main()
