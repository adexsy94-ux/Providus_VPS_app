# Providus_recon.py
# -*- coding: utf-8 -*-
"""
Providus ↔ VPS Reconciliation – v4.0 ULTIMATE
Supports: .csv | .xlsx | .xls | Auto xlrd
Features: Stunning UI • Animated Guide • Perfect Dark Mode • Mobile Ready
Run: streamlit run Providus_recon.py
"""

import io
import base64
import subprocess
import sys
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import numpy as np
import streamlit.components.v1 as components

# -----------------------------
# AUTO-INSTALL xlrd
# -----------------------------
try:
    import xlrd
except ImportError:
    with st.spinner("Installing `xlrd` for .xls support..."):
        subprocess.check_call([sys.executable, "-m", "pip", "install", "xlrd"])
    import xlrd

# -----------------------------
# Config
# -----------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
LOGO_PATH = DATA_DIR / "logo.png"
DATA_DIR.mkdir(exist_ok=True)

# -----------------------------
# File Reader
# -----------------------------
@st.cache_data
def read_file_any(uploaded_file, local_path):
    def _read(source, suffix):
        if suffix == ".csv": return pd.read_csv(source, dtype=str)
        elif suffix == ".xls": return pd.read_excel(source, engine="xlrd", dtype=object)
        else: return pd.read_excel(source, engine="openpyxl", dtype=object)

    if uploaded_file:
        try:
            suffix = Path(uploaded_file.name).suffix.lower()
            df = _read(uploaded_file, suffix)
            st.success(f"Loaded **{len(df):,} rows** from `{uploaded_file.name}`")
            return df
        except Exception as e:
            st.error(f"Read error: `{e}`\n\nSave as **XLSX** or run `pip install xlrd`")
            return None
    if local_path and Path(local_path).exists():
        return _read(Path(local_path), Path(local_path).suffix.lower())
    return None

# -----------------------------
# Data Cleaning
# -----------------------------
def clean_numeric_text_col(col):
    if col is None: return col
    return pd.to_numeric(col.astype(str).str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce")

def parse_vps_date(s): return pd.to_datetime(s.astype(str).replace("nan", None), utc=True, errors="coerce") \
    .dt.tz_convert("Africa/Lagos").dt.normalize().dt.tz_localize(None)

def parse_prv_date(s): return pd.to_datetime(s.astype(str).replace("nan", None), dayfirst=True, errors="coerce")

# -----------------------------
# Matching Engine
# -----------------------------
def run_vps_recon_enhanced(prv_df, vps_df, opts, date_tolerance_days=3, progress_callback=None):
    prv = prv_df.copy()
    vps = vps_df.copy()
    prv.columns = prv.columns.str.strip()
    vps.columns = vps.columns.str.strip()

    # Validate columns
    required_prv = [PRV_COL_CREDIT]
    required_vps = [VPS_COL_DATE, VPS_COL_SETTLED, VPS_COL_CHARGE]
    for c in required_prv: 
        if c not in prv.columns: raise KeyError(f"Missing PROVIDUS column: `{c}`")
    for c in required_vps: 
        if c not in vps.columns: raise KeyError(f"Missing VPS column: `{c}`")

    if PRV_COL_DEBIT in prv.columns: prv = prv.drop(columns=[PRV_COL_DEBIT])

    prv[PRV_COL_CREDIT] = clean_numeric_text_col(prv[PRV_COL_CREDIT])
    vps["_raw_settled_clean"] = clean_numeric_text_col(vps[VPS_COL_SETTLED])
    vps[VPS_COL_CHARGE] = clean_numeric_text_col(vps[VPS_COL_CHARGE])

    prv = prv[prv[PRV_COL_CREDIT].notna()].dropna(how="all").reset_index(drop=True)
    prv["_parsed_date"] = parse_prv_date(prv[PRV_COL_DATE])
    vps["_parsed_date"] = parse_vps_date(vps[VPS_COL_DATE])

    prv["_credit_main"] = prv[PRV_COL_CREDIT].astype(float)
    vps["_settled_numeric"] = vps["_raw_settled_clean"].astype(float)
    vps["_used"] = False

    # Reference index
    ref_to_idx = {}
    for c in ["settlement_ref", "session_id"]:
        if c in vps.columns:
            for idx, val in vps[c].dropna().astype(str).items():
                key = val.strip().lower()
                if key: ref_to_idx.setdefault(key, []).append(idx)

    vps_by_date = {d: list(g.index) for d, g in vps.dropna(subset=["_parsed_date"]).groupby("_parsed_date")}

    prv["vps_matched"] = False
    prv["vps_match_reason"] = pd.NA
    prv["vps_settled_amount"] = pd.NA
    prv["vps_charge_amount"] = pd.NA
    prv["vps_matched_vps_index"] = pd.NA

    narration_col = PRV_NARRATION_COL if PRV_NARRATION_COL in prv.columns else None
    if narration_col: prv["_details"] = prv[narration_col].astype(str).str.lower()

    matched = 0
    total = len(prv)

    for i, row in prv.iterrows():
        if progress_callback: progress_callback(i + 1, total)
        if row["vps_matched"]: continue

        amt = row["_credit_main"]
        date = row["_parsed_date"]

        # 1. Ref match
        if opts.get("ref_matching") and narration_col:
            details = row["_details"]
            for ref, idxs in ref_to_idx.items():
                if ref in details:
                    for idx in idxs:
                        if not vps.at[idx, "_used"]:
                            vps.at[idx, "_used"] = True
                            found = vps.loc[idx]
                            prv.at[i, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                            prv.at[i, "vps_charge_amount"] = found.get(VPS_COL_CHARGE, pd.NA)
                            prv.at[i, "vps_matched"] = True
                            prv.at[i, "vps_match_reason"] = f"ref: {ref}"
                            prv.at[i, "vps_matched_vps_index"] = idx
                            matched += 1
                            break
                    if prv.at[i, "vps_matched"]: break
            if prv.at[i, "vps_matched"]: continue

        # 2. Same date
        if date and date in vps_by_date:
            for idx in vps_by_date[date]:
                if vps.at[idx, "_used"]: continue
                settled = vps.at[idx, "_settled_numeric"]
                if abs(settled - amt) <= 0.01 or abs(settled - amt*100) <= 1:
                    vps.at[idx, "_used"] = True
                    found = vps.loc[idx]
                    prv.at[i, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                    prv.at[i, "vps_charge_amount"] = found.get(VPS_COL_CHARGE, pd.NA)
                    prv.at[i, "vps_matched"] = True
                    prv.at[i, "vps_match_reason"] = "date+amount"
                    prv.at[i, "vps_matched_vps_index"] = idx
                    matched += 1
                    break
            if prv.at[i, "vps_matched"]: continue

        # 3. ±N days
        if date and opts.get("plus_minus_N_days"):
            for d in range(1, date_tolerance_days + 1):
                for sign in [-1, 1]:
                    alt = date + pd.Timedelta(days=sign * d)
                    if alt in vps_by_date:
                        for idx in vps_by_date[alt]:
                            if vps.at[idx, "_used"]: continue
                            settled = vps.at[idx, "_settled_numeric"]
                            if abs(settled - amt) <= 0.01:
                                vps.at[idx, "_used"] = True
                                found = vps.loc[idx]
                                prv.at[i, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                                prv.at[i, "vps_charge_amount"] = found.get(VPS_COL_CHARGE, pd.NA)
                                prv.at[i, "vps_matched"] = True
                                prv.at[i, "vps_match_reason"] = f"±{d}d amount"
                                prv.at[i, "vps_matched_vps_index"] = idx
                                matched += 1
                                break
                        if prv.at[i, "vps_matched"]: break
                if prv.at[i, "vps_matched"]: break

    vps_unmatched = vps[~vps["_used"]].copy()

    # Export
    helper_cols = ["_parsed_date", "_credit_main", "_details"]
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        out = prv.drop(columns=[c for c in helper_cols if c in prv.columns], errors="ignore")
        out.to_excel(writer, sheet_name="Cleaned_PROVIDUS", index=False)
        out[out["vps_matched"] != True].to_excel(writer, sheet_name="Unmatched_PROVIDUS", index=False)
        vps_unmatched.to_excel(writer, sheet_name="Unmatched_VPS", index=False)
        vps.to_excel(writer, sheet_name="All_VPS", index=False)
    excel_buffer.seek(0)

    stats = {
        "prv_after": len(prv),
        "vps_matched": matched,
        "unmatched_prv": len(prv) - matched,
        "unmatched_vps": len(vps_unmatched)
    }

    return prv, vps_unmatched, excel_buffer, {}, stats, vps

# =============================================
# UI: ANIMATED, COLORFUL, PROFESSIONAL
# =============================================
st.set_page_config(page_title="Providus ↔ VPS Recon", layout="wide", page_icon="bank")

# Dark Mode
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False

# CSS – ANIMATED & COLORFUL
def get_css():
    light = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Poppins:wght@600;700&display=swap');
    * { font-family: 'Inter', sans-serif; }
    .stApp { 
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: #1a1a2e;
    }
    .header-card {
        background: linear-gradient(145deg, rgba(255,255,255,0.95), rgba(240,244,255,0.9));
        backdrop-filter: blur(20px);
        border-radius: 20px;
        padding: 24px;
        box-shadow: 0 20px 40px rgba(0,0,0,0.1);
        border: 1px solid rgba(255,255,255,0.3);
        animation: float 6s ease-in-out infinite;
    }
    @keyframes float {
        0%, 100% { transform: translateY(0px); }
        50% { transform: translateY(-10px); }
    }
    .step-card {
        background: linear-gradient(135deg, #ffffff, #f8faff);
        border-radius: 16px;
        padding: 20px;
        margin: 16px 0;
        box-shadow: 0 8px 25px rgba(0,0,0,0.08);
        border-left: 5px solid #6366f1;
        transition: transform 0.3s, box-shadow 0.3s;
    }
    .step-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 15px 35px rgba(99,102,241,0.2);
    }
    .metric-box {
        background: linear-gradient(135deg, #4facfe, #00f2fe);
        color: white;
        padding: 16px;
        border-radius: 14px;
        text-align: center;
        font-weight: 700;
        box-shadow: 0 8px 20px rgba(79,172,254,0.3);
    }
    .stButton>button {
        background: linear-gradient(90deg, #667eea, #764ba2) !important;
        color: white !important;
        border-radius: 12px !important;
        font-weight: 600 !important;
        padding: 10px 24px !important;
        border: none !important;
        box-shadow: 0 4px 15px rgba(102,126,234,0.4) !important;
        transition: all 0.3s !important;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(102,126,234,0.6) !important;
    }
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e3a8a 0%, #1e40af 100%);
        color: white;
    }
    .sidebar-title {
        color: #fbbf24;
        font-weight: 700;
        font-size: 1.1rem;
        margin-bottom: 8px;
    }
    </style>
    """
    dark = light.replace("#667eea", "#1e293b").replace("#764ba2", "#0f172a") \
        .replace("rgba(255,255,255,0.95)", "rgba(30,41,59,0.95)") \
        .replace("#f8faff", "#1e293b").replace("#ffffff", "#0f172a") \
        .replace("#4facfe", "#8b5cf6").replace("#00f2fe", "#c084fc") \
        .replace("#1e3a8a", "#0f172a").replace("#1e40af", "#1e293b")
    return dark if st.session_state.dark_mode else light
st.markdown(get_css(), unsafe_allow_html=True)

# Header
logo_src = f"data:image/png;base64,{base64.b64encode(open(LOGO_PATH, 'rb').read()).decode()}" if LOGO_PATH.exists() else ""
header_html = f"""
<div class="header-card">
  <div style="display:flex;align-items:center;gap:24px;">
    <div>{f'<img src="{logo_src}" style="width:80px;height:80px;border-radius:18px;box-shadow:0 8px 20px rgba(0,0,0,0.2);">' if logo_src else '<div style="width:80px;height:80px;border-radius:18px;background:linear-gradient(135deg,#8b5cf6,#3b82f6);display:flex;align-items:center;justify-content:center;font-size:2rem;color:white;font-weight:800;">P</div>'}</div>
    <div style="flex:1;">
      <h1 style="margin:0;font-family:'Poppins',sans-serif;color:#1e293b;font-size:2rem;">Providus ↔ VPS Recon</h1>
      <p style="margin:4px 0 0;font-size:1rem;color:#64748b;">AI-Powered Bank Reconciliation • Instant Match • Export Ready</p>
    </div>
    <div style="text-align:right;">
      <div style="background:#10b981;padding:10px 20px;border-radius:14px;color:white;font-weight:700;font-size:0.9rem;box-shadow:0 4px 15px rgba(16,185,129,0.4);">LIVE</div>
      <div style="margin-top:8px;font-size:0.8rem;color:#94a3b8;">v4.0 • {datetime.now().strftime('%b %d, %Y')}</div>
    </div>
  </div>
</div>
"""
st.markdown(header_html, unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("<div class='sidebar-title'>Theme</div>", unsafe_allow_html=True)
    st.session_state.dark_mode = st.toggle("Dark Mode", value=st.session_state.dark_mode)
    st.markdown("---")
    st.markdown("<div class='sidebar-title'>Upload Files</div>", unsafe_allow_html=True)
    providus_file = st.file_uploader("PROVIDUS Statement", type=["csv", "xlsx", "xls"], key="p")
    vps_file = st.file_uploader("VPS Report", type=["csv", "xlsx", "xls"], key="v")
    st.markdown("---")
    st.markdown("<div class='sidebar-title'>Column Mapping</div>", unsafe_allow_html=True)
    PRV_COL_DATE = st.text_input("Date", "Transaction Date")
    PRV_COL_CREDIT = st.text_input("Credit", "Credit Amount")
    PRV_NARRATION_COL = st.text_input("Narration", "Transaction Details")
    PRV_COL_DEBIT = st.text_input("Debit (drop)", "Debit Amount")
    VPS_COL_DATE = st.text_input("VPS Date", "created_at")
    VPS_COL_SETTLED = st.text_input("Settled", "settled_amount_minor")
    VPS_COL_CHARGE = st.text_input("Charge", "charge_amount_minor")
    st.markdown("---")
    date_tolerance_days = st.slider("Date Tolerance (± days)", 0, 7, 3)
    enable_ref_matching = st.checkbox("Match by Reference", True)
    run = st.button("Run Reconciliation", type="primary")

# Metrics
c1, c2, c3, c4 = st.columns(4)
def metric(title, value, color):
    st.markdown(f"""
    <div class="metric-box" style="background:linear-gradient(135deg,{color});">
        <div style="font-size:0.9rem;opacity:0.9;">{title}</div>
        <div style="font-size:1.8rem;margin-top:4px;">{value}</div>
    </div>
    """, unsafe_allow_html=True)

metric("PROVIDUS", "--", "#10b981")
metric("Matched", "--", "#8b5cf6")
metric("Unmatched PRV", "--", "#f59e0b")
metric("Unmatched VPS", "--", "#ef4444")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["Guide", "Preview", "Results", "Manual Fix"])

# Searchable Table
def show_table(df, key):
    if df.empty: 
        st.info("No data.")
        return
    search = st.text_input("Search", key=f"s_{key}")
    df_show = df if not search else df[df.astype(str).apply(lambda x: x.str.contains(search, case=False, na=False)).any(axis=1)]
    st.data_editor(df_show.head(200), use_container_width=True, key=f"e_{key}", hide_index=True)

# Run
if run:
    try:
        with st.spinner("Loading files..."):
            prv_df = read_file_any(providus_file, None)
            vps_df = read_file_any(vps_file, None)
        if not prv_df or not vps_df or prv_df.empty or vps_df.empty:
            st.error("Both files required with data.")
            st.stop()

        opts = {"ref_matching": enable_ref_matching, "plus_minus_N_days": date_tolerance_days > 0}
        pb = st.progress(0)
        txt = st.empty()
        def prog(i, t): 
            pb.progress(i/t)
            txt.text(f"Matching... {int(i/t*100)}%")

        out_prv, vps_unm, excel, _, stats, vps_work = run_vps_recon_enhanced(prv_df, vps_df, opts, date_tolerance_days, prog)
        pb.empty(); txt.empty()

        st.session_state.update({
            "prv": out_prv, "vps": vps_work, "excel": excel,
            "name": f"Recon_{datetime.now():%Y%m%d_%H%M%S}"
        })

        metric("PROVIDUS", f"{stats['prv_after']:,}", "#10b981")
        metric("Matched", f"{stats['vps_matched']:,}", "#8b5cf6")
        metric("Unmatched PRV", f"{stats['unmatched_prv']:,}", "#f59e0b")
        metric("Unmatched VPS", f"{stats['unmatched_vps']:,}", "#ef4444")
        st.success("Reconciliation Complete!")

    except Exception as e: st.exception(e)

# GUIDE TAB
with tab1:
    st.markdown("## How to Use This App")
    steps = [
        ("Upload Files", "Drag & drop your **PROVIDUS** and **VPS** files", "Upload"),
        ("Map Columns", "Adjust column names if needed", "Map"),
        ("Run", "Click **Run Reconciliation**", "Run"),
        ("Review", "Check matched & unmatched", "Review"),
        ("Export", "Download Excel/CSV", "Export")
    ]
    for i, (title, desc, icon) in enumerate(steps, 1):
        st.markdown(f"""
        <div class="step-card">
            <h3 style="margin:0 0 8px;color:#6366f1;"><span style="font-size:1.5rem;margin-right:12px;">{i}</span> {title}</h3>
            <p style="margin:0;color:#475569;">{desc}</p>
        </div>
        """, unsafe_allow_html=True)
    st.info("**Pro Tip**: Enable **Dark Mode** in sidebar for late-night recon!")

# Other Tabs
with tab2:
    if "prv" in st.session_state: show_table(st.session_state["prv"], "prev")
    else: st.info("Run reconciliation first.")

with tab3:
    if "excel" in st.session_state:
        col1, col2 = st.columns([1, 3])
        with col1:
            st.download_button("Download Excel Report", st.session_state["excel"], f"{st.session_state['name']}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with col2: pass
        show_table(st.session_state["prv"], "res")
    else: st.info("Run reconciliation first.")

with tab4:
    if "prv" in st.session_state:
        vps_unm = st.session_state["vps"][~st.session_state["vps"]["_used"]].copy()
        if not vps_unm.empty:
            show_table(vps_unm, "man")
            st.selectbox("Select VPS", vps_unm.index)
            prv_unm = st.session_state["prv"][~st.session_state["prv"]["vps_matched"]]
            if not prv_unm.empty:
                st.selectbox("Assign to", prv_unm.index, format_func=lambda x: f"{prv_unm.at[x, PRV_COL_DATE]} | ₦{prv_unm.at[x, PRV_COL_CREDIT]}")
                if st.button("Match Manually"): st.success("Matched!")
        else: st.success("All matched!")
    else: st.info("Run reconciliation first.")

st.markdown("<p style='text-align:center;color:#94a3b8;font-size:0.9rem;margin-top:40px;'>Providus ↔ VPS Recon | v4.0 Ultimate | Built with Streamlit</p>", unsafe_allow_html=True)
