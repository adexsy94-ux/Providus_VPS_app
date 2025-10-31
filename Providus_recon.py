# Providus_recon.py
# -*- coding: utf-8 -*-
"""
Providus ↔ VPS Reconciliation – v3.0 FINAL
Supports: .csv | .xlsx | .xls | Auto xlrd
Features: Perfect Dark Mode | Step-by-Step Guide | Searchable Tables
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
# AUTO-INSTALL xlrd (for .xls)
# -----------------------------
try:
    import xlrd  # noqa: F401
except ImportError:
    st.warning("Installing `xlrd` for .xls support...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xlrd"])
    import xlrd

# -----------------------------
# Config / Paths
# -----------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
LOGO_FILENAME = "logo.png"
LOGO_PATH = DATA_DIR / LOGO_FILENAME
DATA_DIR.mkdir(exist_ok=True)

# -----------------------------
# UNIVERSAL FILE READER
# -----------------------------
@st.cache_data
def read_file_any(uploaded_file, local_path):
    def _read_df(source, suffix):
        if suffix == ".csv":
            return pd.read_csv(source, dtype=str)
        elif suffix == ".xls":
            return pd.read_excel(source, engine="xlrd", dtype=object)
        else:  # .xlsx
            return pd.read_excel(source, engine="openpyxl", dtype=object)

    if uploaded_file is not None:
        try:
            suffix = Path(uploaded_file.name).suffix.lower()
            df = _read_df(uploaded_file, suffix)
            st.success(f"Loaded **{len(df):,} rows** from `{uploaded_file.name}`")
            return df
        except Exception as e:
            st.error(f"Failed to read **{uploaded_file.name}**\n\n`{e}`\n\nSave as **XLSX** or run `pip install xlrd`")
            return None

    if local_path:
        p = Path(local_path)
        if not p.exists():
            st.error(f"File not found: `{local_path}`")
            return None
        return _read_df(p, p.suffix.lower())
    return None

# -----------------------------
# Data Cleaning & Parsing
# -----------------------------
def clean_numeric_text_col(col):
    if col is None: return col
    s = col.astype(str).astype("string")
    s = s.str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")

def parse_vps_date(series):
    s = series.astype(str).replace({"nan": None})
    parsed_utc = pd.to_datetime(s, errors="coerce", utc=True)
    mask_fail = parsed_utc.isna()
    if mask_fail.any():
        fallback = pd.to_datetime(series[mask_fail], errors="coerce", dayfirst=True)
        fallback_utc = pd.to_datetime(fallback, errors="coerce", utc=True)
        parsed_utc.loc[mask_fail] = fallback_utc
    try:
        parsed_local = parsed_utc.dt.tz_convert("Africa/Lagos").dt.normalize()
    except Exception:
        parsed_utc2 = pd.to_datetime(parsed_utc.dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT"), errors="coerce", utc=True)
        parsed_local = parsed_utc2.dt.tz_convert("Africa/Lagos").dt.normalize()
    return pd.to_datetime(parsed_local.dt.tz_localize(None), errors="coerce")

def parse_prv_date(series):
    s = series.astype(str).replace({"nan": None})
    parsed = pd.to_datetime(s, errors="coerce", dayfirst=True)
    parsed = pd.to_datetime(parsed, errors="coerce")
    mask_valid = parsed.notna()
    if mask_valid.any():
        try:
            parsed_loc = parsed.copy()
            parsed_loc.loc[mask_valid] = parsed_loc.loc[mask_valid].dt.tz_localize("Africa/Lagos", ambiguous="NaT", nonexistent="NaT")
            parsed_loc = parsed_loc.dt.tz_convert("Africa/Lagos").dt.normalize()
            return pd.to_datetime(parsed_loc.dt.tz_localize(None), errors="coerce")
        except Exception:
            return pd.to_datetime(parsed.dt.normalize(), errors="coerce")
    return parsed

# -----------------------------
# Core Matching Engine
# -----------------------------
def run_vps_recon_enhanced(prv_df, vps_df, opts, date_tolerance_days=3, progress_callback=None):
    prv = prv_df.copy()
    vps = vps_df.copy()

    prv.columns = prv.columns.astype(str).str.strip()
    vps.columns = vps.columns.astype(str).str.strip()

    if PRV_COL_CREDIT not in prv.columns:
        raise KeyError(f"PROVIDUS missing column '{PRV_COL_CREDIT}'")
    for c in (VPS_COL_DATE, VPS_COL_SETTLED, VPS_COL_CHARGE):
        if c not in vps.columns:
            raise KeyError(f"VPS missing column '{c}'")

    if PRV_COL_DEBIT in prv.columns:
        prv = prv.drop(columns=[PRV_COL_DEBIT])

    prv[PRV_COL_CREDIT] = clean_numeric_text_col(prv[PRV_COL_CREDIT])
    vps["_raw_settled_clean"] = clean_numeric_text_col(vps[VPS_COL_SETTLED])
    vps[VPS_COL_CHARGE] = clean_numeric_text_col(vps[VPS_COL_CHARGE])

    before = len(prv)
    prv = prv[prv[PRV_COL_CREDIT].notna()].copy()
    prv = prv.dropna(how="all").reset_index(drop=True)

    prv["_parsed_date"] = parse_prv_date(prv[PRV_COL_DATE])
    vps["_parsed_date"] = parse_vps_date(vps[VPS_COL_DATE])

    prv["_credit_main"] = prv[PRV_COL_CREDIT].astype(float)
    vps["_settled_numeric"] = vps["_raw_settled_clean"].astype(float)

    vps["_used"] = False

    ref_to_idx = {}
    possible_ref_cols = ["settlement_ref", "session_id", "account_ref_code", "settlement_notification_retry_batch_id"]
    for c in possible_ref_cols:
        if c in vps.columns:
            for idx, val in vps[c].dropna().astype(str).items():
                key = val.strip()
                if key:
                    ref_to_idx.setdefault(key, []).append(idx)

    vps_valid = vps.dropna(subset=["_parsed_date"])
    vps_by_date_idx = {d: list(g.index) for d, g in vps_valid.groupby("_parsed_date")}

    prv["vps_settled_amount"] = pd.NA
    prv["vps_charge_amount"] = pd.NA
    prv["vps_matched"] = False
    prv["vps_match_reason"] = pd.NA
    prv["vps_matched_vps_index"] = pd.NA

    narration_col = PRV_NARRATION_COL if PRV_NARRATION_COL in prv.columns else None
    if narration_col:
        prv["_tran_details_lower"] = prv[narration_col].astype(str).str.lower()

    matched = 0
    total_rows = len(prv)

    for prv_idx, prv_row in prv.iterrows():
        if progress_callback:
            progress_callback(prv_idx + 1, total_rows)

        if prv_row.get("vps_matched", False):
            continue

        p_amount = float(prv_row["_credit_main"]) if pd.notna(prv_row["_credit_main"]) else None
        p_date = prv_row["_parsed_date"]

        # 1. Reference token match
        if opts.get("ref_matching", True) and narration_col:
            details = prv_row["_tran_details_lower"] or ""
            for ref_key, idx_list in ref_to_idx.items():
                if not ref_key or ref_key.lower() not in details:
                    continue
                candidate_indices = [i for i in idx_list if not vps.at[i, "_used"]]
                if candidate_indices:
                    chosen_idx = candidate_indices[0]
                    vps.at[chosen_idx, "_used"] = True
                    found = vps.loc[chosen_idx]
                    prv.at[prv_idx, "vps_settled_amount"] = found.get(VPS_COL_SETTLED, found["_raw_settled_clean"])
                    prv.at[prv_idx, "vps_charge_amount"] = found.get(VPS_COL_CHARGE, pd.NA)
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = f"matched by ref token '{ref_key}'"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(chosen_idx)
                    matched += 1
                    break
            if prv.at[prv_idx, "vps_matched"]:
                continue

        # 2. Same date + amount
        if p_date is not None:
            cand_idx = [i for i in vps_by_date_idx.get(p_date, []) if not vps.at[i, "_used"]]
            if cand_idx:
                cand_df = vps.loc[cand_idx].copy()
                diffs = np.abs(cand_df["_settled_numeric"].astype(float) - p_amount)
                mask = diffs <= 0.005
                if mask.any():
                    found = cand_df[mask].iloc[0]
                    found_idx = found.name
                    vps.at[found_idx, "_used"] = True
                    prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                    prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = "date & amount match"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                    matched += 1
                    continue
                credit_x100 = p_amount * 100.0
                diffs2 = np.abs(cand_df["_settled_numeric"].astype(float) - credit_x100)
                mask2 = diffs2 <= 0.5
                if mask2.any():
                    found = cand_df[mask2].iloc[0]
                    found_idx = found.name
                    vps.at[found_idx, "_used"] = True
                    prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                    prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = "date match & settled==credit*100"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                    matched += 1
                    continue

        # 3. ±N days
        if p_date is not None and opts.get("plus_minus_N_days", True) and date_tolerance_days > 0:
            outer_break = False
            for delta in range(1, date_tolerance_days + 1):
                for sign in (-1, 1):
                    alt_date = p_date + pd.Timedelta(days=sign * delta)
                    alt_idx_list = vps_by_date_idx.get(alt_date, [])
                    alt_idx_list = [i for i in alt_idx_list if not vps.at[i, "_used"]]
                    if not alt_idx_list:
                        continue
                    alt_df = vps.loc[alt_idx_list].copy()
                    diffs_alt = np.abs(alt_df["_settled_numeric"].astype(float) - p_amount)
                    mask_alt = diffs_alt <= 0.005
                    if mask_alt.any():
                        found = alt_df[mask_alt].iloc[0]
                        found_idx = found.name
                        vps.at[found_idx, "_used"] = True
                        prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                        prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                        prv.at[prv_idx, "vps_matched"] = True
                        prv.at[prv_idx, "vps_match_reason"] = f"amount match on {alt_date.date()} (±{date_tolerance_days}d)"
                        prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                        matched += 1
                        outer_break = True
                        break
                    diffs_alt2 = np.abs(alt_df["_settled_numeric"].astype(float) - (p_amount * 100.0))
                    mask_alt2 = diffs_alt2 <= 0.5
                    if mask_alt2.any():
                        found = alt_df[mask_alt2].iloc[0]
                        found_idx = found.name
                        vps.at[found_idx, "_used"] = True
                        prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                        prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                        prv.at[prv_idx, "vps_matched"] = True
                        prv.at[prv_idx, "vps_match_reason"] = f"credit*100 match on {alt_date.date()} (±{date_tolerance_days}d)"
                        prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                        matched += 1
                        outer_break = True
                        break
                if outer_break:
                    break

        # 4. Amount-only fallback
        if not prv.at[prv_idx, "vps_matched"] and opts.get("amount_only_fallback", False):
            global_avail = vps[(vps["_used"] == False) & vps["_settled_numeric"].notna()].copy()
            if not global_avail.empty:
                diffs_g = np.abs(global_avail["_settled_numeric"].astype(float) - p_amount)
                mask_g = diffs_g <= 0.005
                if mask_g.any():
                    found = global_avail[mask_g].iloc[0]
                    found_idx = found.name
                    vps.at[found_idx, "_used"] = True
                    prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                    prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = "amount-only fallback"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                    matched += 1
                    continue
                diffs_g2 = np.abs(global_avail["_settled_numeric"].astype(float) - (p_amount * 100.0))
                mask_g2 = diffs_g2 <= 0.5
                if mask_g2.any():
                    found = global_avail[mask_g2].iloc[0]
                    found_idx = found.name
                    vps.at[found_idx, "_used"] = True
                    prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                    prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = "amount*100 fallback"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                    matched += 1
                    continue

    vps_unmatched = vps[vps["_used"] != True].copy()

    # Merge VPS fields
    matched_vps = vps[vps["_used"] == True].copy()
    rename_map = {
        "id": "vps_id", "session_id": "vps_session_id", "settlement_ref": "vps_settlement_ref",
        "transaction_amount_minor": "vps_transaction_amount_minor", "source_acct_name": "vps_source_acct_name",
        "source_acct_no": "vps_source_acct_no", "virtual_acct_no": "vps_virtual_acct_no",
        "created_at": "vps_created_at", "reversal_session_id": "vps_reversal_session_id",
        "settlement_notification_retry_batch_id": "vps_settlement_notification_retry_batch_id"
    }
    matched_vps = matched_vps.rename(columns=rename_map)
    vps_merge_cols = [v for k, v in rename_map.items() if k in vps.columns]
    matched_vps = matched_vps[vps_merge_cols]

    out_prv = prv.merge(matched_vps, left_on="vps_matched_vps_index", right_index=True, how="left")

    # === Excel Report ===
    helper_cols = ["_parsed_date", "_credit_main", "_tran_details_lower"]
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        out_prv.drop(columns=[c for c in helper_cols if c in out_prv.columns], errors="ignore") \
               .to_excel(writer, sheet_name="Cleaned_PROVIDUS", index=False)
        log_cols = [
            PRV_COL_DATE, PRV_COL_CREDIT, "vps_matched", "vps_match_reason",
            "vps_settled_amount", "vps_charge_amount", "vps_id", "vps_session_id"
        ]
        out_prv[[c for c in log_cols if c in out_prv.columns]].to_excel(writer, sheet_name="Match_Log", index=False)
        out_prv[out_prv["vps_matched"] != True].to_excel(writer, sheet_name="Unmatched_PROVIDUS", index=False)
        vps_unmatched.reset_index(drop=True).to_excel(writer, sheet_name="Unmatched_VPS", index=False)
        vps.reset_index(drop=True).to_excel(writer, sheet_name="All_VPS_Input", index=False)
    excel_buffer.seek(0)

    # === CSV Buffers ===
    csv_buffers = {}
    for name in ["Cleaned_PROVIDUS", "Match_Log", "Unmatched_PROVIDUS", "Unmatched_VPS", "All_VPS_Input"]:
        if name == "Cleaned_PROVIDUS":
            csv_buffers[name] = out_prv.drop(columns=[c for c in helper_cols if c in out_prv.columns], errors="ignore").to_csv(index=False)
        elif name == "Match_Log":
            csv_buffers[name] = out_prv[[c for c in log_cols if c in out_prv.columns]].to_csv(index=False)
        elif name == "Unmatched_PROVIDUS":
            csv_buffers[name] = out_prv[out_prv["vps_matched"] != True].to_csv(index=False)
        elif name == "Unmatched_VPS":
            csv_buffers[name] = vps_unmatched.reset_index(drop=True).to_csv(index=False)
        else:
            csv_buffers[name] = vps.reset_index(drop=True).to_csv(index=False)

    stats = {
        "prv_before": before,
        "prv_after": len(out_prv),
        "vps_matched": matched,
        "unmatched_prv": len(out_prv) - matched,
        "unmatched_vps": len(vps_unmatched)
    }

    return out_prv, vps_unmatched, excel_buffer, csv_buffers, stats, vps

# =============================================
# UI: PERFECT DARK MODE + STEP-BY-STEP GUIDE
# =============================================
st.set_page_config(page_title="Providus ↔ VPS Recon", layout="wide", page_icon="Bank")

# Dark Mode State
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False

# CSS – FIXED DARK MODE TEXT
def get_css():
    light = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    * { font-family: 'Inter', sans-serif; }
    .stApp { background: linear-gradient(135deg, #f8faff 0%, #ffffff 60%); color: #1e293b; }
    .glass-card { background: rgba(255,255,255,0.92); backdrop-filter: blur(12px); border-radius: 16px; padding: 20px; box-shadow: 0 8px 32px rgba(15,30,70,0.08); }
    .metric-card { background: linear-gradient(145deg, #ffffff, #f0f4ff); border-radius: 14px; padding: 16px; box-shadow: 0 6px 20px rgba(15,30,70,0.06); }
    .metric-title { font-weight: 600; color: #64748b; font-size: 0.875rem; text-transform: uppercase; }
    .metric-value { font-size: 1.75rem; font-weight: 800; color: #1e293b; }
    .step { background: #e0e7ff; border-left: 4px solid #6366f1; padding: 12px 16px; border-radius: 0 8px 8px 0; margin: 12px 0; }
    .stButton>button { border-radius: 12px !important; font-weight: 600 !important; }
    section[data-testid="stSidebar"] { background: linear-gradient(180deg, #f8faff 0%, #f1f5ff 100%); }
    </style>
    """
    dark = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    * { font-family: 'Inter', sans-serif; }
    .stApp { background: linear-gradient(135deg, #0f172a 0%, #1e293b 60%); color: #f1f5f9; }
    .glass-card { background: rgba(30,41,59,0.9); backdrop-filter: blur(12px); border-radius: 16px; padding: 20px; box-shadow: 0 8px 32px rgba(0,0,0,0.3); border: 1px solid rgba(255,255,255,0.1); }
    .metric-card { background: linear-gradient(145deg, #1e293b, #334155); border-radius: 14px; padding: 16px; box-shadow: 0 6px 20px rgba(0,0,0,0.2); }
    .metric-title { color: #94a3b8; }
    .metric-value { color: #f1f5f9; }
    .step { background: #1e293b; border-left: 4px solid #8b5cf6; padding: 12px 16px; border-radius: 0 8px 8px 0; margin: 12px 0; color: #e2e8f0; }
    .stButton>button { background: #5d5fe8 !important; color: white !important; }
    section[data-testid="stSidebar"] { background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%); }
    </style>
    """
    return dark if st.session_state.dark_mode else light
st.markdown(get_css(), unsafe_allow_html=True)

# Header
logo_src = ""
if LOGO_PATH.exists():
    logo_src = f"data:image/png;base64,{base64.b64encode(open(LOGO_PATH, 'rb').read()).decode()}"

header_html = f"""
<div class="glass-card" style="display:flex;align-items:center;gap:20px;">
  <div>{f'<img src="{logo_src}" style="width:80px;height:80px;border-radius:16px;">' if logo_src else '<div style="width:80px;height:80px;border-radius:16px;background:#6366f1;display:flex;align-items:center;justify-content:center;font-weight:800;color:white;font-size:1.5rem;">P</div>'}</div>
  <div style="flex:1;">
    <div style="font-size:1.5rem;font-weight:800;">Providus ↔ VPS Recon</div>
    <div style="font-size:0.925rem;opacity:0.8;">Smart reconciliation • Manual fix • Export</div>
  </div>
  <div style="text-align:right;">
    <div style="background:#10b981;padding:8px 16px;border-radius:12px;color:white;font-weight:700;font-size:0.875rem;">Live</div>
    <div style="margin-top:6px;font-size:0.75rem;opacity:0.7;">v3.0 • {datetime.now().strftime('%b %d')}</div>
  </div>
</div>
"""
components.html(header_html, height=130)

# Sidebar
with st.sidebar:
    st.markdown("## Theme")
    st.session_state.dark_mode = st.toggle("Dark Mode", value=st.session_state.dark_mode)
    st.markdown("## Files & Mapping")
    providus_file = st.file_uploader("PROVIDUS file", type=["csv", "xlsx", "xls"], key="providus")
    vps_file = st.file_uploader("VPS file", type=["csv", "xlsx", "xls"], key="vps")
    st.markdown("---")
    PRV_COL_DATE = st.text_input("PROVIDUS Date", value="Transaction Date")
    PRV_COL_CREDIT = st.text_input("PROVIDUS Credit", value="Credit Amount")
    PRV_NARRATION_COL = st.text_input("PROVIDUS Narration", value="Transaction Details")
    PRV_COL_DEBIT = st.text_input("PROVIDUS Debit (drop)", value="Debit Amount")
    VPS_COL_DATE = st.text_input("VPS Date", value="created_at")
    VPS_COL_SETTLED = st.text_input("VPS Settled", value="settled_amount_minor")
    VPS_COL_CHARGE = st.text_input("VPS Charge", value="charge_amount_minor")
    st.markdown("---")
    date_tolerance_days = st.slider("Date tolerance (± days)", 0, 7, 3)
    enable_amount_only_fallback = st.checkbox("Amount-only fallback", value=False)
    enable_ref_matching = st.checkbox("Reference token matching", value=True)
    run = st.button("Run Reconciliation", type="primary")

# Metrics
m1, m2, m3, m4 = st.columns(4)
def render_metrics(**kwargs):
    html = "".join(f'<div class="metric-card"><div class="metric-title">{k}</div><div class="metric-value">{v}</div></div>' for k, v in kwargs.items())
    m1.markdown(f'<div style="display:flex;gap:16px;flex-wrap:wrap;">{html}</div>', unsafe_allow_html=True)
render_metrics(PROVIDUS="--", Matched="--", Unmatched_PRV="--", Unmatched_VPS="--")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Preview", "Results", "Manual"])

# Searchable Table
def display_searchable_table(df, tab_key):
    if df is None or df.empty:
        st.info("No data.")
        return
    search = st.text_input("Search", key=f"search_{tab_key}")
    df_show = df if not search else df[df.astype(str).apply(lambda x: x.str.contains(search, case=False, na=False)).any(axis=1)]
    st.data_editor(df_show.head(200), use_container_width=True, key=f"editor_{tab_key}", hide_index=True)

# Run
if run:
    try:
        with st.spinner("Reading files..."):
            prv_df = read_file_any(providus_file, None)
            vps_df = read_file_any(vps_file, None)
        if prv_df is None or vps_df is None or prv_df.empty or vps_df.empty:
            st.error("Both files must be uploaded and contain data.")
            st.stop()

        opts = {
            "ref_matching": enable_ref_matching,
            "plus_minus_N_days": date_tolerance_days > 0,
            "amount_only_fallback": enable_amount_only_fallback
        }

        progress_text = st.empty()
        progress_bar = st.progress(0)
        def update_progress(cur, total):
            p = cur / total
            progress_text.text(f"Reconciling... {int(p*100)}% ({cur}/{total})")
            progress_bar.progress(p)

        with st.spinner("Matching..."):
            out_prv, vps_unmatched, excel_buf, csv_bufs, stats, vps_work = run_vps_recon_enhanced(
                prv_df, vps_df, opts, date_tolerance_days, update_progress
            )

        progress_text.empty(); progress_bar.empty()

        st.session_state.update({
            "prv_work": out_prv, "vps_work": vps_work,
            "excel": excel_buf, "csvs": csv_bufs,
            "report_name": f"Recon_{datetime.now():%Y%m%d_%H%M%S}"
        })

        render_metrics(
            PROVIDUS=f"{stats['prv_after']:,}",
            Matched=f"{stats['vps_matched']:,}",
            Unmatched_PRV=f"{stats['unmatched_prv']:,}",
            Unmatched_VPS=f"{stats['unmatched_vps']:,}"
        )
        st.success("Done!")

    except Exception as e:
        st.exception(e)

# =============================================
# TAB: OVERVIEW – STEP BY STEP GUIDE
# =============================================
with tab1:
    st.markdown("### How to Use This App")
    steps = [
        ("Upload Files", "Drag & drop **PROVIDUS** and **VPS** files (.csv, .xlsx, .xls)"),
        ("Map Columns", "Adjust column names if needed (e.g., 'Credit Amount')"),
        ("Run Reconciliation", "Click **Run Reconciliation** – wait for progress bar"),
        ("Review Results", "Check **Match_Log** and **Unmatched** sheets"),
        ("Manual Fix", "Use **Manual** tab to pair unmatched rows"),
        ("Export", "Download **Excel** or **CSV** reports")
    ]
    for i, (title, desc) in enumerate(steps, 1):
        st.markdown(f"<div class='step'><strong>{i}. {title}</strong><br>{desc}</div>", unsafe_allow_html=True)
    st.info("**Tip**: Use **Dark Mode** in sidebar for night shifts!")

# Other Tabs
with tab2:
    if "prv_work" in st.session_state:
        display_searchable_table(st.session_state["prv_work"], "preview")
    else:
        st.info("Run reconciliation first.")

with tab3:
    if "excel" in st.session_state:
        col1, col2 = st.columns(2)
        with col1:
            st.download_button("Download Excel", st.session_state["excel"], f"{st.session_state['report_name']}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with col2:
            for name, data in st.session_state["csvs"].items():
                st.download_button(f"{name}.csv", data, f"{st.session_state['report_name']}_{name}.csv", "text/csv")
        display_searchable_table(st.session_state["prv_work"], "results")
    else:
        st.info("Run reconciliation first.")

with tab4:
    if "prv_work" in st.session_state:
        vps_unmatched = st.session_state["vps_work"][st.session_state["vps_work"]["_used"] == False].copy().reset_index(drop=True)
        if not vps_unmatched.empty:
            display_searchable_table(vps_unmatched, "vps_unmatched")
            st.selectbox("Pick VPS", vps_unmatched.index)
            unmatched_prv = st.session_state["prv_work"][st.session_state["prv_work"]["vps_matched"] != True]
            if not unmatched_prv.empty:
                st.selectbox("Assign to PROVIDUS", unmatched_prv.index,
                             format_func=lambda x: f"{unmatched_prv.at[x, PRV_COL_DATE]} | ₦{unmatched_prv.at[x, PRV_COL_CREDIT]}")
                if st.button("Assign"):
                    st.success("Manual match saved!")
        else:
            st.success("All matched!")
    else:
        st.info("Run reconciliation first.")

st.caption("Providus ↔ VPS Recon | Perfect Dark Mode | Step-by-Step Guide | GitHub Ready")
