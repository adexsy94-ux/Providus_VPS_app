# providus_vps_app.py
# -*- coding: utf-8 -*-
"""
Providus ↔ VPS Reconciliation – Fancy UI + Blended Colors + Full Guide
Run: streamlit run providus_vps_app.py
"""

import io
import base64
import re
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import numpy as np
import streamlit.components.v1 as components

# -----------------------------
# Config / Paths
# -----------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
LOGO_PATH = DATA_DIR / "Logo.png"

# Ensure data folder exists
DATA_DIR.mkdir(exist_ok=True)

# -----------------------------
# Helper: File Reader
# -----------------------------
@st.cache_data
def read_file_any(uploaded_file, local_path):
    def read_from_path(p):
        if p.suffix.lower() == '.csv':
            return pd.read_csv(p, dtype=str)
        else:
            return pd.read_excel(p, dtype=object)

    if uploaded_file is not None:
        try:
            if uploaded_file.name.lower().endswith('.csv'):
                return pd.read_csv(uploaded_file, dtype=str)
            else:
                return pd.read_excel(uploaded_file, dtype=object)
        except Exception as e:
            if "xlrd" in str(e):
                raise ImportError("Reading .xls files requires 'xlrd'. Run: pip install xlrd>=2.0.1")
            raise
    elif local_path:
        p = Path(local_path)
        if not p.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")
        return read_from_path(p)
    return None

# -----------------------------
# Cleaning & Parsing
# -----------------------------
def clean_numeric_text_col(col):
    if col is None:
        return col
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
def run_vps_recon_enhanced(prv_df, vps_df, opts, date_tolerance_days=3):
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

    for prv_idx, prv_row in prv.iterrows():
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
                    prv.at[prv_idx, "vps_match_reason"] = "date & amount match (main units)"
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
                    prv.at[prv_idx, "vps_match_reason"] = "date match & settled==credit*100 (minor units)"
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
                    prv.at[prv_idx, "vps_match_reason"] = "amount-only fallback (date ignored)"
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
                    prv.at[prv_idx, "vps_match_reason"] = "amount*100 fallback (date ignored)"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                    matched += 1
                    continue

    vps_unmatched = vps[vps["_used"] != True].copy()

    # Merge VPS fields
    matched_vps = vps[vps["_used"] == True].copy()
    rename_map = {
        "id": "vps_id",
        "session_id": "vps_session_id",
        "settlement_ref": "vps_settlement_ref",
        "transaction_amount_minor": "vps_transaction_amount_minor",
        "source_acct_name": "vps_source_acct_name",
        "source_acct_no": "vps_source_acct_no",
        "virtual_acct_no": "vps_virtual_acct_no",
        "created_at": "vps_created_at",
        "reversal_session_id": "vps_reversal_session_id",
        "settlement_notification_retry_batch_id": "vps_settlement_notification_retry_batch_id"
    }
    matched_vps = matched_vps.rename(columns=rename_map)
    vps_merge_cols = [v for k, v in rename_map.items() if k in vps.columns]
    matched_vps = matched_vps[vps_merge_cols]

    out_prv = prv.merge(matched_vps, left_on="vps_matched_vps_index", right_index=True, how="left")

    # === Excel Report ===
    helper_cols = ["_parsed_date", "_credit_main", "_tran_details_lower"]
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        out_prv.drop(columns=[c for c in helper_cols if c in out_prv.columns], errors="ignore") \
               .to_excel(writer, sheet_name="Cleaned_PROVIDUS", index=False)

        log_cols = [
            PRV_COL_DATE, PRV_COL_CREDIT, "vps_matched", "vps_match_reason",
            "vps_settled_amount", "vps_charge_amount",
            "vps_id", "vps_session_id", "vps_settlement_ref", "vps_transaction_amount_minor",
            "vps_source_acct_name", "vps_source_acct_no", "vps_virtual_acct_no",
            "vps_created_at", "vps_reversal_session_id", "vps_settlement_notification_retry_batch_id"
        ]
        out_prv[[c for c in log_cols if c in out_prv.columns]].to_excel(writer, sheet_name="Match_Log", index=False)
        out_prv[out_prv["vps_matched"] != True].to_excel(writer, sheet_name="Unmatched_PROVIDUS", index=False)
        vps_unmatched.reset_index(drop=True).to_excel(writer, sheet_name="Unmatched_VPS", index=False)
        vps.reset_index(drop=True).to_excel(writer, sheet_name="All_VPS_Input", index=False)

    buffer.seek(0)

    stats = {
        "prv_before": before,
        "prv_after": len(out_prv),
        "vps_matched": matched,
        "unmatched_prv": len(out_prv) - matched,
        "unmatched_vps": len(vps_unmatched)
    }

    return out_prv, vps_unmatched, buffer, stats, vps

# =============================================
# MODERN & FANCY STREAMLIT UI
# =============================================
st.set_page_config(page_title="Providus ↔ VPS Recon", layout="wide", page_icon="bank")

# === CUSTOM CSS ===
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"] {font-family: 'Inter', sans-serif;}
    
    .main > div {padding-top: 1rem;}
    .big-button {
        background: linear-gradient(45deg, #4facfe 0%, #00f2fe 100%);
        color: white;
        font-size: 1.2rem !important;
        font-weight: 700;
        padding: 0.8rem 1rem !important;
        border: none;
        border-radius: 12px;
        box-shadow: 0 8px 25px rgba(79, 172, 254, 0.2);
        transition: all 0.2s ease;
        width: 100%;
        margin: 1rem 0;
    }
    .card {
        background: rgba(255, 255, 255, 0.95);
        backdrop-filter: blur(8px);
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 8px 24px rgba(0,0,0,0.08);
        border: 1px solid rgba(255,255,255,0.12);
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# === HEADER WITH LOGO (use components.html for consistent rendering) ===
logo_src = ""
logo_status = ""
try:
    if LOGO_PATH.exists():
        with open(LOGO_PATH, "rb") as f:
            logo_bytes = f.read()
            logo_base64 = base64.b64encode(logo_bytes).decode("utf-8")
            logo_src = f"data:image/png;base64,{logo_base64}"
            logo_status = "Logo loaded"
    else:
        logo_status = "Logo.png not in data/ folder"
except Exception as e:
    logo_status = f"Logo error: {e}"

# show resolved path for debugging (helpful on hosted platforms)
st.sidebar.write(f"Looking for logo at: {LOGO_PATH.resolve()}")
st.sidebar.info(logo_status)

# Header HTML (components ensures it renders as HTML)
header_html = f"""
<div class="header-container" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding:1rem; border-radius:16px; color:white; display:flex; gap:1rem; align-items:center;">
    {f'<img src="{logo_src}" style="width:120px;height:120px;object-fit:contain;border-radius:12px;" alt="Logo">' if logo_src else '<div style="width:120px;height:120px;display:flex;align-items:center;justify-content:center;border-radius:12px;background:rgba(255,255,255,0.06);font-weight:700;">LOGO</div>'}
    <div style="flex:1;">
        <h1 style="margin:0;font-size:2.2rem;">Providus ↔ VPS Recon</h1>
        <p style="margin:0.25rem 0 0 0; opacity:0.95;">Full VPS fields • Smart Matching • One-Click Excel</p>
    </div>
</div>
"""
components.html(f"<html><body>{header_html}</body></html>", height=150)

# === SIDEBAR INPUTS ===
with st.sidebar:
    st.markdown("### Required Files")
    providus_file = st.file_uploader("**PROVIDUS File**", type=["csv", "xlsx", "xls"], key="providus")
    vps_file = st.file_uploader("**VPS File**", type=["csv", "xlsx", "xls"], key="vps")

    st.markdown("---")
    st.markdown("### Column Names")
    PRV_COL_DATE = st.text_input("PROVIDUS Date", value="Transaction Date")
    PRV_COL_CREDIT = st.text_input("PROVIDUS Credit", value="Credit Amount")
    PRV_NARRATION_COL = st.text_input("PROVIDUS Narration", value="Transaction Details")
    PRV_COL_DEBIT = st.text_input("PROVIDUS Debit (drop)", value="Debit Amount")
    VPS_COL_DATE = st.text_input("VPS Date", value="created_at")
    VPS_COL_SETTLED = st.text_input("VPS Settled", value="settled_amount_minor")
    VPS_COL_CHARGE = st.text_input("VPS Charge", value="charge_amount_minor")

    st.markdown("---")
    st.markdown("### Matching Options")
    date_tolerance_days = st.slider("Date Tolerance (± days)", 0, 7, 3)
    enable_amount_only_fallback = st.checkbox("Amount-only fallback", value=False)
    enable_ref_matching = st.checkbox("Reference token matching", value=True)

    run = st.button("GENERATE REPORT", type="primary", key="run", help="Run reconciliation")

# === TABS ===
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Preview", "Results", "Manual"])

# === METRICS ===
m1, m2, m3, m4 = st.columns(4)
m1.metric("PROVIDUS Rows", "—")
m2.metric("Matched", "—")
m3.metric("Unmatched PROVIDUS", "—")
m4.metric("Unmatched VPS", "—")

# === OVERVIEW TAB: DETAILED GUIDE ===
overview_html = r"""
<div class="card how-to">
    <h3>How to Use Providus ↔ VPS Reconciliation</h3>
    <ol>
        <li><strong>Upload Required Files</strong>:<br>
            <code>PROVIDUS</code> bank statement (CSV/XLSX) and <code>VPS</code> settlement file.<br>
            <small>Supported: .csv, .xlsx, .xls</small>
        </li>
        <li><strong>Verify Column Names</strong>:<br>
            Adjust if your files use different headers (e.g., <code>Amount</code> instead of <code>Credit Amount</code>).
        </li>
        <li><strong>Set Matching Options</strong>:<br>
            <ul>
                <li><strong>Reference Matching</strong>: Looks for <code>session_id</code>, <code>settlement_ref</code> in narration</li>
                <li><strong>Date + Amount</strong>: Exact match on date & amount</li>
                <li><strong>±N Days</strong>: Allows date drift (default ±3 days)</li>
                <li><strong>Amount-only fallback</strong>: Use only if confident (risk of false matches)</li>
            </ul>
        </li>
        <li><strong>Click "GENERATE REPORT"</strong></li>
    </ol>

    <h4>Output Report Includes:</h4>
    <ul>
        <li><strong>Cleaned_PROVIDUS</strong>: All bank rows + <strong>full VPS data</strong> merged</li>
        <li><strong>Match_Log</strong>: Summary of matches with reason</li>
        <li><strong>Unmatched_PROVIDUS</strong>: Bank rows not matched</li>
        <li><strong>Unmatched_VPS</strong>: Settlement rows not used</li>
        <li><strong>All_VPS_Input</strong>: Original VPS file (for audit)</li>
    </ul>

    <h4>Manual Inspector (Tab 4)</h4>
    <ul>
        <li>View unmatched VPS rows</li>
        <li>Select a VPS row and assign to unmatched PROVIDUS row</li>
        <li>Click "Assign Manually" → match is saved</li>
        <li>Download final report with manual matches</li>
    </ul>

    <div style="background:#fff3cd; padding:1rem; border-radius:8px; border-left:4px solid #ffc107; margin-top:1.5rem;">
        <strong>Warning</strong>: Use <code>amount-only fallback</code> carefully — may cause false positives.<br>
        <strong>Best Practice</strong>: Always review <code>Unmatched</code> sheets.
    </div>
</div>
"""
components.html(f"<html><body>{overview_html}</body></html>", height=420)

# === RUN LOGIC ===
if run:
    try:
        with st.spinner("Reading files..."):
            prv_df = read_file_any(providus_file, None)
            vps_df = read_file_any(vps_file, None)

        if prv_df is None:
            st.error("No PROVIDUS file uploaded.")
            st.stop()
        if vps_df is None:
            st.error("No VPS file uploaded.")
            st.stop()

        opts = {
            "ref_matching": enable_ref_matching,
            "plus_minus_N_days": date_tolerance_days > 0,
            "amount_only_fallback": enable_amount_only_fallback
        }
        with st.spinner("Running reconciliation..."):
            out_prv, vps_unmatched, excel_buffer, stats, vps_work = run_vps_recon_enhanced(
                prv_df, vps_df, opts, date_tolerance_days
            )

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        report_name = f"Providus_VPS_Recon_{timestamp}.xlsx"

        # Update metrics
        m1.metric("PROVIDUS Rows", stats["prv_after"])
        m2.metric("Matched", stats["vps_matched"])
        m3.metric("Unmatched PROVIDUS", stats["unmatched_prv"])
        m4.metric("Unmatched VPS", stats["unmatched_vps"])

        st.session_state["prv_work"] = out_prv.copy()
        st.session_state["vps_work"] = vps_work.copy()
        st.session_state["report_buffer"] = excel_buffer
        st.session_state["report_name"] = report_name

        with tab2:
            st.success("Reconciliation Complete!")
            st.dataframe(out_prv.head(200))

        with tab3:
            st.download_button(
                "DOWNLOAD FULL REPORT (Excel)",
                data=excel_buffer,
                file_name=report_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.info(f"**{report_name}** includes **5 sheets** with **all VPS fields**")

    except Exception as e:
        st.exception(e)

# === MANUAL INSPECTOR ===
if "prv_work" in st.session_state and "vps_work" in st.session_state:
    with tab4:
        st.subheader("Manual Inspector")
        prv_work = st.session_state["prv_work"]
        vps_work = st.session_state["vps_work"]

        vps_unmatched = vps_work[vps_work["_used"] == False].copy().reset_index(drop=True)
        if vps_unmatched.empty:
            st.write("No unmatched VPS rows.")
        else:
            st.dataframe(vps_unmatched.head(200))
            pick = st.selectbox("Select VPS row", options=vps_unmatched.index)
            picked = vps_unmatched.loc[pick]
            # keep original index if present
            orig_idx = int(picked.get("index", -1)) if "index" in picked.index else int(pick)

            unmatched_prv = prv_work[prv_work["vps_matched"] != True].copy()
            candidates = unmatched_prv.copy()
            if not candidates.empty:
                sel = st.selectbox("Select PROVIDUS row", options=candidates.index,
                                 format_func=lambda x: f"{candidates.at[x, PRV_COL_DATE]} | ₦{candidates.at[x, PRV_COL_CREDIT]}")
                if st.button("Assign Manually"):
                    # apply manual assignment
                    if orig_idx < 0 or orig_idx not in vps_work.index:
                        st.error("Could not locate chosen VPS row index in working VPS dataset.")
                    else:
                        vps_work.at[orig_idx, "_used"] = True
                        found = vps_work.loc[orig_idx]
                        rename_map = {
                            "id": "vps_id", "session_id": "vps_session_id", "settlement_ref": "vps_settlement_ref",
                            "transaction_amount_minor": "vps_transaction_amount_minor", "source_acct_name": "vps_source_acct_name",
                            "source_acct_no": "vps_source_acct_no", "virtual_acct_no": "vps_virtual_acct_no",
                            "created_at": "vps_created_at", "reversal_session_id": "vps_reversal_session_id",
                            "settlement_notification_retry_batch_id": "vps_settlement_notification_retry_batch_id"
                        }
                        for old, new in rename_map.items():
                            if old in found:
                                prv_work.at[sel, new] = found[old]
                        prv_work.at[sel, "vps_settled_amount"] = found.get(VPS_COL_SETTLED, found.get("_raw_settled_clean", pd.NA))
                        prv_work.at[sel, "vps_charge_amount"] = found.get(VPS_COL_CHARGE, pd.NA)
                        prv_work.at[sel, "vps_matched"] = True
                        prv_work.at[sel, "vps_match_reason"] = "MANUAL"
                        prv_work.at[sel, "vps_matched_vps_index"] = orig_idx
                        st.session_state["prv_work"] = prv_work
                        st.session_state["vps_work"] = vps_work
                        st.success("Manual match applied!")

# === FINAL DOWNLOAD (after manual) ===
if "report_buffer" in st.session_state:
    if st.button("Download Final Report (with manual matches)"):
        buf = io.BytesIO()
        out_prv = st.session_state["prv_work"].copy()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            out_prv.drop(columns=[c for c in ["_parsed_date", "_credit_main", "_tran_details_lower"] if c in out_prv.columns], errors="ignore") \
                   .to_excel(writer, sheet_name="Cleaned_PROVIDUS", index=False)
            log_cols = [
                PRV_COL_DATE, PRV_COL_CREDIT, "vps_matched", "vps_match_reason",
                "vps_settled_amount", "vps_charge_amount",
                "vps_id", "vps_session_id", "vps_settlement_ref", "vps_transaction_amount_minor",
                "vps_source_acct_name", "vps_source_acct_no", "vps_virtual_acct_no",
                "vps_created_at", "vps_reversal_session_id", "vps_settlement_notification_retry_batch_id"
            ]
            out_prv[[c for c in log_cols if c in out_prv.columns]].to_excel(writer, sheet_name="Match_Log", index=False)
            out_prv[out_prv["vps_matched"] != True].to_excel(writer, sheet_name="Unmatched_PROVIDUS", index=False)
            vps_work = st.session_state["vps_work"]
            vps_work[vps_work["_used"] != True].reset_index(drop=True).to_excel(writer, sheet_name="Unmatched_VPS", index=False)
            vps_work.reset_index(drop=True).to_excel(writer, sheet_name="All_VPS_INPUT", index=False)
        buf.seek(0)
        st.download_button(
            "Download Final Report",
            data=buf,
            file_name=f"Providus_VPS_Final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.caption("Providus ↔ VPS Reconciliation | All VPS fields included | Paymeter removed")
