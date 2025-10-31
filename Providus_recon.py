# providus_vps_app.py
# -*- coding: utf-8 -*-
"""
Providus ↔ VPS Reconciliation – Full app (robust .xls/.xlsx/.csv handling + fixed CSS + fancy UI)
Place your logo at: data/logo.png (lowercase)

IMPORTANT:
 - To support reading legacy .xls files on your host, ensure `xlrd>=2.0.1` is installed.
 - Recommended requirements.txt contains at least:
     streamlit
     pandas
     numpy
     openpyxl
     xlrd>=2.0.1
 - If you can't install xlrd on the host, convert .xls files to .xlsx or .csv before uploading.
"""

import io
import base64
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
LOGO_FILENAME = "logo.png"   # user indicated lowercase filename
LOGO_PATH = DATA_DIR / LOGO_FILENAME
DATA_DIR.mkdir(exist_ok=True)

# -----------------------------
# Robust file reader (handles .csv, .xlsx, .xls)
# -----------------------------
@st.cache_data
def read_file_any(uploaded_file, local_path):
    """
    Read uploaded_file (Streamlit UploadedFile) or local_path (Path or str).
    Supports .csv, .xlsx and .xls. For .xls pandas requires 'xlrd' (>=2.0.1).
    Returns a DataFrame or raises a helpful error.
    """
    def read_from_path(p: Path):
        suffix = p.suffix.lower()
        if suffix == ".csv":
            return pd.read_csv(p, dtype=str)
        if suffix == ".xlsx":
            return pd.read_excel(p, dtype=object, engine="openpyxl")
        if suffix == ".xls":
            try:
                return pd.read_excel(p, dtype=object, engine="xlrd")
            except ModuleNotFoundError:
                raise ImportError("Reading local .xls files requires the 'xlrd' package. Install with: pip install xlrd>=2.0.1")
        raise ValueError(f"Unsupported file type: {suffix}")

    # If user uploaded file via Streamlit
    if uploaded_file is not None:
        name = uploaded_file.name.lower()
        if name.endswith(".csv"):
            return pd.read_csv(uploaded_file, dtype=str)
        if name.endswith(".xlsx"):
            return pd.read_excel(uploaded_file, dtype=object, engine="openpyxl")
        if name.endswith(".xls"):
            # Try to read using xlrd engine; give clear error if missing
            try:
                return pd.read_excel(uploaded_file, dtype=object, engine="xlrd")
            except ModuleNotFoundError:
                # Provide clear action inside the app (but still raise to allow outer try/except to handle)
                raise ImportError("Reading uploaded .xls files requires 'xlrd'. Install with: pip install xlrd>=2.0.1 on the host, or convert to .xlsx/.csv locally and re-upload.")
            except Exception:
                # last fallback: let pandas autodetect (may still fail)
                try:
                    return pd.read_excel(uploaded_file, dtype=object)
                except Exception as e:
                    raise e
        raise ValueError(f"Unsupported uploaded file type: {uploaded_file.name}")

    # If reading from a local path
    if local_path:
        p = Path(local_path)
        if not p.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")
        return read_from_path(p)

    return None

# -----------------------------
# Cleaning & Parsing helpers
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
# UI: CSS injection + header (fixed)
# =============================================
st.set_page_config(page_title="Providus ↔ VPS Recon", layout="wide", page_icon="🏦")

GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* Page background */
.stApp { background: linear-gradient(180deg, #f7f9ff 0%, #ffffff 40%); padding: 18px 18px; }

/* Metric Row & Card */
.metric-row { display:flex; gap:14px; margin-bottom:12px; }
.metric-card { flex:1; padding:12px; border-radius:12px; background: linear-gradient(180deg, #ffffff, #fbfdff); box-shadow: 0 8px 24px rgba(20,40,70,0.04); border: 1px solid rgba(80,90,160,0.04); }
.metric-title { font-weight:700; color:#1f2937; font-size:0.92rem; margin-bottom:6px; }
.metric-value { font-size:1.6rem; font-weight:800; color:#0b1220; }

/* Buttons rounded */
.stButton>button { border-radius:10px; padding:8px 12px; font-weight:700; }

/* Small card */
.small-card { padding:12px; border-radius:12px; background:white; box-shadow:0 6px 20px rgba(15,23,42,0.04); }

/* Prevent dataframe overflow */
[data-testid="stDataFrameContainer"] { overflow-x: auto; }
</style>
"""
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

# Header (components.html for consistent rendering)
logo_src = ""
logo_status = ""
try:
    if LOGO_PATH.exists():
        with open(LOGO_PATH, "rb") as f:
            logo_bytes = f.read()
            logo_base64 = base64.b64encode(logo_bytes).decode("utf-8")
            logo_src = f"data:image/png;base64,{logo_base64}"
            logo_status = f"{LOGO_FILENAME} loaded"
    else:
        logo_status = f"{LOGO_FILENAME} not found in data/ folder"
except Exception as e:
    logo_status = f"Logo error: {e}"

st.sidebar.write(f"Logo path: {LOGO_PATH.resolve()}")
st.sidebar.info(logo_status)

header_html = f"""
<html><body style="margin:0;padding:0">
  <div style="display:flex;align-items:center;gap:18px;padding:10px;border-radius:12px;">
    {'<img src="'+logo_src+'" style="width:88px;height:88px;border-radius:12px;object-fit:contain;box-shadow:0 10px 30px rgba(0,0,0,0.06)">' if logo_src else '<div style="width:88px;height:88px;border-radius:12px;background:linear-gradient(90deg,#eef2ff,#fbfbff);display:flex;align-items:center;justify-content:center;font-weight:700;color:#556;letter-spacing:1px;">LOGO</div>'}
    <div style="flex:1;">
      <div style="font-size:20px;font-weight:800;color:#0f172a">Providus ↔ VPS Recon</div>
      <div style="margin-top:6px;color:#475569">Smart matching • Manual inspector • Excel export</div>
    </div>
    <div style="display:flex;flex-direction:column;gap:6px;align-items:flex-end">
      <div style="background:linear-gradient(90deg,#667eea,#764ba2);padding:8px 14px;border-radius:10px;color:white;font-weight:700;box-shadow:0 8px 20px rgba(102,126,234,0.14)">Ready</div>
      <div style="font-size:12px;color:#94a3b8">v1.0</div>
    </div>
  </div>
</body></html>
"""
components.html(header_html, height=120)

# Sidebar controls
with st.sidebar:
    st.markdown("## Files & Mapping")
    providus_file = st.file_uploader("PROVIDUS file", type=["csv", "xlsx", "xls"], key="providus")
    vps_file = st.file_uploader("VPS file", type=["csv", "xlsx", "xls"], key="vps")
    st.markdown("---")
    st.markdown("### Column mapping")
    PRV_COL_DATE = st.text_input("PROVIDUS Date", value="Transaction Date")
    PRV_COL_CREDIT = st.text_input("PROVIDUS Credit", value="Credit Amount")
    PRV_NARRATION_COL = st.text_input("PROVIDUS Narration", value="Transaction Details")
    PRV_COL_DEBIT = st.text_input("PROVIDUS Debit (drop)", value="Debit Amount")
    VPS_COL_DATE = st.text_input("VPS Date", value="created_at")
    VPS_COL_SETTLED = st.text_input("VPS Settled", value="settled_amount_minor")
    VPS_COL_CHARGE = st.text_input("VPS Charge", value="charge_amount_minor")
    st.markdown("---")
    st.markdown("### Matching options")
    date_tolerance_days = st.slider("Date tolerance (± days)", 0, 7, 3)
    enable_amount_only_fallback = st.checkbox("Amount-only fallback", value=False)
    enable_ref_matching = st.checkbox("Reference token matching", value=True)
    st.markdown("---")
    run = st.button("▶ Run reconciliation", key="run", help="Run reconciliation")

# Main tabs & metrics
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Preview", "Results", "Manual"])

# metrics renderer
metric_cols = st.container()
with metric_cols:
    col1, col2, col3, col4 = st.columns(4, gap="small")
    m1 = col1.empty()
    m2 = col2.empty()
    m3 = col3.empty()
    m4 = col4.empty()

def render_metrics(prv_rows="—", matched="—", unmatched_prv="—", unmatched_vps="—"):
    html = f"""
    <div class="metric-row">
      <div class="metric-card">
        <div class="metric-title">PROVIDUS rows</div>
        <div class="metric-value">{prv_rows}</div>
      </div>
      <div class="metric-card">
        <div class="metric-title">Matched</div>
        <div class="metric-value">{matched}</div>
      </div>
      <div class="metric-card">
        <div class="metric-title">Unmatched (PROV)</div>
        <div class="metric-value">{unmatched_prv}</div>
      </div>
      <div class="metric-card">
        <div class="metric-title">Unmatched (VPS)</div>
        <div class="metric-value">{unmatched_vps}</div>
      </div>
    </div>
    """
    # Render into the first placeholder; clear others to avoid duplicates
    m1.markdown(html, unsafe_allow_html=True)
    m2.empty(); m3.empty(); m4.empty()

render_metrics()

# Overview tab content
with tab1:
    overview_component = """
    <html><body style="margin:0;padding:0">
      <div style="padding:16px;border-radius:12px;background:linear-gradient(180deg,#ffffff,#fbfdff);box-shadow:0 12px 40px rgba(15,23,42,0.04);">
        <div style="display:flex;gap:18px;">
          <div style="flex:1">
            <h3 style="margin:0 0 8px 0;color:#0f172a">How to use</h3>
            <p style="margin:0;color:#475569">Upload PROVIDUS and VPS files, confirm column mapping, set matching options and run. Inspect unmatched and optionally assign manually.</p>
            <ul style="color:#334155;margin-top:10px">
              <li><strong>Reference matching</strong> - looks for session_id or settlement_ref in narration</li>
              <li><strong>Date + amount</strong> exact match, with ±N day tolerance</li>
              <li><strong>Manual inspector</strong> - hand match unmatched rows</li>
            </ul>
          </div>
          <div style="width:320px;">
            <div style="padding:12px;border-radius:10px;background:linear-gradient(90deg,#eef2ff,#f7f7ff);box-shadow:0 8px 20px rgba(102,126,234,0.06)">
              <div style="font-weight:700;color:#0f172a">Export</div>
              <div style="color:#475569;margin-top:8px">Excel includes Cleaned_PROVIDUS, Match_Log, Unmatched_PROVIDUS, Unmatched_VPS, All_VPS_Input</div>
            </div>
          </div>
        </div>
      </div>
    </body></html>
    """
    components.html(overview_component, height=240)

# Run logic
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

        # store outputs
        st.session_state["prv_work"] = out_prv.copy()
        st.session_state["vps_work"] = vps_work.copy()
        st.session_state["report_buffer"] = excel_buffer
        st.session_state["report_name"] = f"Providus_VPS_Recon_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        # update metrics
        render_metrics(prv_rows=stats["prv_after"], matched=stats["vps_matched"],
                       unmatched_prv=stats["unmatched_prv"], unmatched_vps=stats["unmatched_vps"])

        st.success("Reconciliation complete — check Results / Manual tabs")

    except ImportError as ie:
        # Friendly guidance when xlrd missing
        st.error(str(ie))
        st.info("If you deployed on Streamlit Cloud, add a requirements.txt with 'xlrd>=2.0.1' and redeploy. Or convert .xls files to .xlsx/.csv and re-upload.")
    except Exception as e:
        st.exception(e)

# Preview tab
with tab2:
    if "prv_work" in st.session_state:
        st.write("Preview (first 200 rows)")
        st.dataframe(st.session_state["prv_work"].head(200))
    else:
        st.info("Run a reconciliation to preview results here.")

# Results tab (download)
with tab3:
    if "report_buffer" in st.session_state:
        st.markdown("### Download full Excel report")
        st.download_button(
            "⬇️ Download Excel report (5 sheets)",
            data=st.session_state["report_buffer"],
            file_name=st.session_state["report_name"],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.markdown("### Quick view")
        st.dataframe(st.session_state["prv_work"].head(200))
    else:
        st.info("No report available yet — run reconciliation first.")

# Manual inspector
with tab4:
    if "prv_work" in st.session_state and "vps_work" in st.session_state:
        prv_work = st.session_state["prv_work"]
        vps_work = st.session_state["vps_work"]

        vps_unmatched = vps_work[vps_work["_used"] == False].copy().reset_index(drop=True)
        if vps_unmatched.empty:
            st.write("No unmatched VPS rows.")
        else:
            st.write("Unmatched VPS (sample):")
            st.dataframe(vps_unmatched.head(200))

            pick = st.selectbox("Select VPS row (index)", options=vps_unmatched.index)
            picked = vps_unmatched.loc[pick]
            orig_idx = int(picked.get("index", -1)) if "index" in picked.index else int(pick)

            unmatched_prv = prv_work[prv_work["vps_matched"] != True].copy()
            if unmatched_prv.empty:
                st.write("No unmatched PROVIDUS rows to assign to.")
            else:
                sel = st.selectbox("Select PROVIDUS row to assign", options=unmatched_prv.index,
                                   format_func=lambda x: f"{unmatched_prv.at[x, PRV_COL_DATE]} | ₦{unmatched_prv.at[x, PRV_COL_CREDIT]}")
                if st.button("Assign manually"):
                    if orig_idx < 0 or orig_idx not in vps_work.index:
                        st.error("Could not locate chosen VPS row in working VPS dataset.")
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
    else:
        st.info("No reconciliation session in memory. Run reconciliation first.")

# Final export after manual adjustments
if "prv_work" in st.session_state and "vps_work" in st.session_state:
    st.markdown("---")
    if st.button("📦 Export final workbook (with manual matches)"):
        buf = io.BytesIO()
        out_prv = st.session_state["prv_work"].copy()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            out_prv.drop(columns=[c for c in ["_parsed_date", "_credit_main", "_tran_details_lower"] if c in out_prv.columns], errors="ignore").to_excel(writer, sheet_name="Cleaned_PROVIDUS", index=False)
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
            vps_work.reset_index(drop=True).to_excel(writer, sheet_name="All_VPS_Input", index=False)
        buf.seek(0)
        st.download_button(
            "⬇️ Download Final Report",
            data=buf,
            file_name=f"Providus_VPS_Final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.caption("Providus ↔ VPS Reconciliation | Robust file handling • Fixed CSS • Fancy UI")
