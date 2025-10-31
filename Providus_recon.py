# providus_vps_app.py
# -*- coding: utf-8 -*-
"""
Providus ↔ VPS Reconciliation – Modern Glassmorphic UI
Run: streamlit run providus_vps_app.py
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
LOGO_FILENAME = "logo.png"
LOGO_PATH = DATA_DIR / LOGO_FILENAME
DATA_DIR.mkdir(exist_ok=True)

# -----------------------------
# Helpers: File Reader + Cleaners
# -----------------------------
@st.cache_data
def read_file_any(uploaded_file, local_path):
    def read_from_path(p):
        if p.suffix.lower() == ".csv":
            return pd.read_csv(p, dtype=str)
        else:
            return pd.read_excel(p, dtype=object)

    if uploaded_file is not None:
        try:
            if uploaded_file.name.lower().endswith(".csv"):
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
# UI: Modern Glassmorphic Design
# =============================================
st.set_page_config(page_title="Providus ↔ VPS Recon", layout="wide", page_icon="💳")

# === GLOBAL CSS (Glassmorphic + Neumorphic) ===
GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* Global */
html, body, [class*="css"] { 
    font-family: 'Inter', system-ui, sans-serif; 
    -webkit-font-smoothing: antialiased;
}

/* Background */
.stApp {
    background: linear-gradient(135deg, #f0f4ff 0%, #ffffff 60%);
    background-attachment: fixed;
    padding: 20px;
    min-height: 100vh;
}

/* Glassmorphic Cards */
.glass-card {
    background: rgba(255, 255, 255, 0.92);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border-radius: 16px;
    border: 1px solid rgba(255, 255, 255, 0.3);
    box-shadow: 
        0 8px 32px rgba(15, 30, 70, 0.08),
        0 0 0 1px rgba(100, 120, 200, 0.05);
    padding: 16px;
    transition: all 0.3s ease;
}
.glass-card:hover {
    transform: translateY(-2px);
    box-shadow: 
        0 12px 40px rgba(15, 30, 70, 0.12),
        0 0 0 1px rgba(100, 120, 200, 0.08);
}

/* Metric Cards */
.metric-row { 
    display: flex; 
    gap: 16px; 
    margin-bottom: 16px; 
    flex-wrap: wrap;
}
.metric-card {
    flex: 1;
    min-width: 180px;
    padding: 16px;
    border-radius: 14px;
    background: linear-gradient(145deg, #ffffff, #f8faff);
    box-shadow: 
        0 6px 20px rgba(15, 30, 70, 0.06),
        inset 0 1px 0 rgba(255, 255, 255, 0.8);
    border: 1px solid rgba(100, 130, 220, 0.1);
    transition: all 0.2s ease;
}
.metric-card:hover {
    transform: translateY(-3px);
    box-shadow: 
        0 10px 28px rgba(15, 30, 70, 0.1),
        inset 0 1px 0 rgba(255, 255, 255, 0.9);
}
.metric-title { 
    font-weight: 600; 
    color: #64748b; 
    font-size: 0.875rem; 
    letter-spacing: 0.5px;
    text-transform: uppercase;
}
.metric-value { 
    font-size: 1.75rem; 
    font-weight: 800; 
    color: #1e293b;
    margin-top: 4px;
}

/* Buttons */
.stButton > button {
    border-radius: 12px !important;
    font-weight: 600 !important;
    padding: 10px 16px !important;
    transition: all 0.2s ease !important;
    border: none !important;
}
.stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 8px 20px rgba(102, 126, 234, 0.25) !important;
}
.primary-btn {
    background: linear-gradient(135deg, #5d5fe8, #7c3aed) !important;
    color: white !important;
}
.primary-btn:hover {
    background: linear-gradient(135deg, #4f46e5, #6d28d9) !important;
}

/* Tabs */
section[data-testid="stTabs"] {
    background: transparent;
}
div[role="tablist"] {
    gap: 8px;
    padding: 4px;
    background: rgba(248, 250, 255, 0.7);
    border-radius: 12px;
    backdrop-filter: blur(8px);
}
div[role="tab"] {
    border-radius: 10px !important;
    font-weight: 600;
    padding: 10px 16px !important;
    transition: all 0.2s ease;
}
div[role="tab"]:hover {
    background: rgba(102, 126, 234, 0.12);
}
div[role="tab"][aria-selected="true"] {
    background: linear-gradient(135deg, #5d5fe8, #7c3aed) !important;
    color: white !important;
    box-shadow: 0 4px 12px rgba(93, 95, 232, 0.3);
}

/* DataFrames */
.stDataFrame {
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 4px 16px rgba(15, 30, 70, 0.06);
}
.stDataFrame > div {
    border: none !important;
}
tr:nth-child(even) {
    background-color: rgba(248, 250, 255, 0.5);
}

/* Download Buttons */
a[kind="primary"] {
    background: linear-gradient(135deg, #10b981, #059669) !important;
    border-radius: 12px !important;
    padding: 12px 20px !important;
    font-weight: 600 !important;
    box-shadow: 0 6px 16px rgba(16, 185, 129, 0.2);
}
a[kind="primary"]:hover {
    background: linear-gradient(135deg, #059669, #047857) !important;
    transform: translateY(-1px);
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #f8faff 0%, #f1f5ff 100%);
    border-right: 1px solid rgba(100, 130, 220, 0.1);
}
section[data-testid="stSidebar"] .css-1d391kg {
    padding-top: 1rem;
}

/* Success Pulse */
@keyframes pulse {
    0% { transform: scale(1); }
    50% { transform: scale(1.02); }
    100% { transform: scale(1); }
}

/* Responsive */
@media (max-width: 768px) {
    .metric-row { flex-direction: column; }
    .metric-card { min-width: 100%; }
}
</style>
"""
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

# === HEADER (Glassmorphic + Live Badge) ===
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

header_html = f"""
<html>
  <body style="margin:0;padding:0;font-family:'Inter',sans-serif;">
    <div class="glass-card" style="display:flex;align-items:center;gap:20px;padding:16px;">
      <div style="position:relative;">
        {'<img src="'+logo_src+'" style="width:80px;height:80px;border-radius:16px;object-fit:contain;box-shadow:0 8px 24px rgba(0,0,0,0.1);border:3px solid white;">' if logo_src else '<div style="width:80px;height:80px;border-radius:16px;background:linear-gradient(135deg,#e0e7ff,#c7d2fe);display:flex;align-items:center;justify-content:center;font-weight:800;color:#4f46e5;font-size:1.5rem;letter-spacing:1px;box-shadow:0 8px 24px rgba(0,0,0,0.1);">P</div>'}
        <div style="position:absolute;-bottom:6px;-right:6px;width:24px;height:24px;background:#10b981;border-radius:50%;border:3px solid white;box-shadow:0 2px 8px rgba(0,0,0,0.1);"></div>
      </div>
      <div style="flex:1;">
        <div style="font-size:1.5rem;font-weight:800;color:#1e293b;letter-spacing:-0.5px;">Providus ↔ VPS Recon</div>
        <div style="margin-top:4px;color:#64748b;font-size:0.925rem;font-weight:500;">Smart reconciliation engine • Manual inspector • Export-ready</div>
      </div>
      <div style="text-align:right;">
        <div style="background:linear-gradient(135deg,#5d5fe8,#7c3aed);padding:8px 16px;border-radius:12px;color:white;font-weight:700;font-size:0.875rem;box-shadow:0 6px 16px rgba(93,95,232,0.25);display:inline-block;">
          Live
        </div>
        <div style="margin-top:6px;font-size:0.75rem;color:#94a3b8;font-weight:500;">v1.2 • {datetime.now().strftime('%b %d')}</div>
      </div>
    </div>
  </body>
</html>
"""
components.html(header_html, height=130)

# === SIDEBAR CONTROLS ===
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
    run = st.button("Run reconciliation", key="run", help="Run reconciliation")

# === METRICS RENDERER ===
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
    m1.markdown(html, unsafe_allow_html=True)
    m2.empty(); m3.empty(); m4.empty()

render_metrics()

# === TABS ===
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Preview", "Results", "Manual"])

# === Overview Tab ===
with tab1:
    overview_component = """
    <html><body style="margin:0;padding:0">
      <div class="glass-card" style="padding:20px;">
        <div style="display:flex;gap:24px;flex-wrap:wrap;">
          <div style="flex:1;min-width:300px;">
            <h3 style="margin:0 0 12px 0;color:#1e293b;font-weight:700;">How to use</h3>
            <p style="margin:0 0 16px 0;color:#475569;line-height:1.6;">Upload PROVIDUS and VPS files, confirm column mapping, set matching options and run. Inspect unmatched and optionally assign manually.</p>
            <ul style="color:#334155;margin:16px 0;padding-left:20px;line-height:1.7;">
              <li><strong>Reference matching</strong> – scans narration for session_id or settlement_ref</li>
              <li><strong>Date + amount</strong> – exact match with ±N day tolerance</li>
              <li><strong>Manual inspector</strong> – hand-match unmatched rows</li>
            </ul>
          </div>
          <div style="width:340px;">
            <div class="glass-card" style="padding:16px;">
              <div style="font-weight:700;color:#1e293b;margin-bottom:8px;">Export</div>
              <div style="color:#475569;font-size:0.925rem;line-height:1.5;">Excel includes 5 sheets: Cleaned_PROVIDUS, Match_Log, Unmatched_PROVIDUS, Unmatched_VPS, All_VPS_Input</div>
            </div>
          </div>
        </div>
      </div>
    </body></html>
    """
    components.html(overview_component, height=280)

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

        st.session_state["prv_work"] = out_prv.copy()
        st.session_state["vps_work"] = vps_work.copy()
        st.session_state["report_buffer"] = excel_buffer
        st.session_state["report_name"] = f"Providus_VPS_Recon_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        render_metrics(
            prv_rows=stats["prv_after"],
            matched=stats["vps_matched"],
            unmatched_prv=stats["unmatched_prv"],
            unmatched_vps=stats["unmatched_vps"]
        )

        st.success("Reconciliation complete — check Results / Manual tabs")
        st.markdown("""
        <script>
        setTimeout(() => {
            const alert = document.querySelector('.stAlert');
            if (alert) alert.style.animation = 'pulse 0.6s ease-out';
        }, 100);
        </script>
        """, unsafe_allow_html=True)

    except Exception as e:
        st.exception(e)

# === Preview Tab ===
with tab2:
    if "prv_work" in st.session_state:
        st.write("Preview (first 200 rows)")
        st.dataframe(st.session_state["prv_work"].head(200), use_container_width=True)
    else:
        st.info("Run a reconciliation to preview results here.")

# === Results Tab ===
with tab3:
    if "report_buffer" in st.session_state:
        st.markdown("### Download full Excel report")
        st.download_button(
            "Download Excel report (5 sheets)",
            data=st.session_state["report_buffer"],
            file_name=st.session_state["report_name"],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.markdown("### Quick view")
        st.dataframe(st.session_state["prv_work"].head(200), use_container_width=True)
    else:
        st.info("No report available yet — run reconciliation first.")

# === Manual Inspector ===
with tab4:
    if "prv_work" in st.session_state and "vps_work" in st.session_state:
        prv_work = st.session_state["prv_work"]
        vps_work = st.session_state["vps_work"]

        vps_unmatched = vps_work[vps_work["_used"] == False].copy().reset_index(drop=True)
        if vps_unmatched.empty:
            st.success("No unmatched VPS rows.")
        else:
            st.write("Unmatched VPS (sample):")
            st.dataframe(vps_unmatched.head(200), use_container_width=True)

            pick = st.selectbox("Select VPS row (index)", options=vps_unmatched.index)
            picked = vps_unmatched.loc[pick]
            orig_idx = int(picked.get("index", -1)) if "index" in picked.index else int(pick)

            unmatched_prv = prv_work[prv_work["vps_matched"] != True].copy()
            if unmatched_prv.empty:
                st.success("No unmatched PROVIDUS rows to assign to.")
            else:
                sel = st.selectbox("Select PROVIDUS row to assign", options=unmatched_prv.index,
                                   format_func=lambda x: f"{unmatched_prv.at[x, PRV_COL_DATE]} | ₦{unmatched_prv.at[x, PRV_COL_CREDIT]}")
                if st.button("Assign manually"):
                    if orig_idx < 0 or orig_idx not in vps_work.index:
                        st.error("Could not locate chosen VPS row.")
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

# === Final Export ===
if "prv_work" in st.session_state and "vps_work" in st.session_state:
    st.markdown("---")
    if st.button("Export final workbook (with manual matches)"):
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
            "Download Final Report",
            data=buf,
            file_name=f"Providus_VPS_Final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.caption("Providus ↔ VPS Reconciliation | Glassmorphic UI • Animated • Mobile-ready")
