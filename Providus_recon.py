# providus_vps_app.py
# -*- coding: utf-8 -*-
"""
Providus VPS Reconciliation – Modern UI + Dark Mode + Progress + CSV + Searchable Tables
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
# Core Matching Engine (with progress callback)
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
    vps_by_date_idx = {d: list(newsgroup.index) for d, newsgroup in vps_valid.groupby("_parsed_date")}

    prv["vps_settled_amount"] = pd.NA
    prv["vps_charge_amount"] = pd.NA
    prv["vps_matched"] = False
    prv["vps_match_reason"] = pd.NA
    prv["vps_matched_vps_index"] = pd.NA

    narration_col = PRV_NARRATION_COL if PRV_NARRATION_COL in prv.columns else None
    if narration_col:
        prv["_tran_details_lower"] = prv[narration_col].astype(str).str.lower()

    matched = 0
    total_prv = len(prv)

    for prv_idx, prv_row in prv.iterrows():
        if progress_callback:
            progress_callback(prv_idx + 1, total_prv)

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
                    prv.at[prv_idx, "vps_matched_vps_index
