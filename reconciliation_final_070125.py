import streamlit as st
import pandas as pd
import json
import io
import zipfile
import re
import logging
from datetime import datetime, timedelta
from streamlit.components.v1 import html
from functools import lru_cache
import tempfile
import os
from openpyxl import Workbook
import unicodedata
import itertools
import uuid
import numpy as np
import plotly.express as px

# Set page config
st.set_page_config(page_title="Reconciliation Dashboard", layout="wide")

# --- UI Styling ---
st.markdown("""
<style>
    /* Target the 'Browse files' buttons for a uniform look */
    section[data-testid="stFileUploadDropzone"] button[data-baseweb="button"] {
        width: 120px !important;
    }
    /* Target the main 'Process Files' button */
    div[data-testid="stButton"] > button[data-baseweb="button"] {
        width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)


# Set up detailed logging
log_stream = io.StringIO()
log_handler = logging.StreamHandler(log_stream)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'))
logger = logging.getLogger(__name__)
logger.handlers = [log_handler]
logger.setLevel(logging.DEBUG)

# Global lists and sets
audit_entries, wsr_error_entries, resolution_logs = [], [], []
matched_bank_refs, matched_settlement_nums, matched_quickbooks_0519_refs, matched_quickbooks_ngc_refs = set(), set(), set(), set()

# Session state initialization
if "processed" not in st.session_state:
    st.session_state.processed = False
    st.session_state.recon_df, st.session_state.summary_df, st.session_state.wsr_recon_df = None, None, None
    st.session_state.audit_df, st.session_state.wsr_error_df = pd.DataFrame(), pd.DataFrame()
    st.session_state.missing_weeks_df = pd.DataFrame()
    st.session_state.log_content, st.session_state.error_message, st.session_state.excel_file = "", None, None

# UI Elements
st.title("Reconciliation Dashboard")
st.markdown("Upload files to perform reconciliation based on store designation.")
st.subheader("Select Reconciliation Month")
recon_month = st.date_input("Select Month (day will be ignored)", value=datetime(2024, 12, 1), format="YYYY/MM/DD")
recon_start, recon_end = pd.to_datetime(recon_month.replace(day=1)), pd.to_datetime((recon_month.replace(day=1) + timedelta(days=31)).replace(day=1) - timedelta(days=1))

st.subheader("Upload Input Files")
col1, col2, col3 = st.columns(3)
with col1:
    merchant_file = st.file_uploader("Merchant Numbers (xlsx)", type=["xlsx"])
    wsr_zip_file = st.file_uploader("WSR Files (zip)", type=["zip"])
with col2:
    bank_file = st.file_uploader("Bank Statement (csv)", type=["csv"])
    settlement_file = st.file_uploader("Amex Settlements (csv)", type=["csv"])
with col3:
    quickbooks_0519_file = st.file_uploader("Atlas 0519 QuickBooks (xlsx)", type=["xlsx"])
    quickbooks_ngc_file = st.file_uploader("Atlas NGC QuickBooks (xlsx)", type=["xlsx"])

# --- Constants ---
DATE_WINDOW_DAYS = 5
FINAL_PASS_DATE_WINDOW_DAYS = 3 # Tighter window for complex matches
TOLERANCE = 0.02
FINAL_PASS_TOLERANCE = 0.10
MIN_FEE_RATE = 0.015  # 1.5%
MAX_FEE_RATE = 0.05   # 5.0%
TYPICAL_AMEX_FEE_RATE = 0.0275 # 2.75% used as a heuristic to find the best match

# --- Functions ---
@lru_cache(maxsize=10000)
def extract_wsr_info(filename):
    match = re.match(r'(?:#)?(\d+)(?:[_\s]WSR)?[_\s]?(\d{2}-\d{2}-\d{2,4})?\.(xls|xlsx)', os.path.basename(filename), re.IGNORECASE)
    if match:
        store_num = match.group(1).lstrip('0')
        date_str = match.group(2) if len(match.groups()) > 1 and match.group(2) else None
        return store_num, date_str
    return None, None

def process_wsr_file(wsr_file, store_number, processed_files, file_content, recon_year):
    file_base = os.path.basename(wsr_file).lower()
    if file_base in processed_files: return None
    processed_files.add(file_base)
    try:
        engine = 'openpyxl' if wsr_file.lower().endswith('.xlsx') else 'xlrd'
        wsr_data = pd.read_excel(io.BytesIO(file_content), sheet_name='Weekly Sales', header=None, engine=engine)
        file_store_num = str(wsr_data.iloc[3, 2]).lstrip('0').replace('#', '').strip()
        if file_store_num != store_number:
            logger.warning(f"Store number mismatch in {wsr_file}: expected {store_number}, found {file_store_num}")
            return None
        
        dates, date_cols = [], []
        for col_idx in range(3, 17):
            date_str = wsr_data.iloc[8, col_idx]
            if pd.notna(date_str):
                date_str = str(date_str).strip()
                if not re.search(r'[\s/,-]\d{2,4}\s*$', date_str): date_str = f"{date_str}/{recon_year}"
                try:
                    date = pd.to_datetime(date_str, errors='coerce')
                    if not pd.isna(date):
                        date_formatted = date.strftime('%Y-%m-%d')
                        if date_formatted not in dates: dates.append(date_formatted); date_cols.append(col_idx)
                except Exception: continue
        if not dates: return None

        shift_map = {}
        for date, col in zip(dates, date_cols):
            shift_map[col] = {'date': date, 'shift': 'AM'}; shift_map[col + 1] = {'date': date, 'shift': 'PM'}

        ar_data = []
        for row_idx in range(wsr_data.shape[0]):
            label_raw = wsr_data.iloc[row_idx, 0]
            if not isinstance(label_raw, str): continue
            label = unicodedata.normalize("NFKD", label_raw).strip()
            if "A/R Due" in label and "CC" in label:
                channel_match = re.search(r'\((InShop|MOTO|ONLINE)\)', label, re.IGNORECASE)
                card_match = re.search(r'\b(Visa|Amex|MC|Discover)\b', label, re.IGNORECASE)
                if channel_match and card_match:
                    current_channel = channel_match.group(1).upper()
                    card_type = card_match.group(1).capitalize()
                    for col, shift in shift_map.items():
                        if col < wsr_data.shape[1]:
                            amount = wsr_data.iloc[row_idx, col]
                            if pd.notna(amount):
                                try:
                                    if float(amount) != 0: 
                                        ar_data.append({'Store': store_number, 'Date': shift['date'], 'Channel': current_channel, 'Card_Type': card_type, 'Amount': float(amount), 'WSR_File': file_base, 'WSR_Label': label})
                                except (ValueError, TypeError):
                                    logger.warning(f"Could not convert amount '{amount}' to float for label '{label}' in {file_base}")
                                    continue
        return pd.DataFrame(ar_data) if ar_data else None
    except Exception as e:
        logger.error(f"Error processing WSR file {wsr_file}: {e}"); return None

def load_merchant_numbers(file_content):
    try:
        try:
            sheets = pd.read_excel(io.BytesIO(file_content), sheet_name=['Merchant Key', 'Amex'], dtype=str)
            general_sheet_name = 'Merchant Key'
        except ValueError:
            logger.warning("Worksheet 'Merchant Key' not found. Trying 'Merchant Numbers'.")
            sheets = pd.read_excel(io.BytesIO(file_content), sheet_name=['Merchant Numbers', 'Amex'], dtype=str)
            general_sheet_name = 'Merchant Numbers'

        general, amex = sheets[general_sheet_name], sheets['Amex']
        general['Store'], amex['Store'] = general['Store'].str.lstrip('0'), amex['Store'].str.lstrip('0')
        general['Designation'], amex['Designation'] = general['Designation'].str.strip(), amex['Designation'].str.strip()
        designation_map = pd.concat([general[['Store', 'Designation']], amex[['Store', 'Designation']]]).dropna(subset=['Store', 'Designation']).drop_duplicates('Store')
        store_to_designation = dict(zip(designation_map['Store'], designation_map['Designation']))
        amex['Merchant Number'], general['Merchant Number'] = amex['Merchant Number'].str.strip(), general['Merchant Number'].str.strip().str.zfill(13)
        amex_map = dict(zip(amex.dropna(subset=['Merchant Number'])['Merchant Number'], amex['Store']))
        general_map = dict(zip(general.dropna(subset=['Merchant Number'])['Merchant Number'], general['Store']))
        return store_to_designation, general, amex, amex_map, general_map
    except Exception as e:
        logger.error(f"Error loading Merchant Numbers. Ensure 'Designation' column exists and sheet names are correct. Error: {e}"); raise

def load_bank_statement(file_content):
    try:
        df = pd.read_csv(io.StringIO(file_content.decode('utf-8')), parse_dates=['As Of'], dtype={'Bank Reference': str})
        df = df[df['Data Type'] == 'Credits'].drop_duplicates(['Bank Reference', 'Amount'])
        df = df[~df['Text'].str.contains('BRINKS|9000979655|BRINKS CAPITAL', case=False, na=False)]
        return df
    except Exception as e:
        logger.error(f"Error loading bank statement: {e}"); raise

def load_amex_settlement(file_content):
    try:
        lines = file_content.decode('utf-8').splitlines()
        skip = next((i for i, line in enumerate(lines) if 'Settlement Date' in line), -1)
        if skip == -1: raise ValueError("Could not find 'Settlement Date' header.")
        df = pd.read_csv(io.StringIO('\n'.join(lines[skip:])), engine='python')
        df.columns = [c.strip() for c in df.columns]
        numeric = ['Total Charges', 'Settlement Amount', 'Discount Amount', 'Fees & Incentives', 'Chargebacks', 'Adjustments', 'Held Funds']
        for col in numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[$,()]', '', regex=True).replace('-', '0'), errors='coerce').fillna(0)
        df['Settlement_Date'] = pd.to_datetime(df['Settlement Date'], errors='coerce')
        for col in ['Chargebacks', 'Adjustments']:
            if col in df.columns: df[col] = -abs(df[col])
        df['Total_Fees'] = df[numeric[2:]].sum(axis=1)
        df = df.rename(columns={'Payee Merchant ID': 'Merchant_Number', 'Settlement Amount': 'Settlement_Amount', 'Settlement Number': 'Settlement_Number'})
        df['Merchant_Number'] = df['Merchant_Number'].astype(str).str.strip()
        return df
    except Exception as e:
        logger.error(f"Error loading Amex settlement: {e}"); raise

def load_quickbooks(file_content, source_name):
    try:
        file_bytes = io.BytesIO(file_content)
        raw_df = pd.read_excel(file_bytes, header=None, engine='openpyxl')
        header_row_index = next((i for i, row in raw_df.head(10).iterrows() if 'Date' in row.values and 'Memo' in row.values), -1)
        if header_row_index == -1: raise ValueError(f"Could not find header row in QuickBooks file '{source_name}'.")
        df = pd.read_excel(io.BytesIO(file_content), header=header_row_index, engine='openpyxl')
        df.columns = df.columns.str.strip()
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Deposit'] = pd.to_numeric(df['Deposit'].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce')
        df.dropna(subset=['Date', 'Deposit'], inplace=True)
        df = df[df['Deposit'] > 0].copy()
        df['Transaction_ID'] = [f"{source_name}_{uuid.uuid4()}" for _ in range(len(df))]
        return df
    except Exception as e:
        logger.error(f"Error loading QuickBooks file {source_name}: {e}"); raise

def extract_merchant_and_store(text, amex_map, general_map, amex_channel_map):
    if not isinstance(text, str): return None, None, None, None
    text_upper = text.upper()
    if 'AMERICAN EXPRESS' in text_upper:
        m = re.search(r'INDN\s*:\s*(?:JIMMY\s*JOHNS\s*)?(\d{9,10})', text, re.IGNORECASE)
        if m:
            merch_num = m.group(1)
            store_num = amex_map.get(merch_num)
            channel = 'ALL' 
            return merch_num, store_num, 'Amex', channel
    else:
        m = re.search(r'ID:(\d{12,13})', text)
        if m:
            merch_num, store_num = m.group(1), general_map.get(m.group(1))
            channel_match = re.search(r'-(EC|M|MO)\s*(?:CO\s*ID:|$)', text_upper)
            channel = {'EC': 'ONLINE', 'M': 'INSHOP', 'MO': 'MOTO'}.get(channel_match.group(1), 'UNKNOWN') if channel_match else 'UNKNOWN'
            return merch_num, store_num, 'Non-Amex', channel
    return None, None, None, None

def create_ar_totals(ar_df, store_num, general_merchants, amex_merchants):
    logger.info(f"Creating AR totals for store {store_num}")
    if ar_df.empty: return pd.DataFrame()
    def agg_labels(x): return '; '.join(x.astype(str).unique())
    agg_dict = {'Amount': 'sum', 'WSR_File': agg_labels, 'WSR_Label': agg_labels}
    ar_df['WSR_Date'] = pd.to_datetime(ar_df['WSR_Date'])
    ar_groups, amex_df, non_amex_df = [], ar_df[ar_df['Card_Type'] == 'Amex'].copy(), ar_df[ar_df['Card_Type'] != 'Amex'].copy()
    if not non_amex_df.empty:
        non_amex_grouped = non_amex_df.groupby(['WSR_Date', 'Channel']).agg(agg_dict).reset_index()
        for _, row in non_amex_grouped.iterrows():
            merch_row = general_merchants[(general_merchants['Store'] == store_num) & (general_merchants['Channel'] == row['Channel'])]
            ar_groups.append({'WSR_Date': row['WSR_Date'], 'Store': store_num, 'Channel': row['Channel'], 'Card_Type': 'Non-Amex', 'Merchant_Type': 'Non-Amex', 'Merchant_Number': merch_row.iloc[0]['Merchant Number'] if not merch_row.empty else None, 'AR_Amount': row['Amount'], 'WSR_File': row['WSR_File'], 'WSR_Label': row['WSR_Label']})
    if not amex_df.empty:
        all_channel_merch_row = amex_merchants[(amex_merchants['Store'] == store_num) & (amex_merchants['Channel'] == 'ALL')]
        if not all_channel_merch_row.empty:
            logger.info(f"Store {store_num}: 'ALL' Amex channel configured. Summing all Amex A/R to daily totals.")
            daily_amex_sum = amex_df.groupby('WSR_Date')['Amount'].sum().reset_index()
            for _, sum_row in daily_amex_sum.iterrows():
                wsr_date, day_df = sum_row['WSR_Date'], amex_df[amex_df['WSR_Date'] == sum_row['WSR_Date']]
                wsr_files, wsr_labels = '; '.join(day_df['WSR_File'].unique()), '; '.join(day_df['WSR_Label'].unique())
                ar_groups.append({'WSR_Date': wsr_date, 'Store': store_num, 'Channel': 'ALL', 'Card_Type': 'Amex', 'Merchant_Type': 'Amex', 'Merchant_Number': all_channel_merch_row.iloc[0]['Merchant Number'], 'AR_Amount': sum_row['Amount'], 'WSR_File': wsr_files, 'WSR_Label': wsr_labels})
        else:
            logger.info(f"Store {store_num}: No 'ALL' Amex channel. Processing Amex channels individually.")
            amex_grouped = amex_df.groupby(['WSR_Date', 'Channel']).agg(agg_dict).reset_index()
            for _, row in amex_grouped.iterrows():
                merch_row = amex_merchants[(amex_merchants['Store'] == store_num) & (amex_merchants['Channel'] == row['Channel'])]
                if not merch_row.empty:
                    ar_groups.append({'WSR_Date': row['WSR_Date'], 'Store': store_num, 'Channel': row['Channel'], 'Card_Type': 'Amex', 'Merchant_Type': 'Amex', 'Merchant_Number': merch_row.iloc[0]['Merchant Number'], 'AR_Amount': row['Amount'], 'WSR_File': row['WSR_File'], 'WSR_Label': row['WSR_Label']})
    return pd.DataFrame(ar_groups)

def reconcile_store_quickbooks(daily_ar, qb_df, designation, matched_qb_refs):
    if qb_df is None or qb_df.empty:
        daily_ar['Status'] = 'No QuickBooks Source File'
        return daily_ar, set()
    recon_results, available_deposits, available_ar, used_ar_indices, used_deposit_indices = [], qb_df[~qb_df['Transaction_ID'].isin(matched_qb_refs)].copy(), daily_ar.copy(), set(), set()
    for ar_idx, ar_row in available_ar.iterrows():
        candidate_deposits = available_deposits[~available_deposits.index.isin(used_deposit_indices)]
        exact_matches = candidate_deposits[abs(candidate_deposits['Deposit'] - ar_row['AR_Amount']) <= TOLERANCE]
        if not exact_matches.empty:
            match_row = exact_matches.iloc[0]
            ar_dict = ar_row.to_dict()
            ar_dict.update({'Bank_Date': match_row['Date'], 'Bank_Amount': match_row['Deposit'], 'Status': 'Matched (Exact Gross)', 'Source': designation, 'Bank_Reference': match_row['Transaction_ID'], 'Bank_Description': match_row['Memo']})
            recon_results.append(ar_dict); used_ar_indices.add(ar_idx); used_deposit_indices.add(match_row.name)
    remaining_ar_amex = available_ar[(~available_ar.index.isin(used_ar_indices)) & (available_ar['Merchant_Type'] == 'Amex')]
    for ar_idx, ar_row in remaining_ar_amex.iterrows():
        candidate_deposits = available_deposits[~available_deposits.index.isin(used_deposit_indices)]
        plausible_deposits = candidate_deposits[(ar_row['AR_Amount'] > candidate_deposits['Deposit']) & (((ar_row['AR_Amount'] - candidate_deposits['Deposit']) / ar_row['AR_Amount']).between(MIN_FEE_RATE, MAX_FEE_RATE))].copy()
        if not plausible_deposits.empty:
            plausible_deposits['Fee_Diff'] = abs(((ar_row['AR_Amount'] - plausible_deposits['Deposit']) / ar_row['AR_Amount']) - TYPICAL_AMEX_FEE_RATE)
            best_match = plausible_deposits.loc[plausible_deposits['Fee_Diff'].idxmin()]
            ar_dict = ar_row.to_dict()
            ar_dict.update({'Bank_Date': best_match['Date'], 'Bank_Amount': best_match['Deposit'], 'Status': 'Matched (Net of Fees)', 'Source': designation, 'Bank_Reference': best_match['Transaction_ID'], 'Bank_Description': best_match['Memo']})
            recon_results.append(ar_dict); used_ar_indices.add(ar_idx); used_deposit_indices.add(best_match.name)
    unmatched_ar = available_ar[~available_ar.index.isin(used_ar_indices)]
    for _, row in unmatched_ar.iterrows():
        recon_results.append(row.to_dict() | {'Status': 'No QuickBooks Match', 'Source': designation})
    final_df = pd.DataFrame(recon_results)
    return final_df, set(final_df.loc[final_df['Bank_Reference'].notna(), 'Bank_Reference'])

def reconcile_store_statement(daily_ar, bank_df_store, settle_df, store_num, matched_bank_refs, matched_settle_nums):
    recon, temp_b, temp_s, matched_ar = [], set(), set(), set()
    amex_ar = daily_ar[daily_ar['Merchant_Type'] == 'Amex'].copy()
    non_amex_ar = daily_ar[daily_ar['Merchant_Type'] != 'Amex'].copy()
    reversal_cols = ['Chargebacks', 'Adjustments']

    # Pass 1: Handle full reversals
    for idx, ar_row in amex_ar.iterrows():
        if idx in matched_ar: continue
        candidate_settles = settle_df[(settle_df['Merchant_Number'] == ar_row['Merchant_Number']) & (settle_df['Settlement_Date'].between(ar_row['WSR_Date'], ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS*2)))]
        for _, settle_row in candidate_settles.iterrows():
            for col in reversal_cols:
                if col in settle_row and np.isclose(settle_row[col], -ar_row['AR_Amount']):
                    logger.info(f"Found reversal for AR {idx} (Amount: {ar_row['AR_Amount']}) in settlement row. Status set to Reversed.")
                    ar_dict = ar_row.to_dict(); ar_dict.update({'Bank_Date': settle_row['Settlement_Date'], 'Bank_Amount': settle_row[col], 'Settlement_Number': settle_row['Settlement_Number'], 'Status': 'Reversed', 'Source': 'Statement', 'Bank_Description': f"Full Reversal via {col}"}); recon.append(ar_dict)
                    matched_ar.add(idx); temp_s.add(settle_row['Settlement_Number'])
                    break
            if idx in matched_ar: break

    # Pass 2: Primary Amex Match via Settlement File (WSR Gross -> Settlement Gross -> Bank Net)
    for idx, ar_row in amex_ar[~amex_ar.index.isin(matched_ar)].iterrows():
        settles = settle_df[(settle_df['Settlement_Date'].between(ar_row['WSR_Date'] - timedelta(days=DATE_WINDOW_DAYS), ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (settle_df['Merchant_Number'] == ar_row['Merchant_Number'])]
        exact_settle = settles[abs(settles['Total Charges'] - ar_row['AR_Amount']) <= TOLERANCE]

        if not exact_settle.empty:
            settle_row = exact_settle.iloc[0]
            banks = bank_df_store[~bank_df_store['Bank Reference'].isin(temp_b)]
            candidate_banks = banks[(banks['As Of'].between(settle_row['Settlement_Date'] - timedelta(days=DATE_WINDOW_DAYS), settle_row['Settlement_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (banks['Merchant_Number'] == ar_row['Merchant_Number'])]
            
            net_bank_match = candidate_banks[abs(candidate_banks['Amount'] - settle_row['Settlement_Amount']) <= TOLERANCE]
            gross_bank_match = candidate_banks[abs(candidate_banks['Amount'] - settle_row['Total Charges']) <= TOLERANCE]
            exact_bank = net_bank_match if not net_bank_match.empty else gross_bank_match
            
            if not exact_bank.empty:
                bank_row = exact_bank.iloc[0]
                logger.info(f"[[[DEPOSIT CLAIMED]]] Bank Ref: {bank_row['Bank Reference']} (Amount: {bank_row['Amount']:.2f}) was claimed by A/R Index: {idx} (AR Amount: {ar_row['AR_Amount']:.2f}) via Settlement.")
                ar_dict = ar_row.to_dict()
                ar_dict.update({'Bank_Date': bank_row['As Of'], 'Bank_Amount': bank_row['Amount'], 'Settlement_Amount': settle_row['Settlement_Amount'], 'Settlement_Number': settle_row['Settlement_Number'], 'Status': 'Matched with Settlement', 'Source': 'Statement', 'Bank_Reference': bank_row['Bank Reference'], 'Bank_Description': bank_row['Text']})
                recon.append(ar_dict)
                matched_ar.add(idx)
                temp_b.add(bank_row['Bank Reference'])
                temp_s.add(settle_row['Settlement_Number'])

    # Pass 3: Fallback Amex Match (Direct WSR Gross -> Bank Net) for items that failed Pass 2
    for idx, ar_row in amex_ar[~amex_ar.index.isin(matched_ar)].iterrows():
        available_banks = bank_df_store[~bank_df_store['Bank Reference'].isin(temp_b)]
        candidate_banks = available_banks[(available_banks['As Of'].between(ar_row['WSR_Date'] - timedelta(days=DATE_WINDOW_DAYS), ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (available_banks['Merchant_Number'] == ar_row['Merchant_Number'])]
        
        plausible_deposits = candidate_banks[(ar_row['AR_Amount'] > candidate_banks['Amount']) & (((ar_row['AR_Amount'] - candidate_banks['Amount']) / ar_row['AR_Amount']).between(MIN_FEE_RATE, MAX_FEE_RATE))].copy()
        
        if not plausible_deposits.empty:
            plausible_deposits['Fee_Diff'] = abs(((ar_row['AR_Amount'] - plausible_deposits['Amount']) / ar_row['AR_Amount']) - TYPICAL_AMEX_FEE_RATE)
            best_match = plausible_deposits.loc[plausible_deposits['Fee_Diff'].idxmin()]
            
            logger.info(f"[[[DEPOSIT CLAIMED (Fallback)]]] Bank Ref: {best_match['Bank Reference']} (Amount: {best_match['Amount']:.2f}) was claimed by A/R Index: {idx} (AR Amount: {ar_row['AR_Amount']:.2f}) via Net-of-Fees match.")
            ar_dict = ar_row.to_dict()
            ar_dict.update({'Bank_Date': best_match['As Of'], 'Bank_Amount': best_match['Amount'], 'Status': 'Matched (Net of Fees)', 'Source': 'Statement', 'Bank_Reference': best_match['Bank Reference'], 'Bank_Description': best_match['Text']})
            recon.append(ar_dict)
            matched_ar.add(idx)
            temp_b.add(best_match['Bank Reference'])

    # Pass 4: Non-Amex and any remaining Amex exact matches
    remaining_ar = pd.concat([non_amex_ar, amex_ar[~amex_ar.index.isin(matched_ar)]])
    for idx, ar_row in remaining_ar.iterrows():
        available_banks = bank_df_store[~bank_df_store['Bank Reference'].isin(temp_b)]
        matches = available_banks[(available_banks['As Of'].between(ar_row['WSR_Date'] - timedelta(days=DATE_WINDOW_DAYS), ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (available_banks['Merchant_Number'] == ar_row['Merchant_Number'])]
        exact = matches[abs(matches['Amount'] - ar_row['AR_Amount']) <= TOLERANCE]
        if not exact.empty:
            bank = exact.iloc[0]
            logger.info(f"[[[DEPOSIT CLAIMED]]] Bank Ref: {bank['Bank Reference']} (Amount: {bank['Amount']:.2f}) was claimed by A/R Index: {idx} (AR Amount: {ar_row['AR_Amount']:.2f}) via Exact Match.")
            ar_dict = ar_row.to_dict()
            ar_dict.update({'Bank_Date': bank['As Of'], 'Bank_Amount': bank['Amount'], 'Status': 'Matched (Exact)', 'Source': 'Statement', 'Bank_Reference': bank['Bank Reference'], 'Bank_Description': bank['Text']})
            recon.append(ar_dict)
            matched_ar.add(idx)
            temp_b.add(bank['Bank Reference'])

    # Pass 5: Settlement Match, No Bank
    for idx, ar_row in amex_ar[~amex_ar.index.isin(matched_ar)].iterrows():
        available_settles = settle_df[~settle_df['Settlement_Number'].isin(temp_s)]
        settles = available_settles[(available_settles['Settlement_Date'].between(ar_row['WSR_Date'] - timedelta(days=DATE_WINDOW_DAYS), ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (available_settles['Merchant_Number'] == ar_row['Merchant_Number'])]
        exact_settle = settles[abs(settles['Total Charges'] - ar_row['AR_Amount']) <= TOLERANCE]
        if not exact_settle.empty:
            settle = exact_settle.iloc[0]
            ar_dict = ar_row.to_dict()
            ar_dict.update({'Settlement_Amount': settle['Settlement_Amount'], 'Settlement_Number': settle['Settlement_Number'], 'Status': 'Settlement Match, No Bank', 'Source': 'Statement'})
            recon.append(ar_dict)
            matched_ar.add(idx)
            temp_s.add(settle['Settlement_Number'])
            
    # Final Pass: Add any truly unmatched A/R items
    for _, ar in daily_ar[~daily_ar.index.isin(matched_ar)].iterrows():
        is_part_of_sum = ar['Channel'] != 'ALL' and any(r['WSR_Date'] == ar['WSR_Date'] and r['Channel'] == 'ALL' for r in recon)
        if not is_part_of_sum:
            ar_dict = ar.to_dict()
            ar_dict.update({'Status': 'No Bank Match', 'Source': 'Statement'})
            recon.append(ar_dict)
            
    return pd.DataFrame(recon), temp_b, temp_s

def final_cleanup_pass(wsr_df, unmatched_source_df):
    logger.info("Starting final cleanup pass...")
    unmatched_ar_all = wsr_df[wsr_df['Status'].str.contains("No .* Match|Settlement Match, No Bank", na=False)].copy()
    protected_ar_indices = set(unmatched_ar_all[unmatched_ar_all['Status'] == 'Settlement Match, No Bank'].index)
    unmatched_ar = unmatched_ar_all[~unmatched_ar_all.index.isin(protected_ar_indices)]
    if unmatched_ar.empty or unmatched_source_df.empty: return {}, unmatched_source_df
    updated_indices = {}
    all_possible_matches = []
    for source_idx, deposit_row in unmatched_source_df.iterrows():
        store, source, merch_num = deposit_row.get('Store'), deposit_row.get('Source'), deposit_row.get('Merchant_Number')
        candidate_ars = unmatched_ar[(unmatched_ar['Store'] == store) & (unmatched_ar['Source'] == source) & (unmatched_ar['Merchant_Number'] == merch_num if source == 'Statement' else True) & (unmatched_ar['WSR_Date'].between(pd.to_datetime(deposit_row['Bank_Date']) - timedelta(days=DATE_WINDOW_DAYS), pd.to_datetime(deposit_row['Bank_Date']) + timedelta(days=DATE_WINDOW_DAYS)))]
        if candidate_ars.empty: continue
        for r in range(1, min(len(candidate_ars), 8) + 1):
            for combo_indices in itertools.combinations(candidate_ars.index, r):
                combo_sum, diff = candidate_ars.loc[list(combo_indices), 'AR_Amount'].sum(), abs(candidate_ars.loc[list(combo_indices), 'AR_Amount'].sum() - deposit_row['Bank_Amount'])
                all_possible_matches.append((diff, list(combo_indices), source_idx))
    all_possible_matches.sort(key=lambda x: x[0])
    ar_used, source_used = set(), set()
    for diff, ar_indices, source_idx in all_possible_matches:
        if source_idx in source_used or any(idx in ar_used for idx in ar_indices): continue
        deposit_row = unmatched_source_df.loc[source_idx]
        current_tolerance = max(TOLERANCE, deposit_row['Bank_Amount'] * FINAL_PASS_TOLERANCE)
        if diff <= current_tolerance:
            status = 'Matched (by Sum)' if len(ar_indices) > 1 else 'Matched (Best Fit)'
            if diff > TOLERANCE: status = 'Matched (Last Resort)'
            logger.info(f"Cleanup Match: Deposit {deposit_row['Bank_Amount']:.2f} matched with WSRs {ar_indices} ({status}). Diff: {diff:.2f}")
            match_data = deposit_row.to_dict()
            if len(ar_indices) > 1: match_data['Match_Group_ID'] = f"SUM-{uuid.uuid4()}"
            for idx in ar_indices: updated_indices[idx] = (match_data, status); ar_used.add(idx)
            source_used.add(source_idx)
    return updated_indices, unmatched_source_df[~unmatched_source_df.index.isin(source_used)]

def find_two_by_two_matches(unmatched_ar, unmatched_source):
    logger.info("Starting Final Cleanup Pass 2 (2-to-2)...")
    if unmatched_ar.empty or unmatched_source.empty or len(unmatched_ar) < 2 or len(unmatched_source) < 2: return {}, set(), set()
    updates, used_ar, used_source, groups = {}, set(), set(), []
    statement_ars, statement_deps = unmatched_ar[unmatched_ar['Source'] == 'Statement'], unmatched_source[unmatched_source['Source'] == 'Statement']
    if not statement_ars.empty and not statement_deps.empty:
        for key, ar_group in statement_ars.groupby(['Store', 'Channel', 'Merchant_Type']):
            dep_group = statement_deps[(statement_deps['Store'] == key[0]) & (statement_deps['Channel'] == key[1]) & (statement_deps['Merchant_Type'] == key[2])]
            if len(ar_group) >= 2 and len(dep_group) >= 2: groups.append((ar_group, dep_group))
    qb_ars, qb_deps = unmatched_ar[unmatched_ar['Source'] != 'Statement'], unmatched_source[unmatched_source['Source'] != 'Statement']
    if not qb_ars.empty and not qb_deps.empty:
        for key, ar_group in qb_ars.groupby(['Store', 'Source']):
            dep_group = qb_deps[(qb_deps['Store'] == key[0]) & (qb_deps['Source'] == key[1])]
            if len(ar_group) >= 2 and len(dep_group) >= 2: groups.append((ar_group, dep_group))
    for ar_group, source_group in groups:
        ar_pairs, source_pairs = list(itertools.combinations(ar_group.index, 2)), list(itertools.combinations(source_group.index, 2))
        for ar_idx1, ar_idx2 in ar_pairs:
            if ar_idx1 in used_ar or ar_idx2 in used_ar: continue
            ar1, ar2 = ar_group.loc[ar_idx1], ar_group.loc[ar_idx2]
            ar_sum = ar1['AR_Amount'] + ar2['AR_Amount']
            found_match_for_ar_pair = False
            for src_idx1, src_idx2 in source_pairs:
                if src_idx1 in used_source or src_idx2 in used_source: continue
                src1, src2 = source_group.loc[src_idx1], source_group.loc[src_idx2]
                all_dates = [ar1['WSR_Date'], ar2['WSR_Date'], src1['Bank_Date'], src2['Bank_Date']]
                if (max(all_dates) - min(all_dates)).days > FINAL_PASS_DATE_WINDOW_DAYS: continue
                source_sum = src1['Bank_Amount'] + src2['Bank_Amount']
                if abs(ar_sum - source_sum) <= TOLERANCE:
                    logger.info(f"Cleanup Match (2-to-2): ARs {ar_idx1},{ar_idx2} (sum {ar_sum:.2f}) match Deps {src_idx1},{src_idx2} (sum {source_sum:.2f})")
                    match_id, status = f"SUM2x2-{uuid.uuid4()}", "Matched (2 AR to 2 Dep)"
                    combo_dep_data = {'Bank_Date': min(src1['Bank_Date'], src2['Bank_Date']), 'Bank_Amount': source_sum, 'Bank_Reference': f"{src1['Bank_Reference']}, {src2['Bank_Reference']}", 'Bank_Description': f"{src1.get('Bank_Description', '')}; {src2.get('Bank_Description', '')}", 'Match_Group_ID': match_id}
                    updates[ar_idx1], updates[ar_idx2] = (combo_dep_data, status), (combo_dep_data, status)
                    used_ar.update([ar_idx1, ar_idx2]); used_source.update([src_idx1, src_idx2])
                    found_match_for_ar_pair = True
                    break 
            if found_match_for_ar_pair: continue
    return updates, used_ar, used_source

def process_files(recon_month_obj, merchant_file, wsr_zip_file, bank_file, settlement_file, quickbooks_0519_file, quickbooks_ngc_file):
    global audit_entries, wsr_error_entries, matched_bank_refs, matched_settlement_nums, matched_quickbooks_0519_refs, matched_quickbooks_ngc_refs
    all_recons, audit_entries, wsr_error_entries = [], [], []
    matched_bank_refs, matched_settlement_nums, matched_quickbooks_0519_refs, matched_quickbooks_ngc_refs = set(), set(), set(), set()
    st.session_state.missing_weeks_df = pd.DataFrame()
    try:
        if not all([merchant_file, wsr_zip_file]): raise ValueError("Merchant Numbers and WSR ZIP files are required.")
        designation_map, general_merch, amex_merch, amex_map, general_map = load_merchant_numbers(merchant_file.read())
        bank_df = load_bank_statement(bank_file.read()) if bank_file else None
        settle_df = load_amex_settlement(settlement_file.read()) if settlement_file else None
        qb_0519_df = load_quickbooks(quickbooks_0519_file.read(), "Atlas 0519") if quickbooks_0519_file else None
        qb_ngc_df = load_quickbooks(quickbooks_ngc_file.read(), "Atlas NGC") if quickbooks_ngc_file else None
        if bank_df is not None: 
            amex_channel_map = amex_merch.dropna(subset=['Merchant Number', 'Channel']).set_index('Merchant Number')['Channel'].to_dict()
            bank_df[['Merchant_Number', 'Store', 'Merchant_Type', 'Channel']] = bank_df['Text'].apply(lambda x: pd.Series(extract_merchant_and_store(x, amex_map, general_map, amex_channel_map)))
            non_amex_channel_map = general_merch.dropna(subset=['Merchant Number', 'Channel']).set_index('Merchant Number')['Channel'].to_dict()
            unknown_mask = (bank_df['Channel'] == 'UNKNOWN') & (bank_df['Merchant_Number'].notna())
            bank_df.loc[unknown_mask, 'Channel'] = bank_df.loc[unknown_mask, 'Merchant_Number'].map(non_amex_channel_map)
        processed_files, recon_year, submitted_weeks_by_store, all_found_week_dates = set(), recon_month_obj.year, {}, set()
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(io.BytesIO(wsr_zip_file.read()), 'r') as zf_outer: zf_outer.extractall(temp_dir)
            while True:
                nested_zips = [os.path.join(r, f) for r, _, fs in os.walk(temp_dir) for f in fs if f.lower().endswith('.zip')]
                if not nested_zips: break
                for zip_path in nested_zips:
                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zf_inner: zf_inner.extractall(os.path.dirname(zip_path))
                        os.remove(zip_path)
                    except Exception as e: logger.warning(f"Could not extract or remove {zip_path}: {e}")
            wsr_files = [os.path.join(r, f) for r, _, fs in os.walk(temp_dir) for f in fs if f.lower().endswith(('.xls', '.xlsx'))]
            for fpath in wsr_files:
                store_num, date_str = extract_wsr_info(fpath)
                if store_num and date_str:
                    try:
                        week_date = pd.to_datetime(date_str, errors='coerce', dayfirst=False, yearfirst=False).strftime('%Y-%m-%d')
                        all_found_week_dates.add(week_date)
                        if store_num not in submitted_weeks_by_store: submitted_weeks_by_store[store_num] = set()
                        submitted_weeks_by_store[store_num].add(week_date)
                    except (ValueError, TypeError): logger.warning(f"Could not parse date '{date_str}' from filename {os.path.basename(fpath)}")
            progress = st.progress(0, "Processing WSR files...")
            all_wsr_ar = []
            for i, fpath in enumerate(wsr_files):
                store_num_from_name, _ = extract_wsr_info(fpath)
                if not store_num_from_name or not (designation := designation_map.get(store_num_from_name)): continue
                progress.progress((i + 1) / len(wsr_files), f"Store {store_num_from_name} ({designation})")
                with open(fpath, 'rb') as f:
                    ar_df = process_wsr_file(fpath, store_num_from_name, processed_files, f.read(), recon_year)
                if ar_df is not None and not ar_df.empty:
                    ar_df_filtered = ar_df[pd.to_datetime(ar_df['Date']).between(recon_start, recon_end)]
                    if not ar_df_filtered.empty: all_wsr_ar.append(ar_df_filtered)
            if not all_wsr_ar: raise ValueError("No valid A/R data was extracted from any WSR file.")
            master_ar_df = pd.concat(all_wsr_ar, ignore_index=True); master_ar_df.rename(columns={'Date': 'WSR_Date'}, inplace=True)
            processed_stores_with_data = set(master_ar_df['Store'].unique())
            missing_weeks_records = [{'Store': store, 'Missing_WSR_Week_Ending_Date': week} for store in processed_stores_with_data for week in sorted(list(all_found_week_dates - submitted_weeks_by_store.get(store, set())))]
            st.session_state.missing_weeks_df = pd.DataFrame(missing_weeks_records)
            for store_num in processed_stores_with_data:
                designation, store_ar_df = designation_map.get(store_num), master_ar_df[master_ar_df['Store'] == store_num]
                daily_ar = create_ar_totals(store_ar_df, store_num, general_merch, amex_merch)
                if daily_ar.empty: continue
                recon_df = pd.DataFrame()
                if 'statement' in designation.lower():
                    if bank_df is not None and settle_df is not None:
                        bank_store_df = bank_df[bank_df['Store'] == store_num].copy()
                        recon_df, new_b, new_s = reconcile_store_statement(daily_ar, bank_store_df, settle_df, store_num, matched_bank_refs, matched_settlement_nums)
                        matched_bank_refs.update(new_b); matched_settlement_nums.update(new_s)
                elif designation == 'Atlas 0519' and qb_0519_df is not None: recon_df, new_q = reconcile_store_quickbooks(daily_ar, qb_0519_df, designation, matched_quickbooks_0519_refs); matched_quickbooks_0519_refs.update(new_q)
                elif designation == 'Atlas NGC' and qb_ngc_df is not None: recon_df, new_q = reconcile_store_quickbooks(daily_ar, qb_ngc_df, designation, matched_quickbooks_ngc_refs); matched_quickbooks_ngc_refs.update(new_q)
                if not recon_df.empty: all_recons.append(recon_df)
        if not all_recons: raise ValueError("No data was reconciled.")
        wsr_final_df = pd.concat(all_recons, ignore_index=True)
        used_source_refs = set(wsr_final_df[wsr_final_df['Bank_Reference'].notna()]['Bank_Reference'])
        unmatched_source_items = []
        if bank_df is not None:
            for _, r in bank_df[~bank_df['Bank Reference'].isin(used_source_refs)].iterrows(): unmatched_source_items.append({'Bank_Date': r['As Of'], 'Store': r['Store'], 'Bank_Amount': r['Amount'], 'Status': 'No WSR Match', 'Source': 'Statement', 'Bank_Reference': r['Bank Reference'], 'Bank_Description': r['Text'], 'Merchant_Number': r['Merchant_Number'], 'Channel': r['Channel'], 'Merchant_Type': r['Merchant_Type']})
        if qb_0519_df is not None:
            for _, r in qb_0519_df[~qb_0519_df['Transaction_ID'].isin(used_source_refs)].iterrows(): unmatched_source_items.append({'Store': 'Atlas 0519 Unassigned', 'Bank_Date': r['Date'], 'Bank_Amount': r['Deposit'], 'Status': 'No WSR Match', 'Source': 'Atlas 0519', 'Bank_Reference': r['Transaction_ID'], 'Bank_Description': r['Memo'], 'Merchant_Type': 'Amex' if 'AMERICAN EXPRESS' in str(r['Memo']).upper() else 'Non-Amex' if '5/3' in str(r['Memo']).upper() else 'Unknown'})
        if qb_ngc_df is not None:
            for _, r in qb_ngc_df[~qb_ngc_df['Transaction_ID'].isin(used_source_refs)].iterrows(): unmatched_source_items.append({'Store': 'Atlas NGC Unassigned', 'Bank_Date': r['Date'], 'Bank_Amount': r['Deposit'], 'Status': 'No WSR Match', 'Source': 'Atlas NGC', 'Bank_Reference': r['Transaction_ID'], 'Bank_Description': r['Memo'], 'Merchant_Type': 'Amex' if 'AMERICAN EXPRESS' in str(r['Memo']).upper() else 'Non-Amex' if '5/3' in str(r['Memo']).upper() else 'Unknown'})
        unmatched_source_df = pd.DataFrame(unmatched_source_items)
        if not unmatched_source_df.empty: unmatched_source_df['Bank_Date'] = pd.to_datetime(unmatched_source_df['Bank_Date'])
        inferred_matches_n1, remaining_source_after_n1 = final_cleanup_pass(wsr_final_df, unmatched_source_df)
        if inferred_matches_n1:
            for idx, (match_data, status) in inferred_matches_n1.items():
                if idx in wsr_final_df.index:
                    for key, val in match_data.items():
                         if key in wsr_final_df.columns: wsr_final_df.loc[idx, key] = val
                    wsr_final_df.loc[idx, 'Status'] = status
        remaining_ar_after_n1 = wsr_final_df[wsr_final_df['Status'].str.contains("No .* Match", na=False)].copy()
        inferred_matches_2x2, used_ar_2x2, used_source_2x2 = find_two_by_two_matches(remaining_ar_after_n1, remaining_source_after_n1)
        if inferred_matches_2x2:
            for idx, (match_data, status) in inferred_matches_2x2.items():
                if idx in wsr_final_df.index:
                    for key, val in match_data.items():
                        if key in wsr_final_df.columns: wsr_final_df.loc[idx, key] = val
                    wsr_final_df.loc[idx, 'Status'] = status
        final_unmatched_source_df = remaining_source_after_n1[~remaining_source_after_n1.index.isin(used_source_2x2)]
        wsr_final_df['Outstanding_AR_Amount'] = np.where(wsr_final_df['Status'].str.contains("Matched|Reversed", na=False), 0, wsr_final_df['AR_Amount'])
        
        # --- Calculate Amex Fees (Post-Processing) ---
        fee_condition = (
            wsr_final_df['Status'].isin(['Matched with Settlement', 'Matched (Net of Fees)'])
        )
        wsr_final_df['Fee_Amount'] = np.where(
            fee_condition, 
            wsr_final_df['AR_Amount'] - wsr_final_df['Bank_Amount'], 
            0
        )
        
        final_unmatched_source_df['Outstanding_AR_Amount'] = 0
        final_df = pd.concat([wsr_final_df, final_unmatched_source_df], ignore_index=True).fillna({'Fee_Amount': 0})

        # --- Final Sorting for Auditability ---
        final_df['Sort_Date'] = final_df['WSR_Date'].fillna(final_df['Bank_Date'])
        final_df['Sort_Priority'] = np.where(final_df['WSR_Date'].notna(), 0, 1)
        wsr_final_df['Sort_Date'] = wsr_final_df['WSR_Date'].fillna(wsr_final_df['Bank_Date'])

        final_df_sorted = final_df.sort_values(['Store', 'Sort_Priority', 'Sort_Date'], na_position='last')
        wsr_final_df_sorted = wsr_final_df.sort_values(['Store', 'Sort_Date'], na_position='first')

        cols_order = ['Store', 'WSR_Date', 'Channel', 'Merchant_Type', 'Card_Type', 'Merchant_Number', 'AR_Amount', 'Outstanding_AR_Amount', 'Bank_Date', 'Bank_Amount', 'Fee_Amount', 'Status', 'Source', 'Bank_Reference', 'Bank_Description', 'Settlement_Amount', 'Settlement_Number', 'Match_Group_ID', 'WSR_File', 'WSR_Label']
        
        final_df_final = final_df_sorted.reindex(columns=cols_order + [c for c in final_df_sorted.columns if c not in cols_order and c not in ['Sort_Date', 'Sort_Priority']]).reset_index(drop=True)
        wsr_final_df_final = wsr_final_df_sorted.reindex(columns=cols_order + [c for c in wsr_final_df_sorted.columns if c not in cols_order and c not in ['Sort_Date']]).reset_index(drop=True)

        st.session_state.processed = True
        st.session_state.recon_df = final_df_final
        st.session_state.wsr_recon_df = wsr_final_df_final
        st.session_state.summary_df = create_summary(final_df)
        st.session_state.audit_df = pd.DataFrame(audit_entries)
        st.session_state.wsr_error_df = pd.DataFrame(wsr_error_entries)
        st.session_state.log_content = log_stream.getvalue()
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            st.session_state.recon_df.to_excel(writer, "Full_Reconciliation", index=False); st.session_state.wsr_recon_df.to_excel(writer, "WSR_Reconciliation", index=False); st.session_state.summary_df.to_excel(writer, "Summary", index=False)
            if not st.session_state.audit_df.empty: st.session_state.audit_df.to_excel(writer, "Audit", index=False)
            if not st.session_state.wsr_error_df.empty: st.session_state.wsr_error_df.to_excel(writer, "WSR Errors", index=False)
            if not st.session_state.get('missing_weeks_df', pd.DataFrame()).empty: st.session_state.missing_weeks_df.to_excel(writer, "Missing_WSR_Weeks", index=False)
        st.session_state.excel_file = output.getvalue()
        return True
    except Exception as e:
        logger.exception("Processing failed"); st.session_state.error_message, st.session_state.log_content = str(e), log_stream.getvalue(); return False

def create_summary(df):
    if df.empty: return pd.DataFrame()
    summary = []
    for store in sorted(df['Store'].dropna().unique()):
        store_df = df[df['Store'] == store]
        total_ar, matched_ar = store_df['AR_Amount'].sum(), store_df[store_df['Status'].str.contains('Matched|Reversed', na=False)]['AR_Amount'].sum()
        unmatched_bank = store_df[store_df['Status'] == 'No WSR Match']['Bank_Amount'].sum()
        summary.append({'Store': store, 'Total_AR_Amount': total_ar, 'Matched_AR_Amount': matched_ar, 'Outstanding_AR_Amount': total_ar - matched_ar, 'Unmatched_Bank_Amount': unmatched_bank})
    comp_total, comp_matched = df['AR_Amount'].sum(), df[df['Status'].str.contains('Matched|Reversed', na=False)]['AR_Amount'].sum()
    summary.append({'Store': 'Company-Wide', 'Total_AR_Amount': comp_total, 'Matched_AR_Amount': comp_matched, 'Outstanding_AR_Amount': comp_total - comp_matched, 'Unmatched_Bank_Amount': df[df['Status'] == 'No WSR Match']['Bank_Amount'].sum()})
    return pd.DataFrame(summary)

def display_visualizations(df):
    st.subheader("Visual Summary")
    ar_df = df[df['AR_Amount'].notna()].copy()
    if ar_df.empty: st.warning("No A/R data available to generate visualizations."); return
    total_ar, outstanding_ar = ar_df['AR_Amount'].sum(), ar_df['Outstanding_AR_Amount'].sum()
    matched_ar, outstanding_pct = total_ar - outstanding_ar, (outstanding_ar / total_ar * 100) if total_ar > 0 else 0
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total A/R", f"${total_ar:,.2f}"); col2.metric("Matched A/R", f"${matched_ar:,.2f}"); col3.metric("Outstanding A/R", f"${outstanding_ar:,.2f}"); col4.metric("Outstanding %", f"{outstanding_pct:.2f}%")
    st.markdown("---")
    v_col1, v_col2 = st.columns(2)
    with v_col1:
        st.markdown("##### Overall Reconciliation Status"); status_data = pd.DataFrame({'Status': ['Matched A/R', 'Outstanding A/R'], 'Amount': [matched_ar, outstanding_ar]}); fig_pie = px.pie(status_data, values='Amount', names='Status', color_discrete_map={'Matched A/R': 'green', 'Outstanding A/R': 'red'}); st.plotly_chart(fig_pie, use_container_width=True)
        st.markdown("##### Amex vs. Non-Amex Reconciliation"); amex_summary = ar_df.groupby('Merchant_Type')[['AR_Amount', 'Outstanding_AR_Amount']].sum().reset_index(); amex_summary['Matched_AR_Amount'] = amex_summary['AR_Amount'] - amex_summary['Outstanding_AR_Amount']; fig_amex = px.bar(amex_summary, x='Merchant_Type', y=['Matched_AR_Amount', 'Outstanding_AR_Amount'], title="Matched vs. Outstanding by Type", labels={'value': 'Total Amount ($)', 'Merchant_Type': 'Merchant Type'}, barmode='stack', color_discrete_map={'Matched_AR_Amount': 'green', 'Outstanding_AR_Amount': 'red'}); st.plotly_chart(fig_amex, use_container_width=True)
    with v_col2:
        st.markdown("##### Reconciliation Health by Store"); store_summary = ar_df.groupby('Store').agg(Total_AR_Amount=('AR_Amount', 'sum'), Outstanding_AR_Amount=('Outstanding_AR_Amount', 'sum')).reset_index(); store_summary = store_summary[store_summary['Total_AR_Amount'] > 0]; store_summary['Outstanding_Pct'] = store_summary['Outstanding_AR_Amount'] / store_summary['Total_AR_Amount']; store_summary = store_summary.sort_values('Outstanding_AR_Amount', ascending=False); fig_store = px.bar(store_summary, x='Store', y='Outstanding_AR_Amount', hover_data={'Outstanding_AR_Amount': ':.2f', 'Total_AR_Amount': ':.2f', 'Outstanding_Pct': ':.2%'}, labels={'Outstanding_AR_Amount': 'Outstanding A/R ($)', 'Store': 'Store Number'}); fig_store.update_traces(marker_color='orange'); st.plotly_chart(fig_store, use_container_width=True)
        st.markdown("##### Reconciliation by Source"); source_summary = ar_df.groupby('Source')[['AR_Amount', 'Outstanding_AR_Amount']].sum().reset_index()
        if not source_summary.empty: source_summary['Matched_AR_Amount'] = source_summary['AR_Amount'] - source_summary['Outstanding_AR_Amount']; fig_source = px.bar(source_summary, x='Source', y=['Matched_AR_Amount', 'Outstanding_AR_Amount'], title="Matched vs. Outstanding by Source", labels={'value': 'Total Amount ($)', 'Source': 'Data Source'}, barmode='stack', color_discrete_map={'Matched_AR_Amount': 'green', 'Outstanding_AR_Amount': 'red'}); st.plotly_chart(fig_source, use_container_width=True)

if st.button("Process Files", key="process_files_button"):
    st.session_state.processed = False; log_stream.truncate(0); log_stream.seek(0)
    with st.spinner('Processing reconciliation...'):
        success = process_files(recon_month, merchant_file, wsr_zip_file, bank_file, settlement_file, quickbooks_0519_file, quickbooks_ngc_file)
    if success: st.success("Processing completed successfully!")
    else: st.error(f"Processing failed: {st.session_state.error_message}"); st.text_area("Error Logs", st.session_state.log_content, height=300)
if st.session_state.processed:
    st.subheader("Summary"); st.dataframe(st.session_state.summary_df.style.format({c: "${:,.2f}" for c in st.session_state.summary_df.columns if 'Amount' in c}))
    display_visualizations(st.session_state.recon_df)
    st.subheader("Reconciliation Details"); view_option = st.radio("Select View", ("WSR Reconciliation", "Full Reconciliation"), horizontal=True); display_df = st.session_state.wsr_recon_df if view_option == "WSR Reconciliation" else st.session_state.recon_df
    f1, f2, f3 = st.columns(3); filters = {'Store': f1.multiselect("Filter by Store", sorted(display_df["Store"].dropna().unique().astype(str))), 'Source': f2.multiselect("Filter by Source", sorted(display_df["Source"].dropna().unique())), 'Status': f3.multiselect("Filter by Status", sorted(display_df["Status"].dropna().unique()))}
    filtered_df = display_df.copy()
    for col, val in filters.items():
        if val: filtered_df = filtered_df[filtered_df[col].isin(val)]
    st.dataframe(filtered_df.style.format({'AR_Amount': '${:,.2f}', 'Outstanding_AR_Amount': '${:,.2f}', 'Bank_Amount': '${:,.2f}', 'Fee_Amount': '${:,.2f}'}))
    st.subheader("Issues")
    if not st.session_state.audit_df.empty: st.write("Audit Issues"); st.dataframe(st.session_state.audit_df)
    if not st.session_state.wsr_error_df.empty: st.write("WSR Errors"); st.dataframe(st.session_state.wsr_error_df)
    if not st.session_state.get('missing_weeks_df', pd.DataFrame()).empty: st.write("Stores With Missing Weekly Reports"); st.dataframe(st.session_state.missing_weeks_df)
    if st.session_state.audit_df.empty and st.session_state.wsr_error_df.empty and st.session_state.get('missing_weeks_df', pd.DataFrame()).empty: st.write("No major processing issues found.")
    st.subheader("Download Results"); st.download_button("Download Report", st.session_state.excel_file, f"reconciliation_{datetime.now():%Ym%d}.xlsx", "application/vnd.ms-excel")
    st.subheader("Logs"); st.text_area("Processing Logs", st.session_state.log_content, height=300)
