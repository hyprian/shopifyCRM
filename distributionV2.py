import os.path
import datetime
import yaml
import pandas as pd
import logging
import sys
import json
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

# --- Configuration ---
SETTINGS_FILE = 'settings.yaml'
SERVICE_ACCOUNT_FILE = 'molten-medley-458604-j9-855f3bdefd90.json'

# Scopes required for reading and writing
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Sheet-specific constants
ORDERS_SHEET_NAME = 'Orders'
ORDERS_HEADER_ROW_INDEX = 1  # Orders sheet header is row 2 (0-indexed)
ORDERS_DATA_START_ROW_INDEX = 2  # Orders sheet data starts row 3 (0-indexed)
ABANDONED_SHEET_NAME = 'Sheet1'
ABANDONED_HEADER_ROW_INDEX = 0  # Abandoned sheet header is row 1 (0-indexed)
ABANDONED_DATA_START_ROW_INDEX = 1  # Abandoned sheet data starts row 2 (0-indexed)

# Define call status priorities and report categories
CALL_PRIORITIES = {
    1: ["NDR"],
    2: ["Confirmation Pending", "Fresh"],
    3: ["Call didn't Pick", "Follow up"],
    4: ["Abandoned", "Number invalid/fake order"]
}

# Report categories mapping
STATUS_TO_REPORT_CATEGORY = {
    "Fresh": "Fresh",
    "Confirmation Pending": "Fresh",
    "Abandoned": "Abandoned",
    "Number invalid/fake order": "Invalid/Fake",
    "Call didn't Pick": "CNP",
    "Follow up": "Follow up",
    "NDR": "NDR"
}

# Column Names for BOTH sheets (mapped)
COL_NAMES_ORDERS = {
    'call_status': 'Call-status',
    'order_status': 'order status',
    'stakeholder': 'Stakeholder',
    'date_col_1': 'Date',
    'date_col_2': 'Date 2',
    'date_col_3': 'Date 3',
    'id': 'Id',
    'name': 'Name',
    'created_at': 'Created At',
    'customer_id': 'Id (Customer)',
    'initial_assignment_category': 'Initial Assignment Category'
}

COL_NAMES_ABANDONED = {
    'calling_status': 'Call status',
    'stakeholder': 'Stake Holder',
    'date_col_1': 'Date 1',
    'date_col_2': 'Date 2',
    'date_col_3': 'Date 3',
    'cart_id': 'cart_id',
    'phone_number': 'phone_number',
    'initial_assignment_category': 'Initial Assignment Category'
}

# --- Logging Setup ---
LOG_FILE = 'distribution_script.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- Load Settings Function ---
def load_settings(filename):
    """Loads configuration from a YAML file."""
    logger.info(f"Loading settings from '{filename}'...")
    try:
        with open(filename, 'r') as f:
            settings = yaml.safe_load(f)
        if not settings:
            logger.warning(f"Settings file '{filename}' is empty.")
            return None

        # Validate required fields
        required_fields = [
            ('sheets.orders_spreadsheet_id', str),
            ('sheets.abandoned_spreadsheet_id', str),
            ('sheets.report_sheet_name', str),
            ('processing_controls.process_orders_sheet', bool),      # New
            ('processing_controls.process_abandoned_sheet', bool), # New
            ('stakeholders', list)
        ]
        for field_path, expected_type in required_fields:
            keys = field_path.split('.')
            value = settings
            is_present = True
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value.get(key)
                else:
                    is_present = False
                    break
            
            if not is_present or value is None: # Check if present before type check
                logger.error(f"Missing or invalid '{field_path}' in settings file.")
                return None
            if not isinstance(value, expected_type):
                logger.error(f"'{field_path}' must be a {expected_type.__name__}, got {type(value).__name__}.")
                return None

        # Validate stakeholders
        for stakeholder in settings['stakeholders']:
            if not isinstance(stakeholder, dict) or 'name' not in stakeholder or 'limit' not in stakeholder:
                logger.error("Each stakeholder must be a dictionary with 'name' and 'limit' keys.")
                return None
            if not isinstance(stakeholder['name'], str) or not isinstance(stakeholder['limit'], int) or stakeholder['limit'] < 0:
                logger.error(f"Invalid stakeholder: name must be string, limit must be non-negative integer. Got name='{stakeholder.get('name')}', limit={stakeholder.get('limit')}.")
                return None

        logger.info(f"Settings loaded successfully: Orders Spreadsheet ID={settings['sheets']['orders_spreadsheet_id']}, "
                    f"Abandoned Spreadsheet ID={settings['sheets']['abandoned_spreadsheet_id']}, "
                    f"Report Sheet={settings['sheets']['report_sheet_name']}, "
                    f"Process Orders={settings['processing_controls']['process_orders_sheet']}, " # New log
                    f"Process Abandoned={settings['processing_controls']['process_abandoned_sheet']}, " # New log
                    f"{len(settings['stakeholders'])} stakeholders.")
        return settings
    except FileNotFoundError:
        logger.error(f"Error: Settings file '{filename}' not found.")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing settings file '{filename}': {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred loading settings: {e}")
        return None
    

# --- Helper Functions ---
def col_index_to_a1(index):
    """Converts column index (0-based) to A1 notation (e.g., 0 -> A, 1 -> B)."""
    col = ''
    while index >= 0:
        col = chr(index % 26 + ord('A')) + col
        index = index // 26 - 1
    return col

def assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments):
    """Assigns a stakeholder to a record if they have not reached their limit."""
    num_stakeholders = len(stakeholder_list)
    for i in range(num_stakeholders):
        index = (current_index + i) % num_stakeholders
        stakeholder = stakeholder_list[index]
        name = stakeholder['name']
        if stakeholder_assignments[name] < stakeholder['limit']:
            stakeholder_assignments[name] += 1
            next_index = (index + 1) % num_stakeholders
            return name, next_index
    logger.debug("No stakeholder has remaining capacity for assignment.")
    return None, current_index

# --- Authentication ---
def authenticate_google_sheets():
    """Authenticates using Streamlit secrets or local service account file."""
    creds = None
    # Check if running in Streamlit Cloud (or secrets are configured)
    try:
        if 'GOOGLE_CREDENTIALS' in st.secrets:
            logger.info("Loading credentials from Streamlit secrets...")
            creds_info = st.secrets["GOOGLE_CREDENTIALS"].to_dict()
            logger.debug(f"Streamlit secrets credentials keys: {list(creds_info.keys())}")
            creds = service_account.Credentials.from_service_account_info(
                creds_info, scopes=SCOPES)
            logger.info("Credentials loaded successfully from secrets.")
    except (KeyError, FileNotFoundError, st.errors.StreamlitAPIException) as e:
        logger.info(f"Streamlit secrets not found or inaccessible: {e}. Falling back to local service account file...")
    except Exception as e:
        logger.error(f"Error parsing Streamlit secrets credentials: {e}")
        return None

    # Fallback to local service account file if secrets are unavailable
    if creds is None:
        logger.info(f"Loading service account credentials from '{SERVICE_ACCOUNT_FILE}'...")
        try:
            creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)
            logger.info("Credentials loaded successfully from local file.")
        except FileNotFoundError:
            logger.error(f"Error: Service account key file '{SERVICE_ACCOUNT_FILE}' not found.")
            return None
        except Exception as e:
            logger.error(f"Error loading service account credentials from file: {e}")
            return None

    if creds is None:
        logger.error("No valid credentials loaded. Authentication failed.")
        return None

    logger.info("Building Google Sheets API service...")
    try:
        service = build('sheets', 'v4', credentials=creds)
        return service
    except RefreshError as e:
        logger.error(f"Authentication failed due to invalid credentials: {e}")
        logger.error("Ensure the service account key is valid and not revoked. If using Streamlit secrets, verify the private_key format.")
        return None
    except HttpError as e:
        logger.error(f"Google Sheets API Error during service build: {e}")
        logger.error("Ensure the service account has Editor access to the spreadsheet.")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during service build: {e}")
        return None

# --- Find Existing Report Range ---
def find_existing_report_range(sheet, spreadsheet_id, report_sheet_name, today_date_str):
    """Searches the report sheet for today's report section."""
    start_title = f"--- Stakeholder Report for Assignments on {today_date_str} ---"
    any_report_start_pattern = "--- Stakeholder Report for Assignments on "

    logger.info(f"Searching for existing report section for {today_date_str} in '{report_sheet_name}'...")
    start_row = None
    next_start_row = None
    last_row_in_sheet = 0

    try:
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{report_sheet_name}!A:A'
        ).execute()
        values = result.get('values', [])
        last_row_in_sheet = len(values)
        logger.debug(f"Read {last_row_in_sheet} rows from column A of '{report_sheet_name}'.")

        for i in range(last_row_in_sheet):
            row_value = values[i][0].strip() if values[i] and values[i][0] else ''
            if row_value == start_title:
                start_row = i + 1
                logger.info(f"Found existing report start for {today_date_str} at row {start_row}.")
                break

        if start_row is None:
            logger.info(f"No existing report found for {today_date_str}.")
            return None, None

        for i in range(start_row, last_row_in_sheet + 1):
            if i > last_row_in_sheet:
                break
            row_value = values[i-1][0].strip() if values[i-1] and values[i-1][0] else ''
            if row_value.startswith(any_report_start_pattern) and i > start_row:
                next_start_row = i
                logger.debug(f"Found start of next report section at row {next_start_row}.")
                break

        end_row_to_clear = next_start_row - 1 if next_start_row else last_row_in_sheet
        end_row_to_clear = max(start_row, end_row_to_clear)
        return start_row, end_row_to_clear

    except HttpError as e:
        if 'Unable to parse range' in str(e) or e.resp.status == 400:
            logger.warning(f"Sheet '{report_sheet_name}' not found. It will be created on write.")
            return None, None
        else:
            logger.error(f"Google Sheets API Error while searching for existing report: {e}")
            raise
    except Exception as e:
        logger.exception(f"Unexpected error while searching for existing report:")
        return None, None

# --- Process Abandoned Orders Sheet --- FULLY UPDATED
def distribute_abandoned_orders(service, stakeholder_list, stakeholder_assignments, abandoned_spreadsheet_id, abandoned_sheet_name):
    logger.info("--- Starting Abandoned Orders Processing ---")
    sheet = service.spreadsheets()
    today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y")
    abandoned_report_counts = {stakeholder['name']: {"Total": 0, "Abandoned": 0} for stakeholder in stakeholder_list}

    initial_assignment_col_name_abandoned = COL_NAMES_ABANDONED['initial_assignment_category']

    try:
        logger.info(f"Reading data from abandoned sheet '{abandoned_sheet_name}'...")
        read_range = f'{abandoned_sheet_name}!A:BI' # Read wide enough
        result = sheet.values().get(spreadsheetId=abandoned_spreadsheet_id, range=read_range).execute()
        values = result.get('values', [])

        if not values:
            logger.warning(f"No data found in abandoned sheet '{abandoned_sheet_name}'.")
            return abandoned_report_counts

        if ABANDONED_HEADER_ROW_INDEX >= len(values):
            logger.error(f"Abandoned sheet header index ({ABANDONED_HEADER_ROW_INDEX}) is out of bounds.")
            return abandoned_report_counts

        abandoned_header_from_sheet = [str(h).strip() if h is not None else '' for h in values[ABANDONED_HEADER_ROW_INDEX]]
        logger.info(f"Abandoned sheet header from sheet (row {ABANDONED_HEADER_ROW_INDEX + 1}): {abandoned_header_from_sheet}")

        df_header_abandoned = abandoned_header_from_sheet[:]
        if initial_assignment_col_name_abandoned not in df_header_abandoned:
            logger.warning(f"DataFrame Creation: Column '{initial_assignment_col_name_abandoned}' not found in Abandoned sheet header. Adding to DataFrame for processing, but it will NOT be written to sheet if missing there.")
            df_header_abandoned.append(initial_assignment_col_name_abandoned)
        
        df_header_abandoned_length = len(df_header_abandoned)
        data_rows_raw = values[ABANDONED_DATA_START_ROW_INDEX:]
        padded_data_rows = []
        for i, row in enumerate(data_rows_raw):
            processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
            if len(processed_row) < df_header_abandoned_length:
                processed_row.extend([''] * (df_header_abandoned_length - len(processed_row)))
            elif len(processed_row) > df_header_abandoned_length:
                processed_row = processed_row[:df_header_abandoned_length]
            padded_data_rows.append(processed_row)

        abandoned_df = pd.DataFrame(padded_data_rows, columns=df_header_abandoned)
        abandoned_df['_original_row_index'] = range(ABANDONED_DATA_START_ROW_INDEX + 1, ABANDONED_DATA_START_ROW_INDEX + 1 + len(abandoned_df))
        
        cols_to_ensure_in_df = [
            COL_NAMES_ABANDONED['calling_status'], COL_NAMES_ABANDONED['stakeholder'],
            COL_NAMES_ABANDONED['date_col_1'], COL_NAMES_ABANDONED.get('date_col_2'),
            COL_NAMES_ABANDONED.get('date_col_3'), initial_assignment_col_name_abandoned
        ]
        for col_name in cols_to_ensure_in_df:
            if col_name and col_name not in abandoned_df.columns:
                abandoned_df[col_name] = ''
            if col_name:
                abandoned_df[col_name] = abandoned_df[col_name].astype(str)

        abandoned_df[COL_NAMES_ABANDONED['calling_status']] = abandoned_df[COL_NAMES_ABANDONED['calling_status']].fillna('').astype(str).str.strip()
        statuses_to_process = ['', "Didn't Pickup", "Follow up"]
        abandoned_to_process_df = abandoned_df[abandoned_df[COL_NAMES_ABANDONED['calling_status']].isin(statuses_to_process)].copy()
        abandoned_filtered_indices = abandoned_to_process_df.index.tolist()

        if not abandoned_filtered_indices:
            logger.info("No abandoned rows matched filter criteria for assignment/reassignment. Skipping.")
            return abandoned_report_counts

        current_index = 0
        assigned_count = 0
        for df_index in abandoned_filtered_indices:
            assigned_stakeholder, current_index = assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments)
            if assigned_stakeholder is None: continue
            
            assigned_count += 1
            abandoned_df.loc[df_index, COL_NAMES_ABANDONED['stakeholder']] = assigned_stakeholder
            if initial_assignment_col_name_abandoned in abandoned_df.columns:
                abandoned_df.loc[df_index, initial_assignment_col_name_abandoned] = "Abandoned"
            
            abandoned_report_counts[assigned_stakeholder]["Total"] += 1
            abandoned_report_counts[assigned_stakeholder]["Abandoned"] += 1

            call_status = str(abandoned_df.loc[df_index, COL_NAMES_ABANDONED['calling_status']]).strip()
            original_date1_val = str(abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']]).strip()
            original_date2_val = str(abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']]).strip() if COL_NAMES_ABANDONED['date_col_2'] in abandoned_df.columns else ""
            original_date3_val = str(abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']]).strip() if COL_NAMES_ABANDONED['date_col_3'] in abandoned_df.columns else ""

            if call_status == '':
                abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = today_date_str_for_sheet
                if COL_NAMES_ABANDONED['date_col_2'] in abandoned_df.columns: abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = ''
                if COL_NAMES_ABANDONED['date_col_3'] in abandoned_df.columns: abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = ''
            elif call_status in ["Didn't Pickup", "Follow Up"]:
                if not original_date1_val:
                    abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = today_date_str_for_sheet
                    if COL_NAMES_ABANDONED['date_col_2'] in abandoned_df.columns: abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = ''
                    if COL_NAMES_ABANDONED['date_col_3'] in abandoned_df.columns: abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = ''
                elif not original_date2_val and COL_NAMES_ABANDONED['date_col_2'] in abandoned_df.columns:
                    abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = today_date_str_for_sheet
                    if COL_NAMES_ABANDONED['date_col_3'] in abandoned_df.columns: abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = ''
                elif not original_date3_val and COL_NAMES_ABANDONED['date_col_3'] in abandoned_df.columns:
                    abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = today_date_str_for_sheet
                else:
                    abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = original_date1_val
                    if COL_NAMES_ABANDONED['date_col_2'] in abandoned_df.columns: abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = original_date2_val
                    if COL_NAMES_ABANDONED['date_col_3'] in abandoned_df.columns: abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = original_date3_val
        logger.info(f"Stakeholders assigned/reassigned to {assigned_count} abandoned rows.")

        # --- BATCH UPDATE SECTION REVISED ---
        logger.info("Preparing batch update for Abandoned sheet...")
        abandoned_updates = []
        cols_we_are_changing_abandoned = [
            COL_NAMES_ABANDONED['stakeholder'], COL_NAMES_ABANDONED['date_col_1'],
            COL_NAMES_ABANDONED['date_col_2'], COL_NAMES_ABANDONED['date_col_3'],
            initial_assignment_col_name_abandoned
        ]
        # Use actual sheet header for determining update range and preserving other columns
        max_sheet_col_index_abandoned = len(abandoned_header_from_sheet) - 1
        update_range_end_col_a1 = col_index_to_a1(max_sheet_col_index_abandoned)

        assigned_indices_in_df = [
            idx for idx in abandoned_filtered_indices 
            if not pd.isna(abandoned_df.loc[idx, COL_NAMES_ABANDONED['stakeholder']]) and \
               abandoned_df.loc[idx, COL_NAMES_ABANDONED['stakeholder']] != ''
        ]

        for df_index in assigned_indices_in_df:
            original_sheet_row_num = abandoned_df.loc[df_index, '_original_row_index']
            
            # Construct the list of values to write for the entire row (as per sheet header)
            row_values_to_write = [''] * len(abandoned_header_from_sheet) # Default to blank
            
            modified_in_df_and_writable_to_sheet = False
            for sheet_col_idx, sheet_col_name in enumerate(abandoned_header_from_sheet):
                if sheet_col_name in abandoned_df.columns: # If this sheet column is in our DataFrame
                    df_value = abandoned_df.loc[df_index, sheet_col_name]
                    row_values_to_write[sheet_col_idx] = df_value if pd.notna(df_value) else ''
                    
                    # Check if this column was one we intended to change AND it's present in the sheet header
                    if sheet_col_name in cols_we_are_changing_abandoned:
                         modified_in_df_and_writable_to_sheet = True # Mark that we are changing something
                # If sheet_col_name is not in df.columns, it remains blank as initialized

            if modified_in_df_and_writable_to_sheet: # Only add to batch if we actually changed a target column
                abandoned_updates.append({
                    'range': f'{abandoned_sheet_name}!A{original_sheet_row_num}:{update_range_end_col_a1}{original_sheet_row_num}',
                    'values': [row_values_to_write]
                })
        
        logger.info(f"Prepared {len(abandoned_updates)} row updates for Abandoned sheet batch write.")
        if abandoned_updates:
            logger.info("Executing batch update to Abandoned sheet...")
            body = {'value_input_option': 'USER_ENTERED', 'data': abandoned_updates} # USER_ENTERED to preserve formats
            try:
                result = sheet.values().batchUpdate(spreadsheetId=abandoned_spreadsheet_id, body=body).execute()
                logger.info(f"Abandoned sheet batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
            except HttpError as e: logger.error(f"API Error during abandoned sheet batch update: {e}")
            except Exception as e: logger.exception("Unexpected error during abandoned sheet batch update:")
        else:
            logger.info("No updates to write back to Abandoned sheet.")

    except HttpError as err: logger.error(f"Google Sheets API Error during abandoned sheet processing: {err}")
    except Exception as e: logger.exception("Unexpected error during abandoned sheet processing:")
    logger.info("--- Finished Abandoned Orders Processing ---")
    return abandoned_report_counts


# --- Main Processing Function ---
def distribute_and_report():
    logger.info("Starting script.")
    settings = load_settings(SETTINGS_FILE)
    if not settings or 'stakeholders' not in settings or 'processing_controls' not in settings:
        logger.error("Failed to load settings or critical settings missing (stakeholders, processing_controls). Aborting.")
        st.error("Critical settings missing. Aborting.")
        return

    ORDERS_SPREADSHEET_ID = settings['sheets']['orders_spreadsheet_id']
    ABANDONED_SPREADSHEET_ID = settings['sheets']['abandoned_spreadsheet_id']
    REPORT_SHEET_NAME = settings['sheets']['report_sheet_name']
    stakeholder_list = settings['stakeholders']
    if not stakeholder_list:
        logger.error("Stakeholder list is empty. Aborting.")
        st.error("Stakeholder list is empty. Aborting.")
        return
    
    process_orders = settings['processing_controls']['process_orders_sheet']
    process_abandoned = settings['processing_controls']['process_abandoned_sheet']

    stakeholder_assignments = {stakeholder['name']: 0 for stakeholder in stakeholder_list}
    stakeholder_names = [stakeholder['name'] for stakeholder in stakeholder_list]
    
    service = authenticate_google_sheets()
    if not service:
        logger.error("Authentication failed. Aborting script.")
        # st.error is handled in authenticate_google_sheets
        return
    sheet = service.spreadsheets()

    _report_categories = list(set(list(STATUS_TO_REPORT_CATEGORY.values()) + ["Abandoned"])) # Ensure "Abandoned" is always a category
    # Initialize report counts
    orders_report_counts = {name: {"Total": 0, **{cat: 0 for cat in _report_categories}} for name in stakeholder_names}
    abandoned_report_counts_from_func = {name: {"Total": 0, "Abandoned": 0} for name in stakeholder_names} # Simplified for abandoned sheet

    if process_orders:
        logger.info("--- Starting Main Orders Processing (enabled by settings) ---")
        today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y")
        initial_assignment_col_name_orders = COL_NAMES_ORDERS['initial_assignment_category']

        try:
            logger.info(f"Reading data from '{ORDERS_SHEET_NAME}'...")
            read_range = f'{ORDERS_SHEET_NAME}!A:BI' # Read wide enough
            result = sheet.values().get(spreadsheetId=ORDERS_SPREADSHEET_ID, range=read_range).execute()
            values = result.get('values', [])

            if not values or ORDERS_HEADER_ROW_INDEX >= len(values):
                logger.warning(f"No data or insufficient rows in '{ORDERS_SHEET_NAME}'. Skipping Orders processing details.")
            else:
                orders_header_from_sheet = [str(h).strip() if h is not None else '' for h in values[ORDERS_HEADER_ROW_INDEX]]
                logger.info(f"Orders sheet header from sheet (row {ORDERS_HEADER_ROW_INDEX + 1}): {orders_header_from_sheet}")

                df_header_orders = orders_header_from_sheet[:]
                if initial_assignment_col_name_orders not in df_header_orders:
                    logger.warning(f"DataFrame Creation: Column '{initial_assignment_col_name_orders}' not found in Orders sheet header. Adding to DataFrame for processing, but it will NOT be written to sheet if missing there.")
                    df_header_orders.append(initial_assignment_col_name_orders)
                
                header_length = len(df_header_orders)
                data_rows_raw = values[ORDERS_DATA_START_ROW_INDEX:]
                padded_data_rows = []
                for i, row in enumerate(data_rows_raw):
                    processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
                    if len(processed_row) < header_length:
                        processed_row.extend([''] * (header_length - len(processed_row)))
                    elif len(processed_row) > header_length:
                        processed_row = processed_row[:header_length]
                    padded_data_rows.append(processed_row)

                df = pd.DataFrame(padded_data_rows, columns=df_header_orders)
                df['_original_row_index'] = range(ORDERS_DATA_START_ROW_INDEX + 1, ORDERS_DATA_START_ROW_INDEX + 1 + len(df))

                cols_to_ensure_in_df_orders = [
                    COL_NAMES_ORDERS['call_status'], COL_NAMES_ORDERS['stakeholder'],
                    COL_NAMES_ORDERS['date_col_1'], COL_NAMES_ORDERS.get('date_col_2'),
                    COL_NAMES_ORDERS.get('date_col_3'), initial_assignment_col_name_orders
                ]
                for col_name in cols_to_ensure_in_df_orders:
                    if col_name and col_name not in df.columns:
                        df[col_name] = ''
                    if col_name: # Ensure string type
                        df[col_name] = df[col_name].astype(str).fillna('')
                
                df[COL_NAMES_ORDERS['call_status']] = df[COL_NAMES_ORDERS['call_status']].fillna('').astype(str).str.strip()
                all_priority_statuses = [status for priority_list in CALL_PRIORITIES.values() for status in priority_list]
                orders_to_process_df = df[df[COL_NAMES_ORDERS['call_status']].isin(all_priority_statuses)].copy()
                orders_filtered_indices = orders_to_process_df.index.tolist()

                if orders_filtered_indices:
                    current_index = 0 # For round-robin
                    assigned_orders_processed_count = 0
                    for df_index in orders_filtered_indices:
                        assigned_stakeholder, current_index = assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments)
                        if assigned_stakeholder is None:
                            logger.warning(f"Orders Row (original index {df.loc[df_index, '_original_row_index']}): No stakeholder has remaining capacity. Skipping assignment.")
                            continue
                        
                        df.loc[df_index, COL_NAMES_ORDERS['stakeholder']] = assigned_stakeholder
                        call_status = str(df.loc[df_index, COL_NAMES_ORDERS['call_status']]).strip()

                        report_category = STATUS_TO_REPORT_CATEGORY.get(call_status)
                        if initial_assignment_col_name_orders in df.columns: # Only set if column exists in DF
                            if report_category:
                                df.loc[df_index, initial_assignment_col_name_orders] = report_category
                            else:
                                df.loc[df_index, initial_assignment_col_name_orders] = "Unknown" # Fallback
                                logger.warning(f"Orders Row {df.loc[df_index, '_original_row_index']}: Could not map call_status '{call_status}' to Initial Assignment. Set to 'Unknown'.")
                        
                        assigned_orders_processed_count += 1
                        orders_report_counts[assigned_stakeholder]["Total"] += 1
                        if report_category and report_category in orders_report_counts[assigned_stakeholder]:
                            orders_report_counts[assigned_stakeholder][report_category] += 1
                        
                        # Date logic
                        date1_val = str(df.loc[df_index, COL_NAMES_ORDERS['date_col_1']]).strip()
                        date2_exists = COL_NAMES_ORDERS.get('date_col_2') in df.columns
                        date3_exists = COL_NAMES_ORDERS.get('date_col_3') in df.columns
                        date2_val = str(df.loc[df_index, COL_NAMES_ORDERS.get('date_col_2')]).strip() if date2_exists else ""
                        date3_val = str(df.loc[df_index, COL_NAMES_ORDERS.get('date_col_3')]).strip() if date3_exists else ""


                        if call_status == "Call didn't Pick":
                            if not date1_val:
                                df.loc[df_index, COL_NAMES_ORDERS['date_col_1']] = today_date_str_for_sheet
                            elif not date2_val and date2_exists:
                                df.loc[df_index, COL_NAMES_ORDERS['date_col_2']] = today_date_str_for_sheet
                            elif not date3_val and date3_exists:
                                df.loc[df_index, COL_NAMES_ORDERS['date_col_3']] = today_date_str_for_sheet
                            # else: all dates filled, stakeholder assigned, no specific date change here by this rule
                        else: # For all other statuses eligible for assignment (Fresh, NDR, etc.)
                            df.loc[df_index, COL_NAMES_ORDERS['date_col_1']] = today_date_str_for_sheet
                            if date2_exists: df.loc[df_index, COL_NAMES_ORDERS['date_col_2']] = ''
                            if date3_exists: df.loc[df_index, COL_NAMES_ORDERS['date_col_3']] = ''
                    logger.info(f"Stakeholder, category, and date logic applied to {assigned_orders_processed_count} Orders rows.")
                else:
                    logger.info("No Orders rows matched filter criteria for assignment/reassignment.")


                # --- BATCH UPDATE SECTION FOR ORDERS ---
                logger.info("Preparing batch update for Orders sheet...")
                orders_updates = []
                cols_we_are_changing_orders = [
                    col for col in [
                        COL_NAMES_ORDERS['stakeholder'], COL_NAMES_ORDERS['date_col_1'],
                        COL_NAMES_ORDERS.get('date_col_2'), COL_NAMES_ORDERS.get('date_col_3'),
                        initial_assignment_col_name_orders
                    ] if col and col in orders_header_from_sheet # ensure col exists in sheet header
                ]
                if not cols_we_are_changing_orders:
                    logger.warning("None of the target columns for Orders sheet update are present in its header. No updates will be written for Orders.")
                else:
                    max_sheet_col_index_orders = len(orders_header_from_sheet) - 1
                    update_range_end_col_a1_orders = col_index_to_a1(max_sheet_col_index_orders)

                    assigned_indices_in_df_orders = [
                        idx for idx in orders_filtered_indices 
                        if not pd.isna(df.loc[idx, COL_NAMES_ORDERS['stakeholder']]) and \
                        df.loc[idx, COL_NAMES_ORDERS['stakeholder']] != ''
                    ]

                    for df_index in assigned_indices_in_df_orders:
                        original_sheet_row_num = df.loc[df_index, '_original_row_index']
                        row_values_to_write = [''] * len(orders_header_from_sheet)
                        
                        modified_a_target_column_orders = False
                        for sheet_col_idx, sheet_col_name in enumerate(orders_header_from_sheet):
                            if sheet_col_name in df.columns:
                                df_value = df.loc[df_index, sheet_col_name]
                                row_values_to_write[sheet_col_idx] = str(df_value) if pd.notna(df_value) else ''
                                if sheet_col_name in cols_we_are_changing_orders:
                                    modified_a_target_column_orders = True
                        
                        if modified_a_target_column_orders:
                            orders_updates.append({
                                'range': f'{ORDERS_SHEET_NAME}!A{original_sheet_row_num}:{update_range_end_col_a1_orders}{original_sheet_row_num}',
                                'values': [row_values_to_write]
                            })
                
                logger.info(f"Prepared {len(orders_updates)} row updates for Orders sheet batch write.")
                if orders_updates:
                    logger.info("Executing batch update to Orders sheet...")
                    body = {'value_input_option': 'USER_ENTERED', 'data': orders_updates}
                    try:
                        result = sheet.values().batchUpdate(spreadsheetId=ORDERS_SPREADSHEET_ID, body=body).execute()
                        logger.info(f"Orders sheet batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
                    except HttpError as e: logger.error(f"API Error during Orders sheet batch update: {e}")
                    except Exception as e: logger.exception("Unexpected error during Orders sheet batch update:")
                else:
                    logger.info("No updates to write back to Orders sheet.")
        except HttpError as err: logger.error(f"Google Sheets API Error during main Orders execution: {err}")
        except Exception as e: logger.exception("Unexpected error during main Orders execution:")
        logger.info("--- Finished Main Orders Processing ---")
    else:
        logger.info("--- Main Orders Processing SKIPPED (disabled by settings) ---")

    if process_abandoned:
        # Pass the current state of stakeholder_assignments so limits are shared
        abandoned_report_counts_from_func = distribute_abandoned_orders(service, stakeholder_list, stakeholder_assignments, ABANDONED_SPREADSHEET_ID, ABANDONED_SHEET_NAME)
    else:
        logger.info("--- Abandoned Orders Processing SKIPPED (disabled by settings) ---")
        # Ensure abandoned_report_counts_from_func is initialized if skipped
        abandoned_report_counts_from_func = {name: {"Total": 0, "Abandoned": 0} for name in stakeholder_names}


    logger.info("Combining report counts from processed sheets...")
    combined_report_counts = {name: {"Total": 0, **{cat: 0 for cat in _report_categories}} for name in stakeholder_names}

    for name in stakeholder_names:
        total_orders = orders_report_counts.get(name, {}).get("Total", 0)
        total_abandoned = abandoned_report_counts_from_func.get(name, {}).get("Total", 0)
        combined_report_counts[name]["Total"] = total_orders + total_abandoned

        for category in _report_categories:
            orders_cat_count = orders_report_counts.get(name, {}).get(category, 0)
            abandoned_cat_val = 0
            if category == "Abandoned": # "Abandoned" category specifically comes from the abandoned sheet's "Abandoned" count
                abandoned_cat_val = abandoned_report_counts_from_func.get(name, {}).get("Abandoned", 0)
            # Other categories (Fresh, CNP etc.) only come from orders_report_counts for this structure
            combined_report_counts[name][category] = orders_cat_count + abandoned_cat_val
    logger.info("Report counts combined.")


    logger.info("Generating Combined Stakeholder Report...")
    formatted_report_values = []
    today_date_str_for_report = datetime.date.today().strftime("%d-%b-%Y")
    formatted_report_values.append([f"--- Stakeholder Report for Assignments on {today_date_str_for_report} ---"])
    formatted_report_values.append(['']) # Blank line
    report_category_order_display = ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"] # Desired display order

    for s_name_iter in stakeholder_names:
        formatted_report_values.append([f"Calls assigned {s_name_iter}"])
        formatted_report_values.append([f"- Total Calls This Run - {combined_report_counts[s_name_iter].get('Total', 0)}"])
        for category_to_display in report_category_order_display:
            count_val = combined_report_counts[s_name_iter].get(category_to_display, 0)
            formatted_report_values.append([f"- {category_to_display} - {count_val}"])
        formatted_report_values.append(['']) # Blank line after each stakeholder
    formatted_report_values.append(['--- End of Report for ' + today_date_str_for_report + ' ---'])
    
    logger.info(f"Writing report to '{REPORT_SHEET_NAME}' in spreadsheet ID: {ORDERS_SPREADSHEET_ID}...") # Report is in Orders Spreadsheet
    start_row_existing, end_row_existing = find_existing_report_range(sheet, ORDERS_SPREADSHEET_ID, REPORT_SHEET_NAME, today_date_str_for_report)
    
    if start_row_existing is not None and end_row_existing is not None:
        range_to_clear = f'{REPORT_SHEET_NAME}!A{start_row_existing}:Z{end_row_existing}' # Clear wide enough
        range_to_write_new = f'{REPORT_SHEET_NAME}!A{start_row_existing}'
        logger.info(f"Clearing existing report range: {range_to_clear}")
        try:
            sheet.values().clear(spreadsheetId=ORDERS_SPREADSHEET_ID, range=range_to_clear).execute()
            body = {'values': formatted_report_values}
            logger.info(f"Updating report at: {range_to_write_new}")
            sheet.values().update(spreadsheetId=ORDERS_SPREADSHEET_ID, range=range_to_write_new, valueInputOption='USER_ENTERED', body=body).execute()
            logger.info("Report updated.")
            if 'st' in sys.modules: st.success("Report updated in Google Sheets.")
        except Exception as e:
            logger.exception(f"Error updating report: {e}")
            if 'st' in sys.modules: st.error(f"Error updating report: {e}")
    else:
        start_row_for_append = 1
        try:
            # Check if sheet exists and get last row if it does
            result_existing_report = sheet.values().get(spreadsheetId=ORDERS_SPREADSHEET_ID, range=f'{REPORT_SHEET_NAME}!A:A').execute()
            existing_values = result_existing_report.get('values', [])
            if existing_values:
                start_row_for_append = len(existing_values) + 2 # Add some spacing
        except HttpError as e:
            if 'Unable to parse range' in str(e) or (hasattr(e, 'resp') and e.resp and e.resp.status == 400):
                # Sheet does not exist, try to create it
                try:
                    logger.warning(f"Sheet '{REPORT_SHEET_NAME}' not found. Creating it for the report.")
                    add_sheet_body = {'requests': [{'addSheet': {'properties': {'title': REPORT_SHEET_NAME}}}]}
                    sheet.batchUpdate(spreadsheetId=ORDERS_SPREADSHEET_ID, body=add_sheet_body).execute()
                    logger.info(f"Sheet '{REPORT_SHEET_NAME}' created.")
                    start_row_for_append = 1 # Start at row 1 in new sheet
                except Exception as create_err:
                    logger.error(f"Error creating sheet '{REPORT_SHEET_NAME}': {create_err}")
                    if 'st' in sys.modules: st.error(f"Error creating report sheet: {create_err}")
                    return # Cannot proceed if sheet creation fails
            else:
                logger.error(f"Google Sheets API Error when checking for report sheet: {e}")
                if 'st' in sys.modules: st.error(f"API error checking report sheet: {e}")
                raise e # Re-raise if it's not a "sheet not found" error

        if formatted_report_values:
            body = {'values': formatted_report_values}
            range_to_write_report = f'{REPORT_SHEET_NAME}!A{start_row_for_append}'
            logger.info(f"Writing new report to: {range_to_write_report}")
            try:
                sheet.values().update(spreadsheetId=ORDERS_SPREADSHEET_ID, range=range_to_write_report, valueInputOption='USER_ENTERED', body=body).execute()
                logger.info(f"Report written to {range_to_write_report}.")
                if 'st' in sys.modules: st.success("New report written to Google Sheets.")
            except Exception as e:
                logger.exception(f"Error writing new report: {e}")
                if 'st' in sys.modules: st.error(f"Error writing new report: {e}")
        else:
            logger.warning("No report data to write.")
            if 'st' in sys.modules: st.warning("No report data generated.")
    logger.info("Script finished execution.")

# --- Main Execution ---
if __name__ == '__main__':
    distribute_and_report()

# distributionV2.py
# import os.path
# import datetime
# import yaml
# import pandas as pd
# import logging
# import sys
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError

# # --- Configuration ---
# # Settings File
# SETTINGS_FILE = 'settings.yaml'
# SERVICE_ACCOUNT_FILE = 'molten-medley-458604-j9-855f3bdefd90.json'

# # Scopes required for reading and writing
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# # Sheet-specific constants
# ORDERS_SHEET_NAME = 'Orders'
# ORDERS_HEADER_ROW_INDEX = 1  # Orders sheet header is row 2 (0-indexed)
# ORDERS_DATA_START_ROW_INDEX = 2  # Orders sheet data starts row 3 (0-indexed)
# ABANDONED_SHEET_NAME = 'Sheet1'
# ABANDONED_HEADER_ROW_INDEX = 0  # Abandoned sheet header is row 1 (0-indexed)
# ABANDONED_DATA_START_ROW_INDEX = 1  # Abandoned sheet data starts row 2 (0-indexed)

# # Define call status priorities and report categories
# CALL_PRIORITIES = {
#     1: ["NDR"],
#     2: ["Confirmation Pending", "Fresh"],
#     3: ["Call didn't Pick", "Follow up"],
#     4: ["Abandoned", "Number invalid/fake order"]
# }

# # Report categories mapping
# STATUS_TO_REPORT_CATEGORY = {
#     "Fresh": "Fresh",
#     "Confirmation Pending": "Fresh",
#     "Abandoned": "Abandoned",
#     "Number invalid/fake order": "Invalid/Fake",
#     "Call didn't Pick": "CNP",
#     "Follow up": "Follow up",
#     "NDR": "NDR"
# }

# # Column Names for BOTH sheets (mapped)
# COL_NAMES_ORDERS = {
#     'call_status': 'Call-status',
#     'order_status': 'order status',
#     'stakeholder': 'Stakeholder',
#     'date_col_1': 'Date',
#     'date_col_2': 'Date 2',
#     'date_col_3': 'Date 3',
#     'id': 'Id',
#     'name': 'Name',
#     'created_at': 'Created At',
#     'customer_id': 'Id (Customer)',
# }

# COL_NAMES_ABANDONED = {
#     'calling_status': 'Call status',
#     'stakeholder': 'Stake Holder',
#     'date_col_1': 'Date 1',
#     'date_col_2': 'Date 2',
#     'date_col_3': 'Date 3',
#     'cart_id': 'cart_id',
#     'phone_number': 'phone_number',
# }

# # --- Logging Setup ---
# LOG_FILE = 'distribution_script.log'
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(LOG_FILE),
#         logging.StreamHandler(sys.stdout)
#     ]
# )
# logger = logging.getLogger(__name__)

# # --- Load Settings Function ---
# def load_settings(filename):
#     """Loads configuration from a YAML file."""
#     logger.info(f"Loading settings from '{filename}'...")
#     try:
#         with open(filename, 'r') as f:
#             settings = yaml.safe_load(f)
#         if not settings:
#             logger.warning(f"Settings file '{filename}' is empty.")
#             return None

#         # Validate required fields
#         required_fields = [
#             ('sheets.orders_spreadsheet_id', str),
#             ('sheets.abandoned_spreadsheet_id', str),
#             ('sheets.report_sheet_name', str),
#             ('stakeholders', list)
#         ]
#         for field_path, expected_type in required_fields:
#             keys = field_path.split('.')
#             value = settings
#             for key in keys:
#                 value = value.get(key)
#                 if value is None:
#                     logger.error(f"Missing or invalid '{field_path}' in settings file.")
#                     return None
#             if not isinstance(value, expected_type):
#                 logger.error(f"'{field_path}' must be a {expected_type.__name__}, got {type(value).__name__}.")
#                 return None

#         # Validate stakeholders
#         for stakeholder in settings['stakeholders']:
#             if not isinstance(stakeholder, dict) or 'name' not in stakeholder or 'limit' not in stakeholder:
#                 logger.error("Each stakeholder must be a dictionary with 'name' and 'limit' keys.")
#                 return None
#             if not isinstance(stakeholder['name'], str) or not isinstance(stakeholder['limit'], int) or stakeholder['limit'] < 0:
#                 logger.error(f"Invalid stakeholder: name must be string, limit must be non-negative integer. Got name='{stakeholder.get('name')}', limit={stakeholder.get('limit')}.")
#                 return None

#         logger.info(f"Settings loaded successfully: Orders Spreadsheet ID={settings['sheets']['orders_spreadsheet_id']}, "
#                     f"Abandoned Spreadsheet ID={settings['sheets']['abandoned_spreadsheet_id']}, "
#                     f"Report Sheet={settings['sheets']['report_sheet_name']}, "
#                     f"{len(settings['stakeholders'])} stakeholders.")
#         return settings
#     except FileNotFoundError:
#         logger.error(f"Error: Settings file '{filename}' not found.")
#         return None
#     except yaml.YAMLError as e:
#         logger.error(f"Error parsing settings file '{filename}': {e}")
#         return None
#     except Exception as e:
#         logger.error(f"An unexpected error occurred loading settings: {e}")
#         return None

# # --- Helper Functions ---
# def col_index_to_a1(index):
#     """Converts column index (0-based) to A1 notation (e.g., 0 -> A, 1 -> B)."""
#     col = ''
#     while index >= 0:
#         col = chr(index % 26 + ord('A')) + col
#         index = index // 26 - 1
#     return col

# def assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments):
#     """Assigns a stakeholder to a record if they have not reached their limit."""
#     num_stakeholders = len(stakeholder_list)
#     for i in range(num_stakeholders):
#         index = (current_index + i) % num_stakeholders
#         stakeholder = stakeholder_list[index]
#         name = stakeholder['name']
#         if stakeholder_assignments[name] < stakeholder['limit']:
#             stakeholder_assignments[name] += 1
#             next_index = (index + 1) % num_stakeholders
#             return name, next_index
#     logger.debug("No stakeholder has remaining capacity for assignment.")
#     return None, current_index

# # --- Authentication ---
# def authenticate_google_sheets():
#     """Authenticates using a service account key file."""
#     creds = None
#     logger.info(f"Loading service account credentials from '{SERVICE_ACCOUNT_FILE}'...")
#     try:
#         creds = service_account.Credentials.from_service_account_file(
#             SERVICE_ACCOUNT_FILE, scopes=SCOPES)
#         logger.info("Credentials loaded successfully.")
#     except FileNotFoundError:
#         logger.error(f"Error: Service account key file '{SERVICE_ACCOUNT_FILE}' not found.")
#         return None
#     except Exception as e:
#         logger.error(f"Error loading service account credentials: {e}")
#         return None

#     logger.info("Building Google Sheets API service...")
#     try:
#         service = build('sheets', 'v4', credentials=creds)
#         return service
#     except HttpError as e:
#         logger.error(f"Google Sheets API Error during service build: {e}")
#         logger.error("Ensure the service account has Editor access to the spreadsheet.")
#         return None
#     except Exception as e:
#         logger.error(f"Unexpected error during service build: {e}")
#         return None

# # --- Find Existing Report Range ---
# def find_existing_report_range(sheet, spreadsheet_id, report_sheet_name, today_date_str):
#     """Searches the report sheet for today's report section."""
#     start_title = f"--- Stakeholder Report for Assignments on {today_date_str} ---"
#     any_report_start_pattern = "--- Stakeholder Report for Assignments on "

#     logger.info(f"Searching for existing report section for {today_date_str} in '{report_sheet_name}'...")
#     start_row = None
#     next_start_row = None
#     last_row_in_sheet = 0

#     try:
#         result = sheet.values().get(
#             spreadsheetId=spreadsheet_id,
#             range=f'{report_sheet_name}!A:A'
#         ).execute()
#         values = result.get('values', [])
#         last_row_in_sheet = len(values)
#         logger.debug(f"Read {last_row_in_sheet} rows from column A of '{report_sheet_name}'.")

#         for i in range(last_row_in_sheet):
#             row_value = values[i][0].strip() if values[i] and values[i][0] else ''
#             if row_value == start_title:
#                 start_row = i + 1
#                 logger.info(f"Found existing report start for {today_date_str} at row {start_row}.")
#                 break

#         if start_row is None:
#             logger.info(f"No existing report found for {today_date_str}.")
#             return None, None

#         for i in range(start_row, last_row_in_sheet + 1):
#             if i > last_row_in_sheet:
#                 break
#             row_value = values[i-1][0].strip() if values[i-1] and values[i-1][0] else ''
#             if row_value.startswith(any_report_start_pattern) and i > start_row:
#                 next_start_row = i
#                 logger.debug(f"Found start of next report section at row {next_start_row}.")
#                 break

#         end_row_to_clear = next_start_row - 1 if next_start_row else last_row_in_sheet
#         end_row_to_clear = max(start_row, end_row_to_clear)
#         return start_row, end_row_to_clear

#     except HttpError as e:
#         if 'Unable to parse range' in str(e) or e.resp.status == 400:
#             logger.warning(f"Sheet '{report_sheet_name}' not found. It will be created on write.")
#             return None, None
#         else:
#             logger.error(f"Google Sheets API Error while searching for existing report: {e}")
#             raise
#     except Exception as e:
#         logger.exception(f"Unexpected error while searching for existing report:")
#         return None, None

# # --- Process Abandoned Orders Sheet ---
# def distribute_abandoned_orders(service, stakeholder_list, stakeholder_assignments, abandoned_spreadsheet_id, abandoned_sheet_name):
#     """Processes abandoned orders (blank, Didn't Pickup, Follow Up) with limits and returns report counts."""
#     logger.info("--- Starting Abandoned Orders Processing ---")
#     sheet = service.spreadsheets()
#     today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y")

#     # Initialize report counts for abandoned orders
#     abandoned_report_counts = {stakeholder['name']: {"Total": 0, "Abandoned": 0} for stakeholder in stakeholder_list}

#     try:
#         # Read data
#         logger.info(f"Reading data from abandoned sheet '{abandoned_sheet_name}'...")
#         read_range = f'{abandoned_sheet_name}!A:BH'  # Keep slightly wider range
#         result = sheet.values().get(spreadsheetId=abandoned_spreadsheet_id, range=read_range).execute()
#         values = result.get('values', [])

#         if not values:
#             logger.warning(f"No data found in abandoned sheet '{abandoned_sheet_name}'.")
#             return abandoned_report_counts

#         logger.info(f"Successfully read {len(values)} rows from abandoned sheet.")

#         # Validate header row
#         if ABANDONED_HEADER_ROW_INDEX >= len(values):
#             logger.error(f"Abandoned sheet header index ({ABANDONED_HEADER_ROW_INDEX}) is out of bounds (total rows: {len(values)}).")
#             return abandoned_report_counts

#         abandoned_header = [str(h).strip() if h is not None else '' for h in values[ABANDONED_HEADER_ROW_INDEX]]
#         abandoned_header_length = len(abandoned_header)
#         logger.info(f"Abandoned sheet header row (row {ABANDONED_HEADER_ROW_INDEX + 1}) with {abandoned_header_length} columns identified.")

#         # Pad data rows
#         data_rows_raw = values[ABANDONED_DATA_START_ROW_INDEX:]
#         padded_data_rows = []
#         for i, row in enumerate(data_rows_raw):
#             processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
#             if len(processed_row) < abandoned_header_length:
#                 processed_row.extend([''] * (abandoned_header_length - len(processed_row)))
#             elif len(processed_row) > abandoned_header_length:
#                 logger.warning(f"Abandoned sheet row {ABANDONED_DATA_START_ROW_INDEX + i + 1} has more columns ({len(processed_row)}) than header ({abandoned_header_length}). Truncating.")
#                 processed_row = processed_row[:abandoned_header_length]
#             padded_data_rows.append(processed_row)

#         logger.info(f"Processed {len(padded_data_rows)} abandoned data rows.")

#         # Create DataFrame
#         abandoned_df = pd.DataFrame(padded_data_rows, columns=abandoned_header)
#         abandoned_df['_original_row_index'] = range(ABANDONED_DATA_START_ROW_INDEX + 1, ABANDONED_DATA_START_ROW_INDEX + 1 + len(abandoned_df))
#         logger.info(f"Created pandas DataFrame for abandoned data with {len(abandoned_df)} rows and {len(abandoned_df.columns)} columns.")

#         # Ensure required columns
#         cols_needed = [
#             COL_NAMES_ABANDONED['calling_status'],
#             COL_NAMES_ABANDONED['stakeholder'],
#             COL_NAMES_ABANDONED['date_col_1'],
#             COL_NAMES_ABANDONED['date_col_2'],
#             COL_NAMES_ABANDONED['date_col_3']
#         ]
#         for col_name in cols_needed:
#             if col_name not in abandoned_df.columns:
#                 logger.warning(f"Column '{col_name}' not found in abandoned DataFrame. Adding it as empty.")
#                 abandoned_df[col_name] = ''
#             abandoned_df[col_name] = abandoned_df[col_name].astype(str)  # Keep as string initially

#         # Clean calling status
#         abandoned_df[COL_NAMES_ABANDONED['calling_status']] = abandoned_df[COL_NAMES_ABANDONED['calling_status']].fillna('').astype(str).str.strip()

#         # Filter rows where Call Status is blank, "Didn't Pickup", or "Follow Up"
#         statuses_to_process = ['', "Didn't Pickup", "Follow Up"]
#         logger.info(f"Filtering abandoned rows with Call Status in {statuses_to_process}...")
#         abandoned_to_process_df = abandoned_df[abandoned_df[COL_NAMES_ABANDONED['calling_status']].isin(statuses_to_process)].copy()
#         abandoned_filtered_indices = abandoned_to_process_df.index.tolist()

#         logger.info(f"Found {len(abandoned_filtered_indices)} abandoned rows matching criteria: {statuses_to_process}.")

#         if not abandoned_filtered_indices:
#             logger.info("No abandoned rows matched filter criteria for assignment/reassignment. Skipping.")
#             return abandoned_report_counts

#         # Assign stakeholders with limits and apply date logic
#         logger.info(f"Assigning/Reassigning stakeholders to {len(abandoned_filtered_indices)} abandoned rows with limits...")
#         current_index = 0
#         assigned_count = 0
#         for df_index in abandoned_filtered_indices:
#             assigned_stakeholder, current_index = assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments)

#             if assigned_stakeholder is None:
#                 logger.debug(f"Abandoned row {abandoned_df.loc[df_index, '_original_row_index']} not assigned/reassigned: all stakeholders at capacity.")
#                 continue  # Skip to next row if no stakeholder available

#             assigned_count += 1
#             row_data = abandoned_df.loc[df_index]
#             original_sheet_row = row_data['_original_row_index']
#             call_status = row_data.get(COL_NAMES_ABANDONED['calling_status'], '').strip()
#             # Get original date values *before* potential updates
#             original_date1_val = str(row_data.get(COL_NAMES_ABANDONED['date_col_1'], '')).strip()
#             original_date2_val = str(row_data.get(COL_NAMES_ABANDONED['date_col_2'], '')).strip()
#             original_date3_val = str(row_data.get(COL_NAMES_ABANDONED['date_col_3'], '')).strip()

#             # --- Assign Stakeholder and Update Report Counts ---
#             abandoned_df.loc[df_index, COL_NAMES_ABANDONED['stakeholder']] = assigned_stakeholder
#             abandoned_report_counts[assigned_stakeholder]["Total"] += 1
#             abandoned_report_counts[assigned_stakeholder]["Abandoned"] += 1  # All processed count as Abandoned

#             # --- Date Logic ---
#             if call_status == '':
#                 # For blank status, ALWAYS update Date 1 to today's date.
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = today_date_str_for_sheet
#                 # Clear Date 2 and Date 3 just in case they had spurious data for a blank status row
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = ''
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = ''

#                 # Log appropriately based on whether Date 1 was overwritten
#                 if not original_date1_val:
#                     logger.debug(f"Abandoned Row {original_sheet_row} (Blank Status): Assigned to {assigned_stakeholder}, set Date 1 to {today_date_str_for_sheet}.")
#                 else:
#                     logger.debug(f"Abandoned Row {original_sheet_row} (Blank Status, existing Date 1 '{original_date1_val}'): Assigned to {assigned_stakeholder}, **overwrote** Date 1 with {today_date_str_for_sheet}.")

#             elif call_status in ["Didn't Pickup", "Follow Up"]:
#                 # This is a reassignment for follow-up (multi-date logic)
#                 if not original_date1_val:
#                     # If Date 1 is somehow blank, fill it first (unlikely flow)
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = today_date_str_for_sheet
#                     # Clear Date 2/3 if we are filling Date 1 here
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = ''
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = ''
#                     logger.warning(f"Abandoned Row {original_sheet_row} ('{call_status}') had no Date 1. Set Date 1 to {today_date_str_for_sheet}. Assigned to {assigned_stakeholder}.")
#                 elif not original_date2_val:
#                     # Date 1 exists, fill Date 2 if empty
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = today_date_str_for_sheet
#                     # Keep Date 1 as is, clear Date 3
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = ''
#                     logger.debug(f"Abandoned Row {original_sheet_row} ('{call_status}', 2nd attempt): Reassigned to {assigned_stakeholder}, set Date 2 to {today_date_str_for_sheet}.")
#                 elif not original_date3_val:
#                     # Date 1 and 2 exist, fill Date 3 if empty
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = today_date_str_for_sheet
#                     # Keep Date 1 and 2 as is
#                     logger.debug(f"Abandoned Row {original_sheet_row} ('{call_status}', 3rd attempt): Reassigned to {assigned_stakeholder}, set Date 3 to {today_date_str_for_sheet}.")
#                 else:
#                     # All 3 dates already filled
#                     logger.debug(f"Abandoned Row {original_sheet_row} ('{call_status}'): Already has 3 dates filled. Reassigned to {assigned_stakeholder}, but no date column updated.")
#                     # Ensure DataFrame reflects original dates if no update happened
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = original_date1_val
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = original_date2_val
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = original_date3_val

#             else:
#                 logger.error(f"Abandoned Row {original_sheet_row} has unexpected status '{call_status}' after filtering. Assigned to {assigned_stakeholder} but date logic skipped.")
#                 # Ensure DataFrame reflects original dates if skipped
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = original_date1_val
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = original_date2_val
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = original_date3_val

#         logger.info(f"Stakeholders assigned/reassigned to {assigned_count} abandoned rows.")

#         # Prepare batch update
#         logger.info("Preparing batch update for Abandoned sheet...")
#         abandoned_updates = []
#         cols_to_update_names_abandoned = [
#             COL_NAMES_ABANDONED['stakeholder'],
#             COL_NAMES_ABANDONED['date_col_1'],
#             COL_NAMES_ABANDONED['date_col_2'],
#             COL_NAMES_ABANDONED['date_col_3']
#         ]
#         sheet_col_indices_abandoned = {}
#         max_col_index_to_write_abandoned = -1

#         for col_name in cols_to_update_names_abandoned:
#             try:
#                 col_index = abandoned_header.index(col_name)
#                 sheet_col_indices_abandoned[col_name] = col_index
#                 max_col_index_to_write_abandoned = max(max_col_index_to_write_abandoned, col_index)
#                 logger.debug(f"Found column '{col_name}' at index {col_index} in abandoned sheet header.")
#             except ValueError:
#                 logger.warning(f"Column '{col_name}' not found in abandoned sheet header. Cannot write to this column.")
#                 sheet_col_indices_abandoned[col_name] = -1

#         if max_col_index_to_write_abandoned != -1:
#             assigned_indices = [idx for idx in abandoned_filtered_indices if not pd.isna(abandoned_df.loc[idx, COL_NAMES_ABANDONED['stakeholder']]) and abandoned_df.loc[idx, COL_NAMES_ABANDONED['stakeholder']] != '']

#             for df_index in assigned_indices:
#                 original_sheet_row = abandoned_df.loc[df_index, '_original_row_index']
#                 row_values_to_write = [None] * (max_col_index_to_write_abandoned + 1)

#                 for col_name in cols_to_update_names_abandoned:
#                     col_idx = sheet_col_indices_abandoned.get(col_name, -1)
#                     if col_idx != -1:
#                         value_to_write = abandoned_df.loc[df_index, col_name]
#                         # Write blank string for empty/None values to clear cells if needed
#                         row_values_to_write[col_idx] = value_to_write if pd.notna(value_to_write) else ''

#                 if any(val is not None for val in row_values_to_write):
#                     abandoned_updates.append({
#                         'range': f'{abandoned_sheet_name}!A{original_sheet_row}:{col_index_to_a1(max_col_index_to_write_abandoned)}{original_sheet_row}',
#                         'values': [row_values_to_write]
#                     })

#             logger.info(f"Prepared {len(abandoned_updates)} row updates for Abandoned sheet batch write.")
#         else:
#             logger.warning("None of the target update columns (Stakeholder, Date 1/2/3) were found in the abandoned sheet header. No updates prepared.")

#         # Execute batch update
#         if abandoned_updates:
#             logger.info("Executing batch update to Abandoned sheet...")
#             body = {'value_input_option': 'RAW', 'data': abandoned_updates}
#             try:
#                 result = sheet.values().batchUpdate(
#                     spreadsheetId=abandoned_spreadsheet_id, body=body).execute()
#                 logger.info(f"Abandoned sheet batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
#             except HttpError as e:
#                 logger.error(f"API Error during abandoned sheet batch update: {e}")
#             except Exception as e:
#                 logger.exception("Unexpected error during abandoned sheet batch update:")
#         else:
#             logger.info("No updates to write back to Abandoned sheet.")

#     except HttpError as err:
#         logger.error(f"Google Sheets API Error during abandoned sheet processing: {err}")
#     except Exception as e:
#         logger.exception("Unexpected error during abandoned sheet processing:")

#     logger.info("--- Finished Abandoned Orders Processing ---")
#     return abandoned_report_counts

# # --- Main Processing Function ---
# def distribute_and_report():
#     logger.info("Starting script.")

#     settings = load_settings(SETTINGS_FILE)
#     if not settings or 'stakeholders' not in settings:
#         logger.error("Failed to load settings. Aborting.")
#         return

#     # Extract configuration
#     ORDERS_SPREADSHEET_ID = settings['sheets']['orders_spreadsheet_id']
#     ABANDONED_SPREADSHEET_ID = settings['sheets']['abandoned_spreadsheet_id']
#     REPORT_SHEET_NAME = settings['sheets']['report_sheet_name']
#     stakeholder_list = settings['stakeholders']
    
#     if not stakeholder_list:
#         logger.error("Stakeholder list is empty. Aborting.")
#         return
#     logger.info(f"Loaded {len(stakeholder_list)} stakeholders: {[s['name'] for s in stakeholder_list]}")
    
#     # Initialize assignment tracking
#     stakeholder_assignments = {stakeholder['name']: 0 for stakeholder in stakeholder_list}
#     stakeholder_names = [stakeholder['name'] for stakeholder in stakeholder_list]

#     service = authenticate_google_sheets()
#     if not service:
#         logger.error("Authentication failed. Aborting script.")
#         return
#     sheet = service.spreadsheets()

#     # Initialize combined report counts
#     combined_report_counts = {
#         name: {"Total": 0, "Fresh": 0, "Abandoned": 0, "Invalid/Fake": 0, "CNP": 0, "Follow up": 0, "NDR": 0}
#         for name in stakeholder_names
#     }

#     # --- Process Main Orders Sheet ---
#     logger.info("--- Starting Main Orders Processing ---")
#     today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y")
#     today_date_str_for_report = datetime.date.today().strftime("%d-%b-%Y")
    
#     orders_report_counts = {
#         name: {"Total": 0, "Fresh": 0, "Abandoned": 0, "Invalid/Fake": 0, "CNP": 0, "Follow up": 0, "NDR": 0}
#         for name in stakeholder_names
#     }

#     try:
#         # Read data
#         logger.info(f"Reading data from '{ORDERS_SHEET_NAME}'...")
#         read_range = f'{ORDERS_SHEET_NAME}!A:BD'
#         result = sheet.values().get(spreadsheetId=ORDERS_SPREADSHEET_ID, range=read_range).execute()
#         values = result.get('values', [])

#         if not values:
#             logger.warning(f"No data found in '{ORDERS_SHEET_NAME}'.")
#         else:
#             logger.info(f"Read {len(values)} rows from '{ORDERS_SHEET_NAME}'.")

#             if len(values) < ORDERS_DATA_START_ROW_INDEX + 1:
#                 logger.error(f"Not enough rows in '{ORDERS_SHEET_NAME}'. Need at least {ORDERS_DATA_START_ROW_INDEX + 1} rows.")
#             elif ORDERS_HEADER_ROW_INDEX >= len(values):
#                 logger.error(f"Orders sheet header index ({ORDERS_HEADER_ROW_INDEX}) is out of bounds (total rows: {len(values)}).")
#             else:
#                 header = [str(h).strip() if h is not None else '' for h in values[ORDERS_HEADER_ROW_INDEX]]
#                 header_length = len(header)
#                 logger.info(f"Orders sheet header row (row {ORDERS_HEADER_ROW_INDEX + 1}) with {header_length} columns identified.")

#                 # Pad data rows
#                 data_rows_raw = values[ORDERS_DATA_START_ROW_INDEX:]
#                 padded_data_rows = []
#                 for i, row in enumerate(data_rows_raw):
#                     processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
#                     if len(processed_row) < header_length:
#                         processed_row.extend([''] * (header_length - len(processed_row)))
#                     elif len(processed_row) > header_length:
#                         logger.warning(f"Orders Row {ORDERS_DATA_START_ROW_INDEX + i + 1} has more columns ({len(processed_row)}) than header ({header_length}). Truncating.")
#                         processed_row = processed_row[:header_length]
#                     padded_data_rows.append(processed_row)

#                 logger.info(f"Processed {len(padded_data_rows)} Orders data rows.")

#                 # Create DataFrame
#                 df = pd.DataFrame(padded_data_rows, columns=header)
#                 df['_original_row_index'] = range(ORDERS_DATA_START_ROW_INDEX + 1, ORDERS_DATA_START_ROW_INDEX + 1 + len(df))
#                 logger.info(f"Created pandas DataFrame for Orders data with {len(df)} rows and {len(df.columns)} columns.")

#                 # Ensure required columns
#                 cols_needed_orders = [
#                     COL_NAMES_ORDERS['call_status'],
#                     COL_NAMES_ORDERS['stakeholder'],
#                     COL_NAMES_ORDERS['date_col_1'],
#                     COL_NAMES_ORDERS['date_col_2'],
#                     COL_NAMES_ORDERS['date_col_3']
#                 ]
#                 for col_name in cols_needed_orders:
#                     if col_name not in df.columns:
#                         logger.warning(f"Column '{col_name}' not found in Orders DataFrame. Adding it as empty.")
#                         df[col_name] = ''
#                     df[col_name] = df[col_name].astype(str)

#                 # Clean status column
#                 df[COL_NAMES_ORDERS['call_status']] = df[COL_NAMES_ORDERS['call_status']].fillna('').astype(str).str.strip()

#                 # Filter rows for processing
#                 logger.info("Filtering Orders rows based on priority statuses...")
#                 all_priority_statuses = [status for priority_list in CALL_PRIORITIES.values() for status in priority_list]
#                 orders_to_process_df = df[df[COL_NAMES_ORDERS['call_status']].isin(all_priority_statuses)].copy()
#                 orders_filtered_indices = orders_to_process_df.index.tolist()

#                 logger.info(f"Found {len(orders_filtered_indices)} Orders rows matching priority statuses.")

#                 # Assign stakeholders and dates
#                 if orders_filtered_indices:
#                     logger.info(f"Assigning stakeholders to {len(orders_filtered_indices)} Orders rows with limits...")
#                     current_index = 0
#                     assigned_orders_processed_count = 0
#                     for df_index in orders_filtered_indices:
#                         assigned_stakeholder, current_index = assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments)
#                         if assigned_stakeholder is None:
#                             logger.debug(f"Orders row {df.loc[df_index, '_original_row_index']} not assigned: all stakeholders at capacity.")
#                             continue
#                         row_data = df.loc[df_index]
#                         df.loc[df_index, COL_NAMES_ORDERS['stakeholder']] = assigned_stakeholder
#                         call_status = row_data.get(COL_NAMES_ORDERS['call_status'], '').strip()
#                         date1_val = str(row_data.get(COL_NAMES_ORDERS['date_col_1'], '')).strip()
#                         date2_val = str(row_data.get(COL_NAMES_ORDERS['date_col_2'], '')).strip()
#                         date3_val = str(row_data.get(COL_NAMES_ORDERS['date_col_3'], '')).strip()

#                         # Update report counts
#                         assigned_orders_processed_count += 1
#                         orders_report_counts[assigned_stakeholder]["Total"] += 1
#                         report_category = STATUS_TO_REPORT_CATEGORY.get(call_status)
#                         if report_category in orders_report_counts[assigned_stakeholder]:
#                             orders_report_counts[assigned_stakeholder][report_category] += 1
#                         else:
#                             logger.warning(f"Report category '{report_category}' for status '{call_status}' not found.")

#                         # Date logic
#                         if call_status == "Call didn't Pick":
#                             if not date1_val:
#                                 df.loc[df_index, COL_NAMES_ORDERS['date_col_1']] = today_date_str_for_sheet
#                                 logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 1st attempt. Set Date to {today_date_str_for_sheet}.")
#                             elif not date2_val:
#                                 df.loc[df_index, COL_NAMES_ORDERS['date_col_2']] = today_date_str_for_sheet
#                                 logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 2nd attempt. Set Date 2 to {today_date_str_for_sheet}.")
#                             elif not date3_val:
#                                 df.loc[df_index, COL_NAMES_ORDERS['date_col_3']] = today_date_str_for_sheet
#                                 logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 3rd attempt. Set Date 3 to {today_date_str_for_sheet}.")
#                             else:
#                                 logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 3 attempts already logged. Dates unchanged.")
#                         else:
#                             df.loc[df_index, COL_NAMES_ORDERS['date_col_1']] = today_date_str_for_sheet
#                             logger.debug(f"Orders Row {row_data['_original_row_index']}: Status '{call_status}'. Set Date to {today_date_str_for_sheet}.")

#                     logger.info(f"Date logic and report counts applied to {assigned_orders_processed_count} Orders rows.")

#                 # Prepare batch update
#                 logger.info("Preparing batch update for Orders sheet...")
#                 orders_updates = []
#                 cols_to_update_names_orders = [
#                     COL_NAMES_ORDERS['stakeholder'],
#                     COL_NAMES_ORDERS['date_col_1'],
#                     COL_NAMES_ORDERS['date_col_2'],
#                     COL_NAMES_ORDERS['date_col_3']
#                 ]
#                 sheet_col_indices_orders = {}
#                 max_col_index_to_write_orders = -1

#                 for col_name in cols_to_update_names_orders:
#                     try:
#                         col_index = header.index(col_name)
#                         sheet_col_indices_orders[col_name] = col_index
#                         max_col_index_to_write_orders = max(max_col_index_to_write_orders, col_index)
#                         logger.debug(f"Found column '{col_name}' at index {col_index} in Orders sheet header.")
#                     except ValueError:
#                         logger.warning(f"Column '{col_name}' not found in Orders sheet header. Cannot write to this column.")
#                         sheet_col_indices_orders[col_name] = -1

#                 if max_col_index_to_write_orders != -1:
#                     for df_index in orders_filtered_indices:
#                         if df.loc[df_index, COL_NAMES_ORDERS['stakeholder']]:
#                             original_sheet_row = df.loc[df_index, '_original_row_index']
#                             row_values_to_write = [None] * (max_col_index_to_write_orders + 1)
#                             for col_name in cols_to_update_names_orders:
#                                 if sheet_col_indices_orders.get(col_name, -1) != -1:
#                                     row_values_to_write[sheet_col_indices_orders[col_name]] = df.loc[df_index, col_name]
#                             orders_updates.append({
#                                 'range': f'{ORDERS_SHEET_NAME}!A{original_sheet_row}',
#                                 'values': [row_values_to_write]
#                             })

#                     logger.info(f"Prepared {len(orders_updates)} row updates for Orders sheet batch write.")
#                 else:
#                     logger.warning("No writeable columns found in Orders header. No updates prepared.")

#                 # Execute batch update
#                 if orders_updates:
#                     logger.info("Executing batch update to Orders sheet...")
#                     body = {'value_input_option': 'RAW', 'data': orders_updates}
#                     try:
#                         result = sheet.values().batchUpdate(
#                             spreadsheetId=ORDERS_SPREADSHEET_ID, body=body).execute()
#                         logger.info(f"Orders sheet batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
#                     except HttpError as e:
#                         logger.error(f"API Error during Orders sheet batch update: {e}")
#                     except Exception as e:
#                         logger.exception("Unexpected error during Orders sheet batch update:")
#                 else:
#                     logger.info("No updates to write back to Orders sheet.")

#         logger.info("--- Finished Main Orders Processing ---")

#     except HttpError as err:
#         logger.error(f"Google Sheets API Error during main Orders execution: {err}")
#     except Exception as e:
#         logger.exception("Unexpected error during main Orders execution:")

#     # --- Process Abandoned Orders Sheet ---
#     abandoned_report_counts = distribute_abandoned_orders(service, stakeholder_list, stakeholder_assignments, ABANDONED_SPREADSHEET_ID, ABANDONED_SHEET_NAME)

#     # --- Combine Report Counts ---
#     logger.info("Combining report counts from Orders and Abandoned sheets...")
#     for name in stakeholder_names:
#         combined_report_counts[name]["Total"] = (
#             orders_report_counts[name]["Total"] + abandoned_report_counts[name]["Total"]
#         )
#         for category in ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]:
#             combined_report_counts[name][category] = (
#                 orders_report_counts[name].get(category, 0) +
#                 abandoned_report_counts[name].get(category, 0)
#             )
#     logger.info("Report counts combined.")

#     # --- Generate Combined Stakeholder Report ---
#     logger.info("Generating Combined Stakeholder Report...")
#     formatted_report_values = []
#     formatted_report_values.append([f"--- Stakeholder Report for Assignments on {today_date_str_for_report} ---"])
#     formatted_report_values.append([''])

#     report_category_order = ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]

#     for stakeholder in stakeholder_names:
#         formatted_report_values.append([f"Calls assigned {stakeholder}"])
#         formatted_report_values.append([f"- Total Calls This Run - {combined_report_counts[stakeholder]['Total']}"])
#         for category in report_category_order:
#             formatted_report_values.append([f"- {category} - {combined_report_counts[stakeholder][category]}"])
#         formatted_report_values.append([''])

#     formatted_report_values.append(['--- End of Report for ' + today_date_str_for_report + ' ---'])
#     logger.info(f"Formatted combined report data ({len(formatted_report_values)} rows).")

#     # --- Write Report ---
#     logger.info(f"Writing report to '{REPORT_SHEET_NAME}'...")
#     start_row_existing, end_row_existing = find_existing_report_range(
#         sheet, ORDERS_SPREADSHEET_ID, REPORT_SHEET_NAME, today_date_str_for_report
#     )

#     if start_row_existing is not None and end_row_existing is not None:
#         logger.info(f"Existing report for {today_date_str_for_report} found. Updating range...")
#         range_to_clear = f'{REPORT_SHEET_NAME}!A{start_row_existing}:Z{end_row_existing}'
#         range_to_write_new = f'{REPORT_SHEET_NAME}!A{start_row_existing}'
#         try:
#             logger.info(f"Clearing range: {range_to_clear}")
#             sheet.values().clear(spreadsheetId=ORDERS_SPREADSHEET_ID, range=range_to_clear).execute()
#             logger.info("Cleared old report data.")
#             logger.info(f"Writing new report data to range: {range_to_write_new}")
#             body = {'values': formatted_report_values}
#             result = sheet.values().update(
#                 spreadsheetId=ORDERS_SPREADSHEET_ID, range=range_to_write_new,
#                 valueInputOption='RAW', body=body).execute()
#             logger.info(f"Report updated. {result.get('updatedCells', 'N/A')} cells updated.")
#         except HttpError as e:
#             logger.error(f"API Error while updating report: {e}")
#         except Exception as e:
#             logger.exception("Unexpected error while updating report:")
#     else:
#         logger.info(f"No existing report for {today_date_str_for_report}. Appending new report...")
#         start_row_for_append = 1
#         try:
#             result_existing_report = sheet.values().get(spreadsheetId=ORDERS_SPREADSHEET_ID, range=f'{REPORT_SHEET_NAME}!A:A').execute()
#             existing_values = result_existing_report.get('values', [])
#             if existing_values:
#                 start_row_for_append = len(existing_values) + 1
#             logger.info(f"Found {len(existing_values)} existing rows. New report starts at row {start_row_for_append}.")
#         except HttpError as e:
#             if 'Unable to parse range' in str(e) or e.resp.status == 400:
#                 logger.warning(f"Sheet '{REPORT_SHEET_NAME}' not found. Creating it.")
#                 try:
#                     body = {'requests': [{'addSheet': {'properties': {'title': REPORT_SHEET_NAME}}}]}
#                     sheet.batchUpdate(spreadsheetId=ORDERS_SPREADSHEET_ID, body=body).execute()
#                     logger.info(f"Created sheet '{REPORT_SHEET_NAME}'. Report starts at row {start_row_for_append}.")
#                 except Exception as create_err:
#                     logger.error(f"Error creating sheet '{REPORT_SHEET_NAME}': {create_err}")
#                     return
#             else:
#                 logger.error(f"API Error while checking/reading sheet for append: {e}")
#                 raise
#         except Exception as e:
#             logger.exception(f"Unexpected error while finding last row:")
#             return

#         if formatted_report_values:
#             body = {'values': formatted_report_values}
#             range_to_write_report = f'{REPORT_SHEET_NAME}!A{start_row_for_append}'
#             logger.info(f"Writing report data to range '{range_to_write_report}'.")
#             try:
#                 result = sheet.values().update(
#                     spreadsheetId=ORDERS_SPREADSHEET_ID, range=range_to_write_report,
#                     valueInputOption='RAW', body=body).execute()
#                 logger.info(f"Report written. {result.get('updatedCells', 'N/A')} cells updated.")
#             except HttpError as e:
#                 logger.error(f"API Error while writing report: {e}")
#             except Exception as e:
#                 logger.exception("Unexpected error while writing report:")
#         else:
#             logger.warning("No report data to write.")

#     logger.info("Script finished execution.")

# # --- Main Execution ---
# if __name__ == '__main__':
#     distribute_and_report()


# import os.path
# import datetime
# import yaml
# import pandas as pd
# import logging
# import sys
# import json
# import streamlit as st
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# from google.auth.exceptions import RefreshError

# # --- Configuration ---
# SETTINGS_FILE = 'settings.yaml'
# SERVICE_ACCOUNT_FILE = 'molten-medley-458604-j9-855f3bdefd90.json'

# # Scopes required for reading and writing
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# # Sheet-specific constants
# ORDERS_SHEET_NAME = 'Orders'
# ORDERS_HEADER_ROW_INDEX = 1  # Orders sheet header is row 2 (0-indexed)
# ORDERS_DATA_START_ROW_INDEX = 2  # Orders sheet data starts row 3 (0-indexed)
# ABANDONED_SHEET_NAME = 'Sheet1'
# ABANDONED_HEADER_ROW_INDEX = 0  # Abandoned sheet header is row 1 (0-indexed)
# ABANDONED_DATA_START_ROW_INDEX = 1  # Abandoned sheet data starts row 2 (0-indexed)

# # Define call status priorities and report categories
# CALL_PRIORITIES = {
#     1: ["NDR"],
#     2: ["Confirmation Pending", "Fresh"],
#     3: ["Call didn't Pick", "Follow up"],
#     4: ["Abandoned", "Number invalid/fake order"]
# }

# # Report categories mapping
# STATUS_TO_REPORT_CATEGORY = {
#     "Fresh": "Fresh",
#     "Confirmation Pending": "Fresh",
#     "Abandoned": "Abandoned",
#     "Number invalid/fake order": "Invalid/Fake",
#     "Call didn't Pick": "CNP",
#     "Follow up": "Follow up",
#     "NDR": "NDR"
# }

# # Column Names for BOTH sheets (mapped)
# COL_NAMES_ORDERS = {
#     'call_status': 'Call-status',
#     'order_status': 'order status',
#     'stakeholder': 'Stakeholder',
#     'date_col_1': 'Date',
#     'date_col_2': 'Date 2',
#     'date_col_3': 'Date 3',
#     'id': 'Id',
#     'name': 'Name',
#     'created_at': 'Created At',
#     'customer_id': 'Id (Customer)',
# }

# COL_NAMES_ABANDONED = {
#     'calling_status': 'Call status',
#     'stakeholder': 'Stake Holder',
#     'date_col_1': 'Date 1',
#     'date_col_2': 'Date 2',
#     'date_col_3': 'Date 3',
#     'cart_id': 'cart_id',
#     'phone_number': 'phone_number',
# }

# # --- Logging Setup ---
# LOG_FILE = 'distribution_script.log'
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(LOG_FILE),
#         logging.StreamHandler(sys.stdout)
#     ]
# )
# logger = logging.getLogger(__name__)

# # --- Load Settings Function ---
# def load_settings(filename):
#     """Loads configuration from a YAML file."""
#     logger.info(f"Loading settings from '{filename}'...")
#     try:
#         with open(filename, 'r') as f:
#             settings = yaml.safe_load(f)
#         if not settings:
#             logger.warning(f"Settings file '{filename}' is empty.")
#             return None

#         # Validate required fields
#         required_fields = [
#             ('sheets.orders_spreadsheet_id', str),
#             ('sheets.abandoned_spreadsheet_id', str),
#             ('sheets.report_sheet_name', str),
#             ('stakeholders', list)
#         ]
#         for field_path, expected_type in required_fields:
#             keys = field_path.split('.')
#             value = settings
#             for key in keys:
#                 value = value.get(key)
#                 if value is None:
#                     logger.error(f"Missing or invalid '{field_path}' in settings file.")
#                     return None
#             if not isinstance(value, expected_type):
#                 logger.error(f"'{field_path}' must be a {expected_type.__name__}, got {type(value).__name__}.")
#                 return None

#         # Validate stakeholders
#         for stakeholder in settings['stakeholders']:
#             if not isinstance(stakeholder, dict) or 'name' not in stakeholder or 'limit' not in stakeholder:
#                 logger.error("Each stakeholder must be a dictionary with 'name' and 'limit' keys.")
#                 return None
#             if not isinstance(stakeholder['name'], str) or not isinstance(stakeholder['limit'], int) or stakeholder['limit'] < 0:
#                 logger.error(f"Invalid stakeholder: name must be string, limit must be non-negative integer. Got name='{stakeholder.get('name')}', limit={stakeholder.get('limit')}.")
#                 return None

#         logger.info(f"Settings loaded successfully: Orders Spreadsheet ID={settings['sheets']['orders_spreadsheet_id']}, "
#                     f"Abandoned Spreadsheet ID={settings['sheets']['abandoned_spreadsheet_id']}, "
#                     f"Report Sheet={settings['sheets']['report_sheet_name']}, "
#                     f"{len(settings['stakeholders'])} stakeholders.")
#         return settings
#     except FileNotFoundError:
#         logger.error(f"Error: Settings file '{filename}' not found.")
#         return None
#     except yaml.YAMLError as e:
#         logger.error(f"Error parsing settings file '{filename}': {e}")
#         return None
#     except Exception as e:
#         logger.error(f"An unexpected error occurred loading settings: {e}")
#         return None

# # --- Helper Functions ---
# def col_index_to_a1(index):
#     """Converts column index (0-based) to A1 notation (e.g., 0 -> A, 1 -> B)."""
#     col = ''
#     while index >= 0:
#         col = chr(index % 26 + ord('A')) + col
#         index = index // 26 - 1
#     return col

# def assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments):
#     """Assigns a stakeholder to a record if they have not reached their limit."""
#     num_stakeholders = len(stakeholder_list)
#     for i in range(num_stakeholders):
#         index = (current_index + i) % num_stakeholders
#         stakeholder = stakeholder_list[index]
#         name = stakeholder['name']
#         if stakeholder_assignments[name] < stakeholder['limit']:
#             stakeholder_assignments[name] += 1
#             next_index = (index + 1) % num_stakeholders
#             return name, next_index
#     logger.debug("No stakeholder has remaining capacity for assignment.")
#     return None, current_index

# # --- Authentication ---
# def authenticate_google_sheets():
#     """Authenticates using Streamlit secrets or local service account file."""
#     creds = None
#     # Check if running in Streamlit Cloud (or secrets are configured)
#     try:
#         if 'GOOGLE_CREDENTIALS' in st.secrets:
#             logger.info("Loading credentials from Streamlit secrets...")
#             creds_info = st.secrets["GOOGLE_CREDENTIALS"].to_dict()
#             logger.debug(f"Streamlit secrets credentials keys: {list(creds_info.keys())}")
#             creds = service_account.Credentials.from_service_account_info(
#                 creds_info, scopes=SCOPES)
#             logger.info("Credentials loaded successfully from secrets.")
#     except (KeyError, FileNotFoundError, st.errors.StreamlitAPIException) as e:
#         logger.info(f"Streamlit secrets not found or inaccessible: {e}. Falling back to local service account file...")
#     except Exception as e:
#         logger.error(f"Error parsing Streamlit secrets credentials: {e}")
#         return None

#     # Fallback to local service account file if secrets are unavailable
#     if creds is None:
#         logger.info(f"Loading service account credentials from '{SERVICE_ACCOUNT_FILE}'...")
#         try:
#             creds = service_account.Credentials.from_service_account_file(
#                 SERVICE_ACCOUNT_FILE, scopes=SCOPES)
#             logger.info("Credentials loaded successfully from local file.")
#         except FileNotFoundError:
#             logger.error(f"Error: Service account key file '{SERVICE_ACCOUNT_FILE}' not found.")
#             return None
#         except Exception as e:
#             logger.error(f"Error loading service account credentials from file: {e}")
#             return None

#     if creds is None:
#         logger.error("No valid credentials loaded. Authentication failed.")
#         return None

#     logger.info("Building Google Sheets API service...")
#     try:
#         service = build('sheets', 'v4', credentials=creds)
#         return service
#     except RefreshError as e:
#         logger.error(f"Authentication failed due to invalid credentials: {e}")
#         logger.error("Ensure the service account key is valid and not revoked. If using Streamlit secrets, verify the private_key format.")
#         return None
#     except HttpError as e:
#         logger.error(f"Google Sheets API Error during service build: {e}")
#         logger.error("Ensure the service account has Editor access to the spreadsheet.")
#         return None
#     except Exception as e:
#         logger.error(f"Unexpected error during service build: {e}")
#         return None

# # --- Find Existing Report Range ---
# def find_existing_report_range(sheet, spreadsheet_id, report_sheet_name, today_date_str):
#     """Searches the report sheet for today's report section."""
#     start_title = f"--- Stakeholder Report for Assignments on {today_date_str} ---"
#     any_report_start_pattern = "--- Stakeholder Report for Assignments on "

#     logger.info(f"Searching for existing report section for {today_date_str} in '{report_sheet_name}'...")
#     start_row = None
#     next_start_row = None
#     last_row_in_sheet = 0

#     try:
#         result = sheet.values().get(
#             spreadsheetId=spreadsheet_id,
#             range=f'{report_sheet_name}!A:A'
#         ).execute()
#         values = result.get('values', [])
#         last_row_in_sheet = len(values)
#         logger.debug(f"Read {last_row_in_sheet} rows from column A of '{report_sheet_name}'.")

#         for i in range(last_row_in_sheet):
#             row_value = values[i][0].strip() if values[i] and values[i][0] else ''
#             if row_value == start_title:
#                 start_row = i + 1
#                 logger.info(f"Found existing report start for {today_date_str} at row {start_row}.")
#                 break

#         if start_row is None:
#             logger.info(f"No existing report found for {today_date_str}.")
#             return None, None

#         for i in range(start_row, last_row_in_sheet + 1):
#             if i > last_row_in_sheet:
#                 break
#             row_value = values[i-1][0].strip() if values[i-1] and values[i-1][0] else ''
#             if row_value.startswith(any_report_start_pattern) and i > start_row:
#                 next_start_row = i
#                 logger.debug(f"Found start of next report section at row {next_start_row}.")
#                 break

#         end_row_to_clear = next_start_row - 1 if next_start_row else last_row_in_sheet
#         end_row_to_clear = max(start_row, end_row_to_clear)
#         return start_row, end_row_to_clear

#     except HttpError as e:
#         if 'Unable to parse range' in str(e) or e.resp.status == 400:
#             logger.warning(f"Sheet '{report_sheet_name}' not found. It will be created on write.")
#             return None, None
#         else:
#             logger.error(f"Google Sheets API Error while searching for existing report: {e}")
#             raise
#     except Exception as e:
#         logger.exception(f"Unexpected error while searching for existing report:")
#         return None, None

# # --- Process Abandoned Orders Sheet ---
# def distribute_abandoned_orders(service, stakeholder_list, stakeholder_assignments, abandoned_spreadsheet_id, abandoned_sheet_name):
#     """Processes abandoned orders (blank, Didn't Pickup, Follow Up) with limits and returns report counts."""
#     logger.info("--- Starting Abandoned Orders Processing ---")
#     sheet = service.spreadsheets()
#     today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y")

#     # Initialize report counts for abandoned orders
#     abandoned_report_counts = {stakeholder['name']: {"Total": 0, "Abandoned": 0} for stakeholder in stakeholder_list}

#     try:
#         # Read data
#         logger.info(f"Reading data from abandoned sheet '{abandoned_sheet_name}'...")
#         read_range = f'{abandoned_sheet_name}!A:BH'  # Keep slightly wider range
#         result = sheet.values().get(spreadsheetId=abandoned_spreadsheet_id, range=read_range).execute()
#         values = result.get('values', [])

#         if not values:
#             logger.warning(f"No data found in abandoned sheet '{abandoned_sheet_name}'.")
#             return abandoned_report_counts

#         logger.info(f"Successfully read {len(values)} rows from abandoned sheet.")

#         # Validate header row
#         if ABANDONED_HEADER_ROW_INDEX >= len(values):
#             logger.error(f"Abandoned sheet header index ({ABANDONED_HEADER_ROW_INDEX}) is out of bounds (total rows: {len(values)}).")
#             return abandoned_report_counts

#         abandoned_header = [str(h).strip() if h is not None else '' for h in values[ABANDONED_HEADER_ROW_INDEX]]
#         abandoned_header_length = len(abandoned_header)
#         logger.info(f"Abandoned sheet header row (row {ABANDONED_HEADER_ROW_INDEX + 1}) with {abandoned_header_length} columns identified.")

#         # Pad data rows
#         data_rows_raw = values[ABANDONED_DATA_START_ROW_INDEX:]
#         padded_data_rows = []
#         for i, row in enumerate(data_rows_raw):
#             processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
#             if len(processed_row) < abandoned_header_length:
#                 processed_row.extend([''] * (abandoned_header_length - len(processed_row)))
#             elif len(processed_row) > abandoned_header_length:
#                 logger.warning(f"Abandoned sheet row {ABANDONED_DATA_START_ROW_INDEX + i + 1} has more columns ({len(processed_row)}) than header ({abandoned_header_length}). Truncating.")
#                 processed_row = processed_row[:abandoned_header_length]
#             padded_data_rows.append(processed_row)

#         logger.info(f"Processed {len(padded_data_rows)} abandoned data rows.")

#         # Create DataFrame
#         abandoned_df = pd.DataFrame(padded_data_rows, columns=abandoned_header)
#         abandoned_df['_original_row_index'] = range(ABANDONED_DATA_START_ROW_INDEX + 1, ABANDONED_DATA_START_ROW_INDEX + 1 + len(abandoned_df))
#         logger.info(f"Created pandas DataFrame for abandoned data with {len(abandoned_df)} rows and {len(abandoned_df.columns)} columns.")

#         # Ensure required columns
#         cols_needed = [
#             COL_NAMES_ABANDONED['calling_status'],
#             COL_NAMES_ABANDONED['stakeholder'],
#             COL_NAMES_ABANDONED['date_col_1'],
#             COL_NAMES_ABANDONED['date_col_2'],
#             COL_NAMES_ABANDONED['date_col_3']
#         ]
#         for col_name in cols_needed:
#             if col_name not in abandoned_df.columns:
#                 logger.warning(f"Column '{col_name}' not found in abandoned DataFrame. Adding it as empty.")
#                 abandoned_df[col_name] = ''
#             abandoned_df[col_name] = abandoned_df[col_name].astype(str)  # Keep as string initially

#         # Clean calling status
#         abandoned_df[COL_NAMES_ABANDONED['calling_status']] = abandoned_df[COL_NAMES_ABANDONED['calling_status']].fillna('').astype(str).str.strip()

#         # Filter rows where Call Status is blank, "Didn't Pickup", or "Follow Up"
#         statuses_to_process = ['', "Didn't Pickup", "Follow Up"]
#         logger.info(f"Filtering abandoned rows with Call Status in {statuses_to_process}...")
#         abandoned_to_process_df = abandoned_df[abandoned_df[COL_NAMES_ABANDONED['calling_status']].isin(statuses_to_process)].copy()
#         abandoned_filtered_indices = abandoned_to_process_df.index.tolist()

#         logger.info(f"Found {len(abandoned_filtered_indices)} abandoned rows matching criteria: {statuses_to_process}.")

#         if not abandoned_filtered_indices:
#             logger.info("No abandoned rows matched filter criteria for assignment/reassignment. Skipping.")
#             return abandoned_report_counts

#         # Assign stakeholders with limits and apply date logic
#         logger.info(f"Assigning/Reassigning stakeholders to {len(abandoned_filtered_indices)} abandoned rows with limits...")
#         current_index = 0
#         assigned_count = 0
#         for df_index in abandoned_filtered_indices:
#             assigned_stakeholder, current_index = assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments)

#             if assigned_stakeholder is None:
#                 logger.debug(f"Abandoned row {abandoned_df.loc[df_index, '_original_row_index']} not assigned/reassigned: all stakeholders at capacity.")
#                 continue  # Skip to next row if no stakeholder available

#             assigned_count += 1
#             row_data = abandoned_df.loc[df_index]
#             original_sheet_row = row_data['_original_row_index']
#             call_status = row_data.get(COL_NAMES_ABANDONED['calling_status'], '').strip()
#             # Get original date values *before* potential updates
#             original_date1_val = str(row_data.get(COL_NAMES_ABANDONED['date_col_1'], '')).strip()
#             original_date2_val = str(row_data.get(COL_NAMES_ABANDONED['date_col_2'], '')).strip()
#             original_date3_val = str(row_data.get(COL_NAMES_ABANDONED['date_col_3'], '')).strip()

#             # --- Assign Stakeholder and Update Report Counts ---
#             abandoned_df.loc[df_index, COL_NAMES_ABANDONED['stakeholder']] = assigned_stakeholder
#             abandoned_report_counts[assigned_stakeholder]["Total"] += 1
#             abandoned_report_counts[assigned_stakeholder]["Abandoned"] += 1  # All processed count as Abandoned

#             # --- Date Logic ---
#             if call_status == '':
#                 # For blank status, ALWAYS update Date 1 to today's date.
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = today_date_str_for_sheet
#                 # Clear Date 2 and Date 3 just in case they had spurious data for a blank status row
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = ''
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = ''

#                 # Log appropriately based on whether Date 1 was overwritten
#                 if not original_date1_val:
#                     logger.debug(f"Abandoned Row {original_sheet_row} (Blank Status): Assigned to {assigned_stakeholder}, set Date 1 to {today_date_str_for_sheet}.")
#                 else:
#                     logger.debug(f"Abandoned Row {original_sheet_row} (Blank Status, existing Date 1 '{original_date1_val}'): Assigned to {assigned_stakeholder}, **overwrote** Date 1 with {today_date_str_for_sheet}.")

#             elif call_status in ["Didn't Pickup", "Follow Up"]:
#                 # This is a reassignment for follow-up (multi-date logic)
#                 if not original_date1_val:
#                     # If Date 1 is somehow blank, fill it first (unlikely flow)
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = today_date_str_for_sheet
#                     # Clear Date 2/3 if we are filling Date 1 here
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = ''
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = ''
#                     logger.warning(f"Abandoned Row {original_sheet_row} ('{call_status}') had no Date 1. Set Date 1 to {today_date_str_for_sheet}. Assigned to {assigned_stakeholder}.")
#                 elif not original_date2_val:
#                     # Date 1 exists, fill Date 2 if empty
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = today_date_str_for_sheet
#                     # Keep Date 1 as is, clear Date 3
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = ''
#                     logger.debug(f"Abandoned Row {original_sheet_row} ('{call_status}', 2nd attempt): Reassigned to {assigned_stakeholder}, set Date 2 to {today_date_str_for_sheet}.")
#                 elif not original_date3_val:
#                     # Date 1 and 2 exist, fill Date 3 if empty
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = today_date_str_for_sheet
#                     # Keep Date 1 and 2 as is
#                     logger.debug(f"Abandoned Row {original_sheet_row} ('{call_status}', 3rd attempt): Reassigned to {assigned_stakeholder}, set Date 3 to {today_date_str_for_sheet}.")
#                 else:
#                     # All 3 dates already filled
#                     logger.debug(f"Abandoned Row {original_sheet_row} ('{call_status}'): Already has 3 dates filled. Reassigned to {assigned_stakeholder}, but no date column updated.")
#                     # Ensure DataFrame reflects original dates if no update happened
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = original_date1_val
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = original_date2_val
#                     abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = original_date3_val

#             else:
#                 logger.error(f"Abandoned Row {original_sheet_row} has unexpected status '{call_status}' after filtering. Assigned to {assigned_stakeholder} but date logic skipped.")
#                 # Ensure DataFrame reflects original dates if skipped
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = original_date1_val
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = original_date2_val
#                 abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = original_date3_val

#         logger.info(f"Stakeholders assigned/reassigned to {assigned_count} abandoned rows.")

#         # Prepare batch update
#         logger.info("Preparing batch update for Abandoned sheet...")
#         abandoned_updates = []
#         cols_to_update_names_abandoned = [
#             COL_NAMES_ABANDONED['stakeholder'],
#             COL_NAMES_ABANDONED['date_col_1'],
#             COL_NAMES_ABANDONED['date_col_2'],
#             COL_NAMES_ABANDONED['date_col_3']
#         ]
#         sheet_col_indices_abandoned = {}
#         max_col_index_to_write_abandoned = -1

#         for col_name in cols_to_update_names_abandoned:
#             try:
#                 col_index = abandoned_header.index(col_name)
#                 sheet_col_indices_abandoned[col_name] = col_index
#                 max_col_index_to_write_abandoned = max(max_col_index_to_write_abandoned, col_index)
#                 logger.debug(f"Found column '{col_name}' at index {col_index} in abandoned sheet header.")
#             except ValueError:
#                 logger.warning(f"Column '{col_name}' not found in abandoned sheet header. Cannot write to this column.")
#                 sheet_col_indices_abandoned[col_name] = -1

#         if max_col_index_to_write_abandoned != -1:
#             assigned_indices = [idx for idx in abandoned_filtered_indices if not pd.isna(abandoned_df.loc[idx, COL_NAMES_ABANDONED['stakeholder']]) and abandoned_df.loc[idx, COL_NAMES_ABANDONED['stakeholder']] != '']

#             for df_index in assigned_indices:
#                 original_sheet_row = abandoned_df.loc[df_index, '_original_row_index']
#                 row_values_to_write = [None] * (max_col_index_to_write_abandoned + 1)

#                 for col_name in cols_to_update_names_abandoned:
#                     col_idx = sheet_col_indices_abandoned.get(col_name, -1)
#                     if col_idx != -1:
#                         value_to_write = abandoned_df.loc[df_index, col_name]
#                         # Write blank string for empty/None values to clear cells if needed
#                         row_values_to_write[col_idx] = value_to_write if pd.notna(value_to_write) else ''

#                 if any(val is not None for val in row_values_to_write):
#                     abandoned_updates.append({
#                         'range': f'{abandoned_sheet_name}!A{original_sheet_row}:{col_index_to_a1(max_col_index_to_write_abandoned)}{original_sheet_row}',
#                         'values': [row_values_to_write]
#                     })

#             logger.info(f"Prepared {len(abandoned_updates)} row updates for Abandoned sheet batch write.")
#         else:
#             logger.warning("None of the target update columns (Stakeholder, Date 1/2/3) were found in the abandoned sheet header. No updates prepared.")

#         # Execute batch update
#         if abandoned_updates:
#             logger.info("Executing batch update to Abandoned sheet...")
#             body = {'value_input_option': 'RAW', 'data': abandoned_updates}
#             try:
#                 result = sheet.values().batchUpdate(
#                     spreadsheetId=abandoned_spreadsheet_id, body=body).execute()
#                 logger.info(f"Abandoned sheet batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
#             except HttpError as e:
#                 logger.error(f"API Error during abandoned sheet batch update: {e}")
#             except Exception as e:
#                 logger.exception("Unexpected error during abandoned sheet batch update:")
#         else:
#             logger.info("No updates to write back to Abandoned sheet.")

#     except HttpError as err:
#         logger.error(f"Google Sheets API Error during abandoned sheet processing: {err}")
#     except Exception as e:
#         logger.exception("Unexpected error during abandoned sheet processing:")

#     logger.info("--- Finished Abandoned Orders Processing ---")
#     return abandoned_report_counts

# # --- Main Processing Function ---
# def distribute_and_report():
#     logger.info("Starting script.")

#     settings = load_settings(SETTINGS_FILE)
#     if not settings or 'stakeholders' not in settings:
#         logger.error("Failed to load settings. Aborting.")
#         return

#     # Extract configuration
#     ORDERS_SPREADSHEET_ID = settings['sheets']['orders_spreadsheet_id']
#     ABANDONED_SPREADSHEET_ID = settings['sheets']['abandoned_spreadsheet_id']
#     REPORT_SHEET_NAME = settings['sheets']['report_sheet_name']
#     stakeholder_list = settings['stakeholders']
    
#     if not stakeholder_list:
#         logger.error("Stakeholder list is empty. Aborting.")
#         return
#     logger.info(f"Loaded {len(stakeholder_list)} stakeholders: {[s['name'] for s in stakeholder_list]}")
    
#     # Initialize assignment tracking
#     stakeholder_assignments = {stakeholder['name']: 0 for stakeholder in stakeholder_list}
#     stakeholder_names = [stakeholder['name'] for stakeholder in stakeholder_list]

#     service = authenticate_google_sheets()
#     if not service:
#         logger.error("Authentication failed. Aborting script.")
#         return
#     sheet = service.spreadsheets()

#     # Initialize combined report counts
#     combined_report_counts = {
#         name: {"Total": 0, "Fresh": 0, "Abandoned": 0, "Invalid/Fake": 0, "CNP": 0, "Follow up": 0, "NDR": 0}
#         for name in stakeholder_names
#     }

#     # --- Process Main Orders Sheet ---
#     logger.info("--- Starting Main Orders Processing ---")
#     today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y")
#     today_date_str_for_report = datetime.date.today().strftime("%d-%b-%Y")
    
#     orders_report_counts = {
#         name: {"Total": 0, "Fresh": 0, "Abandoned": 0, "Invalid/Fake": 0, "CNP": 0, "Follow up": 0, "NDR": 0}
#         for name in stakeholder_names
#     }

#     try:
#         # Read data
#         logger.info(f"Reading data from '{ORDERS_SHEET_NAME}'...")
#         read_range = f'{ORDERS_SHEET_NAME}!A:BD'
#         result = sheet.values().get(spreadsheetId=ORDERS_SPREADSHEET_ID, range=read_range).execute()
#         values = result.get('values', [])

#         if not values:
#             logger.warning(f"No data found in '{ORDERS_SHEET_NAME}'.")
#         else:
#             logger.info(f"Read {len(values)} rows from '{ORDERS_SHEET_NAME}'.")

#             if len(values) < ORDERS_DATA_START_ROW_INDEX + 1:
#                 logger.error(f"Not enough rows in '{ORDERS_SHEET_NAME}'. Need at least {ORDERS_DATA_START_ROW_INDEX + 1} rows.")
#             elif ORDERS_HEADER_ROW_INDEX >= len(values):
#                 logger.error(f"Orders sheet header index ({ORDERS_HEADER_ROW_INDEX}) is out of bounds (total rows: {len(values)}).")
#             else:
#                 header = [str(h).strip() if h is not None else '' for h in values[ORDERS_HEADER_ROW_INDEX]]
#                 header_length = len(header)
#                 logger.info(f"Orders sheet header row (row {ORDERS_HEADER_ROW_INDEX + 1}) with {header_length} columns identified.")

#                 # Pad data rows
#                 data_rows_raw = values[ORDERS_DATA_START_ROW_INDEX:]
#                 padded_data_rows = []
#                 for i, row in enumerate(data_rows_raw):
#                     processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
#                     if len(processed_row) < header_length:
#                         processed_row.extend([''] * (header_length - len(processed_row)))
#                     elif len(processed_row) > header_length:
#                         logger.warning(f"Orders Row {ORDERS_DATA_START_ROW_INDEX + i + 1} has more columns ({len(processed_row)}) than header ({header_length}). Truncating.")
#                         processed_row = processed_row[:header_length]
#                     padded_data_rows.append(processed_row)

#                 logger.info(f"Processed {len(padded_data_rows)} Orders data rows.")

#                 # Create DataFrame
#                 df = pd.DataFrame(padded_data_rows, columns=header)
#                 df['_original_row_index'] = range(ORDERS_DATA_START_ROW_INDEX + 1, ORDERS_DATA_START_ROW_INDEX + 1 + len(df))
#                 logger.info(f"Created pandas DataFrame for Orders data with {len(df)} rows and {len(df.columns)} columns.")

#                 # Ensure required columns
#                 cols_needed_orders = [
#                     COL_NAMES_ORDERS['call_status'],
#                     COL_NAMES_ORDERS['stakeholder'],
#                     COL_NAMES_ORDERS['date_col_1'],
#                     COL_NAMES_ORDERS['date_col_2'],
#                     COL_NAMES_ORDERS['date_col_3']
#                 ]
#                 for col_name in cols_needed_orders:
#                     if col_name not in df.columns:
#                         logger.warning(f"Column '{col_name}' not found in Orders DataFrame. Adding it as empty.")
#                         df[col_name] = ''
#                     df[col_name] = df[col_name].astype(str)

#                 # Clean status column
#                 df[COL_NAMES_ORDERS['call_status']] = df[COL_NAMES_ORDERS['call_status']].fillna('').astype(str).str.strip()

#                 # Filter rows for processing
#                 logger.info("Filtering Orders rows based on priority statuses...")
#                 all_priority_statuses = [status for priority_list in CALL_PRIORITIES.values() for status in priority_list]
#                 orders_to_process_df = df[df[COL_NAMES_ORDERS['call_status']].isin(all_priority_statuses)].copy()
#                 orders_filtered_indices = orders_to_process_df.index.tolist()

#                 logger.info(f"Found {len(orders_filtered_indices)} Orders rows matching priority statuses.")

#                 # Assign stakeholders and dates
#                 if orders_filtered_indices:
#                     logger.info(f"Assigning stakeholders to {len(orders_filtered_indices)} Orders rows with limits...")
#                     current_index = 0
#                     assigned_orders_processed_count = 0
#                     for df_index in orders_filtered_indices:
#                         assigned_stakeholder, current_index = assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments)
#                         if assigned_stakeholder is None:
#                             logger.debug(f"Orders row {df.loc[df_index, '_original_row_index']} not assigned: all stakeholders at capacity.")
#                             continue
#                         row_data = df.loc[df_index]
#                         df.loc[df_index, COL_NAMES_ORDERS['stakeholder']] = assigned_stakeholder
#                         call_status = row_data.get(COL_NAMES_ORDERS['call_status'], '').strip()
#                         date1_val = str(row_data.get(COL_NAMES_ORDERS['date_col_1'], '')).strip()
#                         date2_val = str(row_data.get(COL_NAMES_ORDERS['date_col_2'], '')).strip()
#                         date3_val = str(row_data.get(COL_NAMES_ORDERS['date_col_3'], '')).strip()

#                         # Update report counts
#                         assigned_orders_processed_count += 1
#                         orders_report_counts[assigned_stakeholder]["Total"] += 1
#                         report_category = STATUS_TO_REPORT_CATEGORY.get(call_status)
#                         if report_category in orders_report_counts[assigned_stakeholder]:
#                             orders_report_counts[assigned_stakeholder][report_category] += 1
#                         else:
#                             logger.warning(f"Report category '{report_category}' for status '{call_status}' not found.")

#                         # Date logic
#                         if call_status == "Call didn't Pick":
#                             if not date1_val:
#                                 df.loc[df_index, COL_NAMES_ORDERS['date_col_1']] = today_date_str_for_sheet
#                                 logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 1st attempt. Set Date to {today_date_str_for_sheet}.")
#                             elif not date2_val:
#                                 df.loc[df_index, COL_NAMES_ORDERS['date_col_2']] = today_date_str_for_sheet
#                                 logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 2nd attempt. Set Date 2 to {today_date_str_for_sheet}.")
#                             elif not date3_val:
#                                 df.loc[df_index, COL_NAMES_ORDERS['date_col_3']] = today_date_str_for_sheet
#                                 logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 3rd attempt. Set Date 3 to {today_date_str_for_sheet}.")
#                             else:
#                                 logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 3 attempts already logged. Dates unchanged.")
#                         else:
#                             df.loc[df_index, COL_NAMES_ORDERS['date_col_1']] = today_date_str_for_sheet
#                             logger.debug(f"Orders Row {row_data['_original_row_index']}: Status '{call_status}'. Set Date to {today_date_str_for_sheet}.")

#                     logger.info(f"Date logic and report counts applied to {assigned_orders_processed_count} Orders rows.")

#                 # Prepare batch update
#                 logger.info("Preparing batch update for Orders sheet...")
#                 orders_updates = []
#                 cols_to_update_names_orders = [
#                     COL_NAMES_ORDERS['stakeholder'],
#                     COL_NAMES_ORDERS['date_col_1'],
#                     COL_NAMES_ORDERS['date_col_2'],
#                     COL_NAMES_ORDERS['date_col_3']
#                 ]
#                 sheet_col_indices_orders = {}
#                 max_col_index_to_write_orders = -1

#                 for col_name in cols_to_update_names_orders:
#                     try:
#                         col_index = header.index(col_name)
#                         sheet_col_indices_orders[col_name] = col_index
#                         max_col_index_to_write_orders = max(max_col_index_to_write_orders, col_index)
#                         logger.debug(f"Found column '{col_name}' at index {col_index} in Orders sheet header.")
#                     except ValueError:
#                         logger.warning(f"Column '{col_name}' not found in Orders sheet header. Cannot write to this column.")
#                         sheet_col_indices_orders[col_name] = -1

#                 if max_col_index_to_write_orders != -1:
#                     for df_index in orders_filtered_indices:
#                         if df.loc[df_index, COL_NAMES_ORDERS['stakeholder']]:
#                             original_sheet_row = df.loc[df_index, '_original_row_index']
#                             row_values_to_write = [None] * (max_col_index_to_write_orders + 1)
#                             for col_name in cols_to_update_names_orders:
#                                 if sheet_col_indices_orders.get(col_name, -1) != -1:
#                                     row_values_to_write[sheet_col_indices_orders[col_name]] = df.loc[df_index, col_name]
#                             orders_updates.append({
#                                 'range': f'{ORDERS_SHEET_NAME}!A{original_sheet_row}',
#                                 'values': [row_values_to_write]
#                             })

#                     logger.info(f"Prepared {len(orders_updates)} row updates for Orders sheet batch write.")
#                 else:
#                     logger.warning("No writeable columns found in Orders header. No updates prepared.")

#                 # Execute batch update
#                 if orders_updates:
#                     logger.info("Executing batch update to Orders sheet...")
#                     body = {'value_input_option': 'RAW', 'data': orders_updates}
#                     try:
#                         result = sheet.values().batchUpdate(
#                             spreadsheetId=ORDERS_SPREADSHEET_ID, body=body).execute()
#                         logger.info(f"Orders sheet batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
#                     except HttpError as e:
#                         logger.error(f"API Error during Orders sheet batch update: {e}")
#                     except Exception as e:
#                         logger.exception("Unexpected error during Orders sheet batch update:")
#                 else:
#                     logger.info("No updates to write back to Orders sheet.")

#         logger.info("--- Finished Main Orders Processing ---")

#     except HttpError as err:
#         logger.error(f"Google Sheets API Error during main Orders execution: {err}")
#     except Exception as e:
#         logger.exception("Unexpected error during main Orders execution:")

#     # --- Process Abandoned Orders Sheet ---
#     abandoned_report_counts = distribute_abandoned_orders(service, stakeholder_list, stakeholder_assignments, ABANDONED_SPREADSHEET_ID, ABANDONED_SHEET_NAME)

#     # --- Combine Report Counts ---
#     logger.info("Combining report counts from Orders and Abandoned sheets...")
#     for name in stakeholder_names:
#         combined_report_counts[name]["Total"] = (
#             orders_report_counts[name]["Total"] + abandoned_report_counts[name]["Total"]
#         )
#         for category in ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]:
#             combined_report_counts[name][category] = (
#                 orders_report_counts[name].get(category, 0) +
#                 abandoned_report_counts[name].get(category, 0)
#             )
#     logger.info("Report counts combined.")

#     # --- Generate Combined Stakeholder Report ---
#     logger.info("Generating Combined Stakeholder Report...")
#     formatted_report_values = []
#     formatted_report_values.append([f"--- Stakeholder Report for Assignments on {today_date_str_for_report} ---"])
#     formatted_report_values.append([''])

#     report_category_order = ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]

#     for stakeholder in stakeholder_names:
#         formatted_report_values.append([f"Calls assigned {stakeholder}"])
#         formatted_report_values.append([f"- Total Calls This Run - {combined_report_counts[stakeholder]['Total']}"])
#         for category in report_category_order:
#             formatted_report_values.append([f"- {category} - {combined_report_counts[stakeholder][category]}"])
#         formatted_report_values.append([''])

#     formatted_report_values.append(['--- End of Report for ' + today_date_str_for_report + ' ---'])
#     logger.info(f"Formatted combined report data ({len(formatted_report_values)} rows).")

#     # --- Write Report ---
#     logger.info(f"Writing report to '{REPORT_SHEET_NAME}'...")
#     start_row_existing, end_row_existing = find_existing_report_range(
#         sheet, ORDERS_SPREADSHEET_ID, REPORT_SHEET_NAME, today_date_str_for_report
#     )

#     if start_row_existing is not None and end_row_existing is not None:
#         logger.info(f"Existing report for {today_date_str_for_report} found. Updating range...")
#         range_to_clear = f'{REPORT_SHEET_NAME}!A{start_row_existing}:Z{end_row_existing}'
#         range_to_write_new = f'{REPORT_SHEET_NAME}!A{start_row_existing}'
#         try:
#             logger.info(f"Clearing range: {range_to_clear}")
#             sheet.values().clear(spreadsheetId=ORDERS_SPREADSHEET_ID, range=range_to_clear).execute()
#             logger.info("Cleared old report data.")
#             logger.info(f"Writing new report data to range: {range_to_write_new}")
#             body = {'values': formatted_report_values}
#             result = sheet.values().update(
#                 spreadsheetId=ORDERS_SPREADSHEET_ID, range=range_to_write_new,
#                 valueInputOption='RAW', body=body).execute()
#             logger.info(f"Report updated. {result.get('updatedCells', 'N/A')} cells updated.")
#         except HttpError as e:
#             logger.error(f"API Error while updating report: {e}")
#         except Exception as e:
#             logger.exception("Unexpected error while updating report:")
#     else:
#         logger.info(f"No existing report for {today_date_str_for_report}. Appending new report...")
#         start_row_for_append = 1
#         try:
#             result_existing_report = sheet.values().get(spreadsheetId=ORDERS_SPREADSHEET_ID, range=f'{REPORT_SHEET_NAME}!A:A').execute()
#             existing_values = result_existing_report.get('values', [])
#             if existing_values:
#                 start_row_for_append = len(existing_values) + 1
#             logger.info(f"Found {len(existing_values)} existing rows. New report starts at row {start_row_for_append}.")
#         except HttpError as e:
#             if 'Unable to parse range' in str(e) or e.resp.status == 400:
#                 logger.warning(f"Sheet '{REPORT_SHEET_NAME}' not found. Creating it.")
#                 try:
#                     body = {'requests': [{'addSheet': {'properties': {'title': REPORT_SHEET_NAME}}}]}
#                     sheet.batchUpdate(spreadsheetId=ORDERS_SPREADSHEET_ID, body=body).execute()
#                     logger.info(f"Created sheet '{REPORT_SHEET_NAME}'. Report starts at row {start_row_for_append}.")
#                 except Exception as create_err:
#                     logger.error(f"Error creating sheet '{REPORT_SHEET_NAME}': {create_err}")
#                     return
#             else:
#                 logger.error(f"API Error while checking/reading sheet for append: {e}")
#                 raise
#         except Exception as e:
#             logger.exception(f"Unexpected error while finding last row:")
#             return

#         if formatted_report_values:
#             body = {'values': formatted_report_values}
#             range_to_write_report = f'{REPORT_SHEET_NAME}!A{start_row_for_append}'
#             logger.info(f"Writing report data to range '{range_to_write_report}'.")
#             try:
#                 result = sheet.values().update(
#                     spreadsheetId=ORDERS_SPREADSHEET_ID, range=range_to_write_report,
#                     valueInputOption='RAW', body=body).execute()
#                 logger.info(f"Report written. {result.get('updatedCells', 'N/A')} cells updated.")
#             except HttpError as e:
#                 logger.error(f"API Error while writing report: {e}")
#             except Exception as e:
#                 logger.exception("Unexpected error while writing report:")
#         else:
#             logger.warning("No report data to write.")

#     logger.info("Script finished execution.")

# # --- Main Execution ---
# if __name__ == '__main__':
#     distribute_and_report()