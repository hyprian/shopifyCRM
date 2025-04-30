import os.path
import datetime
import yaml
import pandas as pd
import logging
import sys
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Configuration ---
# Main Orders Sheet
SPREADSHEET_ID = '17x0SVsHU73z6cpshrTtGiXi9X1NfBXnjAUCw6Y0C8H4'
ORDERS_SHEET_NAME = 'Orders'
ORDERS_HEADER_ROW_INDEX = 1  # Orders sheet header is row 2 (0-indexed)
ORDERS_DATA_START_ROW_INDEX = 2  # Orders sheet data starts row 3 (0-indexed)

# Abandoned Orders Sheet
ABANDONED_SPREADSHEET_ID = '14ZnB0AtEzbeDidHWL1mULPnsWaN6EtPu-qhdirKb2SM'
ABANDONED_SHEET_NAME = 'Sheet1'
ABANDONED_HEADER_ROW_INDEX = 0  # Abandoned sheet header is row 1 (0-indexed)
ABANDONED_DATA_START_ROW_INDEX = 1  # Abandoned sheet data starts row 2 (0-indexed)

# Report Sheet
REPORT_SHEET_NAME = 'Stakeholder Report'

# Settings File
SETTINGS_FILE = 'settings.yaml'
SERVICE_ACCOUNT_FILE = 'carbon-pride-374002-2dc0cf329724.json'

# Scopes required for reading and writing
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

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
}

COL_NAMES_ABANDONED = {
    'calling_status': 'Call Status',
    'stakeholder': 'Stake Holder',
    'date_col_1': 'Date 1',
    'date_col_2': 'Date 2',
    'date_col_3': 'Date 3',
    'cart_id': 'cart_id',
    'phone_number': 'phone_number',
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
        logger.info(f"Settings loaded successfully from '{filename}'.")
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
    """Authenticates using a service account key file."""
    creds = None
    logger.info(f"Loading service account credentials from '{SERVICE_ACCOUNT_FILE}'...")
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        logger.info("Credentials loaded successfully.")
    except FileNotFoundError:
        logger.error(f"Error: Service account key file '{SERVICE_ACCOUNT_FILE}' not found.")
        return None
    except Exception as e:
        logger.error(f"Error loading service account credentials: {e}")
        return None

    logger.info("Building Google Sheets API service...")
    try:
        service = build('sheets', 'v4', credentials=creds)
        return service
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

# --- Process Abandoned Orders Sheet ---
def distribute_abandoned_orders(service, stakeholder_list, stakeholder_assignments):
    """Processes abandoned orders with limits and returns report counts."""
    logger.info("--- Starting Abandoned Orders Processing ---")
    sheet = service.spreadsheets()
    today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y")
    
    # Initialize report counts for abandoned orders
    abandoned_report_counts = {stakeholder['name']: {"Total": 0, "Abandoned": 0} for stakeholder in stakeholder_list}

    try:
        # Read data
        logger.info(f"Reading data from abandoned sheet '{ABANDONED_SHEET_NAME}'...")
        read_range = f'{ABANDONED_SHEET_NAME}!A:BF'
        result = sheet.values().get(spreadsheetId=ABANDONED_SPREADSHEET_ID, range=read_range).execute()
        values = result.get('values', [])

        if not values:
            logger.warning(f"No data found in abandoned sheet '{ABANDONED_SHEET_NAME}'.")
            return abandoned_report_counts

        logger.info(f"Successfully read {len(values)} rows from abandoned sheet.")

        # Validate header row
        if ABANDONED_HEADER_ROW_INDEX >= len(values):
            logger.error(f"Abandoned sheet header index ({ABANDONED_HEADER_ROW_INDEX}) is out of bounds (total rows: {len(values)}).")
            return abandoned_report_counts

        abandoned_header = [str(h).strip() if h is not None else '' for h in values[ABANDONED_HEADER_ROW_INDEX]]
        abandoned_header_length = len(abandoned_header)
        logger.info(f"Abandoned sheet header row (row {ABANDONED_HEADER_ROW_INDEX + 1}) with {abandoned_header_length} columns identified.")

        # Pad data rows
        data_rows_raw = values[ABANDONED_DATA_START_ROW_INDEX:]
        padded_data_rows = []
        for i, row in enumerate(data_rows_raw):
            processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
            if len(processed_row) < abandoned_header_length:
                processed_row.extend([''] * (abandoned_header_length - len(processed_row)))
            elif len(processed_row) > abandoned_header_length:
                logger.warning(f"Abandoned sheet row {ABANDONED_DATA_START_ROW_INDEX + i + 1} has more columns ({len(processed_row)}) than header ({abandoned_header_length}). Truncating.")
                processed_row = processed_row[:abandoned_header_length]
            padded_data_rows.append(processed_row)

        logger.info(f"Processed {len(padded_data_rows)} abandoned data rows.")

        # Create DataFrame
        abandoned_df = pd.DataFrame(padded_data_rows, columns=abandoned_header)
        abandoned_df['_original_row_index'] = range(ABANDONED_DATA_START_ROW_INDEX + 1, ABANDONED_DATA_START_ROW_INDEX + 1 + len(abandoned_df))
        logger.info(f"Created pandas DataFrame for abandoned data with {len(abandoned_df)} rows and {len(abandoned_df.columns)} columns.")

        # Ensure required columns
        cols_needed = [
            COL_NAMES_ABANDONED['calling_status'],
            COL_NAMES_ABANDONED['stakeholder'],
            COL_NAMES_ABANDONED['date_col_1']
        ]
        for col_name in cols_needed:
            if col_name not in abandoned_df.columns:
                logger.warning(f"Column '{col_name}' not found in abandoned DataFrame. Adding it as empty.")
                abandoned_df[col_name] = ''
            abandoned_df[col_name] = abandoned_df[col_name].astype(str)

        # Clean calling status
        abandoned_df[COL_NAMES_ABANDONED['calling_status']] = abandoned_df[COL_NAMES_ABANDONED['calling_status']].fillna('').astype(str).str.strip()

        # Filter rows where Call Status is blank
        logger.info("Filtering abandoned rows with blank Call Status...")
        exclude_statuses = ['Confirmed', 'Cancel', 'Whatsapp', "Didn't Pickup", 'Follow Up']
        abandoned_to_process_df = abandoned_df[abandoned_df[COL_NAMES_ABANDONED['calling_status']] == ''].copy()
        abandoned_filtered_indices = abandoned_to_process_df.index.tolist()

        logger.info(f"Found {len(abandoned_filtered_indices)} abandoned rows with blank Call Status.")

        if not abandoned_filtered_indices:
            logger.info("No abandoned rows matched filter criteria. Skipping assignments.")
            return abandoned_report_counts

        # Assign stakeholders with limits
        logger.info(f"Assigning stakeholders to {len(abandoned_filtered_indices)} abandoned rows with limits...")
        current_index = 0
        for df_index in abandoned_filtered_indices:
            assigned_stakeholder, current_index = assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments)
            if assigned_stakeholder is None:
                logger.debug(f"Abandoned row {abandoned_df.loc[df_index, '_original_row_index']} not assigned: all stakeholders at capacity.")
                continue
            row_data = abandoned_df.loc[df_index]
            abandoned_df.loc[df_index, COL_NAMES_ABANDONED['stakeholder']] = assigned_stakeholder
            abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = today_date_str_for_sheet
            abandoned_report_counts[assigned_stakeholder]["Total"] += 1
            abandoned_report_counts[assigned_stakeholder]["Abandoned"] += 1
            logger.debug(f"Abandoned Row {row_data['_original_row_index']}: Assigned to {assigned_stakeholder}, set Date 1 to {today_date_str_for_sheet}.")

        logger.info(f"Assigned stakeholders to {sum(c['Total'] for c in abandoned_report_counts.values())} abandoned rows.")

        # Prepare batch update
        logger.info("Preparing batch update for Abandoned sheet...")
        abandoned_updates = []
        cols_to_update_names_abandoned = [
            COL_NAMES_ABANDONED['stakeholder'],
            COL_NAMES_ABANDONED['date_col_1']
        ]
        sheet_col_indices_abandoned = {}
        max_col_index_to_write_abandoned = -1

        for col_name in cols_to_update_names_abandoned:
            try:
                col_index = abandoned_header.index(col_name)
                sheet_col_indices_abandoned[col_name] = col_index
                max_col_index_to_write_abandoned = max(max_col_index_to_write_abandoned, col_index)
                logger.debug(f"Found column '{col_name}' at index {col_index} in abandoned sheet header.")
            except ValueError:
                logger.warning(f"Column '{col_name}' not found in abandoned sheet header. Cannot write to this column.")
                sheet_col_indices_abandoned[col_name] = -1

        if max_col_index_to_write_abandoned != -1:
            for df_index in abandoned_filtered_indices:
                if abandoned_df.loc[df_index, COL_NAMES_ABANDONED['stakeholder']]:
                    original_sheet_row = abandoned_df.loc[df_index, '_original_row_index']
                    row_values_to_write = [None] * (max_col_index_to_write_abandoned + 1)
                    for col_name in cols_to_update_names_abandoned:
                        if sheet_col_indices_abandoned.get(col_name, -1) != -1:
                            row_values_to_write[sheet_col_indices_abandoned[col_name]] = abandoned_df.loc[df_index, col_name]
                    abandoned_updates.append({
                        'range': f'{ABANDONED_SHEET_NAME}!A{original_sheet_row}',
                        'values': [row_values_to_write]
                    })

            logger.info(f"Prepared {len(abandoned_updates)} row updates for Abandoned sheet batch write.")
        else:
            logger.warning("No writeable columns found in abandoned header. No updates prepared.")

        # Execute batch update
        if abandoned_updates:
            logger.info("Executing batch update to Abandoned sheet...")
            body = {'value_input_option': 'RAW', 'data': abandoned_updates}
            try:
                result = sheet.values().batchUpdate(
                    spreadsheetId=ABANDONED_SPREADSHEET_ID, body=body).execute()
                logger.info(f"Abandoned sheet batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
            except HttpError as e:
                logger.error(f"API Error during abandoned sheet batch update: {e}")
            except Exception as e:
                logger.exception("Unexpected error during abandoned sheet batch update:")
        else:
            logger.info("No updates to write back to Abandoned sheet.")

    except HttpError as err:
        logger.error(f"Google Sheets API Error during abandoned sheet processing: {err}")
    except Exception as e:
        logger.exception("Unexpected error during abandoned sheet processing:")

    logger.info("--- Finished Abandoned Orders Processing ---")
    return abandoned_report_counts

# --- Main Processing Function ---
def distribute_and_report():
    logger.info("Starting script.")

    settings = load_settings(SETTINGS_FILE)
    if not settings or 'stakeholders' not in settings:
        logger.error("Failed to load stakeholders. Aborting.")
        return

    stakeholder_list = settings['stakeholders']
    if not stakeholder_list:
        logger.error("Stakeholder list is empty. Aborting.")
        return
    logger.info(f"Loaded {len(stakeholder_list)} stakeholders: {[s['name'] for s in stakeholder_list]}")
    
    # Initialize assignment tracking
    stakeholder_assignments = {stakeholder['name']: 0 for stakeholder in stakeholder_list}
    stakeholder_names = [stakeholder['name'] for stakeholder in stakeholder_list]

    service = authenticate_google_sheets()
    if not service:
        logger.error("Authentication failed. Aborting script.")
        return
    sheet = service.spreadsheets()

    # Initialize combined report counts
    combined_report_counts = {
        name: {"Total": 0, "Fresh": 0, "Abandoned": 0, "Invalid/Fake": 0, "CNP": 0, "Follow up": 0, "NDR": 0}
        for name in stakeholder_names
    }

    # --- Process Main Orders Sheet ---
    logger.info("--- Starting Main Orders Processing ---")
    today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y")
    today_date_str_for_report = datetime.date.today().strftime("%d-%b-%Y")
    
    orders_report_counts = {
        name: {"Total": 0, "Fresh": 0, "Abandoned": 0, "Invalid/Fake": 0, "CNP": 0, "Follow up": 0, "NDR": 0}
        for name in stakeholder_names
    }

    try:
        # Read data
        logger.info(f"Reading data from '{ORDERS_SHEET_NAME}'...")
        read_range = f'{ORDERS_SHEET_NAME}!A:BD'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=read_range).execute()
        values = result.get('values', [])

        if not values:
            logger.warning(f"No data found in '{ORDERS_SHEET_NAME}'.")
        else:
            logger.info(f"Read {len(values)} rows from '{ORDERS_SHEET_NAME}'.")

            if len(values) < ORDERS_DATA_START_ROW_INDEX + 1:
                logger.error(f"Not enough rows in '{ORDERS_SHEET_NAME}'. Need at least {ORDERS_DATA_START_ROW_INDEX + 1} rows.")
            elif ORDERS_HEADER_ROW_INDEX >= len(values):
                logger.error(f"Orders sheet header index ({ORDERS_HEADER_ROW_INDEX}) is out of bounds (total rows: {len(values)}).")
            else:
                header = [str(h).strip() if h is not None else '' for h in values[ORDERS_HEADER_ROW_INDEX]]
                header_length = len(header)
                logger.info(f"Orders sheet header row (row {ORDERS_HEADER_ROW_INDEX + 1}) with {header_length} columns identified.")

                # Pad data rows
                data_rows_raw = values[ORDERS_DATA_START_ROW_INDEX:]
                padded_data_rows = []
                for i, row in enumerate(data_rows_raw):
                    processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
                    if len(processed_row) < header_length:
                        processed_row.extend([''] * (header_length - len(processed_row)))
                    elif len(processed_row) > header_length:
                        logger.warning(f"Orders Row {ORDERS_DATA_START_ROW_INDEX + i + 1} has more columns ({len(processed_row)}) than header ({header_length}). Truncating.")
                        processed_row = processed_row[:header_length]
                    padded_data_rows.append(processed_row)

                logger.info(f"Processed {len(padded_data_rows)} Orders data rows.")

                # Create DataFrame
                df = pd.DataFrame(padded_data_rows, columns=header)
                df['_original_row_index'] = range(ORDERS_DATA_START_ROW_INDEX + 1, ORDERS_DATA_START_ROW_INDEX + 1 + len(df))
                logger.info(f"Created pandas DataFrame for Orders data with {len(df)} rows and {len(df.columns)} columns.")

                # Ensure required columns
                cols_needed_orders = [
                    COL_NAMES_ORDERS['call_status'],
                    COL_NAMES_ORDERS['stakeholder'],
                    COL_NAMES_ORDERS['date_col_1'],
                    COL_NAMES_ORDERS['date_col_2'],
                    COL_NAMES_ORDERS['date_col_3']
                ]
                for col_name in cols_needed_orders:
                    if col_name not in df.columns:
                        logger.warning(f"Column '{col_name}' not found in Orders DataFrame. Adding it as empty.")
                        df[col_name] = ''
                    df[col_name] = df[col_name].astype(str)

                # Clean status column
                df[COL_NAMES_ORDERS['call_status']] = df[COL_NAMES_ORDERS['call_status']].fillna('').astype(str).str.strip()

                # Filter rows for processing
                logger.info("Filtering Orders rows based on priority statuses...")
                all_priority_statuses = [status for priority_list in CALL_PRIORITIES.values() for status in priority_list]
                orders_to_process_df = df[df[COL_NAMES_ORDERS['call_status']].isin(all_priority_statuses)].copy()
                orders_filtered_indices = orders_to_process_df.index.tolist()

                logger.info(f"Found {len(orders_filtered_indices)} Orders rows matching priority statuses.")

                # Assign stakeholders and dates
                if orders_filtered_indices:
                    logger.info(f"Assigning stakeholders to {len(orders_filtered_indices)} Orders rows with limits...")
                    current_index = 0
                    assigned_orders_processed_count = 0
                    for df_index in orders_filtered_indices:
                        assigned_stakeholder, current_index = assign_stakeholder_with_limits(current_index, stakeholder_list, stakeholder_assignments)
                        if assigned_stakeholder is None:
                            logger.debug(f"Orders row {df.loc[df_index, '_original_row_index']} not assigned: all stakeholders at capacity.")
                            continue
                        row_data = df.loc[df_index]
                        df.loc[df_index, COL_NAMES_ORDERS['stakeholder']] = assigned_stakeholder
                        call_status = row_data.get(COL_NAMES_ORDERS['call_status'], '').strip()
                        date1_val = str(row_data.get(COL_NAMES_ORDERS['date_col_1'], '')).strip()
                        date2_val = str(row_data.get(COL_NAMES_ORDERS['date_col_2'], '')).strip()
                        date3_val = str(row_data.get(COL_NAMES_ORDERS['date_col_3'], '')).strip()

                        # Update report counts
                        assigned_orders_processed_count += 1
                        orders_report_counts[assigned_stakeholder]["Total"] += 1
                        report_category = STATUS_TO_REPORT_CATEGORY.get(call_status)
                        if report_category in orders_report_counts[assigned_stakeholder]:
                            orders_report_counts[assigned_stakeholder][report_category] += 1
                        else:
                            logger.warning(f"Report category '{report_category}' for status '{call_status}' not found.")

                        # Date logic
                        if call_status == "Call didn't Pick":
                            if not date1_val:
                                df.loc[df_index, COL_NAMES_ORDERS['date_col_1']] = today_date_str_for_sheet
                                logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 1st attempt. Set Date to {today_date_str_for_sheet}.")
                            elif not date2_val:
                                df.loc[df_index, COL_NAMES_ORDERS['date_col_2']] = today_date_str_for_sheet
                                logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 2nd attempt. Set Date 2 to {today_date_str_for_sheet}.")
                            elif not date3_val:
                                df.loc[df_index, COL_NAMES_ORDERS['date_col_3']] = today_date_str_for_sheet
                                logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 3rd attempt. Set Date 3 to {today_date_str_for_sheet}.")
                            else:
                                logger.debug(f"Orders Row {row_data['_original_row_index']}: CNP, 3 attempts already logged. Dates unchanged.")
                        else:
                            df.loc[df_index, COL_NAMES_ORDERS['date_col_1']] = today_date_str_for_sheet
                            logger.debug(f"Orders Row {row_data['_original_row_index']}: Status '{call_status}'. Set Date to {today_date_str_for_sheet}.")

                    logger.info(f"Date logic and report counts applied to {assigned_orders_processed_count} Orders rows.")

                # Prepare batch update
                logger.info("Preparing batch update for Orders sheet...")
                orders_updates = []
                cols_to_update_names_orders = [
                    COL_NAMES_ORDERS['stakeholder'],
                    COL_NAMES_ORDERS['date_col_1'],
                    COL_NAMES_ORDERS['date_col_2'],
                    COL_NAMES_ORDERS['date_col_3']
                ]
                sheet_col_indices_orders = {}
                max_col_index_to_write_orders = -1

                for col_name in cols_to_update_names_orders:
                    try:
                        col_index = header.index(col_name)
                        sheet_col_indices_orders[col_name] = col_index
                        max_col_index_to_write_orders = max(max_col_index_to_write_orders, col_index)
                        logger.debug(f"Found column '{col_name}' at index {col_index} in Orders sheet header.")
                    except ValueError:
                        logger.warning(f"Column '{col_name}' not found in Orders sheet header. Cannot write to this column.")
                        sheet_col_indices_orders[col_name] = -1

                if max_col_index_to_write_orders != -1:
                    for df_index in orders_filtered_indices:
                        if df.loc[df_index, COL_NAMES_ORDERS['stakeholder']]:
                            original_sheet_row = df.loc[df_index, '_original_row_index']
                            row_values_to_write = [None] * (max_col_index_to_write_orders + 1)
                            for col_name in cols_to_update_names_orders:
                                if sheet_col_indices_orders.get(col_name, -1) != -1:
                                    row_values_to_write[sheet_col_indices_orders[col_name]] = df.loc[df_index, col_name]
                            orders_updates.append({
                                'range': f'{ORDERS_SHEET_NAME}!A{original_sheet_row}',
                                'values': [row_values_to_write]
                            })

                    logger.info(f"Prepared {len(orders_updates)} row updates for Orders sheet batch write.")
                else:
                    logger.warning("No writeable columns found in Orders header. No updates prepared.")

                # Execute batch update
                if orders_updates:
                    logger.info("Executing batch update to Orders sheet...")
                    body = {'value_input_option': 'RAW', 'data': orders_updates}
                    try:
                        result = sheet.values().batchUpdate(
                            spreadsheetId=SPREADSHEET_ID, body=body).execute()
                        logger.info(f"Orders sheet batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
                    except HttpError as e:
                        logger.error(f"API Error during Orders sheet batch update: {e}")
                    except Exception as e:
                        logger.exception("Unexpected error during Orders sheet batch update:")
                else:
                    logger.info("No updates to write back to Orders sheet.")

        logger.info("--- Finished Main Orders Processing ---")

    except HttpError as err:
        logger.error(f"Google Sheets API Error during main Orders execution: {err}")
    except Exception as e:
        logger.exception("Unexpected error during main Orders execution:")

    # --- Process Abandoned Orders Sheet ---
    abandoned_report_counts = distribute_abandoned_orders(service, stakeholder_list, stakeholder_assignments)

    # --- Combine Report Counts ---
    logger.info("Combining report counts from Orders and Abandoned sheets...")
    for name in stakeholder_names:
        combined_report_counts[name]["Total"] = (
            orders_report_counts[name]["Total"] + abandoned_report_counts[name]["Total"]
        )
        for category in ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]:
            combined_report_counts[name][category] = (
                orders_report_counts[name].get(category, 0) +
                abandoned_report_counts[name].get(category, 0)
            )
    logger.info("Report counts combined.")

    # --- Generate Combined Stakeholder Report ---
    logger.info("Generating Combined Stakeholder Report...")
    formatted_report_values = []
    formatted_report_values.append([f"--- Stakeholder Report for Assignments on {today_date_str_for_report} ---"])
    formatted_report_values.append([''])

    report_category_order = ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]

    for stakeholder in stakeholder_names:
        formatted_report_values.append([f"Calls assigned {stakeholder}"])
        formatted_report_values.append([f"- Total Calls This Run - {combined_report_counts[stakeholder]['Total']}"])
        for category in report_category_order:
            formatted_report_values.append([f"- {category} - {combined_report_counts[stakeholder][category]}"])
        formatted_report_values.append([''])

    formatted_report_values.append(['--- End of Report for ' + today_date_str_for_report + ' ---'])
    logger.info(f"Formatted combined report data ({len(formatted_report_values)} rows).")

    # --- Write Report ---
    logger.info(f"Writing report to '{REPORT_SHEET_NAME}'...")
    start_row_existing, end_row_existing = find_existing_report_range(
        sheet, SPREADSHEET_ID, REPORT_SHEET_NAME, today_date_str_for_report
    )

    if start_row_existing is not None and end_row_existing is not None:
        logger.info(f"Existing report for {today_date_str_for_report} found. Updating range...")
        range_to_clear = f'{REPORT_SHEET_NAME}!A{start_row_existing}:Z{end_row_existing}'
        range_to_write_new = f'{REPORT_SHEET_NAME}!A{start_row_existing}'
        try:
            logger.info(f"Clearing range: {range_to_clear}")
            sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=range_to_clear).execute()
            logger.info("Cleared old report data.")
            logger.info(f"Writing new report data to range: {range_to_write_new}")
            body = {'values': formatted_report_values}
            result = sheet.values().update(
                spreadsheetId=SPREADSHEET_ID, range=range_to_write_new,
                valueInputOption='RAW', body=body).execute()
            logger.info(f"Report updated. {result.get('updatedCells', 'N/A')} cells updated.")
        except HttpError as e:
            logger.error(f"API Error while updating report: {e}")
        except Exception as e:
            logger.exception("Unexpected error while updating report:")
    else:
        logger.info(f"No existing report for {today_date_str_for_report}. Appending new report...")
        start_row_for_append = 1
        try:
            result_existing_report = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=f'{REPORT_SHEET_NAME}!A:A').execute()
            existing_values = result_existing_report.get('values', [])
            if existing_values:
                start_row_for_append = len(existing_values) + 1
            logger.info(f"Found {len(existing_values)} existing rows. New report starts at row {start_row_for_append}.")
        except HttpError as e:
            if 'Unable to parse range' in str(e) or e.resp.status == 400:
                logger.warning(f"Sheet '{REPORT_SHEET_NAME}' not found. Creating it.")
                try:
                    body = {'requests': [{'addSheet': {'properties': {'title': REPORT_SHEET_NAME}}}]}
                    sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
                    logger.info(f"Created sheet '{REPORT_SHEET_NAME}'. Report starts at row {start_row_for_append}.")
                except Exception as create_err:
                    logger.error(f"Error creating sheet '{REPORT_SHEET_NAME}': {create_err}")
                    return
            else:
                logger.error(f"API Error while checking/reading sheet for append: {e}")
                raise
        except Exception as e:
            logger.exception(f"Unexpected error while finding last row:")
            return

        if formatted_report_values:
            body = {'values': formatted_report_values}
            range_to_write_report = f'{REPORT_SHEET_NAME}!A{start_row_for_append}'
            logger.info(f"Writing report data to range '{range_to_write_report}'.")
            try:
                result = sheet.values().update(
                    spreadsheetId=SPREADSHEET_ID, range=range_to_write_report,
                    valueInputOption='RAW', body=body).execute()
                logger.info(f"Report written. {result.get('updatedCells', 'N/A')} cells updated.")
            except HttpError as e:
                logger.error(f"API Error while writing report: {e}")
            except Exception as e:
                logger.exception("Unexpected error while writing report:")
        else:
            logger.warning("No report data to write.")

    logger.info("Script finished execution.")

# --- Main Execution ---
if __name__ == '__main__':
    distribute_and_report()