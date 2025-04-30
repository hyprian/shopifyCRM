import os.path
import pandas as pd
import logging
import sys
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Configuration ---
# Orders Sheet
SPREADSHEET_ID = '17x0SVsHU73z6cpshrTtGiXi9X1NfBXnjAUCw6Y0C8H4'
ORDERS_SHEET_NAME = 'Orders'
ORDERS_HEADER_ROW_INDEX = 1  # Orders sheet header is row 2 (0-indexed)
ORDERS_DATA_START_ROW_INDEX = 2  # Orders sheet data starts row 3 (0-indexed)

# CSV File
MASTER_CSV_FILE = 'master_report_20250401_to_20250430.csv'

# Service Account File
SERVICE_ACCOUNT_FILE = 'carbon-pride-374002-2dc0cf329724.json'

# Scopes for Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Status Mapping (CSV status to Orders sheet dropdown values)
STATUS_MAPPING = {
    'DELIVERED': 'Delivered',
    'RTO': 'RTO',
    'OUT_FOR_DELIVERY': 'Out for delivery',
    'PENDING': 'Pending To Be Dispatch',
    'SHIPPED': 'In-transit'
}

# Column Names in Orders Sheet (updated to match exact header names)
COL_NAMES_ORDERS = {
    'name': 'Name',  # e.g., #1448
    'call_status': 'Call-status',
    'order_status': 'order status'  # Changed to match lowercase header in sheet
}

# --- Logging Setup ---
LOG_FILE = 'order_status_update.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

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

# --- Read and Filter Orders Sheet ---
def read_orders_sheet(service):
    """Reads the Orders sheet and filters rows with Call-status 'Confirmed' or 'Prepaid'."""
    logger.info(f"Reading data from '{ORDERS_SHEET_NAME}' (ID: {SPREADSHEET_ID})...")
    sheet = service.spreadsheets()
    read_range = f'{ORDERS_SHEET_NAME}!A:AZ'  # Wide range to ensure all columns are captured
    try:
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=read_range).execute()
        values = result.get('values', [])

        if not values:
            logger.warning(f"No data found in '{ORDERS_SHEET_NAME}'.")
            return None

        logger.info(f"Read {len(values)} rows from '{ORDERS_SHEET_NAME}'.")

        # Validate header row
        if ORDERS_HEADER_ROW_INDEX >= len(values):
            logger.error(f"Header row index ({ORDERS_HEADER_ROW_INDEX}) is out of bounds (total rows: {len(values)}).")
            return None

        header = [str(h).strip() if h is not None else '' for h in values[ORDERS_HEADER_ROW_INDEX]]
        header_length = len(header)

        # Validate data start row
        if ORDERS_DATA_START_ROW_INDEX >= len(values):
            logger.error(f"Data start row index ({ORDERS_DATA_START_ROW_INDEX}) is out of bounds (total rows: {len(values)}).")
            return None

        # Pad data rows to match header length
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

        # Ensure required columns exist
        required_cols = [COL_NAMES_ORDERS['name'], COL_NAMES_ORDERS['call_status'], COL_NAMES_ORDERS['order_status']]
        for col in required_cols:
            if col not in df.columns:
                logger.error(f"Required column '{col}' not found in Orders sheet header.")
                return None
            df[col] = df[col].astype(str).str.strip()

        # Filter rows where Call-status is 'Confirmed' or 'Prepaid'
        filtered_df = df[df[COL_NAMES_ORDERS['call_status']].isin(['Confirmed', 'Prepaid'])].copy()
        logger.info(f"Filtered {len(filtered_df)} rows with Call-status 'Confirmed' or 'Prepaid'.")

        return filtered_df

    except HttpError as e:
        logger.error(f"Google Sheets API Error while reading Orders sheet: {e}")
        return None
    except Exception as e:
        logger.exception("Unexpected error while reading Orders sheet:")
        return None

# --- Read Master CSV ---
def read_master_csv():
    """Reads the master_report CSV file."""
    logger.info(f"Reading CSV file '{MASTER_CSV_FILE}'...")
    try:
        df = pd.read_csv(MASTER_CSV_FILE, dtype=str, keep_default_na=False)
        logger.info(f"Read {len(df)} rows from '{MASTER_CSV_FILE}'.")

        # Ensure required columns exist
        required_cols = ['Order Name', 'Order Status']
        for col in required_cols:
            if col not in df.columns:
                logger.error(f"Required column '{col}' not found in CSV file.")
                return None
            df[col] = df[col].astype(str).str.strip()

        # Filter for relevant statuses
        relevant_statuses = list(STATUS_MAPPING.keys())
        filtered_df = df[df['Order Status'].isin(relevant_statuses)].copy()
        logger.info(f"Filtered {len(filtered_df)} CSV rows with relevant Order Status: {relevant_statuses}")

        return filtered_df

    except FileNotFoundError:
        logger.error(f"Error: CSV file '{MASTER_CSV_FILE}' not found.")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error while reading CSV file:")
        return None

# --- Match and Prepare Updates ---
def prepare_status_updates(orders_df, csv_df):
    """Matches Orders sheet rows with CSV rows and prepares status updates."""
    logger.info("Matching Orders sheet rows with CSV rows...")
    updates = []

    # Find the Order Status column index in Orders sheet
    order_status_col = COL_NAMES_ORDERS['order_status']
    name_col = COL_NAMES_ORDERS['name']

    # Iterate through filtered Orders rows
    for idx, orders_row in orders_df.iterrows():
        order_name = orders_row[name_col]  # e.g., #1448
        if not order_name:
            logger.debug(f"Skipping Orders row {orders_row['_original_row_index']}: Empty Name.")
            continue

        # Find matching CSV row
        matching_csv_rows = csv_df[csv_df['Order Name'] == order_name]
        if matching_csv_rows.empty:
            logger.debug(f"No CSV match found for Order Name '{order_name}' (Orders row {orders_row['_original_row_index']}).")
            continue

        # Use the first matching row (assuming Order Name is unique)
        csv_row = matching_csv_rows.iloc[0]
        csv_status = csv_row['Order Status']
        mapped_status = STATUS_MAPPING.get(csv_status)

        if not mapped_status:
            logger.debug(f"Order Name '{order_name}' (Orders row {orders_row['_original_row_index']}): CSV status '{csv_status}' not in mapping. Skipping.")
            continue

        # Check if update is needed
        current_status = orders_row[order_status_col]
        if current_status == mapped_status:
            logger.debug(f"Order Name '{order_name}' (Orders row {orders_row['_original_row_index']}): Order Status already '{mapped_status}'. No update needed.")
            continue

        # Prepare update
        original_row = int(orders_row['_original_row_index'])  # 1-based
        updates.append({
            'order_name': order_name,
            'row_index': original_row,
            'new_status': mapped_status
        })
        logger.info(f"Prepared update for Order Name '{order_name}' (row {original_row}): Set Order Status to '{mapped_status}'.")

    logger.info(f"Prepared {len(updates)} status updates.")
    return updates

# --- Execute Batch Update ---
def execute_batch_update(service, updates, orders_df):
    """Executes batch update to Orders sheet for Order Status."""
    if not updates:
        logger.info("No updates to apply to Orders sheet.")
        return

    logger.info(f"Preparing batch update for {len(updates)} rows in Orders sheet...")
    sheet = service.spreadsheets()

    # Find the Order Status column index
    try:
        header_range = f'{ORDERS_SHEET_NAME}!A{ORDERS_HEADER_ROW_INDEX + 1}:AZ{ORDERS_HEADER_ROW_INDEX + 1}'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=header_range).execute()
        header = result.get('values', [[]])[0]
        header = [str(h).strip() if h is not None else '' for h in header]
        order_status_col = COL_NAMES_ORDERS['order_status']
        try:
            status_col_index = header.index(order_status_col)
        except ValueError:
            logger.error(f"Column '{order_status_col}' not found in Orders sheet header.")
            return
    except HttpError as e:
        logger.error(f"Google Sheets API Error while reading header: {e}")
        return
    except Exception as e:
        logger.exception("Unexpected error while reading header:")
        return

    # Prepare batch update data
    batch_updates = []
    max_col_index = status_col_index
    for update in updates:
        row_index = update['row_index']  # 1-based
        new_status = update['new_status']
        row_values = [None] * (max_col_index + 1)
        row_values[status_col_index] = new_status
        batch_updates.append({
            'range': f'{ORDERS_SHEET_NAME}!A{row_index}',
            'values': [row_values]
        })

    # Execute batch update
    logger.info("Executing batch update to Orders sheet...")
    body = {'value_input_option': 'RAW', 'data': batch_updates}
    try:
        result = sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
        logger.info(f"Orders sheet batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
    except HttpError as e:
        logger.error(f"Google Sheets API Error during batch update: {e}")
    except Exception as e:
        logger.exception("Unexpected error during batch update:")

# --- Main Function ---
def update_order_status():
    """Main function to update Order Status in Orders sheet based on master CSV."""
    logger.info("Starting Order Status Update script.")

    # Authenticate
    service = authenticate_google_sheets()
    if not service:
        logger.error("Authentication failed. Aborting script.")
        return

    # Read Orders sheet
    orders_df = read_orders_sheet(service)
    if orders_df is None:
        logger.error("Failed to read Orders sheet. Aborting script.")
        return

    # Read Master CSV
    csv_df = read_master_csv()
    if csv_df is None:
        logger.error("Failed to read master CSV. Aborting script.")
        return

    # Prepare updates
    updates = prepare_status_updates(orders_df, csv_df)
    if not updates:
        logger.info("No status updates needed. Script completed.")
        return

    # Execute batch update
    execute_batch_update(service, updates, orders_df)

    logger.info("Order Status Update script finished execution.")

# --- Main Execution ---
if __name__ == '__main__':
    update_order_status()