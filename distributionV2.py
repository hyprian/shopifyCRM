import os.path
import datetime
import yaml
import pandas as pd
import logging
import sys

# Import necessary modules for Service Account authentication
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Configuration ---
# Main Orders Sheet
SPREADSHEET_ID = '1ZkTB3ahmrQ2-7rz-h1RdkOMPSHmkAhpFJIH64Ca0jxk'
ORDERS_SHEET_NAME = 'Orders'
ORDERS_HEADER_ROW_INDEX = 1 # Orders sheet header is row 2 (0-indexed)
ORDERS_DATA_START_ROW_INDEX = 2 # Orders sheet data starts row 3 (0-indexed)


# Abandoned Orders Sheet
ABANDONED_SPREADSHEET_ID = '1U9R5UM_9Uom48cAhL9j3LMGd5I4o4v5sseGdOCkBDnA'
ABANDONED_SHEET_NAME = 'Sheet1' # <-- **IMPORTANT**: Change this to the actual sheet name for abandoned orders!
ABANDONED_HEADER_ROW_INDEX = 0 # Abandoned sheet header is row 1 (0-indexed)
ABANDONED_DATA_START_ROW_INDEX = 1 # Abandoned sheet data starts row 2 (0-indexed)


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

# MODIFIED: Split 'Abandoned' and 'Number invalid/fake order' for reporting
STATUS_TO_REPORT_CATEGORY = {
    "Fresh": "Fresh",
    "Confirmation Pending": "Fresh",
    "Abandoned": "Abandoned",       # Keep Abandoned as a separate category
    "Number invalid/fake order": "Invalid/Fake", # New category name for reporting
    "Call didn't Pick": "CNP",
    "Follow up": "Follow up",
    "NDR": "NDR"
}

# Column Names for BOTH sheets (mapped)
# Note: These are used to map logical names to potentially different sheet column names
COL_NAMES_ORDERS = {
    'call_status': 'Call-status',
    'order_status': 'order status',
    'stakeholder': 'Stakeholder',
    'date_col_1': 'Date',      # Date 1 in Orders sheet context (named 'Date')
    'date_col_2': 'Date 2',
    'date_col_3': 'Date 3',
    'id': 'Id',
    # Add other columns from Orders header here if needed for checks or data processing
    'name': 'Name',
    'created_at': 'Created At',
    'customer_id': 'Id (Customer)',
}

COL_NAMES_ABANDONED = {
    'calling_status': 'Calling Status', # Use Calling Status for Abandoned
    'stakeholder': 'Stake Holder',      # Use Stake Holder for Abandoned (different name)
    'date_col_1': 'Date 1',             # Date 1 in Abandoned sheet context (named 'Date 1')
    'date_col_2': 'Date 2',
    'date_col_3': 'Date 3',
    # Add other columns from Abandoned header here if needed for checks or data processing
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

# --- Helper function ---
# Not strictly needed for the current batch update logic, but kept
def col_index_to_a1(index):
    col = ''
    while index >= 0:
        col = chr(index % 26 + ord('A')) + col
        index = index // 26 - 1
    return col

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

# --- Function to find an existing report range for today ---
def find_existing_report_range(sheet, spreadsheet_id, report_sheet_name, today_date_str):
    """
    Searches the report sheet for a report section starting with today's date.
    Returns (start_row_1based, end_row_1based) if found, otherwise (None, None).
    The end_row is the last row of the section to be cleared/overwritten.
    """
    start_title = f"--- Stakeholder Report for Assignments on {today_date_str} ---"
    any_report_start_pattern = "--- Stakeholder Report for Assignments on "

    logger.info(f"Searching for existing report section for {today_date_str} in '{report_sheet_name}'...")

    start_row = None # 1-based index of today's report start
    next_start_row = None # Initialize to None
    last_row_in_sheet = 0

    try:
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{report_sheet_name}!A:A' # Read only column A to find markers
        ).execute()
        values = result.get('values', [])
        last_row_in_sheet = len(values)
        logger.debug(f"Read {last_row_in_sheet} rows from column A of '{report_sheet_name}'.")

        # Find the start of today's report
        for i in range(last_row_in_sheet):
            row_value = values[i][0].strip() if values[i] and values[i][0] else ''
            if row_value == start_title:
                start_row = i + 1 # 1-based index
                logger.info(f"Found existing report start for {today_date_str} at row {start_row}.")
                break # Found the start, now look for the end

        if start_row is None:
            logger.info(f"No existing report found for {today_date_str}.")
            return None, None # Not found

        # Search for the start of the *next* report section after today's report started
        # Iterate from the row *after* today's report start (start_row is 1-based)
        for i in range(start_row, last_row_in_sheet + 1): # Loop through 1-based row numbers from start_row to end of sheet
             if i > last_row_in_sheet: # Reached end of sheet
                 break
             # Get value for 0-based index (i-1)
             row_value = values[i-1][0].strip() if values[i-1] and values[i-1][0] else ''
             if row_value.startswith(any_report_start_pattern) and i > start_row:
                  next_start_row = i # 1-based index of the next start
                  logger.debug(f"Found start of next report section at row {next_start_row}.")
                  break # Found the next one, stop searching

        # Determine the end row for clearing: 1 row before the next report start, or the last row of the sheet
        if next_start_row is not None:
             # Clear from the start of today's report up to the row just before the next report starts.
             end_row_to_clear = next_start_row - 1
             logger.debug(f"Calculated clear end row based on next report start: {end_row_to_clear}")
        else:
             # Clear from the start of today's report to the very last row that was read (which is the last row with data in col A)
             end_row_to_clear = last_row_in_sheet
             logger.debug(f"Calculated clear end row based on end of sheet: {end_row_to_clear}")

        # Ensure end_row_to_clear is not less than start_row (handles edge case if markers are right next to each other or sheet ends immediately)
        end_row_to_clear = max(start_row, end_row_to_clear)

        return start_row, end_row_to_clear

    except HttpError as e:
        if 'Unable to parse range' in str(e) or e.resp.status == 400:
            logger.warning(f"Sheet '{report_sheet_name}' not found when searching for existing report. It will be created on write.")
            return None, None # Sheet doesn't exist, no existing report
        else:
            logger.error(f"Google Sheets API Error while searching for existing report: {e}")
            raise # Re-raise other errors
    except Exception as e:
        logger.exception(f"Unexpected error while searching for existing report:")
        return None, None # Treat unexpected errors as "not found" for robustness


# --- Process Abandoned Orders Sheet ---
def distribute_abandoned_orders(service, stakeholder_list, num_stakeholders):
    """Reads abandoned orders, filters, assigns stakeholders and dates.
       Returns report counts specific to abandoned orders processing."""
    logger.info("--- Starting Abandoned Orders Processing ---")

    sheet = service.spreadsheets()
    today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y")

    # Initialize report counts for abandoned orders within this function
    abandoned_report_counts = {}
    for stakeholder in stakeholder_list:
        abandoned_report_counts[stakeholder] = {
            "Total": 0, # Total processed from this sheet
            "Abandoned": 0, # Blank Calling Status -> Abandoned category
            "CNP": 0,       # "Didn't pick up" Calling Status -> CNP category
            # Other categories like Fresh, Invalid/Fake, Follow up, NDR are assumed
            # not to originate from this sheet's filter logic, so we don't count them here.
        }


    try:
        # --- Read Data ---
        logger.info(f"Reading data from abandoned sheet '{ABANDONED_SHEET_NAME}' (ID: {ABANDONED_SPREADSHEET_ID})...")
        # Read a range wide enough to cover all necessary columns
        # Reading from A1 to ensure header (row 1) is included
        read_range = f'{ABANDONED_SHEET_NAME}!A:BF'
        result = sheet.values().get(spreadsheetId=ABANDONED_SPREADSHEET_ID, range=read_range).execute()
        values = result.get('values', [])

        if not values:
            logger.warning(f"No data found in abandoned sheet '{ABANDONED_SHEET_NAME}'.")
            logger.info("--- Finished Abandoned Orders Processing ---")
            # Return initialized counts even if no data
            return abandoned_report_counts

        logger.info(f"Successfully read {len(values)} rows from abandoned sheet.")

        # Identify header and data rows based on Abandoned sheet structure
        abandoned_header_index = ABANDONED_HEADER_ROW_INDEX # 0
        abandoned_data_start_index = ABANDONED_DATA_START_ROW_INDEX # 1

        if len(values) < abandoned_data_start_index + 1:
             logger.error(f"Not enough rows in abandoned sheet. Need at least {abandoned_data_start_index + 1} (header + data). Found {len(values)}.")
             logger.info("--- Finished Abandoned Orders Processing ---")
             # Return initialized counts
             return abandoned_report_counts

        # Check if the specified header row index is valid
        if abandoned_header_index >= len(values):
             logger.error(f"Abandoned sheet header index ({abandoned_header_index}) is out of bounds (total rows: {len(values)}).")
             logger.info("--- Finished Abandoned Orders Processing ---")
             return abandoned_report_counts # Return initialized counts


        abandoned_header = [str(h).strip() if h is not None else '' for h in values[abandoned_header_index]]
        abandoned_header_length = len(abandoned_header)
        logger.info(f"Abandoned sheet header row (row {abandoned_header_index + 1}) with {abandoned_header_length} columns identified.")

        # --- Pad Data Rows ---
        # Data rows start from abandoned_data_start_index
        data_rows_raw = values[abandoned_data_start_index:]
        padded_data_rows = []
        # Iterate through the rows that are actual data rows
        for i, row in enumerate(data_rows_raw):
            processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
            # Pad based on header length
            if len(processed_row) < abandoned_header_length:
                processed_row.extend([''] * (abandoned_header_length - len(processed_row)))
            elif len(processed_row) > abandoned_header_length:
                 logger.warning(f"Abandoned sheet row {abandoned_data_start_index + i + 1} has more columns ({len(processed_row)}) than header ({abandoned_header_length}). Truncating.")
                 processed_row = processed_row[:abandoned_header_length]
            padded_data_rows.append(processed_row)

        logger.info(f"Processed {len(padded_data_rows)} abandoned data rows.")

        # Create DataFrame using the padded data and the extracted header
        abandoned_df = pd.DataFrame(padded_data_rows, columns=abandoned_header)
        # Store original sheet row index for writing back (1-based)
        # These are the rows starting from abandoned_data_start_index + 1 (1-based)
        abandoned_df['_original_row_index'] = range(abandoned_data_start_index + 1, abandoned_data_start_index + 1 + len(abandoned_df))
        logger.info(f"Created pandas DataFrame for abandoned data with {len(abandoned_df)} rows and {len(abandoned_df.columns)} columns.")

        # --- Prepare DataFrame Columns ---
        # Ensure required columns exist in DataFrame, adding them if necessary
        cols_needed = [
            COL_NAMES_ABANDONED['calling_status'],
            COL_NAMES_ABANDONED['stakeholder'],
            COL_NAMES_ABANDONED['date_col_1'],
            COL_NAMES_ABANDONED['date_col_2'],
            COL_NAMES_ABANDONED['date_col_3']
        ]
        for col_name in cols_needed:
            if col_name not in abandoned_df.columns:
                logger.warning(f"Column '{col_name}' not found in abandoned DataFrame after reading. Adding it as an empty column.")
                abandoned_df[col_name] = ''
            # Ensure these columns are string type
            abandoned_df[col_name] = abandoned_df[col_name].astype(str)

        # Clean up calling status column
        abandoned_df[COL_NAMES_ABANDONED['calling_status']] = abandoned_df[COL_NAMES_ABANDONED['calling_status']].fillna('').astype(str).str.strip()


        # --- Filter Rows for Processing ---
        logger.info("Filtering abandoned rows based on Calling Status (blank or 'Didn't pick up')...")
        # Filter for rows where 'Calling Status' is blank OR 'Didn't pick up'
        abandoned_to_process_df = abandoned_df[
            (abandoned_df[COL_NAMES_ABANDONED['calling_status']] == '') |
            (abandoned_df[COL_NAMES_ABANDONED['calling_status']] == "Didn't pick up")
        ].copy()

        logger.info(f"Found {len(abandoned_to_process_df)} abandoned rows matching filter criteria.")

        abandoned_filtered_indices = abandoned_to_process_df.index.tolist() # Indices in the original abandoned_df

        if not abandoned_filtered_indices:
            logger.info("No abandoned rows matched filter criteria. Skipping assignments and updates for abandoned sheet.")
            logger.info("--- Finished Abandoned Orders Processing ---")
            # Return initialized counts
            return abandoned_report_counts

        else:
            # --- Assign Stakeholder and Dates (Abandoned Logic) ---
            logger.info(f"Assigning stakeholders and dates to {len(abandoned_filtered_indices)} abandoned rows.")

            # Assign stakeholders cyclically to ALL filtered abandoned rows
            # Use the num_stakeholders passed from the main function
            assigned_stakeholders = [stakeholder_list[i % num_stakeholders] for i in range(len(abandoned_filtered_indices))]
            abandoned_df.loc[abandoned_filtered_indices, COL_NAMES_ABANDONED['stakeholder']] = assigned_stakeholders
            logger.info(f"Stakeholders assigned cyclically to {len(abandoned_filtered_indices)} abandoned rows.")

            # Handle date assignments specifically for the filtered abandoned rows
            for i, df_index in enumerate(abandoned_filtered_indices):
                row_data = abandoned_df.loc[df_index] # Get the current state from the abandoned_df
                calling_status = row_data.get(COL_NAMES_ABANDONED['calling_status'], '').strip()
                assigned_stakeholder = row_data.get(COL_NAMES_ABANDONED['stakeholder'], '') # Get the newly assigned stakeholder

                # Read existing date values as strings
                date1_val = str(row_data.get(COL_NAMES_ABANDONED['date_col_1'], '')).strip()
                date2_val = str(row_data.get(COL_NAMES_ABANDONED['date_col_2'], '')).strip()
                date3_val = str(row_data.get(COL_NAMES_ABANDONED['date_col_3'], '')).strip()

                # Apply date logic AND update report counts for this row
                if assigned_stakeholder in abandoned_report_counts: # Only count if assigned to a known stakeholder
                     abandoned_report_counts[assigned_stakeholder]["Total"] += 1 # Count this row as processed

                     if calling_status == "Didn't pick up":
                         abandoned_report_counts[assigned_stakeholder]["CNP"] += 1 # Count as CNP for report
                         if not date1_val:
                             abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = today_date_str_for_sheet
                             logger.debug(f"Abandoned Row {row_data['_original_row_index']}: CNP, 1st attempt. Set Date 1 to {today_date_str_for_sheet}.")
                         elif not date2_val:
                             abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_2']] = today_date_str_for_sheet
                             logger.debug(f"Abandoned Row {row_data['_original_row_index']}: CNP, 2nd attempt. Set Date 2 to {today_date_str_for_sheet}.")
                         elif not date3_val:
                             abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_3']] = today_date_str_for_sheet
                             logger.debug(f"Abandoned Row {row_data['_original_row_index']}: CNP, 3rd attempt. Set Date 3 to {today_date_str_for_sheet}.")
                         else:
                             logger.debug(f"Abandoned Row {row_data['_original_row_index']}: CNP, 3 attempts already logged. Dates unchanged.")

                     elif calling_status == '':
                         abandoned_report_counts[assigned_stakeholder]["Abandoned"] += 1 # Count as Abandoned for report
                         # If status is blank, assign today's date to Date 1
                         abandoned_df.loc[df_index, COL_NAMES_ABANDONED['date_col_1']] = today_date_str_for_sheet
                         logger.debug(f"Abandoned Row {row_data['_original_row_index']}: Status blank. Set Date 1 to {today_date_str_for_sheet}.")


            logger.info(f"Date logic and report counts applied to {len(abandoned_filtered_indices)} abandoned rows.")


            # --- Prepare Batch Update for Abandoned Sheet ---
            logger.info("Preparing batch update for Abandoned sheet...")
            abandoned_updates = []
            # Columns to potentially update in the abandoned sheet
            cols_to_update_names_abandoned = [
                COL_NAMES_ABANDONED['stakeholder'],
                COL_NAMES_ABANDONED['date_col_1'],
                COL_NAMES_ABANDONED['date_col_2'],
                COL_NAMES_ABANDONED['date_col_3']
            ]
            sheet_col_indices_abandoned = {}
            max_col_index_to_write_abandoned = -1

            # Find indices based on the abandoned_header derived with ABANDONED_HEADER_ROW_INDEX = 0
            for col_name in cols_to_update_names_abandoned:
                try:
                    # Use the correct header for the abandoned sheet
                    col_index = abandoned_header.index(col_name)
                    sheet_col_indices_abandoned[col_name] = col_index
                    max_col_index_to_write_abandoned = max(max_col_index_to_write_abandoned, col_index)
                    logger.debug(f"Found column '{col_name}' at index {col_index} in abandoned sheet header.")
                except ValueError:
                    logger.warning(f"Column '{col_name}' not found in the abandoned sheet header row. Cannot write to this column.")
                    sheet_col_indices_abandoned[col_name] = -1 # Mark as not found

            if max_col_index_to_write_abandoned != -1:
                # Iterate through the rows that were modified (stakeholder assigned/dates updated)
                for df_index in abandoned_filtered_indices:
                    original_sheet_row = abandoned_df.loc[df_index, '_original_row_index'] # 1-based
                    row_data = abandoned_df.loc[df_index] # Get the potentially updated data from the df

                    # Create a list for the row values, padded up to the max column index needed
                    row_values_to_write = [None] * (max_col_index_to_write_abandoned + 1)

                    # Place the values at their correct sheet column indices if the column was found
                    if sheet_col_indices_abandoned.get(COL_NAMES_ABANDONED['stakeholder'], -1) != -1:
                        row_values_to_write[sheet_col_indices_abandoned[COL_NAMES_ABANDONED['stakeholder']]] = row_data.get(COL_NAMES_ABANDONED['stakeholder'], '')

                    if sheet_col_indices_abandoned.get(COL_NAMES_ABANDONED['date_col_1'], -1) != -1:
                        row_values_to_write[sheet_col_indices_abandoned[COL_NAMES_ABANDONED['date_col_1']]] = row_data.get(COL_NAMES_ABANDONED['date_col_1'], '')

                    if sheet_col_indices_abandoned.get(COL_NAMES_ABANDONED['date_col_2'], -1) != -1:
                        row_values_to_write[sheet_col_indices_abandoned[COL_NAMES_ABANDONED['date_col_2']]] = row_data.get(COL_NAMES_ABANDONED['date_col_2'], '')

                    if sheet_col_indices_abandoned.get(COL_NAMES_ABANDONED['date_col_3'], -1) != -1:
                        row_values_to_write[sheet_col_indices_abandoned[COL_NAMES_ABANDONED['date_col_3']]] = row_data.get(COL_NAMES_ABANDONED['date_col_3'], '')


                    abandoned_updates.append({
                        'range': f'{ABANDONED_SHEET_NAME}!A{original_sheet_row}', # Write starting from A
                        'values': [row_values_to_write] # Must be a list of lists
                    })

                logger.info(f"Prepared {len(abandoned_updates)} row updates for Abandoned sheet batch write.")
            else:
                 logger.warning("No writeable columns found in abandoned header. No Abandoned sheet updates prepared.")


            # Execute batch update for Abandoned Sheet
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
    # Return the counts from this processing step
    return abandoned_report_counts


# --- Main Processing Function ---
def distribute_and_report():
    logger.info("Starting script.")

    settings = load_settings(SETTINGS_FILE)
    if not settings or 'stakeholders' not in settings:
        logger.error("Failed to load stakeholders. Aborting.")
        # Cannot even process abandoned if stakeholders aren't loaded
        return

    stakeholder_list = settings['stakeholders']
    if not stakeholder_list:
        logger.error("Stakeholder list is empty. Aborting.")
        return
    logger.info(f"Loaded {len(stakeholder_list)} stakeholders.")
    num_stakeholders = len(stakeholder_list)


    service = authenticate_google_sheets()
    if not service:
        logger.error("Authentication failed. Aborting script.")
        return
    sheet = service.spreadsheets()

    # Initialize combined report counts before processing any sheets
    combined_report_counts = {}
    for stakeholder in stakeholder_list:
         combined_report_counts[stakeholder] = {
             "Total": 0,
             "Fresh": 0,
             "Abandoned": 0,
             "Invalid/Fake": 0,
             "CNP": 0,
             "Follow up": 0,
             "NDR": 0
         }


    # --- Process Main Orders Sheet ---
    logger.info("--- Starting Main Orders Processing ---")
    today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y") # Date format for sheet writes
    today_date_str_for_report = datetime.date.today().strftime("%d-%b-%Y") # Date format for report titles

    # Initialize orders-specific report counts
    orders_report_counts = {}
    for stakeholder in stakeholder_list:
         orders_report_counts[stakeholder] = {
             "Total": 0, # This will be sum of categories from Orders processing
             "Fresh": 0,
             "Abandoned": 0,
             "Invalid/Fake": 0,
             "CNP": 0,
             "Follow up": 0,
             "NDR": 0
         }


    try:
        # --- Read Data from Orders Sheet ---
        logger.info(f"Reading data from '{ORDERS_SHEET_NAME}' (ID: {SPREADSHEET_ID})...")
        # Read a wider range to ensure Date 2 and Date 3 columns are captured if they exist beyond AZ
        read_range = f'{ORDERS_SHEET_NAME}!A:BD'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=read_range).execute()
        values = result.get('values', [])

        if not values:
            logger.warning(f"No data found in '{ORDERS_SHEET_NAME}'.")
            logger.info("--- Finished Main Orders Processing ---")
             # No Orders data processed, orders_report_counts remains initialized (all zeros)

        else:
            logger.info(f"Read {len(values)} rows from '{ORDERS_SHEET_NAME}'.")

            # Identify header and data rows based on Orders sheet structure
            orders_header_index = ORDERS_HEADER_ROW_INDEX # 1
            orders_data_start_index = ORDERS_DATA_START_ROW_INDEX # 2

            if len(values) < orders_data_start_index + 1:
                 logger.error(f"Not enough rows in '{ORDERS_SHEET_NAME}'. Need at least {orders_data_start_index + 1} (header + data). Found {len(values)}.")
                 logger.info("--- Finished Main Orders Processing ---")
                 # Not enough data, orders_report_counts remains initialized (all zeros)

            else:
                # Check if the specified header row index is valid
                if orders_header_index >= len(values):
                     logger.error(f"Orders sheet header index ({orders_header_index}) is out of bounds (total rows: {len(values)}).")
                     logger.info("--- Finished Main Orders Processing ---")
                     # orders_report_counts remains initialized (all zeros)

                else:
                    header = [str(h).strip() if h is not None else '' for h in values[orders_header_index]]
                    header_length = len(header)
                    logger.info(f"Orders sheet header row (row {orders_header_index + 1}) with {header_length} columns identified.")

                    # --- Pad Data Rows ---
                    # Data rows start from orders_data_start_index
                    data_rows_raw = values[orders_data_start_index:]
                    padded_data_rows = []
                    for i, row in enumerate(data_rows_raw):
                        processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
                        if len(processed_row) < header_length:
                            processed_row.extend([''] * (header_length - len(processed_row)))
                        elif len(processed_row) > header_length:
                             logger.warning(f"Orders Row {orders_data_start_index + i + 1} has more columns ({len(processed_row)}) than header ({header_length}). Truncating.")
                             processed_row = processed_row[:header_length]
                        padded_data_rows.append(processed_row)

                    logger.info(f"Processed {len(padded_data_rows)} Orders data rows.")

                    df = pd.DataFrame(padded_data_rows, columns=header)
                    df['_original_row_index'] = range(orders_data_start_index + 1, orders_data_start_index + 1 + len(df))
                    logger.info(f"Created pandas DataFrame for Orders data with {len(df)} rows and {len(df.columns)} columns.")

                    # --- Prepare DataFrame Columns (Orders) ---
                    # Ensure required columns exist in DataFrame, adding them if necessary
                    cols_needed_orders = [
                        COL_NAMES_ORDERS['call_status'],
                        COL_NAMES_ORDERS['stakeholder'],
                        COL_NAMES_ORDERS['date_col_1'],
                        COL_NAMES_ORDERS['date_col_2'],
                        COL_NAMES_ORDERS['date_col_3']
                    ]
                    for col_name in cols_needed_orders:
                        if col_name not in df.columns:
                            logger.warning(f"Column '{col_name}' not found in Orders DataFrame after reading. Adding it as an empty column.")
                            df[col_name] = ''
                        df[col_name] = df[col_name].astype(str)

                    # Clean up status column
                    df[COL_NAMES_ORDERS['call_status']] = df[COL_NAMES_ORDERS['call_status']].fillna('').astype(str).str.strip()


                    # --- Filter Orders Rows for Processing ---
                    logger.info("Filtering Orders rows based on priority statuses...")
                    all_priority_statuses = [status for priority_list in CALL_PRIORITIES.values() for status in priority_list]
                    orders_to_process_df = df[df[COL_NAMES_ORDERS['call_status']].isin(all_priority_statuses)].copy()

                    logger.info(f"Found {len(orders_to_process_df)} Orders rows matching priority statuses for potential assignment/date update.")

                    orders_filtered_indices = orders_to_process_df.index.tolist()

                    # --- Process Orders Assignments and Dates ---
                    if not orders_filtered_indices:
                        logger.info("No Orders rows matched filter criteria for distribution. Skipping assignments and report counting from Orders sheet.")
                        # orders_report_counts remains initialized (all zeros)

                    else:
                        logger.info(f"Processing {len(orders_filtered_indices)} Orders rows for assignments and date tracking.")

                        # Assign stakeholders cyclically to ALL filtered Orders rows
                        assigned_stakeholders = [stakeholder_list[i % num_stakeholders] for i in range(len(orders_filtered_indices))]
                        df.loc[orders_filtered_indices, COL_NAMES_ORDERS['stakeholder']] = assigned_stakeholders
                        logger.info(f"Stakeholders assigned cyclically to {len(orders_filtered_indices)} Orders rows.")

                        # Handle date assignments row by row for the filtered Orders indices and count for report
                        assigned_orders_processed_count = 0
                        for i, df_index in enumerate(orders_filtered_indices):
                            row_data = df.loc[df_index] # Get the current state from the main df
                            call_status = row_data.get(COL_NAMES_ORDERS['call_status'], '').strip()
                            assigned_stakeholder = row_data.get(COL_NAMES_ORDERS['stakeholder'], '') # Get the newly assigned stakeholder

                            # Read existing date values as strings
                            date1_val = str(row_data.get(COL_NAMES_ORDERS['date_col_1'], '')).strip()
                            date2_val = str(row_data.get(COL_NAMES_ORDERS['date_col_2'], '')).strip()
                            date3_val = str(row_data.get(COL_NAMES_ORDERS['date_col_3'], '')).strip()

                            # Apply date logic AND update report counts for this row from Orders sheet
                            if assigned_stakeholder in orders_report_counts: # Only count if assigned to a known stakeholder
                                assigned_orders_processed_count += 1
                                orders_report_counts[assigned_stakeholder]["Total"] += 1 # Count this row as processed

                                report_category = STATUS_TO_REPORT_CATEGORY.get(call_status)
                                if report_category in orders_report_counts[assigned_stakeholder]: # Check if the category exists in the report structure
                                     orders_report_counts[assigned_stakeholder][report_category] += 1
                                else:
                                     logger.warning(f"Report category '{report_category}' for status '{call_status}' not found in Orders report counts structure.")


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
                                    # Status is in prioritized list but not CNP
                                    df.loc[df_index, COL_NAMES_ORDERS['date_col_1']] = today_date_str_for_sheet
                                    # Date 2 and Date 3 are NOT cleared based on Apps Script behavior observation
                                    logger.debug(f"Orders Row {row_data['_original_row_index']}: Status '{call_status}'. Set Date to {today_date_str_for_sheet}.")

                        logger.info(f"Date logic and report counts applied to {assigned_orders_processed_count} Orders rows.")


                        # --- Prepare Batch Update for Orders Sheet ---
                        logger.info("Preparing batch update for Orders sheet...")
                        orders_updates = []
                        cols_to_update_names_orders = [COL_NAMES_ORDERS['stakeholder'], COL_NAMES_ORDERS['date_col_1'], COL_NAMES_ORDERS['date_col_2'], COL_NAMES_ORDERS['date_col_3']]
                        sheet_col_indices_orders = {}
                        max_col_index_to_write_orders = -1

                        for col_name in cols_to_update_names_orders:
                            try:
                                col_index = header.index(col_name)
                                sheet_col_indices_orders[col_name] = col_index
                                max_col_index_to_write_orders = max(max_col_index_to_write_orders, col_index)
                                logger.debug(f"Found column '{col_name}' at index {col_index} in Orders sheet header.")
                            except ValueError:
                                logger.warning(f"Column '{col_name}' not found in the Orders sheet header row. Cannot write to this column.")
                                sheet_col_indices_orders[col_name] = -1 # Mark as not found

                        if max_col_index_to_write_orders != -1:
                            for df_index in orders_filtered_indices:
                                original_sheet_row = df.loc[df_index, '_original_row_index'] # 1-based
                                row_data = df.loc[df_index] # Get the potentially updated data from the df

                                row_values_to_write = [None] * (max_col_index_to_write_orders + 1)

                                if sheet_col_indices_orders.get(COL_NAMES_ORDERS['stakeholder'], -1) != -1:
                                    row_values_to_write[sheet_col_indices_orders[COL_NAMES_ORDERS['stakeholder']]] = row_data.get(COL_NAMES_ORDERS['stakeholder'], '')

                                if sheet_col_indices_orders.get(COL_NAMES_ORDERS['date_col_1'], -1) != -1:
                                    row_values_to_write[sheet_col_indices_orders[COL_NAMES_ORDERS['date_col_1']]] = row_data.get(COL_NAMES_ORDERS['date_col_1'], '')

                                if sheet_col_indices_orders.get(COL_NAMES_ORDERS['date_col_2'], -1) != -1:
                                    row_values_to_write[sheet_col_indices_orders[COL_NAMES_ORDERS['date_col_2']]] = row_data.get(COL_NAMES_ORDERS['date_col_2'], '')

                                if sheet_col_indices_orders.get(COL_NAMES_ORDERS['date_col_3'], -1) != -1:
                                    row_values_to_write[sheet_col_indices_orders[COL_NAMES_ORDERS['date_col_3']]] = row_data.get(COL_NAMES_ORDERS['date_col_3'], '')

                                orders_updates.append({
                                    'range': f'{ORDERS_SHEET_NAME}!A{original_sheet_row}',
                                    'values': [row_values_to_write]
                                })

                            logger.info(f"Prepared {len(orders_updates)} row updates for Orders sheet batch write.")
                        else:
                             logger.warning("No writeable columns found in Orders header. No Orders sheet updates prepared.")


                        # Execute batch update for Orders Sheet
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

        # --- Finished Main Orders Processing ---
        logger.info("--- Finished Main Orders Processing ---")

    except HttpError as err:
        logger.error(f"Google Sheets API Error during main Orders execution: {err}")
        # orders_report_counts remains initialized (all zeros)
    except Exception as e:
        logger.exception("Unexpected error during main Orders execution:")
        # orders_report_counts remains initialized (all zeros)


    # --- Process Abandoned Orders Sheet AFTER Main Orders ---
    # This ensures abandoned orders are processed even if main orders has issues
    abandoned_report_counts = distribute_abandoned_orders(service, stakeholder_list, num_stakeholders) # Call and get counts

    # --- Combine Report Counts ---
    logger.info("Combining report counts from Orders and Abandoned sheets...")

    # Initialize combined report counts with data from Orders first
    # Create a *copy* to avoid modifying orders_report_counts directly
    combined_report_counts = {}
    for stakeholder in stakeholder_list:
        combined_report_counts[stakeholder] = orders_report_counts.get(stakeholder, {}).copy()
        # Ensure all categories exist even if they were missing in orders_report_counts
        for category in ["Total", "Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]:
             if category not in combined_report_counts[stakeholder]:
                  combined_report_counts[stakeholder][category] = 0


    # Add counts from abandoned orders
    for stakeholder in stakeholder_list:
        if stakeholder in abandoned_report_counts: # Check if stakeholder exists in abandoned counts
             # Add counts from specific categories processed by abandoned logic
             combined_report_counts[stakeholder]["Total"] += abandoned_report_counts[stakeholder].get("Total", 0)
             combined_report_counts[stakeholder]["Abandoned"] += abandoned_report_counts[stakeholder].get("Abandoned", 0)
             combined_report_counts[stakeholder]["CNP"] += abandoned_report_counts[stakeholder].get("CNP", 0)

    logger.info("Report counts combined.")

    # --- Generate Combined Stakeholder Report ---
    logger.info("Generating Combined Stakeholder Report...")

    formatted_report_values = []
    formatted_report_values.append([f"--- Stakeholder Report for Assignments on {today_date_str_for_report} ---"])
    formatted_report_values.append([''])

    # Define the order of categories for the report output
    report_category_order = ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]

    for stakeholder in stakeholder_list:
         # The "Total" is already calculated and stored in combined_report_counts["Total"]
         # based on summing processed rows in each sheet.

         formatted_report_values.append([f"Calls assigned {stakeholder}"])
         formatted_report_values.append([f"- Total Calls This Run - {combined_report_counts[stakeholder].get('Total', 0)}"]) # Use .get for safety

         for category in report_category_order:
             formatted_report_values.append([f"- {category}- {combined_report_counts[stakeholder].get(category, 0)}"])

         formatted_report_values.append(['']) # Blank line after each stakeholder block

    formatted_report_values.append(['--- End of Report for ' + today_date_str_for_report + ' ---'])

    logger.info(f"Formatted combined report data ({len(formatted_report_values)} rows).")

    # --- Write Report (Update or Append) ---
    logger.info(f"Writing report to '{REPORT_SHEET_NAME}'...")

    start_row_existing, end_row_existing = find_existing_report_range(
        sheet, SPREADSHEET_ID, REPORT_SHEET_NAME, today_date_str_for_report
    )

    if start_row_existing is not None and end_row_existing is not None:
        # --- Update Existing Report ---
        logger.info(f"Existing report for {today_date_str_for_report} found. Updating range {REPORT_SHEET_NAME}!A{start_row_existing}:Z{end_row_existing}...")
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
        # --- Append New Report ---
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
                  logger.warning(f"Sheet '{REPORT_SHEET_NAME}' not found when checking append position. Creating it.")
                  try:
                      body = {'requests': [{'addSheet': {'properties': {'title': REPORT_SHEET_NAME}}}]}
                      sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
                      logger.info(f"Created sheet '{REPORT_SHEET_NAME}'. Report starts at row {start_row_for_append}.")
                  except Exception as create_err:
                       logger.error(f"Error creating sheet '{REPORT_SHEET_NAME}': {create_err}")
                       logger.error("Cannot proceed with report. Aborting.")
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