# performance_analyzer.py
import os.path
import datetime
import yaml
import pandas as pd
import logging
import sys
import json # Though not used yet, good to have for future flexibility
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

# --- Configuration ---
SETTINGS_FILE = 'settings.yaml'
SERVICE_ACCOUNT_FILE = 'molten-medley-458604-j9-855f3bdefd90.json' # Make sure this is correct

# Scopes required for reading and writing
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Sheet-specific constants from distributionV2.py (for reading existing sheets)
ORDERS_SHEET_NAME = 'Orders' # As defined in distributionV2
ORDERS_HEADER_ROW_INDEX = 1
ORDERS_DATA_START_ROW_INDEX = 2
ABANDONED_SHEET_NAME = 'Sheet1' # As defined in distributionV2
ABANDONED_HEADER_ROW_INDEX = 0
ABANDONED_DATA_START_ROW_INDEX = 1

# New: Performance Report Sheet Name
PERFORMANCE_REPORT_SHEET_NAME = 'Performance Reports'

# Column Names (copied and potentially extended)
COL_NAMES_ORDERS = {
    'call_status': 'Call-status',
    'order_status': 'order status', # May be useful for context
    'stakeholder': 'Stakeholder',
    'date_col_1': 'Date',
    'date_col_2': 'Date 2',
    'date_col_3': 'Date 3',
    'id': 'Id', # Useful for unique identification if needed
    'initial_assignment_category': 'Initial Assignment Category'
    # Add other columns if they help determine initial vs. final state
}

COL_NAMES_ABANDONED = {
    'calling_status': 'Call status', # Note: 'Call status' vs 'Call-status'
    'stakeholder': 'Stake Holder', # Note: 'Stake Holder' vs 'Stakeholder'
    'date_col_1': 'Date 1',
    'date_col_2': 'Date 2',
    'date_col_3': 'Date 3',
    'cart_id': 'cart_id', # Useful for unique identification
    'initial_assignment_category': 'Initial Assignment Category'
}

# Report categories mapping (from distributionV2.py, for parsing assignment report)
STATUS_TO_REPORT_CATEGORY = {
    "Fresh": "Fresh",
    "Confirmation Pending": "Fresh",
    "Abandoned": "Abandoned", # This refers to 'Call-status' in Orders if used
    "Number invalid/fake order": "Invalid/Fake",
    "Call didn't Pick": "CNP",
    "Follow up": "Follow up",
    "NDR": "NDR"
}

# Initial states that trigger assignment (for identifying "actioned")
# For Orders sheet
INITIAL_ASSIGNMENT_STATUSES_ORDERS = {
    "Fresh": ["Fresh", "Confirmation Pending"],
    "NDR": ["NDR"],
    "CNP": ["Call didn't Pick"], # Assuming 'Follow up' is also a re-assignment of CNP logic
    "Follow up": ["Follow up"],
    "Invalid/Fake": ["Number invalid/fake order"]
    # "Abandoned" status is typically for the separate Abandoned sheet.
    # If you have an "Abandoned" Call-status in Orders sheet that distributionV2 assigns, add it.
}
# For Abandoned sheet
INITIAL_ASSIGNMENT_STATUSES_ABANDONED = ["", "Didn't Pickup", "Follow Up"]


# Define categories for "Assigned" part of the performance report
ASSIGNMENT_CATEGORIES_FOR_PERFORMANCE_REPORT = ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]

# Define "Final Status" categories for the performance report breakdown
# These should reflect the actual statuses stakeholders set after working on a lead
PERFORMANCE_FINAL_STATUSES_ORDERS = [
    "Confirmed", "Cancelled", "RTO", "Delivered",
    "Fake Order Verified", "Invalid Number Verified",
    "NDR Resolved", "NDR - RTO Initiated",
    "Follow-up Scheduled (Post-Contact)", "Customer Unavailable (Post-Contact)",
    "Payment Link Sent", "Order Modified", "Query Resolved"
    # Add any other *final* or *significant progress* statuses from your Orders 'Call-status' column
]
PERFORMANCE_FINAL_STATUSES_ABANDONED = [
    "Converted to Order", "Not Interested (Contacted)", "Callback Requested (Abandoned)",
    "Invalid Number (Abandoned Verified)", "No Answer (Abandoned Attempted)",
    "Wrong Number (Abandoned Verified)"
    # Add any other *final* or *significant progress* statuses from your Abandoned 'Call status' column
]


# --- Logging Setup ---
LOG_FILE = 'performance_analyzer.log' # Separate log file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- Load Settings Function (Copied from distributionV2.py) ---
def load_settings(filename):
    """Loads configuration from a YAML file."""
    logger.info(f"Loading settings from '{filename}'...")
    try:
        with open(filename, 'r') as f:
            settings = yaml.safe_load(f)
        if not settings:
            logger.warning(f"Settings file '{filename}' is empty.")
            return None

        required_fields = [
            ('sheets.orders_spreadsheet_id', str),
            ('sheets.abandoned_spreadsheet_id', str),
            ('sheets.report_sheet_name', str), # Used for reading assignment report
            ('stakeholders', list)
        ]
        for field_path, expected_type in required_fields:
            keys = field_path.split('.')
            value = settings
            for key in keys:
                value = value.get(key)
                if value is None:
                    logger.error(f"Missing or invalid '{field_path}' in settings file.")
                    return None
            if not isinstance(value, expected_type):
                logger.error(f"'{field_path}' must be a {expected_type.__name__}, got {type(value).__name__}.")
                return None

        for stakeholder in settings['stakeholders']:
            if not isinstance(stakeholder, dict) or 'name' not in stakeholder or 'limit' not in stakeholder: # limit not used here but good for consistency
                logger.error("Each stakeholder must be a dictionary with 'name' and 'limit' keys.")
                return None
            if not isinstance(stakeholder['name'], str): # limit check removed for this script's context
                logger.error(f"Invalid stakeholder: name must be string. Got name='{stakeholder.get('name')}'")
                return None
        logger.info(f"Settings loaded successfully.")
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

# --- Authentication (Copied from distributionV2.py - no Streamlit secrets here) ---
def authenticate_google_sheets():
    """Authenticates using local service account file."""
    creds = None
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
        return None
    except HttpError as e:
        logger.error(f"Google Sheets API Error during service build: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during service build: {e}")
        return None

# --- Helper Function: Column Index to A1 (Copied from distributionV2.py) ---
def col_index_to_a1(index):
    """Converts column index (0-based) to A1 notation (e.g., 0 -> A, 1 -> B)."""
    col = ''
    while index >= 0:
        col = chr(index % 26 + ord('A')) + col
        index = index // 26 - 1
    return col

# --- Helper Function: Find Existing Report Range (Adapted from distributionV2.py) ---
def find_existing_report_range(sheet, spreadsheet_id, report_sheet_name, report_title_pattern, today_date_str):
    """
    Searches the report sheet for today's report section.
    report_title_pattern should be a string that the start of the report title matches,
    e.g., "--- Stakeholder Performance Report for "
    """
    start_title_full = f"{report_title_pattern}{today_date_str} ---"
    any_report_start_pattern = report_title_pattern # To find the next report start

    logger.info(f"Searching for existing report section for {today_date_str} matching '{report_title_pattern}' in '{report_sheet_name}'...")
    start_row = None
    next_start_row = None
    last_row_in_sheet = 0

    try:
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{report_sheet_name}!A:A' # Check only column A for titles
        ).execute()
        values = result.get('values', [])
        last_row_in_sheet = len(values)
        logger.debug(f"Read {last_row_in_sheet} rows from column A of '{report_sheet_name}'.")

        for i in range(last_row_in_sheet):
            row_value = values[i][0].strip() if values[i] and values[i][0] else ''
            if row_value == start_title_full:
                start_row = i + 1 # 1-based index for sheet ranges
                logger.info(f"Found existing report start for {today_date_str} at row {start_row}.")
                # Now search for the end of this specific report
                # The end is marked by "--- End of Report for [today_date_str] ---"
                # OR the start of the next day's report, OR end of sheet
                end_title_full = f"--- End of Report for {today_date_str} ---" # Specific end marker
                for j in range(i + 1, last_row_in_sheet):
                    end_row_value = values[j][0].strip() if values[j] and values[j][0] else ''
                    if end_row_value == end_title_full:
                        next_start_row = j + 1 # The row *after* the end marker
                        logger.debug(f"Found specific end marker for {today_date_str} at row {j + 1}.")
                        break
                    elif end_row_value.startswith(any_report_start_pattern) and end_row_value != start_title_full:
                        next_start_row = j + 1 # Start of a *different* report
                        logger.debug(f"Found start of a different report section at row {j + 1}.")
                        break
                if next_start_row is None: # No specific end or next report found
                    next_start_row = last_row_in_sheet + 1 # Clear till the end of actual content
                break # Found our start_title_full

        if start_row is None:
            logger.info(f"No existing report found for {today_date_str} with title pattern '{report_title_pattern}'.")
            return None, None # Start row, End row to clear

        end_row_to_clear = next_start_row -1 if next_start_row else last_row_in_sheet
        end_row_to_clear = max(start_row, end_row_to_clear) # Ensure end_row is at least start_row
        logger.info(f"Report section for {today_date_str} identified: Rows {start_row} to {end_row_to_clear}.")
        return start_row, end_row_to_clear

    except HttpError as e:
        if 'Unable to parse range' in str(e) or (hasattr(e, 'resp') and e.resp.status == 400 and "Unable to parse range" in str(e.content)):
            logger.warning(f"Sheet '{report_sheet_name}' not found or empty. It might be created on write.")
            return None, None
        else:
            logger.error(f"Google Sheets API Error while searching for existing report: {e}")
            raise # Re-raise if it's not a "sheet not found" type error
    except Exception as e:
        logger.exception(f"Unexpected error while searching for existing report:")
        return None, None


def read_todays_assignment_report(service, spreadsheet_id, report_sheet_name, today_date_str, stakeholder_names_list, assignment_categories):
    """
    Reads the assignment report generated by distributionV2.py for a specific date.
    Parses the assigned counts for each stakeholder and category.
    """
    logger.info(f"Reading today's assignment report from '{report_sheet_name}' for date: {today_date_str}...")
    logger.debug(f"Expecting stakeholder names: {stakeholder_names_list}")
    logger.debug(f"Expecting assignment categories: {assignment_categories}")

    # Initialize with expected structure to avoid KeyErrors later if parsing fails for some
    assigned_data = {
        stakeholder_name: {category: 0 for category in assignment_categories}
        for stakeholder_name in stakeholder_names_list
    }
    assigned_data_totals = { stakeholder_name: 0 for stakeholder_name in stakeholder_names_list }

    report_start_title = f"--- Stakeholder Report for Assignments on {today_date_str} ---"
    report_end_title = f"--- End of Report for {today_date_str} ---"
    
    parsed_something_for_stakeholder = False # Flag to check if we actually update counts

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{report_sheet_name}!A:A'
        ).execute()
        values = result.get('values', [])

        if not values:
            logger.warning(f"Assignment report sheet '{report_sheet_name}' is empty or no data found.")
            return assigned_data, assigned_data_totals

        in_today_report_block = False
        current_stakeholder_parsing = None

        for i, row in enumerate(values):
            if not row: continue
            cell_value = str(row[0]).strip()
            # logger.debug(f"Row {i+1} raw value: '{row[0]}', Stripped: '{cell_value}'") # Verbose

            if cell_value == report_start_title:
                logger.info(f"Found start of today's assignment report block at row {i+1}.")
                in_today_report_block = True
                current_stakeholder_parsing = None
                parsed_something_for_stakeholder = False # Reset for this block
                continue

            if in_today_report_block:
                if cell_value == report_end_title:
                    logger.info(f"Found end of today's assignment report block at row {i+1}.")
                    in_today_report_block = False
                    break # Crucial: exit loop once end_title is found for today's report

                if cell_value.startswith("Calls assigned "):
                    name_part = cell_value.replace("Calls assigned ", "").strip()
                    if name_part in stakeholder_names_list:
                        current_stakeholder_parsing = name_part
                        logger.debug(f"Parsing assignments for stakeholder: '{current_stakeholder_parsing}' (matched from list)")
                    else:
                        logger.warning(f"Stakeholder name '{name_part}' from report NOT FOUND in configured stakeholder_names_list: {stakeholder_names_list}. Skipping this section.")
                        current_stakeholder_parsing = None # Ensure we don't attribute data to wrong stakeholder
                    continue

                if current_stakeholder_parsing: # Only proceed if we have a valid stakeholder context
                    if cell_value.startswith("- "):
                        # Example line: "- Fresh - 14" or "- Total Calls This Run - 130"
                        # Split carefully, handling potential extra spaces if any
                        line_content = cell_value[1:].strip() # Remove leading "- "
                        parts = [p.strip() for p in line_content.split('-', 1)] # Split only on the last hyphen (or first if only one number)
                        
                        if len(parts) == 2: # Expecting "Category Name" and "Count"
                            category_name_from_report = parts[0].strip()
                            count_str = parts[1].strip()
                            
                            logger.debug(f"  Parsing line for {current_stakeholder_parsing}: Category='{category_name_from_report}', CountStr='{count_str}'")

                            try:
                                count = int(count_str)
                            except ValueError:
                                logger.warning(f"  Could not parse count '{count_str}' from line: '{cell_value}' for stakeholder {current_stakeholder_parsing}")
                                continue

                            if category_name_from_report == "Total Calls This Run":
                                assigned_data_totals[current_stakeholder_parsing] = count
                                parsed_something_for_stakeholder = True
                                logger.debug(f"    -> Stored Total for {current_stakeholder_parsing}: {count}")
                            elif category_name_from_report in assignment_categories:
                                assigned_data[current_stakeholder_parsing][category_name_from_report] = count
                                parsed_something_for_stakeholder = True
                                logger.debug(f"    -> Stored Category '{category_name_from_report}' for {current_stakeholder_parsing}: {count}")
                            else:
                                logger.debug(f"    -> Category '{category_name_from_report}' not in expected assignment_categories: {assignment_categories}. Ignoring.")
                        else:
                            logger.debug(f"  Line for {current_stakeholder_parsing} did not split into 2 parts: '{cell_value}' -> Parts: {parts}")
        
        # After loop, check if we actually managed to parse and store any data
        # The `any(assigned_data_totals.values())` check was too broad if one stakeholder had data but others didn't.
        # A better check is if `parsed_something_for_stakeholder` was ever true *inside the report block*.
        # However, the `logger.info("Found start/end...")` implies the block was identified.
        # The issue is likely the parsing *within* the block.

        # Let's rely on the stakeholder totals. If all are zero, then parsing probably failed.
        if not any(assigned_data_totals.values()) and any(s_name in stakeholder_names_list for s_name in assigned_data_totals):
             # This condition means we have stakeholders, but all their totals are 0,
             # AND the report block itself was found (otherwise `in_today_report_block` logic would prevent parsing)
            logger.warning(f"Assignment report for {today_date_str} in '{report_sheet_name}' was found, but no assignment counts were successfully parsed for known stakeholders.")
        elif not stakeholder_names_list:
            logger.warning("No stakeholder names provided to parse for; cannot read assignment report meaningfully.")


    except HttpError as e:
        logger.error(f"Google Sheets API Error while reading assignment report: {e}")
        # Return empty initialized data on error
        return assigned_data, assigned_data_totals # Return pre-initialized empty
    except Exception as e:
        logger.exception("Unexpected error while reading assignment report:")
        # Return empty initialized data on error
        return assigned_data, assigned_data_totals # Return pre-initialized empty

    logger.info("Finished reading and parsing today's assignment report.")
    return assigned_data, assigned_data_totals


def process_orders_sheet_for_actions(service, spreadsheet_id, today_date_formats, stakeholder_names, performance_summary_ref):
    """
    Processes Orders sheet... using 'Initial Assignment Category' and multiple date formats.
    Returns counts of skipped rows.
    """
    today_date_display = today_date_formats[0] # For logging clarity
    logger.info(f"--- Processing Orders Sheet for Actions (Dates: {today_date_formats}) using 'Initial Assignment Category' ---")
    sheet = service.spreadsheets()
    
    skipped_blank_cat_count = 0
    skipped_other_count = 0

    initial_assignment_col_name = COL_NAMES_ORDERS.get('initial_assignment_category')
    if not initial_assignment_col_name:
        logger.error("CRITICAL: 'initial_assignment_category' not defined in COL_NAMES_ORDERS. Cannot process Orders accurately.")
        return skipped_blank_cat_count, skipped_other_count # Return 0 counts

    try:
        logger.info(f"Reading data from Orders sheet '{ORDERS_SHEET_NAME}'...")
        read_range = f'{ORDERS_SHEET_NAME}!A:BI'
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=read_range).execute()
        values = result.get('values', [])

        if not values or len(values) <= ORDERS_HEADER_ROW_INDEX:
            logger.warning(f"No data or no header found in Orders sheet '{ORDERS_SHEET_NAME}'. Skipping.")
            return skipped_blank_cat_count, skipped_other_count

        header = [str(h).strip() for h in values[ORDERS_HEADER_ROW_INDEX]]
        logger.debug(f"Orders sheet header: {header}")

        try:
            col_idx_stakeholder = header.index(COL_NAMES_ORDERS['stakeholder'])
            col_idx_call_status = header.index(COL_NAMES_ORDERS['call_status'])
            col_idx_date1 = header.index(COL_NAMES_ORDERS['date_col_1'])
            col_idx_date2 = header.index(COL_NAMES_ORDERS['date_col_2']) if COL_NAMES_ORDERS.get('date_col_2') in header else -1
            col_idx_date3 = header.index(COL_NAMES_ORDERS['date_col_3']) if COL_NAMES_ORDERS.get('date_col_3') in header else -1
            col_idx_initial_category = header.index(initial_assignment_col_name)
        except ValueError as e:
            logger.error(f"Missing required column in Orders sheet header: {e}. Cannot process Orders sheet.")
            return skipped_blank_cat_count, skipped_other_count

        data_rows = values[ORDERS_DATA_START_ROW_INDEX:]
        logger.info(f"Processing {len(data_rows)} data rows from Orders sheet.")

        for i, row_data in enumerate(data_rows):
            original_sheet_row_num = ORDERS_DATA_START_ROW_INDEX + i + 1
            max_needed_idx = max(col_idx_stakeholder, col_idx_call_status, col_idx_date1,
                                 (col_idx_date2 if col_idx_date2 != -1 else 0),
                                 (col_idx_date3 if col_idx_date3 != -1 else 0),
                                 col_idx_initial_category)
            if len(row_data) <= max_needed_idx:
                skipped_other_count += 1 # Count skips due to row length
                continue

            stakeholder = str(row_data[col_idx_stakeholder]).strip()
            if not stakeholder or stakeholder not in stakeholder_names:
                skipped_other_count += 1 # Count skips due to stakeholder
                continue

            # --- MODIFIED DATE CHECK ---
            assigned_today = False
            date1_val = str(row_data[col_idx_date1]).strip()
            date2_val = str(row_data[col_idx_date2]).strip() if col_idx_date2 != -1 else ""
            date3_val = str(row_data[col_idx_date3]).strip() if col_idx_date3 != -1 else ""
            if date1_val in today_date_formats or \
               date2_val in today_date_formats or \
               date3_val in today_date_formats:
                assigned_today = True
            # --- END MODIFIED DATE CHECK ---

            if not assigned_today:
                skipped_other_count += 1 # Count skips due to date
                continue

            # --- If assigned today, proceed ---
            current_call_status = str(row_data[col_idx_call_status]).strip()
            initial_category_from_sheet = str(row_data[col_idx_initial_category]).strip()

            logger.debug(f"ROW {original_sheet_row_num}: Stakeholder='{stakeholder}', InitialCat='{initial_category_from_sheet}', CurrentCallStatus='{current_call_status}'")

            if not initial_category_from_sheet:
                logger.warning(f"  ROW {original_sheet_row_num}: 'Initial Assignment Category' is BLANK. Skipping for actioned/pending classification.")
                skipped_blank_cat_count += 1
                continue
            
            if initial_category_from_sheet not in ASSIGNMENT_CATEGORIES_FOR_PERFORMANCE_REPORT and initial_category_from_sheet != "Unknown":
                logger.warning(f"  Row {original_sheet_row_num}: Unexpected Initial Category '{initial_category_from_sheet}'.")

            # --- Enhanced Debugging for Actioned/Pending Logic ---
            logger.debug(f"  ROW {original_sheet_row_num}: Evaluating Actioned/Pending:")
            logger.debug(f"    Current Call Status: '{current_call_status}'")
            logger.debug(f"    Initial Category from Sheet: '{initial_category_from_sheet}'")

            is_actioned = True # Default to actioned
            
            if initial_category_from_sheet in INITIAL_ASSIGNMENT_STATUSES_ORDERS:
                logger.debug(f"    Initial category '{initial_category_from_sheet}' IS a key in INITIAL_ASSIGNMENT_STATUSES_ORDERS.")
                pending_statuses_for_this_category = INITIAL_ASSIGNMENT_STATUSES_ORDERS[initial_category_from_sheet]
                logger.debug(f"    Expected pending statuses for '{initial_category_from_sheet}': {pending_statuses_for_this_category}")
                if current_call_status in pending_statuses_for_this_category:
                    is_actioned = False
                    logger.debug(f"    MATCH FOUND: Current status '{current_call_status}' is in expected pending list. Setting is_actioned = False.")
                else:
                    logger.debug(f"    NO MATCH: Current status '{current_call_status}' NOT in expected pending list {pending_statuses_for_this_category}. is_actioned remains True (for now).")
            elif initial_category_from_sheet == "Unknown":
                logger.debug(f"    Initial category is 'Unknown'. Checking against generic initial statuses.")
                generic_initial_statuses = [status for sublist in INITIAL_ASSIGNMENT_STATUSES_ORDERS.values() for status in sublist]
                logger.debug(f"    Generic initial statuses list: {generic_initial_statuses}")
                if current_call_status in generic_initial_statuses:
                    is_actioned = False
                    logger.debug(f"    MATCH FOUND (Generic): Current status '{current_call_status}' is in generic list. Setting is_actioned = False.")
                else:
                    logger.debug(f"    NO MATCH (Generic): Current status '{current_call_status}' NOT in generic list. is_actioned remains True.")
            else: # initial_category_from_sheet is not "Unknown" AND not a key in INITIAL_ASSIGNMENT_STATUSES_ORDERS
                logger.warning(f"    Initial category '{initial_category_from_sheet}' is NOT 'Unknown' and NOT a key in INITIAL_ASSIGNMENT_STATUSES_ORDERS. is_actioned will remain True by default. Review configuration.")
            # --- END Enhanced Debugging ---

            # --- Update Summary Based on is_actioned ---
            if is_actioned:
                logger.debug(f"  Row {original_sheet_row_num}: FINAL: Status '{current_call_status}' considered ACTIONED for initial category '{initial_category_from_sheet}'.")
                performance_summary_ref[stakeholder]["actioned_total"] += 1
                category_key_for_summary = initial_category_from_sheet if initial_category_from_sheet in performance_summary_ref[stakeholder]["actioned_by_initial_category"] else "Unknown Initial Category Actioned"
                performance_summary_ref[stakeholder]["actioned_by_initial_category"][category_key_for_summary] += 1
                
                final_status_category_found = False
                for final_status in PERFORMANCE_FINAL_STATUSES_ORDERS:
                    if current_call_status == final_status:
                        performance_summary_ref[stakeholder]["final_statuses_orders"][final_status] += 1
                        final_status_category_found = True
                        logger.debug(f"    -> Final status classified as: '{final_status}'")
                        break
                if not final_status_category_found:
                    logger.debug(f"    -> Row {original_sheet_row_num}: Actioned status '{current_call_status}' not in defined PERFORMANCE_FINAL_STATUSES_ORDERS. Counted in 'Other Actioned (Orders)'.")
                    performance_summary_ref[stakeholder]["final_statuses_orders"]["Other Actioned (Orders)"] += 1
            else: # is_actioned is False
                logger.debug(f"  Row {original_sheet_row_num}: FINAL: Status '{current_call_status}' considered PENDING for initial category '{initial_category_from_sheet}'.")
                performance_summary_ref[stakeholder]["pending_total"] += 1
                category_key_for_summary = initial_category_from_sheet if initial_category_from_sheet in performance_summary_ref[stakeholder]["pending_by_initial_category"] else "Unknown Initial Category Pending"
                performance_summary_ref[stakeholder]["pending_by_initial_category"][category_key_for_summary] += 1

    except HttpError as e:
        logger.error(f"Google Sheets API Error during Orders sheet processing: {e}")
    except KeyError as e:
        logger.error(f"KeyError during Orders sheet processing - likely misconfigured COL_NAMES_ORDERS or sheet structure mismatch for column: {e}")
    except Exception as e:
        logger.exception("Unexpected error during Orders sheet processing:")

    logger.info("--- Finished Processing Orders Sheet for Actions ---")
    return skipped_blank_cat_count, skipped_other_count


# --- UPDATED: process_abandoned_sheet_for_actions (accepts date tuple, returns skips) ---
def process_abandoned_sheet_for_actions(service, spreadsheet_id, today_date_formats, stakeholder_names, performance_summary_ref):
    """Processes Abandoned sheet... using 'Initial Assignment Category' and multiple date formats. Returns counts of skipped rows."""
    today_date_display = today_date_formats[0] # For logging
    logger.info(f"--- Processing Abandoned Sheet for Actions (Dates: {today_date_formats}) using 'Initial Assignment Category' ---")
    sheet = service.spreadsheets()
    
    skipped_blank_cat_count = 0
    skipped_other_count = 0

    initial_assignment_col_name = COL_NAMES_ABANDONED.get('initial_assignment_category')
    calling_status_col_name = COL_NAMES_ABANDONED.get('calling_status')
    stakeholder_col_name = COL_NAMES_ABANDONED.get('stakeholder')
    date1_col_name = COL_NAMES_ABANDONED.get('date_col_1')
    date2_col_name = COL_NAMES_ABANDONED.get('date_col_2')
    date3_col_name = COL_NAMES_ABANDONED.get('date_col_3')

    if not all([initial_assignment_col_name, calling_status_col_name, stakeholder_col_name, date1_col_name]):
         logger.error("CRITICAL: One or more required keys missing in COL_NAMES_ABANDONED. Cannot process Abandoned sheet.")
         return skipped_blank_cat_count, skipped_other_count

    try:
        logger.info(f"Reading data from Abandoned sheet '{ABANDONED_SHEET_NAME}'...")
        read_range = f'{ABANDONED_SHEET_NAME}!A:BI'
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=read_range).execute()
        values = result.get('values', [])

        if not values or len(values) <= ABANDONED_HEADER_ROW_INDEX:
            logger.warning(f"No data or no header found in Abandoned sheet '{ABANDONED_SHEET_NAME}'. Skipping.")
            return skipped_blank_cat_count, skipped_other_count

        header = [str(h).strip() for h in values[ABANDONED_HEADER_ROW_INDEX]]
        logger.debug(f"Abandoned sheet header: {header}")

        try:
            col_idx_stakeholder = header.index(stakeholder_col_name)
            col_idx_calling_status = header.index(calling_status_col_name)
            col_idx_date1 = header.index(date1_col_name)
            col_idx_date2 = header.index(date2_col_name) if date2_col_name and date2_col_name in header else -1
            col_idx_date3 = header.index(date3_col_name) if date3_col_name and date3_col_name in header else -1
            col_idx_initial_category = header.index(initial_assignment_col_name)
        except ValueError as e:
            logger.error(f"Missing required column in Abandoned sheet header: {e}. Cannot process Abandoned sheet.")
            return skipped_blank_cat_count, skipped_other_count

        data_rows = values[ABANDONED_DATA_START_ROW_INDEX:]
        logger.info(f"Processing {len(data_rows)} data rows from Abandoned sheet.")

        for i, row_data in enumerate(data_rows):
            original_sheet_row_num = ABANDONED_DATA_START_ROW_INDEX + i + 1
            max_needed_idx = max(col_idx_stakeholder, col_idx_calling_status, col_idx_date1,
                                 (col_idx_date2 if col_idx_date2 != -1 else 0),
                                 (col_idx_date3 if col_idx_date3 != -1 else 0),
                                 col_idx_initial_category)
            if len(row_data) <= max_needed_idx:
                 skipped_other_count += 1
                 continue

            stakeholder = str(row_data[col_idx_stakeholder]).strip()
            if not stakeholder or stakeholder not in stakeholder_names:
                 skipped_other_count += 1
                 continue

            # --- MODIFIED DATE CHECK ---
            assigned_today = False
            date1_val = str(row_data[col_idx_date1]).strip()
            date2_val = str(row_data[col_idx_date2]).strip() if col_idx_date2 != -1 else ""
            date3_val = str(row_data[col_idx_date3]).strip() if col_idx_date3 != -1 else ""
            if date1_val in today_date_formats or \
               date2_val in today_date_formats or \
               date3_val in today_date_formats:
                assigned_today = True
            # --- END MODIFIED DATE CHECK ---
                
            if not assigned_today:
                 skipped_other_count += 1
                 continue

            # --- If assigned today, proceed ---
            current_calling_status = str(row_data[col_idx_calling_status]).strip()
            initial_category_from_sheet = str(row_data[col_idx_initial_category]).strip()

            logger.debug(f"ROW {original_sheet_row_num} (Abandoned): Stakeholder='{stakeholder}', InitialCat='{initial_category_from_sheet}', CurrentCallingStatus='{current_calling_status}'")

            if not initial_category_from_sheet:
                logger.warning(f"  ROW {original_sheet_row_num} (Abandoned): 'Initial Assignment Category' is BLANK. Skipping.")
                skipped_blank_cat_count += 1
                continue
            
            if initial_category_from_sheet != "Abandoned":
                 logger.warning(f"  ROW {original_sheet_row_num} (Abandoned): Expected Initial Category 'Abandoned' but found '{initial_category_from_sheet}'.")
            
            # --- Actioned/Pending Logic for Abandoned ---
            is_actioned = True # Default to actioned
            if current_calling_status in INITIAL_ASSIGNMENT_STATUSES_ABANDONED: # ['','Didn't Pickup','Follow Up']
                is_actioned = False
            logger.debug(f"  ROW {original_sheet_row_num} (Abandoned): Evaluating Actioned/Pending. Current Status='{current_calling_status}'. Is Actioned={is_actioned}")
            # --- End Actioned/Pending Logic ---

            # --- Update Summary Based on is_actioned ---
            if is_actioned:
                logger.debug(f"  Row {original_sheet_row_num} (Abandoned): FINAL: Status '{current_calling_status}' considered ACTIONED for initial category '{initial_category_from_sheet}'.")
                performance_summary_ref[stakeholder]["actioned_total"] += 1
                category_key_for_summary = initial_category_from_sheet if initial_category_from_sheet == "Abandoned" else "Unknown Initial Category Actioned" # Should be "Abandoned"
                performance_summary_ref[stakeholder]["actioned_by_initial_category"][category_key_for_summary] += 1
                
                final_status_category_found = False
                for final_status in PERFORMANCE_FINAL_STATUSES_ABANDONED:
                    if current_calling_status == final_status:
                        performance_summary_ref[stakeholder]["final_statuses_abandoned"][final_status] += 1
                        final_status_category_found = True
                        logger.debug(f"    -> Final status classified as (Abandoned): '{final_status}'")
                        break
                if not final_status_category_found:
                    logger.debug(f"    -> Row {original_sheet_row_num} (Abandoned): Actioned status '{current_calling_status}' not in defined PERFORMANCE_FINAL_STATUSES_ABANDONED. Counted in 'Other Actioned (Abandoned)'.")
                    performance_summary_ref[stakeholder]["final_statuses_abandoned"]["Other Actioned (Abandoned)"] += 1
            else: # is_actioned is False
                logger.debug(f"  Row {original_sheet_row_num} (Abandoned): FINAL: Status '{current_calling_status}' considered PENDING for initial category '{initial_category_from_sheet}'.")
                performance_summary_ref[stakeholder]["pending_total"] += 1
                category_key_for_summary = initial_category_from_sheet if initial_category_from_sheet == "Abandoned" else "Unknown Initial Category Pending"
                performance_summary_ref[stakeholder]["pending_by_initial_category"][category_key_for_summary] += 1

    except HttpError as e:
        logger.error(f"Google Sheets API Error during Abandoned sheet processing: {e}")
    except KeyError as e:
        logger.error(f"KeyError during Abandoned sheet processing - likely misconfigured COL_NAMES_ABANDONED or sheet structure mismatch for column: {e}")
    except Exception as e:
        logger.exception("Unexpected error during Abandoned sheet processing:")

    logger.info("--- Finished Processing Abandoned Sheet for Actions ---")
    return skipped_blank_cat_count, skipped_other_count

# --- NEW Function: Format and Write Performance Report ---
def format_and_write_performance_report(service, settings, performance_summary, today_date_str_display):
    """
    Formats the performance data and writes it to the specified Google Sheet.
    """
    logger.info(f"--- Formatting and Writing Performance Report for {today_date_str_display} ---")
    
    spreadsheet_id = settings['sheets']['orders_spreadsheet_id'] # Report goes to Orders sheet
    report_sheet_name = PERFORMANCE_REPORT_SHEET_NAME # e.g., "Performance Reports"
    sheet = service.spreadsheets()
    
    stakeholder_names = list(performance_summary.keys())
    
    formatted_values = []
    
    # --- Report Header ---
    formatted_values.append([f"--- Stakeholder Performance Report for {today_date_str_display} ---"])
    formatted_values.append(["Date:", today_date_str_display])
    formatted_values.append([]) # Blank row

    # --- Data for each Stakeholder ---
    for name in stakeholder_names:
        summary = performance_summary[name]
        assigned_total_from_report = summary.get('assigned_total', 0) # Use total read from assignment report
        actioned_total_calculated = summary.get('actioned_total', 0)
        pending_total_calculated = summary.get('pending_total', 0)

        formatted_values.append([f"Stakeholder: {name}"])
        formatted_values.append(["-" * 60]) # Separator line
        # Main Table Header
        formatted_values.append([
            "Initial Category", "Assigned Today", "Actioned Today", "Pending Today", "Final Status Breakdown (Actioned)"
        ])
        formatted_values.append(["-" * 60]) # Separator line

        # Data Rows for each assignment category
        # Use the order defined in ASSIGNMENT_CATEGORIES_FOR_PERFORMANCE_REPORT for consistency
        for category in ASSIGNMENT_CATEGORIES_FOR_PERFORMANCE_REPORT:
            assigned_count = summary['assigned_categories'].get(category, 0)
            actioned_count = summary['actioned_by_initial_category'].get(category, 0)
            pending_count = summary['pending_by_initial_category'].get(category, 0)
            
            # Build the breakdown string (only for actioned items)
            breakdown_parts = []
            if category == "Abandoned": # Use Abandoned statuses
                 for status, count in summary['final_statuses_abandoned'].items():
                     # Only show breakdown relevant to this initial category if possible - difficult without more data
                     # For now, show all actioned Abandoned outcomes here.
                     if count > 0 and status != "Other Actioned (Abandoned)": # Exclude "Other" for cleaner look? Or include it?
                         breakdown_parts.append(f"{status}: {count}")
                 # Add "Other" if it has counts
                 other_count = summary['final_statuses_abandoned'].get("Other Actioned (Abandoned)", 0)
                 if other_count > 0:
                      breakdown_parts.append(f"Other Actioned (Abandoned): {other_count}")

            else: # Use Orders statuses for other categories
                 for status, count in summary['final_statuses_orders'].items():
                     # Similar challenge: relating final status back to initial category isn't perfect
                     # Show all actioned Order outcomes here.
                     if count > 0 and status != "Other Actioned (Orders)":
                         breakdown_parts.append(f"{status}: {count}")
                 other_count = summary['final_statuses_orders'].get("Other Actioned (Orders)", 0)
                 if other_count > 0:
                     breakdown_parts.append(f"Other Actioned (Orders): {other_count}")
            
            breakdown_string = ", ".join(breakdown_parts) if breakdown_parts else "-" # Use "-" if no actioned outcomes

            formatted_values.append([
                category, assigned_count, actioned_count, pending_count, breakdown_string
            ])

        # Handle potentially "Unknown" initial categories if they occurred
        unknown_actioned = summary['actioned_by_initial_category'].get("Unknown Initial Category Actioned", 0)
        unknown_pending = summary['pending_by_initial_category'].get("Unknown Initial Category Pending", 0)
        if unknown_actioned > 0 or unknown_pending > 0:
             formatted_values.append([
                "Unknown Initial", 0, unknown_actioned, unknown_pending, "N/A (Check Logs/Config)"
            ])


        # Totals Row
        formatted_values.append(["-" * 60]) # Separator line
        formatted_values.append([
            "TOTAL", assigned_total_from_report, actioned_total_calculated, pending_total_calculated, f"(Discrepancy vs Assigned: {assigned_total_from_report - (actioned_total_calculated + pending_total_calculated)})"
        ])
        formatted_values.append([]) # Blank row between stakeholders

    # --- Report Footer ---
    formatted_values.append([f"--- End of Performance Report for {today_date_str_display} ---"])

    # --- Writing Logic ---
    report_title_pattern = "--- Stakeholder Performance Report for " # For finding existing report
    
    # Use find_existing_report_range (ensure it's defined correctly in your script)
    start_row_existing, end_row_existing = find_existing_report_range(
        sheet, spreadsheet_id, report_sheet_name, report_title_pattern, today_date_str_display
    )

    if start_row_existing is not None and end_row_existing is not None:
        logger.info(f"Existing performance report for {today_date_str_display} found. Clearing and updating rows {start_row_existing}-{end_row_existing}.")
        # Clear existing range first
        # Note: Clearing more columns (e.g., A:Z) ensures old data with different widths is removed.
        range_to_clear = f"'{report_sheet_name}'!A{start_row_existing}:Z{end_row_existing}"
        try:
            clear_body = {} # Empty body for clear
            sheet.values().clear(spreadsheetId=spreadsheet_id, range=range_to_clear, body=clear_body).execute()
            logger.info(f"Cleared existing report range: {range_to_clear}")
        except HttpError as e:
            logger.error(f"API Error clearing existing report range {range_to_clear}: {e}")
            # Decide if you want to proceed with writing anyway or stop
        except Exception as e:
             logger.exception(f"Unexpected error clearing existing report range:")
             # Decide if you want to proceed

        # Write new data starting at the original start row
        range_to_write = f"'{report_sheet_name}'!A{start_row_existing}"
        try:
            body = {'values': formatted_values}
            result = sheet.values().update(
                spreadsheetId=spreadsheet_id, range=range_to_write,
                valueInputOption='USER_ENTERED', body=body).execute() # USER_ENTERED preserves formats
            logger.info(f"Performance report updated in '{report_sheet_name}'. {result.get('updatedCells', 'N/A')} cells updated.")
        except HttpError as e:
            logger.error(f"API Error updating performance report: {e}")
        except Exception as e:
            logger.exception("Unexpected error updating performance report:")

    else: # Append new report
        logger.info(f"No existing performance report found for {today_date_str_display}. Appending new report to '{report_sheet_name}'.")
        start_row_for_append = 1
        try:
            # Check last row of sheet (safer: check column A)
             result_existing = sheet.values().get(spreadsheetId=spreadsheet_id, range=f"'{report_sheet_name}'!A:A").execute()
             existing_values = result_existing.get('values', [])
             if existing_values:
                 start_row_for_append = len(existing_values) + 2 # Add blank line before new report
        except HttpError as e:
            # If sheet doesn't exist, create it
            if 'Unable to parse range' in str(e) or (hasattr(e, 'resp') and e.resp.status == 400):
                 logger.warning(f"Performance report sheet '{report_sheet_name}' not found. Creating it.")
                 try:
                     add_sheet_body = {'requests': [{'addSheet': {'properties': {'title': report_sheet_name}}}]}
                     sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=add_sheet_body).execute()
                     logger.info(f"Created sheet '{report_sheet_name}'.")
                     start_row_for_append = 1 # Start at row 1 in new sheet
                 except Exception as create_err:
                     logger.error(f"Failed to create sheet '{report_sheet_name}': {create_err}")
                     return # Cannot write report
            else: # Re-raise other API errors
                 logger.error(f"API Error checking for last row in '{report_sheet_name}': {e}")
                 return # Cannot safely append
        except Exception as e:
             logger.exception("Unexpected error finding last row for append:")
             return

        # Append the formatted data
        range_to_append = f"'{report_sheet_name}'!A{start_row_for_append}"
        try:
            body = {'values': formatted_values}
            result = sheet.values().update(
                spreadsheetId=spreadsheet_id, range=range_to_append,
                valueInputOption='USER_ENTERED', body=body).execute()
            logger.info(f"Performance report appended to '{report_sheet_name}'. {result.get('updatedCells', 'N/A')} cells updated.")
        except HttpError as e:
            logger.error(f"API Error appending performance report: {e}")
        except Exception as e:
            logger.exception("Unexpected error appending performance report:")

# --- Main Performance Analysis Function (generate_performance_report) --- MODIFIED
def generate_performance_report(service, settings):
    logger.info("Starting Performance Report Generation.")
    
    # <<< START DATE MODIFICATION >>>
    today = datetime.date.today()
    today_date_str_padded = today.strftime("%d-%b-%Y") # e.g., 08-May-2025
    today_date_str_unpadded = today.strftime(f"{today.day}-%b-%Y") # e.g., 8-May-2025 or 10-May-2025
    # Use a tuple of formats to check against sheet data
    today_date_formats_to_check = (today_date_str_padded, today_date_str_unpadded)
    # Use the padded format for consistency when referring to 'today' in report titles etc.
    today_date_display = today_date_str_padded
    logger.info(f"Analyzing performance for assignments with dates: {today_date_formats_to_check}")
    # <<< END DATE MODIFICATION >>>

    stakeholder_list_config = settings['stakeholders']
    stakeholder_names = [s['name'] for s in stakeholder_list_config]
    orders_spreadsheet_id = settings['sheets']['orders_spreadsheet_id']
    abandoned_spreadsheet_id = settings['sheets']['abandoned_spreadsheet_id']
    assignment_report_sheet_name = settings['sheets']['report_sheet_name']
    
    # Ensure all expected categories + potential unknowns are keys in the summary dicts
    _assignment_cats_extended = ASSIGNMENT_CATEGORIES_FOR_PERFORMANCE_REPORT + ["Unknown Initial Category Actioned", "Unknown Initial Category Pending", "Abandoned"]
    _assignment_cats_extended = sorted(list(set(_assignment_cats_extended))) # Unique and sorted

    performance_summary = {
        s_name: {
            "assigned_categories": {cat: 0 for cat in ASSIGNMENT_CATEGORIES_FOR_PERFORMANCE_REPORT},
            "assigned_total": 0,
            "actioned_total": 0,
            "pending_total": 0,
            "actioned_by_initial_category": {cat: 0 for cat in _assignment_cats_extended},
            "pending_by_initial_category": {cat: 0 for cat in _assignment_cats_extended},
            "final_statuses_orders": {fs: 0 for fs in PERFORMANCE_FINAL_STATUSES_ORDERS + ["Other Actioned (Orders)"]},
            "final_statuses_abandoned": {fs: 0 for fs in PERFORMANCE_FINAL_STATUSES_ABANDONED + ["Other Actioned (Abandoned)"]},
        } for s_name in stakeholder_names
    }
    
    # Initialize skip counters
    skipped_orders_blank_initial_cat = 0
    skipped_orders_other = 0
    skipped_abandoned_blank_initial_cat = 0
    skipped_abandoned_other = 0

    # --- 1. Read Today's Assignment Data ---
    # Use the display date (padded) for matching the report title format
    assigned_category_counts, assigned_total_counts = read_todays_assignment_report(
        service, orders_spreadsheet_id, assignment_report_sheet_name,
        today_date_display, # Use consistent format for report title
        stakeholder_names, ASSIGNMENT_CATEGORIES_FOR_PERFORMANCE_REPORT
    )
    for s_name in stakeholder_names:
        performance_summary[s_name]["assigned_categories"] = assigned_category_counts.get(s_name, {cat: 0 for cat in ASSIGNMENT_CATEGORIES_FOR_PERFORMANCE_REPORT})
        performance_summary[s_name]["assigned_total"] = assigned_total_counts.get(s_name, 0)
        # Log the assigned counts read from the report
        logger.info(f"Read from Assignment Report - {s_name}: Assigned Total = {performance_summary[s_name]['assigned_total']}")
        logger.debug(f"  Assigned Categories: {performance_summary[s_name]['assigned_categories']}")


    # --- 2. Process "Orders" Sheet for Actions ---
    # Pass the TUPLE of date formats to check against sheet data
    skipped_orders_blank_initial_cat, skipped_orders_other = process_orders_sheet_for_actions(
        service,
        orders_spreadsheet_id,
        today_date_formats_to_check, # <<< Pass tuple
        stakeholder_names,
        performance_summary # Pass by reference
    )

    # --- 3. Process "Abandoned" Sheet for Actions ---
    # Pass the TUPLE of date formats to check against sheet data
    skipped_abandoned_blank_initial_cat, skipped_abandoned_other = process_abandoned_sheet_for_actions(
        service,
        abandoned_spreadsheet_id,
        today_date_formats_to_check, # <<< Pass tuple
        stakeholder_names,
        performance_summary # Update the same summary dict
    )

    # --- Log final summary and SKIPS ---
    logger.info(f"--- Final Performance Summary & Skipped Counts ({today_date_display}) ---")
    total_skipped_blank_cat = skipped_orders_blank_initial_cat + skipped_abandoned_blank_initial_cat
    total_skipped_other = skipped_orders_other + skipped_abandoned_other
    logger.info(f"Total rows skipped due to BLANK Initial Category: {total_skipped_blank_cat}")
    logger.info(f"Total rows skipped for OTHER reasons (e.g., no stakeholder, date mismatch): {total_skipped_other}")

    for s_name in stakeholder_names:
        assigned_total = performance_summary[s_name]['assigned_total'] # From assignment report
        actioned_total = performance_summary[s_name]['actioned_total'] # Calculated from sheets
        pending_total = performance_summary[s_name]['pending_total']   # Calculated from sheets
        calculated_total = actioned_total + pending_total
        # Compare assigned total (from report) with calculated total (from sheet analysis)
        discrepancy = assigned_total - calculated_total
        
        logger.info(f"Summary - {s_name}: Assigned={assigned_total}, Actioned={actioned_total}, Pending={pending_total} (Calculated Total={calculated_total}, Discrepancy={discrepancy})")
        # Log details only if DEBUG is enabled potentially
        logger.debug(f"  Actioned by Initial Cat: {performance_summary[s_name]['actioned_by_initial_category']}")
        logger.debug(f"  Pending by Initial Cat: {performance_summary[s_name]['pending_by_initial_category']}")
        logger.debug(f"  Final Order Statuses: {performance_summary[s_name]['final_statuses_orders']}")
        logger.debug(f"  Final Abandoned Statuses: {performance_summary[s_name]['final_statuses_abandoned']}")

    # --- 4. Format and Write Performance Report --- <<< MODIFIED CALL
    format_and_write_performance_report(
        service,
        settings,
        performance_summary,
        today_date_display # Pass the consistent display date string
    )
    logger.info("Placeholder for Formatting and Writing Performance Report...")
    # Example call (function needs to be created):
    # format_and_write_performance_report(service, settings, performance_summary, today_date_display)

    logger.info("Performance Report Generation process completed.")

# --- Main Execution ---
if __name__ == '__main__':
    logger.info("--- Performance Analyzer Script Started ---")
    settings = load_settings(SETTINGS_FILE)
    if not settings or 'stakeholders' not in settings:
        logger.error("Failed to load settings or stakeholders missing. Aborting.")
        sys.exit(1)

    service = authenticate_google_sheets()
    if not service:
        logger.error("Google Sheets authentication failed. Aborting script.")
        sys.exit(1)

    generate_performance_report(service, settings) # Pass service and settings
    logger.info("--- Performance Analyzer Script Finished ---")