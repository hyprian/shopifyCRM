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
SPREADSHEET_ID = '1ZkTB3ahmrQ2-7rz-h1RdkOMPSHmkAhpFJIH64Ca0jxk'
ORDERS_SHEET_NAME = 'Orders'
REPORT_SHEET_NAME = 'Stakeholder Report'
SETTINGS_FILE = 'settings.yaml'
SERVICE_ACCOUNT_FILE = 'carbon-pride-374002-2dc0cf329724.json'

# Scopes required for reading and writing
# CORRECTED SCOPE: Removed the duplicate part
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Define call status priorities and report categories
CALL_PRIORITIES = {
    1: ["NDR"],
    2: ["Confirmation Pending", "Fresh"],
    3: ["Call didn't Pick", "Follow up"],
    4: ["Abandoned", "Number invalid/fake order"]
}

STATUS_TO_REPORT_CATEGORY = {
    "Fresh": "Fresh",
    "Confirmation Pending": "Fresh",
    "Abandoned": "Abandoned",
    "Number invalid/fake order": "Abandoned",
    "Call didn't Pick": "CNP",
    "Follow up": "Follow up",
    "NDR": "NDR"
}

# Sheet structure definition
HEADER_ROW_INDEX = 1 # 0-indexed
DATA_START_ROW_INDEX = 2 # 0-indexed

COL_NAMES = {
    'call_status': 'Call-status',
    'order_status': 'order status',
    'stakeholder': 'Stakeholder',
    'date_col': 'Date',
    'date_col_2': 'Date 2',
    'date_col_3': 'Date 3',
    'id': 'Id',
    'name': 'Name',
    'created_at': 'Created At',
    'customer_id': 'Id (Customer)',
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

# --- Load Settings Function (with Logging) ---
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
def col_index_to_a1(index):
    col = ''
    while index >= 0:
        col = chr(index % 26 + ord('A')) + col
        index = index // 26 - 1
    return col

# --- Authentication ---
def authenticate_google_sheets():
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
        # Iterate from the row *after* today's report start
        for i in range(start_row, last_row_in_sheet): # i is 1-based here
             row_value = values[i][0].strip() if values[i] and values[i][0] else ''
             if row_value.startswith(any_report_start_pattern):
                  next_start_row = i + 1 # 1-based index of the next start
                  logger.debug(f"Found start of next report section at row {next_start_row}.")
                  break # Found the next one, stop searching

        # Determine the end row for clearing: 1 row before the next report start, or the last row of the sheet
        if next_start_row is not None:
             # Clear from the start of today's report up to the row just before the next report starts.
             end_row_to_clear = next_start_row - 1
             logger.debug(f"Calculated clear end row based on next report start: {end_row_to_clear}")
        else:
             # Clear from the start of today's report to the very last row of the sheet that has data.
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
    logger.info(f"Loaded {len(stakeholder_list)} stakeholders.")

    service = authenticate_google_sheets()
    if not service:
        logger.error("Authentication failed. Aborting script.")
        return
    sheet = service.spreadsheets()

    try:
        # --- Read Data ---
        logger.info(f"Reading data from '{ORDERS_SHEET_NAME}'...")
        # Read a wider range to ensure Date 2 and Date 3 columns are captured if they exist beyond AZ
        read_range = f'{ORDERS_SHEET_NAME}!A:BD' # Increased range to BD (56 columns)
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=read_range).execute()
        values = result.get('values', [])

        if not values:
            logger.warning(f"No data found in '{ORDERS_SHEET_NAME}'.")
            return

        logger.info(f"Read {len(values)} rows from '{ORDERS_SHEET_NAME}'.")

        if len(values) < DATA_START_ROW_INDEX + 1:
             logger.error(f"Not enough rows in '{ORDERS_SHEET_NAME}'. Need at least {DATA_START_ROW_INDEX + 1}. Found {len(values)}.")
             return

        header = [str(h).strip() if h is not None else '' for h in values[HEADER_ROW_INDEX]]
        header_length = len(header)
        logger.info(f"Header row (row {HEADER_ROW_INDEX + 1}) with {header_length} columns identified.")

        # --- Pad Data Rows ---
        data_rows_raw = values[DATA_START_ROW_INDEX:]
        padded_data_rows = []
        for i, row in enumerate(data_rows_raw):
            processed_row = [str(cell).strip() if cell is not None else '' for cell in row]
            if len(processed_row) < header_length:
                processed_row.extend([''] * (header_length - len(processed_row)))
            elif len(processed_row) > header_length:
                 logger.warning(f"Row {DATA_START_ROW_INDEX + i + 1} has more columns ({len(processed_row)}) than header ({header_length}). Truncating to {header_length}.")
                 processed_row = processed_row[:header_length]
            padded_data_rows.append(processed_row)

        logger.info(f"Processed {len(padded_data_rows)} data rows.")

        df = pd.DataFrame(padded_data_rows, columns=header)
        df['_original_row_index'] = range(DATA_START_ROW_INDEX + 1, DATA_START_ROW_INDEX + 1 + len(df))
        logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns.")

        # --- Prepare DataFrame Columns ---
        # Ensure required columns exist in DataFrame, adding them if necessary
        for col_key, col_name in COL_NAMES.items():
            if col_name not in df.columns:
                logger.warning(f"Column '{col_name}' not found in DataFrame after reading. Adding it as an empty column.")
                df[col_name] = ''
            # Ensure all relevant columns are string type for consistency
            df[col_name] = df[col_name].astype(str)

        # Clean up status column
        df[COL_NAMES['call_status']] = df[COL_NAMES['call_status']].fillna('').astype(str).str.strip()


        # --- Filter Rows for Processing ---
        logger.info("Filtering rows based on priority statuses...")
        all_priority_statuses = [status for priority_list in CALL_PRIORITIES.values() for status in priority_list]
        rows_to_process_df = df[df[COL_NAMES['call_status']].isin(all_priority_statuses)].copy()

        logger.info(f"Found {len(rows_to_process_df)} rows matching priority statuses for potential assignment/date update.")

        filtered_indices = rows_to_process_df.index.tolist() # Indices in the original df

        if not filtered_indices:
            logger.info("No rows matched filter criteria. Skipping assignments and report.")
            return
        else:
            # --- Assign Date and Stakeholder ---
            today_date_str_for_sheet = datetime.date.today().strftime("%d-%b-%Y") # Use YYYY for consistency with example
            today_date_str_for_report = datetime.date.today().strftime("%d-%b-%Y") # Also YYYY for report title
            num_stakeholders = len(stakeholder_list)

            logger.info(f"Processing {len(filtered_indices)} filtered rows for assignments and date tracking.")

            # Assign stakeholders cyclically to ALL filtered rows first
            assigned_stakeholders = [stakeholder_list[i % num_stakeholders] for i in range(len(filtered_indices))]
            df.loc[filtered_indices, COL_NAMES['stakeholder']] = assigned_stakeholders
            logger.info(f"Stakeholders assigned cyclically to {len(filtered_indices)} rows.")

            # Now handle date assignments row by row for the filtered indices
            for i, df_index in enumerate(filtered_indices):
                row_data = df.loc[df_index] # Get the current state from the main df
                call_status = row_data.get(COL_NAMES['call_status'], '').strip()

                # Read existing date values as strings
                date1_val = str(row_data.get(COL_NAMES['date_col'], '')).strip()
                date2_val = str(row_data.get(COL_NAMES['date_col_2'], '')).strip()
                date3_val = str(row_data.get(COL_NAMES['date_col_3'], '')).strip()

                if call_status == "Call didn't Pick":
                    if not date1_val:
                        df.loc[df_index, COL_NAMES['date_col']] = today_date_str_for_sheet
                        logger.debug(f"Row {row_data['_original_row_index']}: CNP, 1st attempt. Set Date to {today_date_str_for_sheet}.")
                    elif not date2_val:
                        df.loc[df_index, COL_NAMES['date_col_2']] = today_date_str_for_sheet
                        logger.debug(f"Row {row_data['_original_row_index']}: CNP, 2nd attempt. Set Date 2 to {today_date_str_for_sheet}.")
                    elif not date3_val:
                        df.loc[df_index, COL_NAMES['date_col_3']] = today_date_str_for_sheet
                        logger.debug(f"Row {row_data['_original_row_index']}: CNP, 3rd attempt. Set Date 3 to {today_date_str_for_sheet}.")
                    else:
                         logger.debug(f"Row {row_data['_original_row_index']}: CNP, 3 attempts already logged. Dates unchanged.")

                else:
                    # Status is in prioritized list but not CNP
                    df.loc[df_index, COL_NAMES['date_col']] = today_date_str_for_sheet
                    # Note: Date 2 and Date 3 are NOT cleared based on Apps Script behavior observation
                    logger.debug(f"Row {row_data['_original_row_index']}: Status '{call_status}'. Set Date to {today_date_str_for_sheet}.")

            logger.info(f"Date logic applied to {len(filtered_indices)} rows.")

            # --- Prepare Batch Update ---
            logger.info("Preparing batch update for Orders sheet...")
            updates = []
            # Get 0-indexed sheet column positions for ALL columns we might update
            cols_to_update_names = [COL_NAMES['stakeholder'], COL_NAMES['date_col'], COL_NAMES['date_col_2'], COL_NAMES['date_col_3']]
            sheet_col_indices = {}
            max_col_index_to_write = -1

            for col_name in cols_to_update_names:
                try:
                    col_index = header.index(col_name)
                    sheet_col_indices[col_name] = col_index
                    max_col_index_to_write = max(max_col_index_to_write, col_index)
                    logger.debug(f"Found column '{col_name}' at index {col_index}.")
                except ValueError:
                    logger.warning(f"Column '{col_name}' not found in the sheet header row. Cannot write to this column.")
                    sheet_col_indices[col_name] = -1 # Mark as not found

            if max_col_index_to_write != -1:
                for df_index in filtered_indices:
                    original_sheet_row = df.loc[df_index, '_original_row_index']
                    row_data = df.loc[df_index] # Get the potentially updated data

                    # Create a list for the row values, padded up to the max column index needed
                    row_values_to_write = [None] * (max_col_index_to_write + 1)

                    # Place the values at their correct sheet column indices if the column was found
                    if sheet_col_indices.get(COL_NAMES['stakeholder'], -1) != -1:
                        row_values_to_write[sheet_col_indices[COL_NAMES['stakeholder']]] = row_data.get(COL_NAMES['stakeholder'], '')

                    if sheet_col_indices.get(COL_NAMES['date_col'], -1) != -1:
                        row_values_to_write[sheet_col_indices[COL_NAMES['date_col']]] = row_data.get(COL_NAMES['date_col'], '')

                    if sheet_col_indices.get(COL_NAMES['date_col_2'], -1) != -1:
                        row_values_to_write[sheet_col_indices[COL_NAMES['date_col_2']]] = row_data.get(COL_NAMES['date_col_2'], '')

                    if sheet_col_indices.get(COL_NAMES['date_col_3'], -1) != -1:
                        row_values_to_write[sheet_col_indices[COL_NAMES['date_col_3']]] = row_data.get(COL_NAMES['date_col_3'], '')

                    updates.append({
                        'range': f'{ORDERS_SHEET_NAME}!A{original_sheet_row}',
                        'values': [row_values_to_write]
                    })

                logger.info(f"Prepared {len(updates)} row updates for Orders sheet batch write.")
            else:
                 logger.warning("No writeable columns found in header. No Orders sheet updates prepared.")

            # Execute batch update
            if updates:
                logger.info("Executing batch update to Orders sheet...")
                body = {'value_input_option': 'RAW', 'data': updates}
                try:
                    result = sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
                    logger.info(f"Batch update completed. {result.get('totalUpdatedCells', 'N/A')} cells updated.")
                except HttpError as e:
                    logger.error(f"API Error during batch update: {e}")
                except Exception as e:
                     logger.exception("Unexpected error during batch update:")
            else:
                 logger.info("No updates to write back to Orders sheet.")

            # --- Generate Stakeholder Report for THIS RUN ---
            logger.info("Generating Stakeholder Report for this batch...")

            report_counts = {}
            for stakeholder in stakeholder_list:
                report_counts[stakeholder] = {"Total": 0, "Fresh": 0, "Abandoned": 0, "CNP": 0, "Follow up": 0, "NDR": 0}
            logger.info("Initialized report counts.")

            # Count calls for each stakeholder based ONLY on the rows that were just filtered and assigned
            assigned_rows_processed_count = 0
            for index, row_in_filtered_df in rows_to_process_df.iterrows():
                 assigned_stakeholder = df.loc[index, COL_NAMES['stakeholder']]
                 call_status = row_in_filtered_df.get(COL_NAMES['call_status'], '').strip()

                 if assigned_stakeholder in report_counts:
                     assigned_rows_processed_count += 1
                     report_category = STATUS_TO_REPORT_CATEGORY.get(call_status)
                     if report_category:
                         report_counts[assigned_stakeholder][report_category] += 1

            logger.info(f"Calculated report counts for {assigned_rows_processed_count} rows processed and assigned in this run.")

            formatted_report_values = []
            formatted_report_values.append([f"--- Stakeholder Report for Assignments on {today_date_str_for_report} ---"])
            formatted_report_values.append([''])

            for stakeholder in stakeholder_list:
                 total_assigned_this_run = sum(report_counts[stakeholder][cat] for cat in ["Fresh", "Abandoned", "CNP", "Follow up", "NDR"])
                 report_counts[stakeholder]["Total"] = total_assigned_this_run

                 formatted_report_values.append([f"Calls assigned {stakeholder}"])
                 formatted_report_values.append([f"- Total Calls This Run - {report_counts[stakeholder]['Total']}"])
                 formatted_report_values.append([f"- Fresh- {report_counts[stakeholder]['Fresh']}"])
                 formatted_report_values.append([f"- Abandand- {report_counts[stakeholder]['Abandoned']}"])
                 formatted_report_values.append([f"- CNP- {report_counts[stakeholder]['CNP']}"])
                 formatted_report_values.append([f"- Follow up- {report_counts[stakeholder]['Follow up']}"])
                 formatted_report_values.append([f"- NDR - {report_counts[stakeholder]['NDR']}"])
                 formatted_report_values.append([''])

            formatted_report_values.append(['--- End of Report for ' + today_date_str_for_report + ' ---'])

            logger.info(f"Formatted report data ({len(formatted_report_values)} rows).")

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

    except HttpError as err:
        logger.error(f"Google Sheets API Error during main execution: {err}")
    except Exception as e:
        logger.exception("Unexpected error during main execution:")

# --- Main Execution ---
if __name__ == '__main__':
    distribute_and_report()