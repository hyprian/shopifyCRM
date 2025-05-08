# pages/4_📊_Performance_Report.py

import streamlit as st
import pandas as pd
import datetime
import plotly.express as px
import sys
from pathlib import Path
import re # For parsing the breakdown string

# --- Google Sheets Authentication (Simplified for Streamlit page) ---
# Add necessary imports from the other scripts if needed
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Assume PROJECT_ROOT and necessary constants are accessible or redefined
# If running as part of a larger streamlit app, these might be inherited
# If running standalone, define them here.
try:
    # Try importing from a potential utils script or the main dashboard script
    # This assumes your project structure allows finding 'authenticate_google_sheets'
    # You might need to adjust sys.path or import structure
    # Example: Add project root to path if pages are nested
    PROJECT_ROOT = Path(__file__).parent.parent.resolve()
    sys.path.append(str(PROJECT_ROOT))
    # Attempt to import from distributionV2 (might be messy) or a dedicated utils.py
    # For simplicity here, we'll redefine a minimal auth function
    # Ideally, refactor auth to a shared utils.py
except ImportError:
    st.error("Could not configure project paths correctly. Ensure structure allows imports.")
    st.stop()

# Constants (should ideally come from settings or a central config)
SERVICE_ACCOUNT_FILE = PROJECT_ROOT / 'molten-medley-458604-j9-855f3bdefd90.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'] # READONLY scope is enough here
PERFORMANCE_REPORT_SHEET_NAME = 'Performance Reports'
# Assume ORDERS_SPREADSHEET_ID is known or loaded from settings
# For this example, let's hardcode it - replace with settings loading
try:
    import yaml
    SETTINGS_FILE = PROJECT_ROOT / 'settings.yaml'
    with open(SETTINGS_FILE, 'r') as f:
        settings = yaml.safe_load(f)
    ORDERS_SPREADSHEET_ID = settings['sheets']['orders_spreadsheet_id']
except Exception as e:
    st.error(f"Error loading ORDERS_SPREADSHEET_ID from settings.yaml: {e}")
    ORDERS_SPREADSHEET_ID = "YOUR_SPREADSHEET_ID_HERE" # Fallback placeholder
    st.warning(f"Using fallback Spreadsheet ID: {ORDERS_SPREADSHEET_ID}. Please ensure settings.yaml is correct.")


# --- Authentication Function (Streamlit Specific) ---
@st.cache_resource(ttl=3600) # Cache resource for 1 hour
def authenticate_google_sheets_st():
    """Authenticates using Streamlit secrets or local service account file."""
    creds = None
    try:
        # Prioritize Streamlit secrets if available
        creds_info = st.secrets["GOOGLE_CREDENTIALS"].to_dict()
        st.info("Using Streamlit secrets for authentication.")
        creds = service_account.Credentials.from_service_account_info(
            creds_info, scopes=SCOPES)
    except Exception:
        # st.info("Streamlit secrets not found. Falling back to local service account file.")
        try:
            creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        except FileNotFoundError:
            st.error(f"Service account file not found at: {SERVICE_ACCOUNT_FILE}")
            return None
        except Exception as e:
            st.error(f"Error loading credentials from file: {e}")
            return None

    if creds is None:
        st.error("Authentication failed. Could not load credentials.")
        return None

    try:
        service = build('sheets', 'v4', credentials=creds)
        # st.success("Google Sheets authenticated successfully.")
        return service
    except HttpError as e:
        st.error(f"API Error building Sheets service: {e}")
        return None
    except Exception as e:
        st.error(f"Unexpected error building Sheets service: {e}")
        return None

@st.cache_data(ttl=300) # Reduce cache time during debugging
def load_and_parse_performance_data(_service, spreadsheet_id, sheet_name):
    """Reads the performance report sheet and parses it into a DataFrame (Simplified Logic)."""
    # st.info(f"Attempting to load data from sheet: '{sheet_name}'")
    if not _service:
        st.error("Authentication service not available.")
        return pd.DataFrame(), []

    try:
        read_range = f"'{sheet_name}'!A:J" # Read wide
        # st.info(f"Reading range: {read_range}")
        result = _service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=read_range
        ).execute()
        values = result.get('values', [])
        # st.info(f"Read {len(values)} rows from the sheet.")
    except HttpError as e:
        st.error(f"API Error reading sheet '{sheet_name}': {e}")
        # ... (error handling for auth) ...
        return pd.DataFrame(), []
    except Exception as e:
        st.error(f"Unexpected error reading sheet '{sheet_name}': {e}")
        return pd.DataFrame(), []

    if not values:
        st.warning(f"No data found in performance report sheet: '{sheet_name}'")
        return pd.DataFrame(), []

    parsed_data = []
    available_dates = set()
    current_date = None
    current_stakeholder = None
    
    # Define expected categories to identify data rows
    expected_categories = {"Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"}

    for i, row in enumerate(values):
        # Uncomment below to see raw rows during debugging
        # st.write(f"Processing Row {i+1}: {row}")

        if not row or not any(str(c).strip() for c in row): continue # Skip blank rows

        cell_a_value = str(row[0]).strip()

        # --- Identify Context: Date and Stakeholder ---
        report_header_prefix = "--- Stakeholder Performance Report for "
        if cell_a_value.startswith(report_header_prefix):
            match = re.search(r"for\s+(.+?)\s+---", cell_a_value)
            if match:
                current_date = match.group(1).strip()
                available_dates.add(current_date)
                current_stakeholder = None # Reset stakeholder when new date found
            continue

        stakeholder_prefix = "Stakeholder:"
        if cell_a_value.startswith(stakeholder_prefix):
            current_stakeholder = cell_a_value.replace(stakeholder_prefix, "").strip()
            continue

        # --- Attempt to Parse Data Rows Directly ---
        # Check if we have date/stakeholder context AND if row looks like data
        if current_date and current_stakeholder:
            # Condition: First cell is one of the expected categories AND row has enough numeric-like columns
            if cell_a_value in expected_categories and len(row) >= 4:
                category = cell_a_value
                assigned_str = str(row[1]).strip()
                actioned_str = str(row[2]).strip()
                pending_str = str(row[3]).strip()
                breakdown_str = str(row[4]).strip() if len(row) > 4 else ""

                # Check if the next few columns look like numbers
                if assigned_str.isdigit() and actioned_str.isdigit() and pending_str.isdigit():
                     # Uncomment below for debugging parsing attempts
                     # st.write(f"  -> Looks like data row: {row[:5]}")
                     try:
                        assigned = int(assigned_str)
                        actioned = int(actioned_str)
                        pending = int(pending_str)

                        parsed_data.append({
                            "Date": current_date, "Stakeholder": current_stakeholder, "Category": category,
                            "Assigned": assigned, "Actioned": actioned, "Pending": pending,
                            "Breakdown": breakdown_str
                        })
                        # Uncomment below to confirm successful parsing
                        # st.write(f"    -> Successfully parsed and added.")
                     except ValueError:
                          st.warning(f"Row {i+1}: Failed converting to int even after isdigit check (should not happen). Row: {row}")
                          continue
                # else:
                     # Uncomment below to see why a potential data row was skipped
                     # st.write(f"  -> Row {i+1} skipped: First cell matched category '{category}', but cols 2-4 not all digits ('{assigned_str}', '{actioned_str}', '{pending_str}').")


    # st.info(f"Finished processing rows. Parsed {len(parsed_data)} data records using simplified logic.")

    if not parsed_data:
        st.warning("Simplified parsing logic also failed to extract structured data.")
        if values:
            st.subheader("Raw Data Snippet (First 20 Rows)")
            st.dataframe(pd.DataFrame(values[:20]))
        # ... (return empty df and sorted dates) ...
        try:
            sorted_dates = sorted(list(available_dates), key=lambda d: datetime.datetime.strptime(d, '%d-%b-%Y'), reverse=True)
        except Exception:
             sorted_dates = sorted(list(available_dates), reverse=True)
        return pd.DataFrame(), sorted_dates

    # --- Post-processing DataFrame ---
    df = pd.DataFrame(parsed_data)
    try:
        df['Date_dt'] = pd.to_datetime(df['Date'], format='%d-%b-%Y')
    except Exception:
        st.warning("Could not parse all dates into datetime objects. Sorting may be alphabetical.")
        df['Date_dt'] = df['Date']

    sorted_dates = sorted(list(available_dates), key=lambda d: datetime.datetime.strptime(d, '%d-%b-%Y'), reverse=True)
    return df, sorted_dates


# --- Streamlit Page Configuration & UI ---
st.set_page_config(page_title="Performance Report", page_icon="📊", layout="wide")
st.title("📊 Stakeholder Performance Report")
st.markdown("View historical performance based on the generated daily reports.")

# --- Authentication ---
service = authenticate_google_sheets_st()

if service:
    # --- Load and Parse Data ---
    perf_df, report_dates = load_and_parse_performance_data(service, ORDERS_SPREADSHEET_ID, PERFORMANCE_REPORT_SHEET_NAME)

    if not report_dates:
        st.warning("No performance report dates found or parsed.")
        st.stop()
    if perf_df.empty:
         st.warning("Dataframe is empty after parsing. Cannot display report.")
         st.stop()

    # --- Date Selection ---
    selected_date = st.selectbox(
        "Select Report Date:",
        report_dates,
        index=0 # Default to the latest date
    )

    if selected_date:
        df_filtered_date = perf_df[perf_df['Date'] == selected_date].copy()

        if df_filtered_date.empty:
            st.warning(f"No performance data parsed for the selected date: {selected_date}")
            st.stop()

        # --- Stakeholder Selection ---
        stakeholder_list = sorted(df_filtered_date['Stakeholder'].unique())
        all_stakeholders_option = "All Stakeholders"
        # Use columns for better layout
        col_filter1, col_filter2 = st.columns([1, 3]) # Adjust column ratios if needed
        with col_filter1:
            selected_stakeholder = st.selectbox(
                "Select Stakeholder:",
                [all_stakeholders_option] + stakeholder_list,
                key="stakeholder_select" # Add unique key
            )

        # --- Filter by Stakeholder ---
        if selected_stakeholder == all_stakeholders_option:
            df_display = df_filtered_date
            title_level = "Overall"
        else:
            df_display = df_filtered_date[df_filtered_date['Stakeholder'] == selected_stakeholder]
            title_level = f"for {selected_stakeholder}"

        st.subheader(f"{title_level} Performance Summary ({selected_date})")

        if df_display.empty:
            st.warning("No data available for the current selection.")
            st.stop()

        # --- Display Key Metrics in Boxes (Columns) ---
        total_assigned = df_display['Assigned'].sum()
        total_actioned = df_display['Actioned'].sum()
        total_pending = df_display['Pending'].sum()
        
        # Calculate totals from assignment report for comparison (more accurate 'Assigned')
        # This requires parsing the TOTAL line or summing categories from the df
        # For now, use the sum from the parsed df rows
        # assigned_from_report = # Need to parse TOTAL row if available or sum categories

        metric_cols = st.columns(3)
        metric_cols[0].metric("Assigned Today", f"{total_assigned:,}") # Format with comma
        metric_cols[1].metric("Actioned Today", f"{total_actioned:,}")
        metric_cols[2].metric("Pending Today", f"{total_pending:,}")
        
        # Optional: Show discrepancy if calculated assigned differs from report's total line
        # calculated_assigned_sum = total_actioned + total_pending # Check this sum
        # if assigned_from_report and assigned_from_report != calculated_assigned_sum:
        #     st.caption(f"Note: Assigned total from report was {assigned_from_report}. Discrepancy: {assigned_from_report-calculated_assigned_sum}")


        st.markdown("---") # Separator

        # --- Visualizations ---
        st.subheader("Visualizations")

        viz_cols = st.columns(2)

        with viz_cols[0]:
            st.markdown("**Performance by Initial Category**")
            if not df_display.empty:
                # Group data by category for charting
                df_grouped = df_display.groupby('Category')[['Assigned', 'Actioned', 'Pending']].sum().reset_index()
                df_melt = df_grouped.melt(
                    id_vars=['Category'],
                    value_vars=['Actioned', 'Pending'], # Chart actioned/pending only
                    var_name='Status',
                    value_name='Count'
                )
                fig_bar = px.bar(df_melt, x='Category', y='Count', color='Status',
                                 barmode='group', # Use group instead of stack for clearer comparison? or 'stack'
                                 title="Actioned vs. Pending by Category",
                                 color_discrete_map={'Actioned':'#1f77b4', 'Pending':'#ff7f0e'}) # Example colors
                fig_bar.update_layout(yaxis_title="Number of Tasks", height=400)
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("No category data to display.")

        with viz_cols[1]:
            st.markdown("**Actioned vs. Pending (Overall)**")
            if total_actioned > 0 or total_pending > 0:
                df_pie = pd.DataFrame({
                    'Status': ['Actioned', 'Pending'],
                    'Count': [total_actioned, total_pending]
                })
                fig_pie = px.pie(df_pie, names='Status', values='Count', hole=0.4,
                                 title="Overall Actioned/Pending Ratio",
                                 color_discrete_map={'Actioned':'#1f77b4', 'Pending':'#ff7f0e'})
                fig_pie.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=400)
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No actioned or pending tasks.")

        # --- Final Status Breakdown Chart ---
        if total_actioned > 0:
            st.markdown("---")
            st.subheader("Final Status Breakdown (Actioned Tasks)")
            final_status_counts = {}
            for breakdown_string in df_display[df_display['Actioned'] > 0]['Breakdown']:
                if breakdown_string and breakdown_string != '-':
                    parts = breakdown_string.split(',')
                    for part in parts:
                         match = re.match(r"^\s*(.+?)\s*:\s*(\d+)\s*$", part.strip())
                         if match:
                             status = match.group(1).strip()
                             try:
                                 count = int(match.group(2).strip())
                                 final_status_counts[status] = final_status_counts.get(status, 0) + count
                             except ValueError: pass # Ignore if count isn't integer
            
            if final_status_counts:
                df_final_status = pd.DataFrame(list(final_status_counts.items()), columns=['Final Status', 'Count'])
                df_final_status = df_final_status[df_final_status['Count'] > 0] # Only show statuses with counts > 0
                df_final_status = df_final_status.sort_values(by='Count', ascending=False)

                if not df_final_status.empty:
                    fig_final = px.bar(df_final_status, x='Final Status', y='Count',
                                       title="Breakdown of Actioned Task Outcomes")
                    fig_final.update_layout(yaxis_title="Number of Tasks")
                    st.plotly_chart(fig_final, use_container_width=True)
                else:
                    st.info("No final statuses with counts > 0 found in breakdown.")
            else:
                st.info("No detailed final status breakdown available or could not parse strings.")
        else:
             st.info("No tasks were actioned for the selected scope.")

    else:
        st.info("Select a date to view the performance report.")

else:
    st.warning("Google Sheets Authentication failed.")