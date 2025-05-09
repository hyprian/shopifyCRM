# pages/4_📊_Performance_Report.py

import streamlit as st
import pandas as pd
import datetime
import plotly.express as px
import plotly.graph_objects as go # For more advanced charts
import sys
from pathlib import Path
import re

# --- Project Setup & Constants ---
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.append(str(PROJECT_ROOT))

SERVICE_ACCOUNT_FILE = PROJECT_ROOT / 'molten-medley-458604-j9-855f3bdefd90.json' # Ensure filename
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
PERFORMANCE_REPORT_SHEET_NAME = 'Performance Reports' # From performance_analyzer.py

# --- Google Sheets Authentication & Data Loading (from your provided script) ---
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

@st.cache_data(show_spinner=False)
def load_settings_for_spreadsheet_id():
    try:
        import yaml
        SETTINGS_FILE = PROJECT_ROOT / 'settings.yaml'
        with open(SETTINGS_FILE, 'r') as f:
            settings_config = yaml.safe_load(f)
        return settings_config['sheets']['orders_spreadsheet_id']
    except Exception as e:
        st.error(f"Critical Error: Could not load ORDERS_SPREADSHEET_ID from settings.yaml: {e}")
        return None

ORDERS_SPREADSHEET_ID = load_settings_for_spreadsheet_id()

@st.cache_resource(ttl=3600)
def authenticate_google_sheets_st_perf(): # Renamed to avoid conflict if imported elsewhere
    creds = None
    try:
        creds_info = st.secrets["GOOGLE_CREDENTIALS"].to_dict()
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    except Exception:
        try:
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        except Exception as e:
            st.error(f"Perf Report Auth Error: {e}")
            return None
    if creds is None: return None
    return build('sheets', 'v4', credentials=creds)

# --- NEW: Robust Parser for the Text-Based Performance Report Sheet ---
@st.cache_data(ttl=300, show_spinner="Loading performance data...")
def load_and_parse_text_performance_report(_service, spreadsheet_id, sheet_name):
    if not _service: return pd.DataFrame(), [], "Authentication service not available."
    
    all_records = []
    available_dates = set()
    
    try:
        # Read a wider range to ensure all data for a day's report is captured.
        # The report seems to be appended, so this needs to be large enough for many days or we read all.
        # For simplicity in parsing, let's read a large chunk.
        # Adjust 'A:J' if your breakdown strings get very long.
        range_to_read = f"'{sheet_name}'!A1:J" # Read up to column J, all rows
        result = _service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_to_read
        ).execute()
        values = result.get('values', [])
    except HttpError as e:
        return pd.DataFrame(), [], f"API Error reading performance sheet '{sheet_name}': {e}"
    except Exception as e:
        return pd.DataFrame(), [], f"Unexpected error reading performance sheet '{sheet_name}': {e}"

    if not values:
        return pd.DataFrame(), [], f"No data found in performance report sheet: '{sheet_name}'."

    current_date_str = None
    current_stakeholder = None
    # Categories that mark a data row (excluding TOTAL)
    data_row_categories = ["Fresh", "Abandoned", "Invalid/Fake", "CNP", "Follow up", "NDR"]

    for row_num, row_cells in enumerate(values):
        if not row_cells or not any(str(c).strip() for c in row_cells): continue # Skip empty rows

        cell_a = str(row_cells[0]).strip()

        # Detect Report Date
        if cell_a.startswith("--- Stakeholder Performance Report for"):
            match_date = re.search(r"for\s+(.+?)\s+---", cell_a)
            if match_date:
                current_date_str = match_date.group(1).strip()
                available_dates.add(current_date_str)
                current_stakeholder = None # Reset stakeholder for new report block
            continue
        
        # Detect Stakeholder Block
        if cell_a.startswith("Stakeholder:"):
            current_stakeholder = cell_a.replace("Stakeholder:", "").strip()
            continue

        # Detect Header Row for categories (and skip it)
        if cell_a == "Initial Category" and len(row_cells) > 1 and str(row_cells[1]).strip() == "Assigned Today":
            continue
        
        # Detect Separator lines (and skip them)
        if cell_a.startswith("------------------"):
            continue

        # Parse Data Rows
        if current_date_str and current_stakeholder and cell_a in data_row_categories:
            if len(row_cells) >= 5: # Need at least Initial Cat, Assigned, Actioned, Pending, Breakdown
                try:
                    category = cell_a
                    assigned = int(str(row_cells[1]).strip())
                    actioned = int(str(row_cells[2]).strip())
                    pending = int(str(row_cells[3]).strip())
                    breakdown_raw = str(row_cells[4]).strip()

                    record = {
                        "Date": current_date_str,
                        "Stakeholder": current_stakeholder,
                        "Category": category,
                        "Assigned": assigned,
                        "Actioned": actioned,
                        "Pending": pending,
                        "RawBreakdown": breakdown_raw if breakdown_raw != '-' else ""
                    }
                    all_records.append(record)
                except ValueError:
                    # st.warning(f"Skipping row {row_num+1} for {current_stakeholder} on {current_date_str} due to number parsing error: {row_cells[:4]}")
                    pass # Silently skip if a row within a stakeholder block isn't numeric as expected
                except IndexError:
                    # st.warning(f"Skipping row {row_num+1} for {current_stakeholder} on {current_date_str} due to not enough columns.")
                    pass
        
        # Detect TOTAL row for a stakeholder (can be used for validation or overall numbers)
        if current_date_str and current_stakeholder and cell_a == "TOTAL":
            # Optionally parse totals here if needed for cross-checking
            current_stakeholder = None # Reset stakeholder after their total block to avoid misattribution

    if not all_records:
        return pd.DataFrame(), sorted(list(available_dates), reverse=True), "No structured performance records parsed."

    df = pd.DataFrame(all_records)
    
    # Convert Date string to datetime for proper sorting later if needed by selectors
    try:
        df['Date_dt'] = pd.to_datetime(df['Date'], format='%d-%b-%Y')
        # Sort available_dates chronologically (most recent first)
        sorted_dates = sorted(list(available_dates), key=lambda d: datetime.datetime.strptime(d, '%d-%b-%Y'), reverse=True)
    except ValueError: # Fallback if date format is inconsistent
        st.warning("Some dates in the report could not be parsed correctly. Sorting may be alphabetical.")
        df['Date_dt'] = df['Date'] # Keep as string
        sorted_dates = sorted(list(available_dates), reverse=True) # Simple reverse sort

    return df, sorted_dates, None # Data, Dates, Error Message


# --- Helper to Parse Breakdown String ---
def parse_breakdown_to_df(breakdown_series):
    """ Parses the 'Final Status Breakdown (Actioned)' string into a count dictionary. """
    all_status_counts = {}
    for breakdown_str in breakdown_series:
        if pd.isna(breakdown_str) or not breakdown_str.strip() or breakdown_str.strip() == '-':
            continue
        
        # Regex to find "Status Name: Count" pairs, robust to variations
        # Matches: "Status Name: Count", "Status Name : Count"
        # It will also handle "Other Actioned (Orders): 21"
        parts = breakdown_str.split(',')
        for part in parts:
            match = re.match(r"^\s*(.+?)\s*:\s*(\d+)\s*$", part.strip())
            if match:
                status_name = match.group(1).strip()
                try:
                    count = int(match.group(2))
                    all_status_counts[status_name] = all_status_counts.get(status_name, 0) + count
                except ValueError:
                    pass # Ignore if count is not a number
    
    if not all_status_counts:
        return pd.DataFrame(columns=['Final Status', 'Count'])
        
    return pd.DataFrame(list(all_status_counts.items()), columns=['Final Status', 'Count']).sort_values(by="Count", ascending=False)


# --- Page Styling (Optional) ---
# st.markdown("""
# <style>
# /* Add custom CSS here if needed */
# .stMetric {
#     border: 1px solid #_placeholder;
#     padding: 10px;
#     border-radius: 5px;
#     background-color: #f8f9fa;
# }
# </style>
# """, unsafe_allow_html=True)


# --- Streamlit Page UI ---
st.set_page_config(page_title="Stakeholder Performance", page_icon="🚀", layout="wide")
st.title("🚀 Stakeholder Performance Dashboard")
st.markdown("Analyze daily performance metrics and task outcomes.")
st.markdown("---")

# --- Authentication and Data Loading ---
if not ORDERS_SPREADSHEET_ID:
    st.error("Spreadsheet ID not loaded. Dashboard cannot proceed.")
    st.stop()

google_sheets_service = authenticate_google_sheets_st_perf()
perf_df, report_dates, error_message = pd.DataFrame(), [], "Initializing..." # Default values

if google_sheets_service:
    perf_df, report_dates, error_message = load_and_parse_text_performance_report(
        google_sheets_service, ORDERS_SPREADSHEET_ID, PERFORMANCE_REPORT_SHEET_NAME
    )
else:
    error_message = "Google Sheets authentication failed."

if error_message and not perf_df.empty: # Data loaded but there was a minor warning perhaps
    st.info(error_message)
elif error_message:
    st.error(error_message)
    st.stop()

if perf_df.empty:
    st.warning("No performance data available to display.")
    st.stop()

# --- Global Filters: Date and Stakeholder ---
filter_cols = st.columns([1, 2, 1]) # Date, Stakeholder, (empty for spacing or future filter)
selected_date = filter_cols[0].selectbox(
    "📅 Select Report Date",
    options=report_dates,
    index=0,
    help="Choose the date for which you want to view the performance report."
)

df_by_date = perf_df[perf_df['Date'] == selected_date].copy()

if df_by_date.empty:
    st.warning(f"No data found for selected date: {selected_date}")
    st.stop()

stakeholder_options = [None] + sorted(df_by_date['Stakeholder'].unique()) # Add "None" for overall view
selected_stakeholder = filter_cols[1].selectbox(
    "👤 Select Stakeholder (Optional)",
    options=stakeholder_options,
    format_func=lambda x: "Overall Team" if x is None else x,
    index=0,
    help="View performance for a specific stakeholder or the overall team."
)

# --- Filter Data Based on Selection ---
if selected_stakeholder:
    df_display = df_by_date[df_by_date['Stakeholder'] == selected_stakeholder].copy()
    report_title_prefix = f"{selected_stakeholder}'s"
else:
    df_display = df_by_date.copy() # For overall, we'll aggregate
    report_title_prefix = "Overall Team"

st.header(f"{report_title_prefix} Performance Summary – {selected_date}")

if df_display.empty and selected_stakeholder: # Check if a specific stakeholder had no data for that date
    st.info(f"No data found for {selected_stakeholder} on {selected_date}.")
    st.stop()
elif df_display.empty:
     st.info(f"No data found for {selected_date}.") # Should be caught earlier by df_by_date check
     st.stop()


# --- KPIs / Metrics Section ---
# Aggregate if "Overall Team" is selected
if not selected_stakeholder: # Overall view
    kpi_assigned = df_display['Assigned'].sum()
    kpi_actioned = df_display['Actioned'].sum()
    kpi_pending = df_display['Pending'].sum()
else: # Single stakeholder view
    kpi_assigned = df_display['Assigned'].sum()
    kpi_actioned = df_display['Actioned'].sum()
    kpi_pending = df_display['Pending'].sum()

action_rate = (kpi_actioned / kpi_assigned * 100) if kpi_assigned > 0 else 0

kpi_cols_main = st.columns(4)
kpi_cols_main[0].metric("Total Assigned", f"{kpi_assigned:,}", help="Total tasks assigned from the daily distribution.")
kpi_cols_main[1].metric("Total Actioned", f"{kpi_actioned:,}", help="Tasks whose status changed from the initial assignment.")
kpi_cols_main[2].metric("Action Rate", f"{action_rate:.1f}%", help="(Actioned / Assigned) * 100")
kpi_cols_main[3].metric("Total Pending", f"{kpi_pending:,}", help="Tasks still in their initial assigned state.")
st.markdown("---")


# --- Detailed Breakdown and Visualizations ---
st.subheader("Performance Details by Initial Category")

# If overall, group by category first
if not selected_stakeholder:
    df_category_summary = df_display.groupby("Category")[["Assigned", "Actioned", "Pending"]].sum().reset_index()
else:
    df_category_summary = df_display[["Category", "Assigned", "Actioned", "Pending"]] # Already filtered

# Melt for grouped bar chart
df_melt = df_category_summary.melt(
    id_vars=['Category'],
    value_vars=['Actioned', 'Pending'], # Focus on outcome
    var_name='Status',
    value_name='Count'
)
# Add Assigned back for context if needed, or plot separately
df_assigned_for_chart = df_category_summary[["Category", "Assigned"]]


col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.markdown("**Task Status by Initial Category**")
    if not df_melt.empty:
        fig_cat_status = px.bar(
            df_melt,
            x='Category',
            y='Count',
            color='Status',
            barmode='group',
            title="Actioned vs. Pending Tasks",
            labels={"Count": "Number of Tasks", "Category": "Initial Assignment Category"},
            color_discrete_map={'Actioned': 'mediumseagreen', 'Pending': 'coral'}
        )
        fig_cat_status.update_layout(legend_title_text='Task Status', height=450)
        st.plotly_chart(fig_cat_status, use_container_width=True)
    else:
        st.info("No data to display for category status breakdown.")

with col_chart2:
    st.markdown("**Overall Actioned vs. Pending Distribution**")
    if kpi_actioned > 0 or kpi_pending > 0:
        df_pie_overall = pd.DataFrame({
            'Status': ['Actioned', 'Pending'],
            'Count': [kpi_actioned, kpi_pending]
        })
        fig_pie_overall = px.pie(df_pie_overall, names='Status', values='Count',
                                 title="Overall Task Completion", hole=0.5,
                                 color_discrete_map={'Actioned': 'mediumseagreen', 'Pending': 'coral'})
        fig_pie_overall.update_traces(textinfo='percent+label')
        fig_pie_overall.update_layout(height=450, margin=dict(t=50)) # legend_title_text='Overall Status',
        st.plotly_chart(fig_pie_overall, use_container_width=True)
    else:
        st.info("No overall actioned or pending data.")

st.markdown("---")

# --- Final Status Breakdown of Actioned Tasks ---
st.subheader("Outcomes of Actioned Tasks")
if kpi_actioned > 0:
    # Aggregate RawBreakdown strings if 'Overall Team'
    if not selected_stakeholder:
        actioned_breakdowns_series = df_display[df_display['Actioned'] > 0]['RawBreakdown']
    else:
        actioned_breakdowns_series = df_display[df_display['Actioned'] > 0]['RawBreakdown']
        
    df_final_statuses = parse_breakdown_to_df(actioned_breakdowns_series)

    if not df_final_statuses.empty:
        # Show as a bar chart
        fig_final_breakdown_bar = px.bar(
            df_final_statuses,
            x="Final Status",
            y="Count",
            color="Final Status",
            title=f"Final Statuses for {kpi_actioned} Actioned Tasks",
            labels={"Count": "Number of Tasks"}
        )
        fig_final_breakdown_bar.update_layout(showlegend=False, height=450, xaxis_tickangle=-45)
        st.plotly_chart(fig_final_breakdown_bar, use_container_width=True)

        # Option to show as a treemap for a different visual
        with st.expander("View Final Statuses as Treemap & Table"):
            fig_final_treemap = px.treemap(
                df_final_statuses,
                path=[px.Constant("All Actioned"), 'Final Status'],
                values='Count',
                title="Treemap of Final Statuses"
            )
            fig_final_treemap.update_layout(margin = dict(t=50, l=25, r=25, b=25))
            st.plotly_chart(fig_final_treemap, use_container_width=True)
            st.dataframe(df_final_statuses.set_index("Final Status"), use_container_width=True)

    else:
        st.info("No detailed final status breakdown available for actioned tasks, or breakdown strings were empty/unparsable.")
else:
    st.info("No tasks were actioned in the selected scope to show final status breakdown.")


# --- Display Raw Filtered Data (Optional) ---
with st.expander("View Filtered Data Table"):
    st.dataframe(df_display.drop(columns=['Date_dt'], errors='ignore'), use_container_width=True)