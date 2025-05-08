# dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import yaml
import sys
from pathlib import Path
import re # For parsing potentially non-numeric counts

# --- Google Sheets Authentication & Data Loading ---
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Page Configuration ---
st.set_page_config(
    page_title="Shopify CRM Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Determine Project Root Directory ---
PROJECT_ROOT = Path(__file__).parent.resolve()
SETTINGS_FILE = PROJECT_ROOT / "settings.yaml"
PAGES_DIR = PROJECT_ROOT / "pages"
SERVICE_ACCOUNT_FILE = PROJECT_ROOT / 'molten-medley-458604-j9-855f3bdefd90.json' # Ensure filename is correct

# --- Load Settings ---
@st.cache_data # Cache settings loading
def load_dashboard_settings():
    try:
        with open(SETTINGS_FILE, 'r') as f:
            settings = yaml.safe_load(f)
        # Validate essential settings needed for the dashboard homepage
        if not settings or 'sheets' not in settings or \
           'orders_spreadsheet_id' not in settings['sheets'] or \
           'summary_sheet_name' not in settings['sheets']:
            st.error("Settings file is missing required sheet configuration (orders_spreadsheet_id, summary_sheet_name).")
            return None
        return settings
    except FileNotFoundError:
        st.error(f"Error: `settings.yaml` not found at `{SETTINGS_FILE}`. Please create it.")
        return None
    except Exception as e:
        st.error(f"Error loading settings: {e}")
        return None

settings = load_dashboard_settings()

# Define constants from settings if loaded, else use placeholders/stop
if settings:
    ORDERS_SPREADSHEET_ID = settings['sheets']['orders_spreadsheet_id']
    SUMMARY_SHEET_NAME = settings['sheets']['summary_sheet_name']
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'] # Readonly needed
else:
    st.error("Dashboard cannot load due to missing settings.")
    st.stop() # Stop execution if settings are missing

# --- Authentication Function (Streamlit Specific - Reusable) ---
@st.cache_resource(ttl=3600)
def authenticate_google_sheets_st():
    creds = None
    try:
        creds_info = st.secrets["GOOGLE_CREDENTIALS"].to_dict()
        # st.info("Using Streamlit secrets for authentication.")
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    except Exception:
        # st.info("Streamlit secrets not found. Falling back to local service account file.")
        if not SERVICE_ACCOUNT_FILE.is_file():
             st.error(f"Service account file not found: {SERVICE_ACCOUNT_FILE}")
             return None
        try:
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        except Exception as e:
            st.error(f"Error loading credentials from file: {e}")
            return None
    if creds is None: return None
    try:
        service = build('sheets', 'v4', credentials=creds)
        # st.success("Google Sheets authenticated successfully.")
        return service
    except Exception as e:
        st.error(f"Error building Sheets service: {e}")
        return None

# --- Data Loading and Parsing for Summary Sheet ---
@st.cache_data(ttl=600)
def load_summary_data(_service, spreadsheet_id, sheet_name):
    """Reads the Summary sheet and parses key metrics."""
    if not _service: return None
    st.info(f"Loading data from summary sheet: '{sheet_name}'")
    try:
        # Read a wider range to be safe
        range_to_read = f"'{sheet_name}'!A1:I30" # Adjust if your summary expands
        result = _service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_to_read
        ).execute()
        values = result.get('values', [])
    except HttpError as e:
        st.error(f"API Error reading sheet '{sheet_name}': {e}")
        return None
    except Exception as e:
        st.error(f"Unexpected error reading sheet '{sheet_name}': {e}")
        return None

    if not values:
        st.warning(f"No data found in summary sheet: '{sheet_name}'")
        return None

    # --- Parsing Logic ---
    # Use a dictionary to store parsed values {Metric_Name: Count}
    summary_metrics = {}

    def safe_int_parse(value_str):
        """Safely attempts to convert a string to an int, handling commas etc."""
        if not isinstance(value_str, str):
             value_str = str(value_str) # Convert potential numbers/other types to string first
        # Remove commas, whitespace, potentially percentage signs if needed
        cleaned_str = re.sub(r'[,\s%]', '', value_str).strip()
        try:
            # Handle potential non-numeric values gracefully
            if cleaned_str.isdigit():
                 return int(cleaned_str)
            else:
                 # Attempt float conversion if it looks like a number with decimals, then int
                 try:
                      return int(float(cleaned_str))
                 except ValueError:
                      # If cleaning and conversion fail, return 0 or None
                      return 0 # Default to 0 if parsing fails
        except ValueError:
            return 0 # Default to 0 if any conversion error occurs

    # Iterate through rows to find specific metrics
    for row in values:
        if not row or len(row) < 2: continue # Need at least Metric and Count columns

        metric_label = str(row[0]).strip()
        count_val_raw = row[1] # Value from Col B

        # Parse metrics from columns A & B
        if metric_label: # Only process if label exists
             count = safe_int_parse(count_val_raw)
             summary_metrics[metric_label] = count

        # Parse metrics from columns F & G (High-Level Summary)
        if len(row) > 6: # Check if columns F & G exist
             metric_label_f = str(row[5]).strip() # Col F (index 5)
             count_val_g_raw = row[6] # Col G (index 6)
             if metric_label_f:
                 count_g = safe_int_parse(count_val_g_raw)
                 # Avoid overwriting if label exists from Col A, maybe prefix?
                 summary_metrics[f"{metric_label_f} (Summary)"] = count_g # Add suffix

        # Parse metrics from columns F & G (Shipment/Payment Statuses - lower section)
        # These labels might conflict, need careful handling or specific row checks
        # Example: Check if row index is within the expected range for these summaries
        row_index = values.index(row) # Get current row index (0-based)

        # --- Specific Parsing for Shipment/Payment (needs adjustment based on exact row numbers) ---
        # This requires knowing the exact rows or more robust label finding
        # For demonstration, let's assume these specific labels are unique enough for now
        
        if metric_label == "Cod": # Check label in Col A for Cod/Prepaid Totals
             if len(row) > 7: # Check if Col H exists
                  cod_count = safe_int_parse(row[7]) # Count seems to be in Col H for Cod/Prepaid
                  summary_metrics["Cod Orders"] = cod_count
                  
        if metric_label == "Prepaid": # Check label in Col A
             if len(row) > 7:
                  prepaid_count = safe_int_parse(row[7])
                  summary_metrics["Prepaid Orders"] = prepaid_count

        # Add more specific parsing logic here if needed based on layout

    if not summary_metrics:
         st.warning("Failed to parse any key metrics from the Summary sheet.")
         return None

    # st.success(f"Successfully loaded and parsed summary data. Found {len(summary_metrics)} metrics.")
    return summary_metrics


# --- Main Page Content ---
st.title("🛒 Shopify CRM Dashboard")
st.sidebar.success("Select a tool above.")

# Display basic info (optional)
# st.info(f"Project Root: `{PROJECT_ROOT}`")
# st.info(f"Settings File: `{SETTINGS_FILE}`")
# if SETTINGS_FILE.is_file(): st.success("`settings.yaml` found.")
# else: st.error("`settings.yaml` not found.")

st.markdown("### Overall CRM Summary")
st.caption("Data loaded from the 'Summery' sheet.")

# --- Authenticate and Load Data ---
service = authenticate_google_sheets_st()
summary_data = None
if service and ORDERS_SPREADSHEET_ID:
    summary_data = load_summary_data(service, ORDERS_SPREADSHEET_ID, SUMMARY_SHEET_NAME)

# --- Display Dashboard Elements ---
if summary_data:
    # --- Top Row Metrics ---
    st.markdown("#### Key Totals")
    m_cols = st.columns(4) # Create 4 columns for metrics

    total_orders = summary_data.get('Total Orders', 0)
    confirmed_orders = summary_data.get('Confirmed', 0)
    # Use the "Total Delivered (Summary)" if available, else the one from Col A/B
    delivered_orders = summary_data.get('Total Delivered (Summary)', summary_data.get('Delivered', 0))
    # Use the "Total Cancelled (Summary)" if available
    cancelled_orders = summary_data.get('Total Cancelled (Summary)', summary_data.get('Cancelled', 0))

    m_cols[0].metric("Total Orders", f"{total_orders:,}")
    m_cols[1].metric("Confirmed Orders", f"{confirmed_orders:,}",
                     f"{confirmed_orders/total_orders:.1%}" if total_orders else "0%")
    m_cols[2].metric("Delivered Orders", f"{delivered_orders:,}",
                     f"{delivered_orders/total_orders:.1%}" if total_orders else "0%")
    m_cols[3].metric("Cancelled Orders", f"{cancelled_orders:,}",
                     f"{cancelled_orders/total_orders:.1%}" if total_orders else "0%")

    st.markdown("---") # Separator

    # --- Charts Row 1 ---
    st.markdown("#### Status Breakdowns")
    chart_cols1 = st.columns(2)

    # --- Pie Chart: High-Level Order Funnel ---
    with chart_cols1[0]:
        st.markdown("**Order Funnel Status**")
        # Use summary values preferentially
        delivered = summary_data.get('Total Delivered (Summary)', 0)
        rto = summary_data.get('Total RTO (Summary)', 0)
        dispatched = summary_data.get('Total Dispatched (Summary)', 0) # 'Dispatched' includes Delivered+RTO+InTransit
        cancelled = summary_data.get('Total Cancelled (Summary)', 0)
        in_progress = summary_data.get('Calling In Progress (Summary)', 0) # May need adjustment based on label

        # Estimate 'In Transit' if possible: Dispatched - Delivered - RTO
        in_transit_est = max(0, dispatched - delivered - rto) # Ensure non-negative

        # Estimate 'Pending / New / Confirmation'
        pending_new = max(0, total_orders - (delivered + rto + in_transit_est + cancelled + in_progress))

        funnel_data = {
            'Status': ['Delivered', 'RTO', 'In Transit (Est.)', 'Cancelled', 'Calling In Progress', 'Pending/New'],
            'Count': [delivered, rto, in_transit_est, cancelled, in_progress, pending_new]
        }
        df_funnel = pd.DataFrame(funnel_data)
        df_funnel = df_funnel[df_funnel['Count'] > 0] # Only plot categories with counts

        if not df_funnel.empty:
            fig_funnel = px.pie(df_funnel, names='Status', values='Count',
                                title="High-Level Order Status", hole=0.4)
            fig_funnel.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=400)
            st.plotly_chart(fig_funnel, use_container_width=True)
        else:
            st.info("Insufficient data for Funnel Status chart.")

    # --- Pie Chart: COD vs Prepaid ---
    with chart_cols1[1]:
        st.markdown("**Payment Method**")
        cod_count = summary_data.get('Cod Orders', 0) # Using parsed key
        prepaid_count = summary_data.get('Prepaid Orders', summary_data.get('Prepaid',0)) # Use specific key or fallback

        if cod_count > 0 or prepaid_count > 0:
            payment_data = {'Method': ['COD', 'Prepaid'], 'Count': [cod_count, prepaid_count]}
            df_payment = pd.DataFrame(payment_data)
            fig_payment = px.pie(df_payment, names='Method', values='Count',
                                 title="COD vs. Prepaid Orders", hole=0.4,
                                 color_discrete_map={'COD':'skyblue', 'Prepaid':'lightgreen'})
            fig_payment.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=400)
            st.plotly_chart(fig_payment, use_container_width=True)
        else:
            st.info("No COD/Prepaid data found.")

    st.markdown("---") # Separator

    # --- Charts Row 2 ---
    st.markdown("#### Detailed Statuses")
    chart_cols2 = st.columns(2)

    # --- Bar Chart: Calling / Initial Status ---
    with chart_cols2[0]:
        st.markdown("**Calling Status (Initial)**")
        call_status_metrics = [
            "Confirmation Pending", "Call didn't Pick", "Number invalid/fake order",
            "Follow Up", "Whatsapp msg-sent"
            # Add others from Col A if relevant like "Confirmed", "Cancelled" pre-fulfillment
        ]
        call_status_data = {metric: summary_data.get(metric, 0) for metric in call_status_metrics}
        # Add Confirmed/Cancelled counts if they primarily represent pre-dispatch outcomes
        call_status_data["Confirmed (Pre-Dispatch?)"] = summary_data.get('Confirmed', 0)
        call_status_data["Cancelled (Pre-Dispatch?)"] = summary_data.get('Cancelled', 0)

        df_call_status = pd.DataFrame(list(call_status_data.items()), columns=['Status', 'Count'])
        df_call_status = df_call_status[df_call_status['Count'] > 0].sort_values('Count', ascending=False)

        if not df_call_status.empty:
            fig_call = px.bar(df_call_status, x='Status', y='Count', title="Calling & Initial Status Counts")
            fig_call.update_layout(yaxis_title="Number of Orders", height=400)
            st.plotly_chart(fig_call, use_container_width=True)
        else:
            st.info("No detailed calling status data found.")

    # --- Bar Chart: Fulfillment Status ---
    with chart_cols2[1]:
        st.markdown("**Fulfillment / Post-Dispatch Status**")
        fulfillment_metrics = [
            "Delivered", "Out for delivery", "In-transit", "Pending Pick up",
            "NDR", "RTO", "Pending To Be Dispatch", "Cancel" # Note: Cancel might be pre or post dispatch
        ]
        fulfillment_data = {metric: summary_data.get(metric, 0) for metric in fulfillment_metrics}
        df_fulfillment = pd.DataFrame(list(fulfillment_data.items()), columns=['Status', 'Count'])
        df_fulfillment = df_fulfillment[df_fulfillment['Count'] > 0].sort_values('Count', ascending=False)

        if not df_fulfillment.empty:
            fig_fulfill = px.bar(df_fulfillment, x='Status', y='Count', title="Fulfillment & Post-Dispatch Status Counts")
            fig_fulfill.update_layout(yaxis_title="Number of Orders", height=400)
            st.plotly_chart(fig_fulfill, use_container_width=True)
        else:
            st.info("No detailed fulfillment status data found.")


    st.markdown("---")
    st.caption("Note: Chart data is parsed directly from the 'Summery' sheet. Ensure the sheet format and labels are consistent.")

elif service:
    st.warning("Could not load or parse data from the Summary sheet. Please check sheet name, format, and permissions.")
else:
    st.error("Google Sheets Authentication failed. Cannot load summary data.")

# --- Add link to other pages (Optional footer) ---
st.sidebar.markdown("---")
st.sidebar.info("Navigate using the options above.")