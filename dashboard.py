# dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go # For more complex charts like funnels
import yaml
from pathlib import Path
import re

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

# --- NEW: More Structured Data Parsing for Summary Sheet ---
@st.cache_data(ttl=300) # Cache for 5 minutes
def load_and_parse_summary_sheet(_service, spreadsheet_id, sheet_name):
    if not _service: return None, "Authentication service not available."
    st.info(f"Loading data from summary sheet: '{sheet_name}'")
    try:
        # Read a fairly large, fixed range that covers all expected data blocks.
        # This is simpler than trying to dynamically find blocks if the layout is stable.
        range_to_read = f"'{sheet_name}'!A1:H30" # Adjust H30 if your sheet grows beyond
        result = _service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_to_read
        ).execute()
        raw_values = result.get('values', [])
    except HttpError as e:
        return None, f"API Error reading sheet '{sheet_name}': {e}"
    except Exception as e:
        return None, f"Unexpected error reading sheet '{sheet_name}': {e}"

    if not raw_values:
        return None, f"No data found in summary sheet: '{sheet_name}'"

    # Create a DataFrame for easier slicing and searching (optional, but can be helpful)
    # Pad rows to have consistent length for DataFrame creation
    max_cols = max(len(row) for row in raw_values) if raw_values else 0
    padded_values = [row + [''] * (max_cols - len(row)) for row in raw_values]
    df_summary = pd.DataFrame(padded_values)

    parsed_data = {
        "kpis": {},
        "call_statuses": {},
        "fulfillment_statuses": {},
        "high_level_summary": {},
        "payment_summary": {}
    }
    
    def safe_int(val_str):
        if val_str is None or val_str == '': return 0
        # Remove non-numeric characters except decimal point if present
        cleaned = re.sub(r'[^\d\.]', '', str(val_str))
        try:
            return int(float(cleaned)) # Convert to float first to handle "1,202.00" then int
        except (ValueError, TypeError):
            return 0

    # --- Parse Block 1: Initial Call/Order Statuses (A2:C12 approx) ---
    # Assuming labels in col 0 (A), counts in col 1 (B)
    # Define expected labels to look for to make it robust
    expected_call_labels = [
        "Total Orders", "Confirmed", "COD to Prepaid", "Cancelled",
        "Call didn't Pick", "Whatsapp msg-sent", "Follow Up",
        "Number invalid/fake order", "Prepaid", "NDR", "Confirmation Pending"
    ]
    for r_idx in range(1, 15): # Iterate roughly where these labels are (rows 2 to 16)
        if r_idx < len(df_summary):
            label = str(df_summary.iloc[r_idx, 0]).strip()
            if label in expected_call_labels:
                parsed_data["call_statuses"][label] = safe_int(df_summary.iloc[r_idx, 1])
                if label == "Total Orders": # Also a KPI
                    parsed_data["kpis"]["Total Orders (Block1)"] = safe_int(df_summary.iloc[r_idx, 1])


    # --- Parse Block 2: High-Level Summary (F2:H7 approx) ---
    # Labels in col 5 (F), counts in col 6 (G)
    expected_high_level_labels = {
        "Total Orders": "Total Orders (Summary)", # Sheet Label : Key in parsed_data
        "Total Dispatched": "Total Dispatched",
        "Total Pending To Dispatch": "Total Pending To Dispatch",
        "Total Cancelled": "Total Cancelled (Summary)",
        "Total RTO": "Total RTO (Summary)",
        "Call In Progress": "Call In Progress"
    }
    for r_idx in range(1, 10): # Iterate roughly where these are
         if r_idx < len(df_summary) and len(df_summary.columns) > 6:
            label = str(df_summary.iloc[r_idx, 5]).strip()
            if label in expected_high_level_labels:
                data_key = expected_high_level_labels[label]
                parsed_data["high_level_summary"][data_key] = safe_int(df_summary.iloc[r_idx, 6])
                if data_key == "Total Orders (Summary)": # Also a KPI
                     parsed_data["kpis"]["Total Orders (Summary)"] = safe_int(df_summary.iloc[r_idx, 6])


    # --- Parse Block 3: Fulfillment/Detailed Statuses (A14:C21 approx) ---
    # Labels in col 0 (A), counts in col 1 (B)
    expected_fulfillment_labels = [
        "Delivered", "Out for delivery", "In-transit", "Pending Pick up",
        "Cancel", "RTO", "Pending To Be Dispatch"
        # "Confirmation Pending" appears here again, decide how to handle (overwrite or sum?)
    ]
    # Handle potential duplicate "Confirmation Pending" - maybe use the one from call_statuses
    # or sum if they represent different things. For now, we'll take the first one from Block 1.
    for r_idx in range(13, 25): # Iterate roughly where these are
        if r_idx < len(df_summary):
            label = str(df_summary.iloc[r_idx, 0]).strip()
            if label in expected_fulfillment_labels:
                 # Avoid overwriting if label also exists in call_statuses (e.g. "Cancel")
                 # unless it's explicitly a fulfillment version.
                 if label == "Cancel" and "Cancelled" in parsed_data["call_statuses"]:
                     # Assuming this "Cancel" in fulfillment block is more specific or a duplicate
                     # For now, let's prioritize the one from high-level summary if available
                     if "Total Cancelled (Summary)" not in parsed_data["high_level_summary"]:
                        parsed_data["fulfillment_statuses"][label] = safe_int(df_summary.iloc[r_idx, 1])
                 else:
                    parsed_data["fulfillment_statuses"][label] = safe_int(df_summary.iloc[r_idx, 1])


    # --- Parse Block 5: Payment Summary (F21:H23 approx) ---
    # Assuming "Cod" label in F22, "Prepaid" in F23, counts in G22, G23
    # Based on image: Col F has label, Col G has count (for this specific block)
    # The image shows Cod/Prepaid counts in Col H relative to their labels in Col A though...
    # Let's reconcile. The image seems to show "Cod" in A22, count in B22.
    # And "Prepaid" in A10, count in B10.
    # The block around F21-H23 has Cod/Prepaid labels in F and counts in G. Let's try that.
    if 21 < len(df_summary) and len(df_summary.columns) > 6:
        cod_label = str(df_summary.iloc[21, 5]).strip() # Expected "Cod" at F22 (index 21,5)
        if "Cod" in cod_label: # Flexible match
            parsed_data["payment_summary"]["COD"] = safe_int(df_summary.iloc[21, 6])
    if 22 < len(df_summary) and len(df_summary.columns) > 6:
        prepaid_label = str(df_summary.iloc[22, 5]).strip() # Expected "Prepaid" at F23 (index 22,5)
        if "Prepaid" in prepaid_label:
            parsed_data["payment_summary"]["Prepaid"] = safe_int(df_summary.iloc[22, 6])
    
    # Fallback if the F column parsing didn't work, use values from call_statuses
    if "COD" not in parsed_data["payment_summary"] :
        parsed_data["payment_summary"]["COD"] = parsed_data["call_statuses"].get("Total Orders",0) - parsed_data["call_statuses"].get("Prepaid",0)

    if "Prepaid" not in parsed_data["payment_summary"] or parsed_data["payment_summary"]["Prepaid"] == 0:
         parsed_data["payment_summary"]["Prepaid"] = parsed_data["call_statuses"].get("Prepaid",0)


    # --- Final KPI calculations and cleanup ---
    # Prefer summary block total orders if available
    parsed_data["kpis"]["Total Orders"] = parsed_data["kpis"].get("Total Orders (Summary)", parsed_data["kpis"].get("Total Orders (Block1)", 0))
    
    # Confirmed orders (main KPI)
    parsed_data["kpis"]["Confirmed"] = parsed_data["call_statuses"].get("Confirmed", 0)

    # Dispatched from high-level summary
    parsed_data["kpis"]["Dispatched"] = parsed_data["high_level_summary"].get("Total Dispatched", 0)
    
    # Delivered from fulfillment statuses
    parsed_data["kpis"]["Delivered"] = parsed_data["fulfillment_statuses"].get("Delivered", 0)

    # RTO from fulfillment or high-level
    parsed_data["kpis"]["RTO"] = parsed_data["high_level_summary"].get("Total RTO (Summary)", parsed_data["fulfillment_statuses"].get("RTO",0) )
    
    # Cancelled from high-level or call_statuses
    parsed_data["kpis"]["Cancelled"] = parsed_data["high_level_summary"].get("Total Cancelled (Summary)", parsed_data["call_statuses"].get("Cancelled",0))

    # Call In Progress
    parsed_data["kpis"]["Call In Progress"] = parsed_data["high_level_summary"].get("Call In Progress", 0)
    if parsed_data["kpis"]["Call In Progress"] == 0: # Estimate from "Confirmation Pending" if Call In Progress is 0
        parsed_data["kpis"]["Call In Progress"] = parsed_data["call_statuses"].get("Confirmation Pending",0)


    if not parsed_data["kpis"].get("Total Orders"):
        return None, "Failed to parse 'Total Orders' metric, essential for dashboard."

    st.success(f"Summary data parsed. Total Orders: {parsed_data['kpis']['Total Orders']}")
    return parsed_data, None # Data, Error Message

# --- Main Page Content ---
st.title("🛒 CRM & Order Fulfillment Dashboard")
st.sidebar.success("Select a tool or view above.")

# --- Authenticate and Load Data ---
service = authenticate_google_sheets_st()
parsed_summary_data, error_msg = None, None

if service:
    parsed_summary_data, error_msg = load_and_parse_summary_sheet(service, ORDERS_SPREADSHEET_ID, SUMMARY_SHEET_NAME)
    if error_msg:
        st.error(error_msg)
elif not service:
     st.error("Google Sheets Authentication failed. Cannot load summary data.")


# --- NEW Dashboard Layout ---
if parsed_summary_data:
    kpis = parsed_summary_data["kpis"]
    call_statuses = parsed_summary_data["call_statuses"]
    fulfillment_statuses = parsed_summary_data["fulfillment_statuses"]
    high_level = parsed_summary_data["high_level_summary"]
    # payment_summary = parsed_summary_data["payment_summary"] # You mentioned it's all prepaid

    total_orders_main = kpis.get("Total Orders", 0)

    # --- Section 1: KPIs ---
    st.header("📊 Key Performance Indicators")
    kpi_cols = st.columns(5)
    kpi_cols[0].metric("Total Orders", f"{total_orders_main:,}")
    
    confirmed = kpis.get("Confirmed", 0)
    kpi_cols[1].metric("Confirmed", f"{confirmed:,}", f"{confirmed/total_orders_main:.1%}" if total_orders_main else "0%",
                       help="Orders confirmed by calling team.")

    dispatched = kpis.get("Dispatched", 0)
    kpi_cols[2].metric("Dispatched", f"{dispatched:,}", f"{dispatched/total_orders_main:.1%}" if total_orders_main else "0%",
                       help="Total orders handed over for delivery (includes In-Transit, Delivered, RTO initiated).")
    
    delivered = kpis.get("Delivered", 0)
    kpi_cols[3].metric("Delivered", f"{delivered:,}", f"{delivered/total_orders_main:.1%}" if total_orders_main else "0%",
                       delta_color="inverse", help="Successfully delivered to customer.")
    
    cancelled = kpis.get("Cancelled", 0) # Using the most reliable 'Cancelled' figure
    kpi_cols[4].metric("Cancelled", f"{cancelled:,}", f"{-cancelled/total_orders_main:.1%}" if total_orders_main else "0%", # Negative for loss
                       delta_color="normal", help="Orders cancelled before or after confirmation.")
    
    st.markdown("---")

    # --- Section 2: Order Lifecycle Funnel & Calling Insights ---
    st.header("📞 Calling Performance & Order Lifecycle")
    lc_cols = st.columns([2, 1]) # 2/3 for funnel, 1/3 for calling stats

    with lc_cols[0]:
        st.subheader("Order Lifecycle Funnel")
        # Define funnel stages based on available data
        # This needs careful thought based on your process flow
        total_orders = kpis.get("Total Orders", 0)
        confirmed = kpis.get("Confirmed", 0) # Confirmed by calling
        dispatched = kpis.get("Dispatched",0) # From summary block F
        delivered = kpis.get("Delivered",0) # From block A or F
        rto = kpis.get("RTO",0)

        # Calculate intermediate/loss stages for the funnel
        pending_confirmation = call_statuses.get("Confirmation Pending", 0) + \
                               call_statuses.get("Call didn't Pick",0) + \
                               call_statuses.get("Follow Up",0) + \
                               kpis.get("Call In Progress",0)
        
        # Ensure this isn't double counting with 'Cancelled' KPI
        cancelled_pre_dispatch = call_statuses.get("Cancelled", 0) # Assuming this is pre-dispatch cancel

        # Dispatched but not yet delivered or RTO'd = In Transit / Out for Delivery
        in_transit_ofd = max(0, dispatched - delivered - rto)


        funnel_stages = [
            go.Funnel(
                name="Main Funnel",
                y=["Total Orders", "Pending Confirmation", "Confirmed", "Dispatched", "In Transit/OFD", "Delivered"],
                x=[total_orders_main, pending_confirmation, confirmed, dispatched, in_transit_ofd, delivered],
                textposition="inside",
                textinfo="value+percent initial",
                opacity=0.65,
                marker={"color": ["deepskyblue", "lightsalmon", "tan", "teal", "silver","lightgreen"]},
                connector={"line": {"color": "royalblue", "dash": "dot", "width": 3}}
            )
        ]
        # You can add another trace for losses (e.g., Cancelled at each stage)
        losses_x = [total_orders - pending_confirmation, pending_confirmation - confirmed, confirmed - dispatched, dispatched - in_transit_ofd, in_transit_ofd - delivered]
        losses_y = ["Lost Pre-Confirm", "Lost Pre-Dispatch", "Lost Pre-Transit", "Lost Pre-Delivery", "RTO/Lost In-Transit"]

        fig_funnel = go.Figure(funnel_stages)
        fig_funnel.update_layout(
            title_text="Order Processing Funnel",
            margin=dict(l=50, r=50, t=50, b=20),
            height=450
        )
        st.plotly_chart(fig_funnel, use_container_width=True)

    with lc_cols[1]:
        st.subheader("Calling Team Status")
        # Data for pie chart - focus on actionable calling statuses
        calling_pie_data = {
            "Confirmation Pending": call_statuses.get("Confirmation Pending",0),
            "Call didn't Pick": call_statuses.get("Call didn't Pick",0),
            "Follow Up": call_statuses.get("Follow Up",0),
            "Number Invalid/Fake": call_statuses.get("Number invalid/fake order",0),
            "Whatsapp Sent": call_statuses.get("Whatsapp msg-sent",0),
            # "Confirmed by Call": call_statuses.get("Confirmed",0) # Already a KPI
        }
        df_calling_pie = pd.DataFrame(list(calling_pie_data.items()), columns=['Status', 'Count'])
        df_calling_pie = df_calling_pie[df_calling_pie['Count'] > 0]

        if not df_calling_pie.empty:
            fig_pie_call = px.pie(df_calling_pie, names='Status', values='Count', hole=0.4,
                                  title="Current Calling Pipeline")
            fig_pie_call.update_traces(textposition='inside', textinfo='percent+label')
            fig_pie_call.update_layout(legend_title_text='Calling Statuses', height=450, margin=dict(t=50))
            st.plotly_chart(fig_pie_call, use_container_width=True)
        else:
            st.info("No active calling pipeline data.")

    st.markdown("---")

    # --- Section 3: Fulfillment Insights ---
    st.header("🚚 Fulfillment & Dispatch Overview")
    ff_cols = st.columns(2)

    with ff_cols[0]:
        st.subheader("Current Shipment Statuses")
        # Focus on post-dispatch statuses
        shipment_data = {
            "Delivered": fulfillment_statuses.get("Delivered", 0),
            "Out for Delivery": fulfillment_statuses.get("Out for delivery", 0),
            "In-Transit": fulfillment_statuses.get("In-transit", 0),
            "Pending Pick up": fulfillment_statuses.get("Pending Pick up", 0),
            "RTO (Post-Dispatch)": kpis.get("RTO", 0), # Using cleaned RTO KPI
            "Pending To Be Dispatch": high_level.get("Total Pending To Dispatch", fulfillment_statuses.get("Pending To Be Dispatch",0))
        }
        df_shipment = pd.DataFrame(list(shipment_data.items()), columns=['Status', 'Count'])
        df_shipment = df_shipment[df_shipment['Count'] > 0].sort_values(by="Count", ascending=False)

        if not df_shipment.empty:
            fig_shipment = px.bar(df_shipment, x="Status", y="Count", color="Status",
                                  title="Shipment Progress",
                                  labels={"Count": "Number of Orders"})
            fig_shipment.update_layout(xaxis_title="", yaxis_title="Orders", showlegend=False, height=400)
            st.plotly_chart(fig_shipment, use_container_width=True)
        else:
            st.info("No shipment status data to display.")

    with ff_cols[1]:
        st.subheader("Non-Delivery Reasons")
        non_delivery_data = {
            "Cancelled": kpis.get("Cancelled", 0), # Overall cancelled
            "RTO": kpis.get("RTO", 0),
            "Invalid/Fake Order": call_statuses.get("Number invalid/fake order", 0),
            "NDR (Calling)": call_statuses.get("NDR", 0) # Assuming this is from calling stage
        }
        df_non_delivery = pd.DataFrame(list(non_delivery_data.items()), columns=['Reason', 'Count'])
        df_non_delivery = df_non_delivery[df_non_delivery['Count'] > 0]

        if not df_non_delivery.empty:
            fig_non_delivery = px.pie(df_non_delivery, names='Reason', values='Count', hole=0.4,
                                      title="Reasons for Non-Delivery/Cancellation")
            fig_non_delivery.update_traces(textposition='inside', textinfo='percent+label')
            fig_non_delivery.update_layout(legend_title_text='Reasons', height=400, margin=dict(t=50))
            st.plotly_chart(fig_non_delivery, use_container_width=True)
        else:
            st.info("No data for non-delivery reasons.")
            
    st.markdown("---")
    # --- Data Table (Optional - for detailed view) ---
    with st.expander("View Raw Parsed Summary Data"):
        st.json(parsed_summary_data) # Display all parsed data for debugging/verification

else:
    if service and not error_msg: # Service connected but parsing failed somehow (should be caught by error_msg)
        st.warning("Summary data loaded but could not be parsed into a displayable format. Check parsing logic and sheet structure.")

# --- Footer ---
st.sidebar.markdown("---")
st.sidebar.info("Dashboard showing CRM summary. Navigate using options above for other tools.")