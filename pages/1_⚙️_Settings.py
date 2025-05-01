# pages/1_⚙️_Settings.py
import streamlit as st
import yaml
from pathlib import Path
import os
import copy # Needed for deep copying settings

# --- Configuration ---
PROJECT_ROOT = Path(__file__).parent.parent.resolve() # Go up one level from pages
SETTINGS_FILE = PROJECT_ROOT / "settings.yaml"

# --- Helper Functions ---
def load_settings():
    """Loads settings from the YAML file."""
    if not SETTINGS_FILE.is_file():
        st.error(f"Settings file not found at: {SETTINGS_FILE}")
        # Initialize with default structure if not found? Or just return None.
        # Returning None seems safer to force user awareness.
        return None
    try:
        with open(SETTINGS_FILE, 'r') as f:
            # Use deepcopy to avoid modifying the original loaded dict accidentally
            return yaml.safe_load(f)
    except Exception as e:
        st.error(f"Error loading settings: {e}")
        return None

def save_settings(data):
    """Saves settings to the YAML file."""
    try:
        # Ensure 'stakeholders' is a list of dicts before saving
        if 'stakeholders' in data and isinstance(data['stakeholders'], list):
             data['stakeholders'] = [
                 {'name': s.get('name', ''), 'limit': int(s.get('limit', 0))}
                 for s in data['stakeholders'] if s.get('name') # Ensure name is not empty
            ]
        else:
            data['stakeholders'] = [] # Ensure it's at least an empty list

        with open(SETTINGS_FILE, 'w') as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)
        st.success("Settings saved successfully!")
        return True
    except Exception as e:
        st.error(f"Error saving settings: {e}")
        return False

# --- Initialize Session State ---
# Load initial settings into session state only once or if settings file changes
if 'settings_loaded' not in st.session_state:
    st.session_state.settings_cache = load_settings()
    st.session_state.settings_loaded = True
    if st.session_state.settings_cache:
        # Initialize editable fields in session state
        st.session_state.orders_spreadsheet_id = st.session_state.settings_cache.get('sheets', {}).get('orders_spreadsheet_id', '')
        st.session_state.abandoned_spreadsheet_id = st.session_state.settings_cache.get('sheets', {}).get('abandoned_spreadsheet_id', '')
        st.session_state.report_sheet_name = st.session_state.settings_cache.get('sheets', {}).get('report_sheet_name', '')
        # Use deepcopy for the list of dicts to avoid modifying cache indirectly
        st.session_state.stakeholders_list = copy.deepcopy(st.session_state.settings_cache.get('stakeholders', []))
    else:
        # Handle case where settings couldn't be loaded initially
        st.session_state.orders_spreadsheet_id = ''
        st.session_state.abandoned_spreadsheet_id = ''
        st.session_state.report_sheet_name = ''
        st.session_state.stakeholders_list = []


# --- Stakeholder Management Functions ---
def add_stakeholder():
    new_name = st.session_state.get('new_stakeholder_name', '').strip()
    new_limit = st.session_state.get('new_stakeholder_limit', 0)
    if new_name: # Only add if name is provided
        st.session_state.stakeholders_list.append({'name': new_name, 'limit': new_limit})
        # Clear the input fields after adding
        st.session_state.new_stakeholder_name = ""
        # Don't clear limit, maybe user wants to add another with same limit
        # st.session_state.new_stakeholder_limit = 0
    else:
        st.warning("Please enter a name for the new stakeholder.")

def remove_stakeholder(index_to_remove):
     if 0 <= index_to_remove < len(st.session_state.stakeholders_list):
        del st.session_state.stakeholders_list[index_to_remove]
     else:
        st.warning("Invalid index for stakeholder removal.") # Should not happen with button clicks

# --- Main Save Function ---
def save_all_settings():
    if st.session_state.settings_cache is None:
        st.error("Cannot save, initial settings failed to load.")
        return

    # Create a new settings dictionary based on current state
    # Start with a deep copy of the last known valid state or an empty dict
    new_settings_data = copy.deepcopy(st.session_state.settings_cache) if st.session_state.settings_cache else {}

    # Update Sheets section
    if 'sheets' not in new_settings_data: new_settings_data['sheets'] = {}
    new_settings_data['sheets']['orders_spreadsheet_id'] = st.session_state.get('orders_spreadsheet_id_input', '')
    new_settings_data['sheets']['abandoned_spreadsheet_id'] = st.session_state.get('abandoned_spreadsheet_id_input', '')
    new_settings_data['sheets']['report_sheet_name'] = st.session_state.get('report_sheet_name_input', '')

    # Update Files section (keep existing value)
    if 'files' not in new_settings_data: new_settings_data['files'] = {}
    # Ensure master_csv exists, even if empty initially or if settings were corrupted
    current_master_csv = new_settings_data.get('files',{}).get('master_csv', '')
    new_settings_data['files']['master_csv'] = current_master_csv # Preserve this value

    # Update Stakeholders from the dynamic list in session state
    current_stakeholders = []
    for i, _ in enumerate(st.session_state.stakeholders_list):
         # Retrieve potentially edited values directly from the input widget states
         name = st.session_state.get(f'stakeholder_name_{i}', '').strip()
         limit = st.session_state.get(f'stakeholder_limit_{i}', 0)
         if name: # Only include stakeholders with non-empty names
             current_stakeholders.append({'name': name, 'limit': int(limit)})

    new_settings_data['stakeholders'] = current_stakeholders

    # Perform the save
    if save_settings(new_settings_data):
        # Update the cache and session state inputs to reflect the saved state
        st.session_state.settings_cache = copy.deepcopy(new_settings_data)
        st.session_state.orders_spreadsheet_id = new_settings_data['sheets']['orders_spreadsheet_id']
        st.session_state.abandoned_spreadsheet_id = new_settings_data['sheets']['abandoned_spreadsheet_id']
        st.session_state.report_sheet_name = new_settings_data['sheets']['report_sheet_name']
        st.session_state.stakeholders_list = copy.deepcopy(new_settings_data['stakeholders'])
        st.success("Settings applied and internal state updated.")
        # Force rerun might clear momentary success message, let Streamlit handle it.
        # st.rerun()
    else:
        st.error("Failed to save settings. Please check logs or file permissions.")


# --- Page Content ---
st.set_page_config(page_title="Settings", page_icon="⚙️")
st.title("⚙️ Configure Settings")
st.markdown("Modify application settings and stakeholder details below.")

if st.session_state.settings_cache is None and SETTINGS_FILE.is_file():
    st.error("Failed to load settings initially. Cannot proceed with editing.")
elif not SETTINGS_FILE.is_file():
     st.warning(f"Settings file `{SETTINGS_FILE}` not found. You can configure and save settings here to create it.")
     # Allow editing even if file doesn't exist yet

# --- Editable Sheet Configuration ---
st.subheader("📄 Sheet Configuration")
st.text_input(
    "Orders Spreadsheet ID",
    value=st.session_state.get('orders_spreadsheet_id', ''), # Use session state value
    key='orders_spreadsheet_id_input' # Key to retrieve value on save
)
st.text_input(
    "Abandoned Spreadsheet ID",
    value=st.session_state.get('abandoned_spreadsheet_id', ''),
    key='abandoned_spreadsheet_id_input'
)
st.text_input(
    "Report Sheet Name",
    value=st.session_state.get('report_sheet_name', ''),
    key='report_sheet_name_input'
)

# --- Read-Only File Configuration ---
st.subheader("📁 File Configuration (Read-Only on this page)")
st.info("The Master CSV file is managed via the 'Order Status Update' page.")
# Display value from the cache if available
master_csv_display = st.session_state.settings_cache.get('files', {}).get('master_csv', 'N/A') if st.session_state.settings_cache else 'N/A'
st.text_input("Current Master CSV", master_csv_display, disabled=True)

st.divider()

# --- Dynamic Stakeholder Management ---
st.subheader("👥 Stakeholder Call Limits")
st.markdown("Add, remove, or edit stakeholders and their call limits.")

# Section to Add New Stakeholders
with st.expander("➕ Add New Stakeholder", expanded=False):
    col_add1, col_add2, col_add3 = st.columns([3, 1, 1])
    with col_add1:
        st.text_input("New Stakeholder Name", key="new_stakeholder_name", placeholder="Enter name...")
    with col_add2:
        st.number_input("Limit", min_value=0, step=1, key="new_stakeholder_limit")
    with col_add3:
        st.button("Add", on_click=add_stakeholder, use_container_width=True, key="add_stakeholder_button") # Add button triggers function

# Display Existing Stakeholders for Editing/Removal
if not st.session_state.stakeholders_list:
    st.info("No stakeholders defined yet. Use the 'Add New Stakeholder' section.")
else:
    st.markdown("---") # Separator
    # Header row (optional)
    col_h1, col_h2, col_h3 = st.columns([4, 2, 1])
    col_h1.markdown("**Name**")
    col_h2.markdown("**Limit**")
    col_h3.markdown("**Action**")

    # Iterate through the list in session state for dynamic display
    indices_to_remove = [] # To handle removals safely after iteration
    for i, stakeholder in enumerate(st.session_state.stakeholders_list):
        col1, col2, col3 = st.columns([4, 2, 1]) # Align with header
        with col1:
            st.text_input(
                f"Name##{i}", # Use ## for hidden label part
                value=stakeholder.get('name', ''),
                key=f"stakeholder_name_{i}", # Unique key for state
                label_visibility="collapsed" # Hide label visually
            )
        with col2:
            st.number_input(
                f"Limit##{i}",
                min_value=0,
                value=int(stakeholder.get('limit', 0)),
                key=f"stakeholder_limit_{i}",
                label_visibility="collapsed"
            )
        with col3:
            # The on_click directly calls the remove function with the index
            if st.button("➖ Remove", key=f"remove_{i}", use_container_width=True):
                 # We don't call remove_stakeholder directly here in the button's on_click
                 # because Streamlit reruns immediately. We mark for removal instead.
                 # Correction: on_click *is* suitable here if it modifies session state
                 # and we let Streamlit rerun. Let's stick with that.
                 remove_stakeholder(i)
                 st.rerun() # Rerun immediately to reflect removal in UI

st.divider()

# --- Save Button ---
st.button("💾 Save All Settings", on_click=save_all_settings, type="primary", use_container_width=True)

st.divider()

# --- Raw Settings View ---
st.subheader("⚙️ Raw Settings File Content (After Save)")
with st.expander("View Current `settings.yaml` Content"):
     # Attempt to load and display the *current* file content
     current_file_settings = load_settings()
     if current_file_settings:
         try:
            st.code(yaml.dump(current_file_settings, sort_keys=False, default_flow_style=False), language='yaml')
         except Exception as e:
             st.error(f"Could not format current settings for display: {e}")
     elif SETTINGS_FILE.is_file():
          st.warning("Could not reload settings file for display after potential save.")
     else:
          st.info("Settings file does not exist yet.")