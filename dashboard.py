# dashboard.py
import streamlit as st
import os
from pathlib import Path

# --- Page Configuration ---
st.set_page_config(
    page_title="Shopify CRM Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Determine Project Root Directory ---
# Assumes dashboard.py is in the project root
PROJECT_ROOT = Path(__file__).parent.resolve()
SETTINGS_FILE = PROJECT_ROOT / "settings.yaml"
PAGES_DIR = PROJECT_ROOT / "pages"

# --- Main Page Content ---
st.title("🛒 Shopify CRM Dashboard")
st.sidebar.success("Select a tool above.")

st.markdown(
    """
    Welcome to the Shopify CRM Dashboard.

    Use the sidebar to navigate between different tools:

    - **⚙️ Settings:** Configure stakeholder limits and view sheet/file settings.
    - **📞 Call Distribution:** Run the script to distribute calls to stakeholders based on limits.
    - **📊 Order Status Update:** Upload the latest master report and run the script to update Google Sheets.

    ---
    """
)

st.info(f"Project Root: `{PROJECT_ROOT}`")
st.info(f"Settings File: `{SETTINGS_FILE}`")

# You can add more overview information or status checks here if needed.
# For example, check if settings.yaml exists
if not SETTINGS_FILE.is_file():
    st.error(f"Error: `settings.yaml` not found at `{SETTINGS_FILE}`. Please create it.")
else:
    st.success(f"`settings.yaml` found.")

# Check if scripts exist (optional but good practice)
dist_script = PROJECT_ROOT / "distributionV2.py"
update_script = PROJECT_ROOT / "order_status_update.py"

if not dist_script.is_file():
    st.warning(f"`distributionV2.py` not found at `{dist_script}`.")
if not update_script.is_file():
    st.warning(f"`order_status_update.py` not found at `{update_script}`.")