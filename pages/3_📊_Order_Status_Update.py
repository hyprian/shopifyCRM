# pages/3_📊_Order_Status_Update.py
import streamlit as st
import subprocess
import time
import sys
import yaml
from pathlib import Path
import os
import select # For non-blocking reads later

# --- Configuration ---
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
SETTINGS_FILE = PROJECT_ROOT / "settings.yaml"
SCRIPT_PATH = PROJECT_ROOT / "order_status_update.py"
PYTHON_EXECUTABLE = sys.executable # Use the same python that runs streamlit

# --- Helper Functions ---  <<<< MOVED UP
# (Consider moving load/save to a utils.py file)
def load_settings():
    """Loads settings, returns None on failure."""
    if not SETTINGS_FILE.is_file():
        # Don't show error here, let calling function decide
        return None
    try:
        with open(SETTINGS_FILE, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        # Use st.warning for non-critical load errors during init maybe?
        # Or just let it return None silently during init.
        # st.warning(f"Warning: Error loading settings file ({SETTINGS_FILE}): {e}")
        print(f"Warning: Error loading settings file ({SETTINGS_FILE}): {e}") # Print if st isn't safe yet
        return None

def save_settings(data):
    """Saves settings, returns True/False."""
    try:
        with open(SETTINGS_FILE, 'w') as f:
            yaml.dump(data, f, sort_keys=False, default_flow_style=False)
        return True
    except Exception as e:
        st.error(f"Error saving settings: {e}")
        return False

def run_script():
    """Starts the order status update script after performing checks."""
    st.info("Attempting to start script...") # Feedback

    if not SCRIPT_PATH.is_file():
        st.error(f"❌ Script not found: {SCRIPT_PATH}")
        return # Stop execution

    settings = load_settings() # Calling load_settings defined above
    if not settings:
        st.error("❌ Cannot run script: Failed to load `settings.yaml`.")
        return # Stop execution
    if 'files' not in settings or 'master_csv' not in settings.get('files', {}):
         st.error("❌ Cannot run script: `files -> master_csv` path not found in `settings.yaml`.")
         return # Stop execution

    master_csv_filename = settings['files'].get('master_csv') # Use .get() for safety
    if not master_csv_filename:
        st.error("❌ Cannot run script: `master_csv` value in `settings.yaml` is empty or missing.")
        return # Stop execution

    master_csv_path = PROJECT_ROOT / master_csv_filename
    st.info(f"Checking for master CSV at: {master_csv_path}") # Feedback

    if not master_csv_path.is_file():
         st.error(f"❌ Cannot run script: Master CSV file '{master_csv_filename}' not found at expected location.")
         st.warning("Did you upload and confirm the file first?")
         return # Stop execution

    # --- All checks passed, proceed to run ---
    st.info(f"✅ Pre-run checks passed. Starting `{SCRIPT_PATH.name}`...")
    try:
        process = subprocess.Popen(
            [PYTHON_EXECUTABLE, str(SCRIPT_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # Capture errors too
            text=True,
            bufsize=1,
            universal_newlines=True,
            cwd=PROJECT_ROOT,
            encoding='utf-8',
            errors='replace' # Handle potential encoding errors in output
        )
        st.session_state.order_process = process
        st.session_state.order_running = True
         # Reset output only when starting
        st.session_state.order_output = f"Starting script: {SCRIPT_PATH.name}...\nUsing master CSV: {master_csv_filename}\n" + "="*30 + "\n"
        st.success(f"🚀 Started script: {SCRIPT_PATH.name}")

    except Exception as e:
        st.error(f"❌ Failed to start script process: {e}")
        st.session_state.order_running = False
        st.session_state.order_process = None # Clear process on failure

def stop_script():
    """Stops the running order status update script."""
    proc = st.session_state.get('order_process')
    if proc and st.session_state.get('order_running'):
        try:
            # Add to output before stopping
            st.session_state.order_output += "\n" + "="*30 + "\nStopping script...\n"
            proc.terminate()
            try:
                proc.wait(timeout=5)
                st.session_state.order_output += "Script terminated gracefully.\n"
            except subprocess.TimeoutExpired:
                st.warning("Process did not terminate gracefully, sending SIGKILL.")
                proc.kill()
                st.session_state.order_output += "Script force killed.\n"
            except Exception as wait_err:
                 st.warning(f"Error during process wait: {wait_err}")
                 st.session_state.order_output += f"Script stop state uncertain: {wait_err}\n"

            st.info("Script stop initiated.")
        except Exception as e:
            st.error(f"Error stopping script: {e}")
            st.session_state.order_output += f"\nError during stop: {e}\n"
        finally:
             # Always update state regardless of exceptions during stop
            st.session_state.order_process = None
            st.session_state.order_running = False


# --- Initialize Session State --- <<< NOW RUNS AFTER FUNCTIONS ARE DEFINED
if 'order_process' not in st.session_state:
    st.session_state.order_process = None
if 'order_output' not in st.session_state:
    st.session_state.order_output = "" # Stores accumulated output
if 'order_running' not in st.session_state:
    st.session_state.order_running = False
if 'current_master_csv_display' not in st.session_state:
     # Initialize display value from settings if possible
     # This call is now valid because load_settings is defined above
     settings_init = load_settings()
     st.session_state.current_master_csv_display = settings_init.get('files', {}).get('master_csv', 'N/A') if settings_init else 'N/A'


# --- Page Content ---
st.set_page_config(page_title="Order Status Update", page_icon="📊")
st.title("📊 Run Order Status Update")
st.markdown(f"Upload a new Master Report CSV and run the `{SCRIPT_PATH.name}` script.")
st.divider()

# --- File Upload Section ---
st.subheader("📤 Upload New Master Report")
st.markdown("""
Drag and drop your CSV file onto the box below, or click 'Browse files' to select it.
**Important:** After selecting a file, you **must** click the **'Confirm & Replace Master CSV'** button that appears below to process it before starting the update script.
""")
uploaded_file = st.file_uploader(
    "Drop CSV here or Browse", # Simplified label
    type=["csv"],
    accept_multiple_files=False,
    # help="This component supports drag-and-drop. Drop your file directly onto this area." # Default help is usually sufficient
    key="order_csv_uploader" # Add a key for stability
)

if uploaded_file is not None:
    # Show info about the *staged* file
    st.info(f"File selected: `{uploaded_file.name}` ({uploaded_file.size} bytes). Click below to process.")

    # Confirmation button - This performs the actual update
    if st.button(f"✅ Confirm & Replace Master CSV with `{uploaded_file.name}`", key="confirm_upload_button"):
        st.info("Processing uploaded file...")
        settings = load_settings() # Calling function defined above
        if settings is None:
             # Attempt to create a default settings structure if file missing/corrupt
             st.warning("`settings.yaml` not found or failed to load. Creating default structure.")
             settings = {'sheets': {}, 'files': {}, 'stakeholders': []}

        # Safely get old CSV name using .get() with defaults
        old_csv_name = settings.get('files', {}).get('master_csv')
        old_csv_path = PROJECT_ROOT / old_csv_name if old_csv_name else None

        new_csv_name = uploaded_file.name
        new_csv_path = PROJECT_ROOT / new_csv_name

        try:
            # 1. Save the new file
            with open(new_csv_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            st.success(f"💾 Successfully saved new master report as `{new_csv_name}`.")

            # 2. Update settings dictionary (ensure 'files' key exists)
            if 'files' not in settings: settings['files'] = {}
            settings['files']['master_csv'] = new_csv_name

            # 3. Save updated settings back to YAML
            if save_settings(settings): # Calling function defined above
                st.success(f"⚙️ Updated `settings.yaml` to use `{new_csv_name}`.")
                # Update the display variable in session state
                st.session_state.current_master_csv_display = new_csv_name

                # 4. Delete the old file (if different and exists)
                if old_csv_path and old_csv_path.is_file() and old_csv_path != new_csv_path:
                    try:
                        os.remove(old_csv_path)
                        st.info(f"🗑️ Removed old master report file: `{old_csv_name}`.")
                    except Exception as del_err:
                        st.warning(f"⚠️ Could not remove old file `{old_csv_name}`: {del_err}")
                elif old_csv_path == new_csv_path:
                     st.info("ℹ️ New filename is the same as the old one. File overwritten.")

                st.rerun() # Rerun to update the display and button states

            else:
                st.error("❌ Failed to update `settings.yaml`. The new file was saved, but the configuration wasn't updated.")

        except Exception as e:
            st.error(f"❌ An error occurred during file processing: {e}")

st.divider()

# --- Script Execution Section ---
st.subheader(f"🚀 Execute `{SCRIPT_PATH.name}`")

# Display the *current* master CSV from session state (updated by confirm button)
st.markdown(f"This will use the currently configured Master CSV: `{st.session_state.get('current_master_csv_display', 'N/A')}`")

col1, col2 = st.columns(2)
with col1:
    # Disable button if already running
    start_disabled = st.session_state.get('order_running', False)
    if st.button("▶️ Start Update", disabled=start_disabled, type="primary"):
        run_script() # Calling function defined above
        st.rerun() # Rerun to show feedback from run_script and update buttons

with col2:
    # Disable button if not running
    stop_disabled = not st.session_state.get('order_running', False)
    if st.button("⏹️ Stop Update", disabled=stop_disabled):
        stop_script() # Calling function defined above
        st.rerun() # Rerun to update button state and stop output loop

# --- Live Output Area ---
st.subheader("📜 Live Script Output")
# Create ONE placeholder for the code output
output_placeholder = st.empty()

# Display current accumulated output in the placeholder
output_placeholder.code(st.session_state.get('order_output', "No output yet."), language="bash")

# If running, continuously check for new output
if st.session_state.get('order_running') and st.session_state.get('order_process'):
    proc = st.session_state.order_process
    try:
        # Non-blocking read setup (optional, better for responsiveness on Linux/Mac)
        if os.name != 'nt':
            import fcntl
            flags = fcntl.fcntl(proc.stdout, fcntl.F_GETFL)
            fcntl.fcntl(proc.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        while True: # Loop to read output line-by-line
            line = proc.stdout.readline()
            if line:
                st.session_state.order_output += line
                # Update the *content* of the placeholder
                output_placeholder.code(st.session_state.order_output, language="bash")
            else:
                # Check if process is still running
                return_code = proc.poll()
                if return_code is not None:
                    # Process finished
                    st.session_state.order_output += "\n" + "="*30 + "\n" # Separator
                    if return_code == 0:
                        st.session_state.order_output += "Script finished successfully.\n"
                        st.success("Script finished successfully.")
                    else:
                        st.session_state.order_output += f"Script finished with Exit Code: {return_code}\n"
                        st.error(f"Script finished with errors (Exit Code: {return_code}).")

                    # Update final output and clean up state
                    output_placeholder.code(st.session_state.order_output, language="bash")
                    st.session_state.order_process = None
                    st.session_state.order_running = False
                    st.rerun() # Rerun to update button states etc.
                    break # Exit the read loop

            # Small sleep to prevent busy-waiting and allow UI to refresh
            time.sleep(0.1)

    except Exception as e:
        # Handle exceptions during output reading
        st.error(f"An error occurred while reading script output: {e}")
        st.session_state.order_output += f"\nError reading output: {e}\n"
        output_placeholder.code(st.session_state.order_output, language="bash") # Show error in output
        stop_script() # Try to stop the script if reading fails
        st.rerun()