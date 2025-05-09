# pages/5_⚙️_Generate_Performance_Report.py

import streamlit as st
import subprocess
import time
import sys
from pathlib import Path
import os
import select # For non-blocking read on Unix-like systems
import datetime # Added for timestamp handling

# --- Configuration ---
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
SCRIPT_NAME = "performance_analyzer.py"
SCRIPT_PATH = PROJECT_ROOT / SCRIPT_NAME
PYTHON_EXECUTABLE = sys.executable

# --- NEW: Timestamp File ---
LAST_PERF_RUN_TIMESTAMP_FILE = PROJECT_ROOT / "last_perf_run.txt"

# --- Initialize Session State ---
if 'perf_analyzer_process' not in st.session_state:
    st.session_state.perf_analyzer_process = None
if 'perf_analyzer_output' not in st.session_state:
    st.session_state.perf_analyzer_output = ""
if 'perf_analyzer_running' not in st.session_state:
    st.session_state.perf_analyzer_running = False
if 'perf_analyzer_last_run_status_msg' not in st.session_state: # For success/failure message
    st.session_state.perf_analyzer_last_run_status_msg = "<p>Script is idle.</p>" # Default message
if 'perf_analyzer_last_success_time' not in st.session_state: # To store the actual last success time
    st.session_state.perf_analyzer_last_success_time = None


# --- Helper Functions ---
def run_performance_script():
    if not SCRIPT_PATH.is_file():
        st.error(f"Script not found: {SCRIPT_PATH}")
        st.session_state.perf_analyzer_last_run_status_msg = f"<p style='color:red;'>Script not found: {SCRIPT_PATH.name}</p>"
        return
    try:
        process = subprocess.Popen(
            [PYTHON_EXECUTABLE, str(SCRIPT_PATH)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            bufsize=1, universal_newlines=True, cwd=PROJECT_ROOT,
            encoding='utf-8', errors='replace'
        )
        st.session_state.perf_analyzer_process = process
        st.session_state.perf_analyzer_running = True
        st.session_state.perf_analyzer_output = f"🚀 Starting Performance Analyzer: {SCRIPT_NAME}...\n" + "="*40 + "\n"
        st.session_state.perf_analyzer_last_run_status_msg = "<p style='color:blue;'>Script started...</p>"
        st.success(f"Performance Analyzer script '{SCRIPT_NAME}' started.")
    except Exception as e:
        st.error(f"Failed to start Performance Analyzer script: {e}")
        st.session_state.perf_analyzer_running = False
        st.session_state.perf_analyzer_process = None
        st.session_state.perf_analyzer_last_run_status_msg = f"<p style='color:red;'>Failed to start script: {e}</p>"

def stop_performance_script():
    proc = st.session_state.get('perf_analyzer_process')
    if proc and st.session_state.get('perf_analyzer_running'):
        try:
            st.session_state.perf_analyzer_output += "\n" + "="*40 + "\n🛑 Stopping script...\n"
            proc.terminate()
            try:
                proc.wait(timeout=5)
                st.session_state.perf_analyzer_output += "Script terminated.\n"
                st.session_state.perf_analyzer_last_run_status_msg = "<p style='color:orange;'>Script terminated by user.</p>"
            except subprocess.TimeoutExpired:
                st.warning("Script did not terminate gracefully, killing.")
                proc.kill()
                st.session_state.perf_analyzer_output += "Script force killed.\n"
                st.session_state.perf_analyzer_last_run_status_msg = "<p style='color:orange;'>Script force killed by user.</p>"
            st.info("Script stop initiated.")
        except Exception as e:
            st.error(f"Error stopping script: {e}")
            st.session_state.perf_analyzer_output += f"\nError during stop: {e}\n"
            st.session_state.perf_analyzer_last_run_status_msg = f"<p style='color:red;'>Error stopping script: {e}</p>"
        finally:
            st.session_state.perf_analyzer_process = None
            st.session_state.perf_analyzer_running = False

# --- NEW: Function to read last successful run time ---
def get_last_successful_run_time():
    if LAST_PERF_RUN_TIMESTAMP_FILE.is_file():
        try:
            with open(LAST_PERF_RUN_TIMESTAMP_FILE, "r") as f:
                timestamp_str = f.read().strip()
            if timestamp_str:
                dt_obj = datetime.datetime.fromisoformat(timestamp_str)
                return dt_obj.strftime("%d-%b-%Y %I:%M:%S %p") # Example: 09-May-2025 04:30:15 PM
        except Exception:
            return "Error reading timestamp." # File might be corrupted or unparseable
    return None # No timestamp file found or empty

# Update session state on initial load or if not set
if st.session_state.perf_analyzer_last_success_time is None:
    st.session_state.perf_analyzer_last_success_time = get_last_successful_run_time()


# --- Page Content ---
st.set_page_config(page_title="Generate Performance Report", page_icon="⚙️")
st.image("https://img.icons8.com/fluency/96/positive-dynamic.png", width=80)
st.title("⚙️ Generate Daily Performance Report")
st.markdown(f"""
This tool runs the `{SCRIPT_NAME}` script to analyze today's call data and generate
the comprehensive performance report in the 'Performance Reports' Google Sheet tab.
""")
st.markdown("---")

# --- Last Run Info and Controls ---
cols_info_run = st.columns([2,1,1]) # Last run info, Run button, Stop button

with cols_info_run[0]:
    st.markdown("**Report Generation Status**")
    last_run_display = st.session_state.get('perf_analyzer_last_success_time', "Not run yet or timestamp unavailable.")
    if last_run_display:
        st.caption(f"Last successful report generation recorded at: **{last_run_display}**")
    else:
        st.caption("Last successful report generation time: Not available.")
    
    # Display current status message (running, failed, idle)
    if st.session_state.get('perf_analyzer_running'):
        st.info("⏳ Performance Analyzer script is currently running...")
    else:
        st.markdown(st.session_state.get('perf_analyzer_last_run_status_msg', "<p>Script is idle.</p>"), unsafe_allow_html=True)


with cols_info_run[1]:
    start_disabled = st.session_state.get('perf_analyzer_running', False)
    if st.button("▶️ Generate Today's Report", disabled=start_disabled, type="primary", help="Run the script to generate today's performance report.", use_container_width=True):
        run_performance_script()
        st.rerun()

with cols_info_run[2]:
    stop_disabled = not st.session_state.get('perf_analyzer_running', False)
    if st.button("⏹️ Stop Script", disabled=stop_disabled, help="Force stop the script if it's running.", use_container_width=True):
        stop_performance_script()
        st.rerun()

st.markdown("---")

# --- Live Output Area ---
st.subheader("📜 Live Script Output")
output_placeholder = st.empty()
output_placeholder.code(st.session_state.get('perf_analyzer_output', "Script output will appear here..."), language="bash")

if st.session_state.get('perf_analyzer_running') and st.session_state.get('perf_analyzer_process'):
    proc = st.session_state.perf_analyzer_process
    if os.name != 'nt':
        import fcntl
        flags = fcntl.fcntl(proc.stdout, fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    while True:
        line = ""
        if os.name == 'nt':
            if proc.stdout: line = proc.stdout.readline()
        else:
            ready_to_read, _, _ = select.select([proc.stdout], [], [], 0.1)
            if ready_to_read and proc.stdout: line = proc.stdout.readline()
        
        if line:
            st.session_state.perf_analyzer_output += line
            output_placeholder.code(st.session_state.perf_analyzer_output, language="bash")
        else:
            return_code = proc.poll()
            if return_code is not None:
                st.session_state.perf_analyzer_output += "\n" + "="*40 + "\n"
                if return_code == 0: # Success
                    st.session_state.perf_analyzer_output += "✅ Script finished successfully.\n"
                    st.session_state.perf_analyzer_last_run_status_msg = "<p style='color:green;'>✅ Report generated successfully!</p>"
                    # --- NEW: Update last success time in session state ---
                    st.session_state.perf_analyzer_last_success_time = get_last_successful_run_time() # Re-read from file
                    st.success("Performance report generated successfully!")
                else: # Failure
                    st.session_state.perf_analyzer_output += f"❌ Script finished with errors (Exit Code: {return_code}).\n"
                    st.session_state.perf_analyzer_last_run_status_msg = f"<p style='color:red;'>❌ Script failed (Exit Code: {return_code}). Check output.</p>"
                    st.error(f"Performance Analyzer script failed (Exit Code: {return_code}).")
                
                output_placeholder.code(st.session_state.perf_analyzer_output, language="bash")
                st.session_state.perf_analyzer_process = None
                st.session_state.perf_analyzer_running = False
                st.rerun()
                break
        
        if os.name == 'nt' and not line and proc.poll() is None: time.sleep(0.2)
        elif proc.poll() is None: time.sleep(0.05)