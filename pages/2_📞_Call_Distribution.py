# pages/2_📞_Call_Distribution.py
import streamlit as st
import subprocess
import time
import sys
from pathlib import Path
import os
import select # Needed for non-blocking read (optional but better)

# --- Configuration ---
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
SCRIPT_PATH = PROJECT_ROOT / "distributionV2.py"
PYTHON_EXECUTABLE = sys.executable # Use the same python that runs streamlit

# --- Initialize Session State ---
if 'dist_process' not in st.session_state:
    st.session_state.dist_process = None
if 'dist_output' not in st.session_state:
    st.session_state.dist_output = "" # Stores the accumulated output
if 'dist_running' not in st.session_state:
    st.session_state.dist_running = False

# --- Helper Function ---
def run_script():
    """Starts the distribution script."""
    if not SCRIPT_PATH.is_file():
        st.error(f"Script not found: {SCRIPT_PATH}")
        return
    try:
        # Start the process
        process = subprocess.Popen(
            [PYTHON_EXECUTABLE, str(SCRIPT_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # Combine stdout and stderr
            text=True,
            bufsize=1,  # Line-buffered
            universal_newlines=True,
            cwd=PROJECT_ROOT,
            encoding='utf-8', # Be explicit about encoding
            errors='replace' # Handle potential decoding errors
        )
        st.session_state.dist_process = process
        st.session_state.dist_running = True
        # Reset output only when starting
        st.session_state.dist_output = f"Starting script: {SCRIPT_PATH.name}...\n" + "="*30 + "\n"
        st.success(f"Started script: {SCRIPT_PATH.name}")
    except Exception as e:
        st.error(f"Failed to start script: {e}")
        st.session_state.dist_running = False
        st.session_state.dist_process = None # Ensure process is cleared on failure

def stop_script():
    """Stops the running distribution script."""
    proc = st.session_state.get('dist_process')
    if proc and st.session_state.get('dist_running'):
        try:
            # Add to output before stopping
            st.session_state.dist_output += "\n" + "="*30 + "\nStopping script...\n"
            proc.terminate()  # Send SIGTERM
            try:
                proc.wait(timeout=5) # Wait briefly for graceful exit
                st.session_state.dist_output += "Script terminated gracefully.\n"
            except subprocess.TimeoutExpired:
                st.warning("Process did not terminate gracefully, sending SIGKILL.")
                proc.kill() # Force kill
                st.session_state.dist_output += "Script force killed.\n"
            except Exception as wait_err: # Catch potential errors during wait (e.g., process already dead)
                 st.warning(f"Error during process wait: {wait_err}")
                 st.session_state.dist_output += f"Script stop state uncertain: {wait_err}\n"

            st.info("Script stop initiated.")

        except Exception as e:
            st.error(f"Error stopping script: {e}")
            st.session_state.dist_output += f"\nError during stop: {e}\n"
        finally:
             # Always update state regardless of exceptions during stop
            st.session_state.dist_process = None
            st.session_state.dist_running = False


# --- Page Content ---
# Note: layout="wide" is set in dashboard.py and applies here
st.set_page_config(page_title="Call Distribution", page_icon="📞")
st.title("📞 Run Call Distribution")
st.markdown(f"Execute the `{SCRIPT_PATH.name}` script to distribute calls/leads.")

# --- Control Buttons ---
col1, col2 = st.columns(2)
with col1:
    # Disable button if already running
    start_disabled = st.session_state.get('dist_running', False)
    if st.button("▶️ Start Distribution", disabled=start_disabled, type="primary"):
        run_script()
        st.rerun() # Rerun to update button state and start output loop

with col2:
    # Disable button if not running
    stop_disabled = not st.session_state.get('dist_running', False)
    if st.button("⏹️ Stop Distribution", disabled=stop_disabled):
        stop_script()
        st.rerun() # Rerun to update button state and stop output loop

# --- Live Output Area ---
st.subheader("📜 Live Script Output")

# Create ONE placeholder for the code output
output_placeholder = st.empty()

# Display current accumulated output in the placeholder
# This handles the display both when running and when stopped
output_placeholder.code(st.session_state.get('dist_output', "No output yet."), language="bash")

# If running, continuously check for new output
if st.session_state.get('dist_running') and st.session_state.get('dist_process'):
    proc = st.session_state.dist_process
    new_output = ""
    try:
        # Non-blocking read (optional, but better for responsiveness)
        # This requires the 'select' module
        # Set the stdout stream to non-blocking
        # Note: Non-blocking I/O might behave differently on Windows
        if os.name != 'nt': # select works best on Unix-like systems
            import fcntl
            flags = fcntl.fcntl(proc.stdout, fcntl.F_GETFL)
            fcntl.fcntl(proc.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        while True:
            line = proc.stdout.readline()
            if line:
                new_output += line
                st.session_state.dist_output += line
                # Update the *content* of the placeholder
                output_placeholder.code(st.session_state.dist_output, language="bash")
            else:
                # Check if process is still running
                return_code = proc.poll()
                if return_code is not None:
                    # Process finished
                    st.session_state.dist_output += "\n" + "="*30 + "\n"
                    if return_code == 0:
                        st.session_state.dist_output += "Script finished successfully.\n"
                        st.success("Script finished successfully.")
                    else:
                        st.session_state.dist_output += f"Script finished with Exit Code: {return_code}\n"
                        st.error(f"Script finished with errors (Exit Code: {return_code}).")

                    # Update final output and clean up state
                    output_placeholder.code(st.session_state.dist_output, language="bash")
                    st.session_state.dist_process = None
                    st.session_state.dist_running = False
                    st.rerun() # Rerun to update button states etc.
                    break # Exit the read loop

            # Small sleep to prevent busy-waiting and allow UI to refresh
            time.sleep(0.1)

    except Exception as e:
        # Handle exceptions during output reading
        st.error(f"An error occurred while reading script output: {e}")
        st.session_state.dist_output += f"\nError reading output: {e}\n"
        output_placeholder.code(st.session_state.dist_output, language="bash") # Show error in output
        stop_script() # Try to stop the script if reading fails
        st.rerun()
