import streamlit as st
import json
from config.config_validator import run_config_validation
from config.config_loader import ConfigLoader
from src.minio_utils import list_minio_files, get_file_from_minio
import os
import logging
from streamlit_ace import st_ace

# --- Streamlit Logging ---
class StreamlitHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        if record.levelno >= logging.ERROR:
            st.error(msg)
        elif record.levelno >= logging.WARNING:
            st.warning(msg)
        #elif record.levelno >= logging.INFO:
        #    st.info(msg)
        #else:
        #    st.text(msg)

def get_streamlit_logger(name="streamlit_logger"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # 🚨 Clear all handlers (only once per session)
    if "logger_initialized" not in st.session_state:
        logger.handlers.clear()
        handler = StreamlitHandler()
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        st.session_state["logger_initialized"] = True

    return logger


# --- UI ---
st.set_page_config(page_title="Config Validator UI", layout="wide")
st.title("📊 Config Validator")

st.sidebar.header("Upload Config File")
uploaded_file = st.sidebar.file_uploader("Upload JSON config from local", type="json")
minio_bucket = st.sidebar.text_input("MinIO bucket", value="cdm-lake")
minio_prefix = st.sidebar.text_input("MinIO prefix (folder)", value="config-json/")
minio_file = st.sidebar.selectbox("Choose file from MinIO", list_minio_files(minio_bucket, minio_prefix))

# 1. Render editor with session state
#st_ace(value=st.session_state.get("json_editor", ""))

# --- Initialize session state ---
if "json_editor" not in st.session_state:
    st.session_state["json_editor"] = ""
if "selected_minio_file" not in st.session_state:
    st.session_state["selected_minio_file"] = None

# --- Load Config before rendering editor ---
if uploaded_file:
    config_json_str = uploaded_file.read().decode("utf-8")
    st.session_state["json_editor"] = config_json_str
    st.session_state["selected_minio_file"] = None  # Clear MinIO state
elif minio_file and minio_file != st.session_state["selected_minio_file"]:
    key = os.path.join(minio_prefix, minio_file).replace("\\", "/")
    config_json_str = get_file_from_minio(minio_bucket, key)
    st.session_state["json_editor"] = config_json_str
    st.session_state["selected_minio_file"] = minio_file

# --- JSON Editor ---
st.subheader("📝 JSON Editor")
config_text = st_ace(
    value=st.session_state["json_editor"],
    language="json",
    theme="monokai",
    key=f"json_editor_{st.session_state.get('selected_minio_file')}",
    tab_size=2,
    show_gutter=True,
    show_print_margin=False,
    wrap=True,
    height=400
)


# --- Validate JSON Syntax ---
try:
    config_dict = json.loads(config_text)
    st.success("✅ JSON format looks good")
    valid_json = True
except json.JSONDecodeError as e:
    st.error(f"❌ JSON Error: {str(e)}")
    valid_json = False

# --- Run Validation ---
if st.button("Validate Config") and valid_json:
    with st.spinner("Running validation..."):
        logger = get_streamlit_logger()
        loader = ConfigLoader.from_dict(config_dict, logger)
        run_config_validation(loader, logger)
