import streamlit as st
import json
from config.config_validator import run_config_validation
from config.config_loader import ConfigLoader
from src.minio_utils import list_minio_files, get_file_from_minio
import os
import logging
from streamlit_ace import st_ace
from datetime import datetime

# --- Streamlit Logging ---
class StreamlitHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        if record.levelno >= logging.ERROR:
            st.error(msg)
        elif record.levelno >= logging.WARNING:
            st.warning(msg)


def get_streamlit_logger(name="streamlit_logger"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if "logger_initialized" not in st.session_state:
        logger.handlers.clear()
        handler = StreamlitHandler()
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        st.session_state["logger_initialized"] = True
    return logger

# --- UI Setup ---
st.set_page_config(page_title="Config Validator UI", layout="wide")
st.title("📊 Config Validator")

st.sidebar.header("Upload Config File")
uploaded_file = st.sidebar.file_uploader("Upload JSON config from local", type="json")
minio_bucket = st.sidebar.text_input("MinIO bucket", value="cdm-lake")
minio_prefix = st.sidebar.text_input("MinIO prefix (folder)", value="config-json/")
minio_file = st.sidebar.selectbox("Choose file from MinIO", list_minio_files(minio_bucket, minio_prefix))

# --- Session State Init ---
if "json_editor" not in st.session_state:
    st.session_state["json_editor"] = ""
if "selected_minio_file" not in st.session_state:
    st.session_state["selected_minio_file"] = None

# --- Load Config ---
if uploaded_file:
    config_json_str = uploaded_file.read().decode("utf-8")
    st.session_state["json_editor"] = config_json_str
    st.session_state["selected_minio_file"] = None
elif minio_file and minio_file != st.session_state["selected_minio_file"]:
    key = os.path.join(minio_prefix, minio_file).replace("\\", "/")
    config_json_str = get_file_from_minio(minio_bucket, key)
    st.session_state["json_editor"] = config_json_str
    st.session_state["selected_minio_file"] = minio_file

# --- Tabs for JSON and Form Editor ---
tabs = st.tabs(["📝 JSON Editor", "🧩 Form Editor"])

with tabs[0]:
    config_text = st_ace(
        value=st.session_state["json_editor"],
        language="json",
        theme="monokai",
        key=f"json_editor_{st.session_state.get('selected_minio_file')}",
        #key="json_editor_ace",
        tab_size=2,
        show_gutter=True,
        show_print_margin=False,
        wrap=True,
        height=400
    )

    try:
        config_dict = json.loads(config_text)
        st.success("✅ JSON format looks good")
        valid_json = True
    except json.JSONDecodeError as e:
        st.error(f"❌ JSON Error: {str(e)}")
        valid_json = False

    if st.button("Validate Config") and valid_json:
        with st.spinner("Running validation..."):
            logger = get_streamlit_logger()
            loader = ConfigLoader.from_dict(config_dict, logger)
            run_config_validation(loader, logger)

with tabs[1]:
    st.subheader("🧩 Form-Based Config Editor")
    
    target_table = st.text_input("Target Table", value="pangenome.genome")
    schema_file_path = st.text_input("Schema File Path", value="s3a://cdm-lake/schemas/pangenome-schema.yaml")
    
    transformations = st.text_area("Transformations (JSON)", height=100)
    validations = st.text_area("Validations (JSON)", height=100)
    referential_integrity = st.text_area("Referential Integrity Rules (JSON)", height=100)
    great_expectations = st.text_area("Great Expectations Rules (JSON)", height=100)

    input_files = st.text_area("Input Files (JSON)", height=100)
    output_file = st.text_area("Output File (JSON)", height=100)
    bronze = st.text_area("Bronze (JSON)", height=100)
    silver = st.text_area("Silver (JSON)", height=100)
    schema_drift_handling = st.text_area("Schema Drift Handling (JSON)", height=100)
    logging_conf = st.text_area("Logging (JSON)", height=100)
    versioning = st.text_area("Versioning (JSON)", height=100)

    if st.button("Generate Config JSON from Form"):
        try:
            generated = {
                "target_table": target_table,
                "schema_file_path": schema_file_path,
                "input_files": json.loads(input_files or "[]"),
                "output_file": json.loads(output_file or "{}"),
                "bronze": json.loads(bronze or "{}"),
                "silver": json.loads(silver or "{}"),
                "transformations": json.loads(transformations or "[]"),
                "validations": json.loads(validations or "[]"),
                "referential_integrity": json.loads(referential_integrity or "[]"),
                "great_expectations_validations": json.loads(great_expectations or "[]"),
                "schema_drift_handling": json.loads(schema_drift_handling or "{}"),
                "logging": json.loads(logging_conf or "{}"),
                "versioning": json.loads(versioning or "{}")
            }
            json_output = json.dumps(generated, indent=2)
            st.session_state["json_editor"] = json_output
            st.success("✅ JSON generated and populated into the editor tab.")
            st.download_button("📥 Download Config JSON", json_output, file_name="config.json", mime="application/json")
        except Exception as e:
            st.error(f"Failed to generate config: {e}")
