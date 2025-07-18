import streamlit as st
import json
from config.config_validator import run_config_validation
from src.minio_utils import list_minio_files, get_file_from_minio
import os

st.set_page_config(page_title="Config Validator UI", layout="wide")
st.title("📊 Config Validator")

st.sidebar.header("Upload Config File")

# Upload from local
uploaded_file = st.sidebar.file_uploader("Upload JSON config from local", type="json")

# OR load from MinIO
minio_bucket = st.sidebar.text_input("MinIO bucket", value="cdm-lake")
minio_prefix = st.sidebar.text_input("MinIO prefix (folder)", value="config-json/")
minio_file = st.sidebar.selectbox("Choose file from MinIO", list_minio_files(minio_bucket, minio_prefix))

config_json_str = ""

if uploaded_file:
    config_json_str = uploaded_file.read().decode("utf-8")
elif minio_file:
    #config_json_str = get_file_from_minio(minio_bucket, f"{minio_prefix}/{minio_file}")
    key = os.path.join(minio_prefix, minio_file).replace("\\", "/")  # For Windows safety
    config_json_str = get_file_from_minio(minio_bucket, key)

# JSON Editor
st.subheader("📝 JSON Editor")
config_text = st.text_area("Edit JSON Config", config_json_str, height=400, key="json_editor")

# Live JSON syntax check
try:
    config_dict = json.loads(config_text)
    st.success("✅ JSON format looks good")
    valid_json = True
except json.JSONDecodeError as e:
    st.error(f"❌ JSON Error: {str(e)}")
    valid_json = False

# Validate button
if st.button("Validate Config") and valid_json:
    with st.spinner("Running validation..."):
        from config.config_loader import ConfigLoader
        import logging

        class DummyLogger:
            def info(self, msg): st.info(msg)
            def warning(self, msg): st.warning(msg)
            def error(self, msg): st.error(msg)
            def debug(self, msg): st.write(f"DEBUG: {msg}")

        loader = ConfigLoader.from_dict(config_dict, DummyLogger())
        run_config_validation(loader, DummyLogger())
