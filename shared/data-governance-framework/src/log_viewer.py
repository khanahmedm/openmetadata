import streamlit as st
import pandas as pd
import boto3
import io
import json


st.set_page_config(page_title="Pipeline Log Viewer", layout="wide")

# ---- MinIO config ----
S3_ENDPOINT = "http://minio:9000"  # or localhost:9000 if running outside Docker
AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin"
BUCKET = "cdm-lake"
PREFIX = "logs/pangenome/"  # S3 key prefix where logs are stored

# Connect to MinIO (S3-compatible)
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# List all log files under the prefix
response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
log_objects = [obj['Key'] for obj in response.get("Contents", []) if obj['Key'].endswith(".log")]

# --- Log File Selection ---
log_file_names = [key.split("/")[-1] for key in log_objects]
selected_files = st.sidebar.multiselect("📂 Log Files", options=log_file_names, default=log_file_names)

# Filter S3 keys to only those selected
filtered_keys = [key for key in log_objects if key.split("/")[-1] in selected_files]

# Load and concatenate logs
dfs = []
for key in filtered_keys:
    log_obj = s3.get_object(Bucket=BUCKET, Key=key)
    content = log_obj['Body'].read().decode("utf-8")
    try:
        df = pd.read_json(io.StringIO(content), lines=True)
        df['time'] = pd.to_datetime(df['time'], format="%Y-%m-%d %H:%M:%S,%f")
        df["log_file"] = key.split("/")[-1]
        dfs.append(df)
    except Exception as e:
        st.warning(f"⚠️ Skipped {key} due to error: {e}")


# Combine all logs
if not dfs:
    st.error("❌ No log files found in MinIO.")
    st.stop()

df = pd.concat(dfs, ignore_index=True)

# --- Streamlit UI ---
st.title("📋 Pipeline Log Viewer (MinIO Logs)")

# Filters
st.sidebar.header("🔍 Filters")

pipeline_options = sorted(df['pipeline'].dropna().unique())
pipeline = st.sidebar.selectbox("Pipeline", options=["ALL"] + pipeline_options)
if pipeline != "ALL":
    df = df[df["pipeline"] == pipeline]

schema_options = sorted(df['schema'].dropna().unique())
schema = st.sidebar.selectbox("Schema", options=["ALL"] + schema_options)
if schema != "ALL":
    df = df[df["schema"] == schema]

table_options = sorted(df['table'].dropna().unique())
table = st.sidebar.selectbox("Target Table", options=["ALL"] + table_options)
if table != "ALL":
    df = df[df["table"] == table]

level_options = sorted(df['level'].dropna().unique())
level = st.sidebar.selectbox("Log Level", options=["ALL"] + level_options)
if level != "ALL":
    df = df[df["level"] == level]

# Date range filter
start_date = df['time'].min().date()
end_date = df['time'].max().date()
date_range = st.sidebar.date_input("Date Range", value=(start_date, end_date), min_value=start_date, max_value=end_date)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_dt = pd.to_datetime(date_range[0])
    end_dt = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1)
    df = df[(df['time'] >= start_dt) & (df['time'] < end_dt)]

# Display
st.markdown("### 🔎 Filtered Logs")
st.dataframe(df[["time", "pipeline", "schema", "table", "level", "module", "msg", "log_file"]], use_container_width=True, height=800)
