import streamlit as st
import json
import uuid

# --- Simulate loading from MinIO ---
example_json = {
    "target_table": "pangenome.genome",
    "schema_file_path": "s3a://cdm-lake/schemas/pangenome-schema.yaml",
    "validations": [
        {
            "id": str(uuid.uuid4()),
            "column": "genome_id",
            "validation_type": "not_null",
            "error_message": "Missing genome_id"
        },
        {
            "id": str(uuid.uuid4()),
            "column": "gtdb_taxonomy_id",
            "validation_type": "not_null",
            "error_message": "Missing taxonomy ID"
        }
    ]
}

# --- Session State Init ---
if "form_data" not in st.session_state:
    st.session_state.form_data = example_json
if "validations" not in st.session_state:
    st.session_state.validations = st.session_state.form_data.get("validations", [])
if "to_remove_id" not in st.session_state:
    st.session_state.to_remove_id = None

st.title("🧩 Minimal Form Editor")

# --- Top-Level Fields ---
st.text_input("Target Table", key="target_table", value=st.session_state.form_data.get("target_table", ""))
st.text_input("Schema File Path", key="schema_file_path", value=st.session_state.form_data.get("schema_file_path", ""))

st.markdown("### 🔍 Validations")

# --- Render each validation entry ---
for validation in st.session_state.validations:
    val_id = validation["id"]
    with st.expander(f"Validation: {validation.get('column', '') or 'New'}", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("Column", key=f"val_col_{val_id}", value=validation.get("column", ""))
        with col2:
            st.text_input("Validation Type", key=f"val_type_{val_id}", value=validation.get("validation_type", ""))
        st.text_input("Error Message", key=f"val_msg_{val_id}", value=validation.get("error_message", ""))
        if st.button("❌ Remove", key=f"remove_{val_id}"):
            st.session_state.to_remove_id = val_id
            st.rerun()  # triggers re-execution after setting removal ID

# --- If a remove button was clicked ---
if st.session_state.to_remove_id:
    st.session_state.validations = [
        v for v in st.session_state.validations if v["id"] != st.session_state.to_remove_id
    ]
    st.session_state.to_remove_id = None
    st.rerun()

# --- Add new entry ---
if st.button("➕ Add Validation"):
    st.session_state.validations.append({
        "id": str(uuid.uuid4()),
        "column": "",
        "validation_type": "",
        "error_message": ""
    })
    st.rerun()

# --- Generate JSON ---
if st.button("📄 Generate JSON"):
    json_output = {
        "target_table": st.session_state.get("target_table", ""),
        "schema_file_path": st.session_state.get("schema_file_path", ""),
        "validations": [
            {
                "column": st.session_state.get(f"val_col_{v['id']}", ""),
                "validation_type": st.session_state.get(f"val_type_{v['id']}", ""),
                "error_message": st.session_state.get(f"val_msg_{v['id']}", "")
            }
            for v in st.session_state.validations
        ]
    }
    st.code(json.dumps(json_output, indent=2), language="json")
