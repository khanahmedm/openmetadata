import streamlit as st
import json

# --- Simulate loading from MinIO ---
example_json = {
    "target_table": "pangenome.genome",
    "schema_file_path": "s3a://cdm-lake/schemas/pangenome-schema.yaml",
    "validations": [
        {
            "column": "genome_id",
            "validation_type": "not_null",
            "error_message": "Missing genome_id"
        },
        {
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

st.title("🧩 Minimal Form Editor")

# --- Top-Level Fields ---
st.text_input("Target Table", key="target_table", value=st.session_state.form_data.get("target_table", ""))
st.text_input("Schema File Path", key="schema_file_path", value=st.session_state.form_data.get("schema_file_path", ""))

st.markdown("### 🔍 Validations")

# --- Render each validation entry ---
for i, validation in enumerate(st.session_state.validations):
    with st.expander(f"Validation #{i+1}", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("Column", key=f"val_col_{i}", value=validation.get("column", ""))
        with col2:
            st.text_input("Validation Type", key=f"val_type_{i}", value=validation.get("validation_type", ""))
        st.text_input("Error Message", key=f"val_msg_{i}", value=validation.get("error_message", ""))
        if st.button(f"❌ Remove", key=f"remove_{i}"):
            st.session_state.validations.pop(i)
            #st.experimental_rerun()
            st.rerun()

# --- Add new entry ---
if st.button("➕ Add Validation"):
    st.session_state.validations.append({"column": "", "validation_type": "", "error_message": ""})
    #st.experimental_rerun()
    st.rerun()


# --- Generate JSON ---
if st.button("📄 Generate JSON"):
    json_output = {
        "target_table": st.session_state.get("target_table", ""),
        "schema_file_path": st.session_state.get("schema_file_path", ""),
        "validations": [
            {
                "column": st.session_state.get(f"val_col_{i}", ""),
                "validation_type": st.session_state.get(f"val_type_{i}", ""),
                "error_message": st.session_state.get(f"val_msg_{i}", "")
            } for i in range(len(st.session_state.validations))
        ]
    }
    st.code(json.dumps(json_output, indent=2), language="json")
