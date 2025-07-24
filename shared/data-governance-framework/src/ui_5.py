import streamlit as st
import json
import uuid

# --- Default JSON Config ---
example_json = {
    "target_table": "pangenome.genome",
    "schema_file_path": "s3a://cdm-lake/schemas/pangenome-schema.yaml",
    "input_files": [],
    "output_file": {},
    "bronze": {},
    "silver": {},
    "transformations": [],
    "validations": [],
    "referential_integrity": [],
    "great_expectations_validations": [],
    "schema_drift_handling": {},
    "logging": {},
    "versioning": {}
}

# --- Session State Initialization ---
if "form_data" not in st.session_state:
    st.session_state.form_data = example_json
if "validation_to_remove" not in st.session_state:
    st.session_state.validation_to_remove = None
if "transform_to_remove" not in st.session_state:
    st.session_state.transform_to_remove = None

st.title("🧩 Full Config Form Editor")

# --- Top-Level Fields ---
st.text_input("Target Table", key="target_table", value=st.session_state.form_data["target_table"])
st.text_input("Schema File Path", key="schema_file_path", value=st.session_state.form_data["schema_file_path"])

# --- Input Files (List of Dicts) ---
st.markdown("### 📁 Input Files")
input_files = st.session_state.form_data.get("input_files", [])
for i, entry in enumerate(input_files):
    with st.expander(f"Input File #{i+1}", expanded=True):
        for field in ["source", "file_path", "file_type", "delimiter", "ignore_first_line"]:
            entry[field] = st.text_input(f"{field}", value=entry.get(field, ""), key=f"input_{i}_{field}")
if st.button("➕ Add Input File"):
    input_files.append({f: "" for f in ["source", "file_path", "file_type", "delimiter", "ignore_first_line"]})

# --- Output File ---
st.markdown("### 📤 Output File")
output_file = st.session_state.form_data.get("output_file", {})
for field in ["output_file_path", "file_type", "delimiter", "ignore_first_line"]:
    output_file[field] = st.text_input(f"Output: {field}", value=output_file.get(field, ""), key=f"output_{field}")

# --- Bronze and Silver ---
st.text_input("Bronze Input Path", key="bronze_path", value=st.session_state.form_data["bronze"].get("input_path", ""))
st.text_input("Silver Output Delta Path", key="silver_path", value=st.session_state.form_data["silver"].get("output_delta_path", ""))
st.text_input("Silver Delta Table Name", key="silver_table", value=st.session_state.form_data["silver"].get("delta_table_name", ""))

# --- Transformations ---
st.markdown("### 🔄 Transformations")
transformations = st.session_state.form_data.get("transformations", [])
for i, t in enumerate(transformations):
    with st.expander(f"Transformation #{i+1}", expanded=True):
        t["column_name"] = st.text_input("Column Name", value=t.get("column_name", ""), key=f"tr_col_{i}")
        t["operation"] = st.text_input("Operation", value=t.get("operation", ""), key=f"tr_op_{i}")
        t["value"] = st.text_input("Value (optional)", value=t.get("value", ""), key=f"tr_val_{i}")
if st.button("➕ Add Transformation"):
    transformations.append({"column_name": "", "operation": "", "value": ""})

# --- Validations ---
st.markdown("### ✅ Validations")
validations = st.session_state.form_data.get("validations", [])
for i, val in enumerate(validations):
    val_id = val.get("id") or str(uuid.uuid4())
    val["id"] = val_id
    with st.expander(f"Validation #{i+1}", expanded=True):
        val["column"] = st.text_input("Column", value=val.get("column", ""), key=f"val_col_{val_id}")
        val["validation_type"] = st.text_input("Validation Type", value=val.get("validation_type", ""), key=f"val_type_{val_id}")
        val["error_message"] = st.text_input("Error Message", value=val.get("error_message", ""), key=f"val_msg_{val_id}")
        val["pattern"] = st.text_input("Pattern (if any)", value=val.get("pattern", ""), key=f"val_pat_{val_id}")
        if st.button(f"❌ Remove", key=f"remove_val_{val_id}"):
            st.session_state.validation_to_remove = val_id
            st.rerun()
if st.session_state.validation_to_remove:
    validations[:] = [v for v in validations if v.get("id") != st.session_state.validation_to_remove]
    st.session_state.validation_to_remove = None
    st.rerun()
if st.button("➕ Add Validation"):
    validations.append({"id": str(uuid.uuid4()), "column": "", "validation_type": "", "error_message": ""})
    st.rerun()

# --- Referential Integrity Rules ---
st.markdown("### 🔗 Referential Integrity")
ri_rules = st.session_state.form_data.get("referential_integrity", [])
for i, r in enumerate(ri_rules):
    with st.expander(f"Rule #{i+1}", expanded=True):
        for f in ["foreign_key", "reference_table", "reference_column", "database", "action"]:
            r[f] = st.text_input(f"{f}", value=r.get(f, ""), key=f"ri_{i}_{f}")
if st.button("➕ Add RI Rule"):
    ri_rules.append({f: "" for f in ["foreign_key", "reference_table", "reference_column", "database", "action"]})

# --- GE Expectations ---
st.markdown("### 🧪 Great Expectations")
ge_rules = st.session_state.form_data.get("great_expectations_validations", [])
for i, rule in enumerate(ge_rules):
    with st.expander(f"Expectation #{i+1}", expanded=True):
        rule["expectation_type"] = st.text_input("Expectation Type", value=rule.get("expectation_type", ""), key=f"ge_type_{i}")
        params = rule.setdefault("params", {})
        params["column"] = st.text_input("Column", value=params.get("column", ""), key=f"ge_col_{i}")
        params["regex"] = st.text_input("Regex (if applicable)", value=params.get("regex", ""), key=f"ge_regex_{i}")
if st.button("➕ Add GE Validation"):
    ge_rules.append({"expectation_type": "", "params": {"column": ""}})

# --- Schema Drift, Logging, Versioning ---
st.text_input("Schema Drift Action", key="drift_action", value=st.session_state.form_data["schema_drift_handling"].get("action", ""))
st.text_input("Drift Log Table", key="drift_log_table", value=st.session_state.form_data["schema_drift_handling"].get("log_table", ""))
st.text_input("Drift Log Path", key="drift_log_path", value=st.session_state.form_data["schema_drift_handling"].get("log_path", ""))

st.text_input("Error Table", key="log_err_table", value=st.session_state.form_data["logging"].get("error_table", ""))
st.text_input("Error Log Path", key="log_err_path", value=st.session_state.form_data["logging"].get("output_delta_path", ""))

st.text_input("Version Tag", key="ver_tag", value=st.session_state.form_data["versioning"].get("version_tag", ""))
st.text_input("Created By", key="ver_by", value=st.session_state.form_data["versioning"].get("created_by", ""))
st.text_input("Created At", key="ver_at", value=st.session_state.form_data["versioning"].get("created_at", ""))

# --- Generate Output JSON ---
if st.button("📄 Generate Full Config JSON"):
    result = {
        "target_table": st.session_state["target_table"],
        "schema_file_path": st.session_state["schema_file_path"],
        "input_files": input_files,
        "output_file": output_file,
        "bronze": {"input_path": st.session_state["bronze_path"]},
        "silver": {
            "output_delta_path": st.session_state["silver_path"],
            "delta_table_name": st.session_state["silver_table"]
        },
        "transformations": transformations,
        "validations": [
            {k: v for k, v in item.items() if k != "id"} for item in validations
        ],
        "referential_integrity": ri_rules,
        "great_expectations_validations": ge_rules,
        "schema_drift_handling": {
            "action": st.session_state["drift_action"],
            "log_table": st.session_state["drift_log_table"],
            "log_path": st.session_state["drift_log_path"]
        },
        "logging": {
            "error_table": st.session_state["log_err_table"],
            "output_delta_path": st.session_state["log_err_path"]
        },
        "versioning": {
            "version_tag": st.session_state["ver_tag"],
            "created_by": st.session_state["ver_by"],
            "created_at": st.session_state["ver_at"]
        }
    }
    st.success("✅ Config JSON Generated")
    st.code(json.dumps(result, indent=2), language="json")
