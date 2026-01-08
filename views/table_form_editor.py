import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from typing import Dict, Any, List, Optional
import json

st.header(body="Tables", divider=True)
st.subheader("Form-based Table Editor")
st.write(
    "Use this form-based interface to read, edit, and manage records in Unity Catalog tables. "
    "This provides a more user-friendly form interface compared to the grid editor."
)

cfg = Config()
w = WorkspaceClient()

# Initialize session state
if 'selected_record' not in st.session_state:
    st.session_state.selected_record = None
if 'form_data' not in st.session_state:
    st.session_state.form_data = {}
if 'table_data' not in st.session_state:
    st.session_state.table_data = None
if 'table_schema' not in st.session_state:
    st.session_state.table_schema = None

@st.cache_resource(ttl="1h")
def get_connection(http_path):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def get_table_schema(table_name: str, conn) -> Dict[str, str]:
    """Get table schema information"""
    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table_name}")
        schema_info = cursor.fetchall()
        return {row[0]: row[1] for row in schema_info}

def read_table(table_name: str, conn) -> pd.DataFrame:
    """Read table data"""
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall_arrow().to_pandas()

def insert_record(table_name: str, record_data: Dict[str, Any], conn):
    """Insert a new record"""
    columns = list(record_data.keys())
    values = list(record_data.values())
    
    columns_str = ", ".join(columns)
    placeholders = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in values])
    
    with conn.cursor() as cursor:
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        cursor.execute(query)

def update_record(table_name: str, record_data: Dict[str, Any], where_clause: str, conn):
    """Update an existing record"""
    set_clause = ", ".join([
        f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
        for col, val in record_data.items()
    ])
    
    with conn.cursor() as cursor:
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        cursor.execute(query)

def delete_record(table_name: str, where_clause: str, conn):
    """Delete a record"""
    with conn.cursor() as cursor:
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor.execute(query)

def render_form_field(column_name: str, column_type: str, current_value: Any = None):
    """Render appropriate form field based on column type"""
    if current_value is None:
        current_value = ""
    
    # Convert column type to appropriate Streamlit input
    if "int" in column_type.lower() or "bigint" in column_type.lower():
        return st.number_input(
            f"{column_name} ({column_type})",
            value=int(current_value) if current_value != "" else 0,
            step=1
        )
    elif "float" in column_type.lower() or "double" in column_type.lower() or "decimal" in column_type.lower():
        return st.number_input(
            f"{column_name} ({column_type})",
            value=float(current_value) if current_value != "" else 0.0,
            step=0.01
        )
    elif "boolean" in column_type.lower():
        return st.checkbox(
            f"{column_name} ({column_type})",
            value=bool(current_value) if current_value != "" else False
        )
    elif "date" in column_type.lower():
        return st.date_input(f"{column_name} ({column_type})")
    elif "timestamp" in column_type.lower():
        return st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value) if current_value != "" else "",
            help="Format: YYYY-MM-DD HH:MM:SS"
        )
    else:  # Default to text input for strings and other types
        return st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value) if current_value != "" else ""
        )

# Get available resources
warehouses = w.warehouses.list()
warehouse_paths = {wh.name: wh.odbc_params.path for wh in warehouses}
catalogs = w.catalogs.list()

def get_schema_names(catalog_name):
    schemas = w.schemas.list(catalog_name=catalog_name)
    return [schema.name for schema in schemas]

def get_table_names(catalog_name, schema_name):
    tables = w.tables.list(catalog_name=catalog_name, schema_name=schema_name)
    return [table.name for table in tables]

# Main interface
tab_form, tab_view, tab_code, tab_requirements = st.tabs(["**Form Editor**", "**Table View**", "**Code**", "**Requirements**"])

with tab_form:
    # Initialize variables to avoid NameError
    schema_name = ""
    table_name = ""
    
    # Connection setup
    col1, col2 = st.columns(2)
    
    with col1:
        http_path_input = st.selectbox(
            "Select SQL Warehouse:", 
            [""] + list(warehouse_paths.keys())
        )
    
    with col2:
        catalog_name = st.selectbox(
            "Select Catalog:", 
            [""] + [catalog.name for catalog in catalogs]
        )
    
    if catalog_name and catalog_name != "":
        schema_names = get_schema_names(catalog_name)
        col3, col4 = st.columns(2)
        
        with col3:
            schema_name = st.selectbox("Select Schema:", [""] + schema_names)
        
        with col4:
            if schema_name and schema_name != "":
                table_names = get_table_names(catalog_name, schema_name)
                table_name = st.selectbox("Select Table:", [""] + table_names)
    
    # Load table data when all selections are made
    if all([http_path_input, catalog_name, schema_name, table_name]) and table_name != "":
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        http_path = warehouse_paths[http_path_input]
        conn = get_connection(http_path)
        
        # Load table data and schema
        if st.session_state.table_data is None or st.button("🔄 Refresh Data"):
            with st.spinner("Loading table data..."):
                st.session_state.table_data = read_table(full_table_name, conn)
                st.session_state.table_schema = get_table_schema(full_table_name, conn)
        
        if st.session_state.table_data is not None:
            st.success(f"✅ Connected to {full_table_name} ({len(st.session_state.table_data)} records)")
            
            # Action selection
            action = st.radio(
                "Select Action:",
                ["View/Edit Record", "Add New Record", "Delete Record"],
                horizontal=True
            )
            
            if action == "View/Edit Record":
                st.subheader("📝 Edit Existing Record")
                
                # Record selection
                if len(st.session_state.table_data) > 0:
                    record_options = [
                        f"Row {i}: {' | '.join([f'{col}={val}' for col, val in row.items()][:3])}"
                        for i, row in st.session_state.table_data.iterrows()
                    ]
                    
                    selected_idx = st.selectbox(
                        "Select record to edit:",
                        range(len(record_options)),
                        format_func=lambda x: record_options[x]
                    )
                    
                    if selected_idx is not None:
                        selected_record = st.session_state.table_data.iloc[selected_idx]
                        
                        st.write("**Current Record:**")
                        st.json(selected_record.to_dict())
                        
                        # Form for editing
                        with st.form("edit_record_form"):
                            st.write("**Edit Record:**")
                            form_data = {}
                            
                            for column, dtype in st.session_state.table_schema.items():
                                current_value = selected_record.get(column, "")
                                form_data[column] = render_form_field(column, dtype, current_value)
                            
                            col_save, col_cancel = st.columns(2)
                            with col_save:
                                save_changes = st.form_submit_button("💾 Save Changes", type="primary")
                            with col_cancel:
                                cancel_changes = st.form_submit_button("❌ Cancel")
                            
                            if save_changes:
                                try:
                                    # Create WHERE clause using first column as identifier
                                    first_col = list(st.session_state.table_schema.keys())[0]
                                    where_clause = f"{first_col} = '{selected_record[first_col]}'"
                                    
                                    update_record(full_table_name, form_data, where_clause, conn)
                                    st.success("✅ Record updated successfully!")
                                    st.session_state.table_data = None  # Force refresh
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error updating record: {str(e)}")
                else:
                    st.info("No records found in the table.")
            
            elif action == "Add New Record":
                st.subheader("➕ Add New Record")
                
                with st.form("add_record_form"):
                    form_data = {}
                    
                    for column, dtype in st.session_state.table_schema.items():
                        form_data[column] = render_form_field(column, dtype)
                    
                    col_add, col_clear = st.columns(2)
                    with col_add:
                        add_record = st.form_submit_button("➕ Add Record", type="primary")
                    with col_clear:
                        clear_form = st.form_submit_button("🗑️ Clear Form")
                    
                    if add_record:
                        try:
                            insert_record(full_table_name, form_data, conn)
                            st.success("✅ Record added successfully!")
                            st.session_state.table_data = None  # Force refresh
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error adding record: {str(e)}")
            
            elif action == "Delete Record":
                st.subheader("🗑️ Delete Record")
                st.warning("⚠️ This action cannot be undone!")
                
                if len(st.session_state.table_data) > 0:
                    record_options = [
                        f"Row {i}: {' | '.join([f'{col}={val}' for col, val in row.items()][:3])}"
                        for i, row in st.session_state.table_data.iterrows()
                    ]
                    
                    selected_idx = st.selectbox(
                        "Select record to delete:",
                        range(len(record_options)),
                        format_func=lambda x: record_options[x]
                    )
                    
                    if selected_idx is not None:
                        selected_record = st.session_state.table_data.iloc[selected_idx]
                        
                        st.write("**Record to Delete:**")
                        st.json(selected_record.to_dict())
                        
                        if st.button("🗑️ Confirm Delete", type="secondary"):
                            try:
                                # Create WHERE clause using first column as identifier
                                first_col = list(st.session_state.table_schema.keys())[0]
                                where_clause = f"{first_col} = '{selected_record[first_col]}'"
                                
                                delete_record(full_table_name, where_clause, conn)
                                st.success("✅ Record deleted successfully!")
                                st.session_state.table_data = None  # Force refresh
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error deleting record: {str(e)}")
                else:
                    st.info("No records found in the table.")

with tab_view:
    st.subheader("📊 Table Data View")
    
    if st.session_state.table_data is not None:
        st.dataframe(
            st.session_state.table_data,
            use_container_width=True,
            hide_index=True
        )
        
        # Table statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", len(st.session_state.table_data))
        with col2:
            st.metric("Columns", len(st.session_state.table_data.columns))
        with col3:
            st.metric("Memory Usage", f"{st.session_state.table_data.memory_usage(deep=True).sum() / 1024:.1f} KB")
    else:
        st.info("Select a table in the Form Editor tab to view data here.")

with tab_code:
    st.code("""
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

cfg = Config()
w = WorkspaceClient()

@st.cache_resource(ttl="1h")
def get_connection(http_path):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def read_table(table_name: str, conn) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall_arrow().to_pandas()

def insert_record(table_name: str, record_data: dict, conn):
    columns = list(record_data.keys())
    values = list(record_data.values())
    columns_str = ", ".join(columns)
    placeholders = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in values])
    
    with conn.cursor() as cursor:
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        cursor.execute(query)

def update_record(table_name: str, record_data: dict, where_clause: str, conn):
    set_clause = ", ".join([
        f"{col} = '{val}'" if isinstance(val, str) else f"{col} = {val}"
        for col, val in record_data.items()
    ])
    
    with conn.cursor() as cursor:
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        cursor.execute(query)

# Usage example:
conn = get_connection("/sql/1.0/warehouses/your-warehouse-id")
df = read_table("catalog.schema.table", conn)

# Insert new record
new_record = {"column1": "value1", "column2": 123}
insert_record("catalog.schema.table", new_record, conn)

# Update record
update_data = {"column1": "new_value"}
update_record("catalog.schema.table", update_data, "id = 1", conn)
""")

with tab_requirements:
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **Permissions (app service principal)**
        * `SELECT` on the Unity Catalog table
        * `MODIFY` on the Unity Catalog table (for edits)
        * `CAN USE` on the SQL warehouse
        """)
    
    with col2:
        st.markdown("""
        **Databricks resources**
        * SQL warehouse
        * Unity Catalog table
        * Appropriate permissions for CRUD operations
        """)
    
    with col3:
        st.markdown("""
        **Dependencies**
        * [Databricks SDK](https://pypi.org/project/databricks-sdk/) - `databricks-sdk`
        * [Databricks SQL Connector](https://pypi.org/project/databricks-sql-connector/) - `databricks-sql-connector`
        * [Pandas](https://pypi.org/project/pandas/) - `pandas`
        * [Streamlit](https://pypi.org/project/streamlit/) - `streamlit`
        """)