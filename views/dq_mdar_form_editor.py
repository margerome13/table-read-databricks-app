import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from typing import Dict, Any, List, Optional
import json

st.header(body="DQ MDAR Inventory Data Mart", divider=True)
st.subheader("Form-based Table Editor")
st.write(
    "Manage records in the **dg_dev.sandbox.dq_mdar_inventory_data_mart** table using this user-friendly form interface."
)

# Pre-configured connection details
DATABRICKS_HOST = "dbc-7d305f7c-9def.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/80e5636f05f63c9b"
TABLE_NAME = "dg_dev.sandbox.dq_mdar_inventory_data_mart"

# Initialize session state
if 'selected_record' not in st.session_state:
    st.session_state.selected_record = None
if 'form_data' not in st.session_state:
    st.session_state.form_data = {}
if 'table_data' not in st.session_state:
    st.session_state.table_data = None
if 'table_schema' not in st.session_state:
    st.session_state.table_schema = None
if 'connection_established' not in st.session_state:
    st.session_state.connection_established = False

@st.cache_resource(ttl="1h")
def get_connection(server_hostname: str, http_path: str):
    """Create connection to Databricks SQL warehouse"""
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: Config().authenticate,
    )

def get_table_schema(table_name: str, conn) -> Dict[str, str]:
    """Get table schema information"""
    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table_name}")
        schema_info = cursor.fetchall()
        return {row[0]: row[1] for row in schema_info}

def read_table(table_name: str, conn, limit: int = 1000) -> pd.DataFrame:
    """Read table data with optional limit"""
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

def insert_record(table_name: str, record_data: Dict[str, Any], conn):
    """Insert a new record"""
    columns = list(record_data.keys())
    values = []
    
    for val in record_data.values():
        if val is None or val == "":
            values.append("NULL")
        elif isinstance(val, str):
            values.append(f"'{val.replace(\"'\", \"''\")}'")  # Escape single quotes
        else:
            values.append(str(val))
    
    columns_str = ", ".join(columns)
    values_str = ", ".join(values)
    
    with conn.cursor() as cursor:
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        cursor.execute(query)

def update_record(table_name: str, record_data: Dict[str, Any], where_clause: str, conn):
    """Update an existing record"""
    set_clauses = []
    
    for col, val in record_data.items():
        if val is None or val == "":
            set_clauses.append(f"{col} = NULL")
        elif isinstance(val, str):
            set_clauses.append(f"{col} = '{val.replace(\"'\", \"''\")}'")  # Escape single quotes
        else:
            set_clauses.append(f"{col} = {val}")
    
    set_clause = ", ".join(set_clauses)
    
    with conn.cursor() as cursor:
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        cursor.execute(query)

def delete_record(table_name: str, where_clause: str, conn):
    """Delete a record"""
    with conn.cursor() as cursor:
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor.execute(query)

def render_form_field(column_name: str, column_type: str, current_value: Any = None, key_suffix: str = ""):
    """Render appropriate form field based on column type"""
    if current_value is None:
        current_value = ""
    
    # Handle pandas NaN values
    if pd.isna(current_value):
        current_value = ""
    
    field_key = f"{column_name}_{key_suffix}" if key_suffix else column_name
    
    # Convert column type to appropriate Streamlit input
    if "int" in column_type.lower() or "bigint" in column_type.lower():
        try:
            default_val = int(current_value) if current_value != "" and current_value is not None else 0
        except (ValueError, TypeError):
            default_val = 0
        return st.number_input(
            f"{column_name} ({column_type})",
            value=default_val,
            step=1,
            key=field_key
        )
    elif "float" in column_type.lower() or "double" in column_type.lower() or "decimal" in column_type.lower():
        try:
            default_val = float(current_value) if current_value != "" and current_value is not None else 0.0
        except (ValueError, TypeError):
            default_val = 0.0
        return st.number_input(
            f"{column_name} ({column_type})",
            value=default_val,
            step=0.01,
            key=field_key
        )
    elif "boolean" in column_type.lower():
        try:
            default_val = bool(current_value) if current_value != "" and current_value is not None else False
        except (ValueError, TypeError):
            default_val = False
        return st.checkbox(
            f"{column_name} ({column_type})",
            value=default_val,
            key=field_key
        )
    elif "date" in column_type.lower() and "timestamp" not in column_type.lower():
        return st.date_input(
            f"{column_name} ({column_type})",
            key=field_key
        )
    elif "timestamp" in column_type.lower():
        return st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value) if current_value != "" and current_value is not None else "",
            help="Format: YYYY-MM-DD HH:MM:SS",
            key=field_key
        )
    else:  # Default to text input for strings and other types
        return st.text_input(
            f"{column_name} ({column_type})",
            value=str(current_value) if current_value != "" and current_value is not None else "",
            key=field_key
        )

# Main interface
tab_form, tab_view, tab_code, tab_requirements = st.tabs(["**Form Editor**", "**Table View**", "**Code**", "**Requirements**"])

with tab_form:
    # Connection info display
    st.info(f"🔗 **Connection Details:**\n- Host: `{DATABRICKS_HOST}`\n- Warehouse: `{HTTP_PATH}`\n- Table: `{TABLE_NAME}`")
    
    # Connect button
    if st.button("🔌 Connect to Table", type="primary"):
        try:
            with st.spinner("Connecting to Databricks..."):
                conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                st.session_state.table_data = read_table(TABLE_NAME, conn)
                st.session_state.table_schema = get_table_schema(TABLE_NAME, conn)
                st.session_state.connection_established = True
            st.success("✅ Successfully connected to the table!")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Connection failed: {str(e)}")
            st.session_state.connection_established = False
    
    # Show connection status
    if st.session_state.connection_established and st.session_state.table_data is not None:
        st.success(f"✅ Connected to {TABLE_NAME} ({len(st.session_state.table_data)} records)")
        
        # Refresh button
        if st.button("🔄 Refresh Data"):
            try:
                with st.spinner("Refreshing data..."):
                    conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                    st.session_state.table_data = read_table(TABLE_NAME, conn)
                    st.session_state.table_schema = get_table_schema(TABLE_NAME, conn)
                st.success("✅ Data refreshed!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Refresh failed: {str(e)}")
        
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
                # Create more readable record options
                record_options = []
                for i, row in st.session_state.table_data.iterrows():
                    # Show first few non-null columns for identification
                    display_cols = []
                    for col, val in row.items():
                        if pd.notna(val) and val != "" and len(display_cols) < 3:
                            display_cols.append(f"{col}={val}")
                    record_display = f"Row {i}: {' | '.join(display_cols)}"
                    record_options.append(record_display)
                
                selected_idx = st.selectbox(
                    "Select record to edit:",
                    range(len(record_options)),
                    format_func=lambda x: record_options[x]
                )
                
                if selected_idx is not None:
                    selected_record = st.session_state.table_data.iloc[selected_idx]
                    
                    # Show current record in expandable section
                    with st.expander("📋 Current Record Details", expanded=False):
                        st.json(selected_record.to_dict())
                    
                    # Form for editing
                    with st.form("edit_record_form"):
                        st.write("**Edit Record:**")
                        form_data = {}
                        
                        # Create form fields in columns for better layout
                        cols = st.columns(2)
                        col_idx = 0
                        
                        for column, dtype in st.session_state.table_schema.items():
                            with cols[col_idx % 2]:
                                current_value = selected_record.get(column, "")
                                form_data[column] = render_form_field(column, dtype, current_value, "edit")
                            col_idx += 1
                        
                        col_save, col_cancel = st.columns(2)
                        with col_save:
                            save_changes = st.form_submit_button("💾 Save Changes", type="primary")
                        with col_cancel:
                            cancel_changes = st.form_submit_button("❌ Cancel")
                        
                        if save_changes:
                            try:
                                conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                                # Create WHERE clause using first column as identifier
                                first_col = list(st.session_state.table_schema.keys())[0]
                                first_val = selected_record[first_col]
                                if isinstance(first_val, str):
                                    where_clause = f"{first_col} = '{first_val.replace(\"'\", \"''\"))}'"
                                else:
                                    where_clause = f"{first_col} = {first_val}"
                                
                                update_record(TABLE_NAME, form_data, where_clause, conn)
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
                
                # Create form fields in columns for better layout
                cols = st.columns(2)
                col_idx = 0
                
                for column, dtype in st.session_state.table_schema.items():
                    with cols[col_idx % 2]:
                        form_data[column] = render_form_field(column, dtype, key_suffix="add")
                    col_idx += 1
                
                col_add, col_clear = st.columns(2)
                with col_add:
                    add_record_btn = st.form_submit_button("➕ Add Record", type="primary")
                with col_clear:
                    clear_form = st.form_submit_button("🗑️ Clear Form")
                
                if add_record_btn:
                    try:
                        conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                        insert_record(TABLE_NAME, form_data, conn)
                        st.success("✅ Record added successfully!")
                        st.session_state.table_data = None  # Force refresh
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error adding record: {str(e)}")
        
        elif action == "Delete Record":
            st.subheader("🗑️ Delete Record")
            st.warning("⚠️ This action cannot be undone!")
            
            if len(st.session_state.table_data) > 0:
                # Create more readable record options
                record_options = []
                for i, row in st.session_state.table_data.iterrows():
                    display_cols = []
                    for col, val in row.items():
                        if pd.notna(val) and val != "" and len(display_cols) < 3:
                            display_cols.append(f"{col}={val}")
                    record_display = f"Row {i}: {' | '.join(display_cols)}"
                    record_options.append(record_display)
                
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
                            conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
                            # Create WHERE clause using first column as identifier
                            first_col = list(st.session_state.table_schema.keys())[0]
                            first_val = selected_record[first_col]
                            if isinstance(first_val, str):
                                where_clause = f"{first_col} = '{first_val.replace(\"'\", \"''\"))}'"
                            else:
                                where_clause = f"{first_col} = {first_val}"
                            
                            delete_record(TABLE_NAME, where_clause, conn)
                            st.success("✅ Record deleted successfully!")
                            st.session_state.table_data = None  # Force refresh
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error deleting record: {str(e)}")
            else:
                st.info("No records found in the table.")
    else:
        st.info("👆 Click 'Connect to Table' to start managing your data.")

with tab_view:
    st.subheader("📊 Table Data View")
    
    if st.session_state.table_data is not None:
        # Search functionality
        search_term = st.text_input("🔍 Search records:", placeholder="Enter search term...")
        
        display_data = st.session_state.table_data
        if search_term:
            # Simple search across all string columns
            mask = display_data.astype(str).apply(
                lambda x: x.str.contains(search_term, case=False, na=False)
            ).any(axis=1)
            display_data = display_data[mask]
            st.info(f"Found {len(display_data)} records matching '{search_term}'")
        
        st.dataframe(
            display_data,
            use_container_width=True,
            hide_index=True
        )
        
        # Table statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(st.session_state.table_data))
        with col2:
            st.metric("Displayed", len(display_data))
        with col3:
            st.metric("Columns", len(st.session_state.table_data.columns))
        with col4:
            memory_kb = st.session_state.table_data.memory_usage(deep=True).sum() / 1024
            st.metric("Memory Usage", f"{memory_kb:.1f} KB")
    else:
        st.info("Connect to the table in the Form Editor tab to view data here.")

with tab_code:
    st.code(f"""
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

# Connection details for DQ MDAR Inventory Data Mart
DATABRICKS_HOST = "{DATABRICKS_HOST}"
HTTP_PATH = "{HTTP_PATH}"
TABLE_NAME = "{TABLE_NAME}"

@st.cache_resource(ttl="1h")
def get_connection(server_hostname: str, http_path: str):
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: Config().authenticate,
    )

def read_table(table_name: str, conn, limit: int = 1000) -> pd.DataFrame:
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {{table_name}} LIMIT {{limit}}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

def insert_record(table_name: str, record_data: dict, conn):
    columns = list(record_data.keys())
    values = []
    
    for val in record_data.values():
        if val is None or val == "":
            values.append("NULL")
        elif isinstance(val, str):
            values.append(f"'{{val.replace(\\"'\\", \\"''\\")}}}'")
        else:
            values.append(str(val))
    
    columns_str = ", ".join(columns)
    values_str = ", ".join(values)
    
    with conn.cursor() as cursor:
        query = f"INSERT INTO {{table_name}} ({{columns_str}}) VALUES ({{values_str}})"
        cursor.execute(query)

# Usage example:
conn = get_connection(DATABRICKS_HOST, HTTP_PATH)
df = read_table(TABLE_NAME, conn)

# Insert new record
new_record = {{"column1": "value1", "column2": 123}}
insert_record(TABLE_NAME, new_record, conn)
""")

with tab_requirements:
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **Permissions Required**
        * `SELECT` on dg_dev.sandbox.dq_mdar_inventory_data_mart
        * `MODIFY` on dg_dev.sandbox.dq_mdar_inventory_data_mart
        * `CAN USE` on the SQL warehouse
        * Valid Databricks authentication
        """)
    
    with col2:
        st.markdown(f"""
        **Databricks Resources**
        * Host: {DATABRICKS_HOST}
        * Warehouse: {HTTP_PATH}
        * Table: {TABLE_NAME}
        """)
    
    with col3:
        st.markdown("""
        **Dependencies**
        * databricks-sdk
        * databricks-sql-connector
        * pandas
        * streamlit
        """)