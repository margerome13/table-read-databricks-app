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
    values = []
    
    for val in record_data.values():
        if val is None or val == "":
            values.append("NULL")
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")  # Escape single quotes
            values.append(f"'{escaped_val}'")
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
            escaped_val = val.replace("'", "''")  # Escape single quotes
            set_clauses.append(f"{col} = '{escaped_val}'")
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

# Get available resources
try:
    warehouses = w.warehouses.list()
    warehouse_paths = {wh.name: wh.odbc_params.path for wh in warehouses}
    catalogs = w.catalogs.list()
except Exception as e:
    st.error(f"Error connecting to Databricks: {str(e)}")
    st.stop()

def get_schema_names(catalog_name):
    schemas = w.schemas.list(catalog_name=catalog_name)
    return [schema.name for schema in schemas]

def get_table_names(catalog_name, schema_name):
    tables = w.tables.list(catalog_name=catalog_name, schema_name=schema_name)
    return [table.name for table in tables]

# Main interface
tab_form, tab_view, tab_code, tab_requirements = st.tabs(["**Form Editor**", "**Table View**", "**Code**", "**Requirements**"])

with tab_form:
    st.info("💡 **Quick Setup for DQ MDAR Table**: Use the pre-configured DQ MDAR Editor for faster access to your specific table.")
    
    # Connection method selection
    connection_method = st.radio(
        "Choose connection method:",
        ["Select from dropdowns", "Direct table input"],
        horizontal=True
    )
    
    # Initialize variables for dropdown method
    schema_name = None
    table_name = None
    http_path_input = None
    catalog_name = None
    
    if connection_method == "Direct table input":
        # Direct input method
        st.subheader("🔗 Direct Connection")
        
        col1, col2 = st.columns(2)
        with col1:
            warehouse_input = st.text_input(
                "SQL Warehouse HTTP Path:",
                value="/sql/1.0/warehouses/80e5636f05f63c9b",
                help="Example: /sql/1.0/warehouses/your-warehouse-id"
            )
        
        with col2:
            table_input = st.text_input(
                "Full Table Name:",
                value="dg_dev.sandbox.dq_mdar_inventory_data_mart",
                help="Format: catalog.schema.table"
            )
        
        if warehouse_input and table_input:
            # Find matching warehouse path
            http_path = None
            for wh_name, wh_path in warehouse_paths.items():
                if wh_path == warehouse_input:
                    http_path = wh_path
                    break
            
            if not http_path:
                # Use the input directly if not found in list
                http_path = warehouse_input
            
            try:
                conn = get_connection(http_path)
                
                # Load table data and schema
                if st.session_state.table_data is None or st.button("🔄 Refresh Data"):
                    with st.spinner("Loading table data..."):
                        st.session_state.table_data = read_table(table_input, conn)
                        st.session_state.table_schema = get_table_schema(table_input, conn)
                
                if st.session_state.table_data is not None:
                    st.success(f"✅ Connected to {table_input} ({len(st.session_state.table_data)} records)")
                    
                    # Show the form interface
                    show_table_form_interface(conn, table_input)
                    
            except Exception as e:
                st.error(f"❌ Connection failed: {str(e)}")
    
    else:
        # Dropdown selection method
        st.subheader("📋 Select from Available Resources")
        
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
        
        # Schema selection
        if catalog_name and catalog_name != "":
            try:
                schema_names = get_schema_names(catalog_name)
                col3, col4 = st.columns(2)
                
                with col3:
                    schema_name = st.selectbox("Select Schema:", [""] + schema_names)
                
                # Table selection
                with col4:
                    if schema_name and schema_name != "":
                        try:
                            table_names = get_table_names(catalog_name, schema_name)
                            table_name = st.selectbox("Select Table:", [""] + table_names)
                        except Exception as e:
                            st.error(f"Error loading tables: {str(e)}")
                            table_name = None
            except Exception as e:
                st.error(f"Error loading schemas: {str(e)}")
                schema_name = None
                table_name = None
        
        # Load table data when all selections are made
        if (http_path_input and catalog_name and schema_name and table_name and 
            http_path_input != "" and catalog_name != "" and schema_name != "" and table_name != ""):
            
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            http_path = warehouse_paths[http_path_input]
            
            try:
                conn = get_connection(http_path)
                
                # Load table data and schema
                if st.session_state.table_data is None or st.button("🔄 Refresh Data"):
                    with st.spinner("Loading table data..."):
                        st.session_state.table_data = read_table(full_table_name, conn)
                        st.session_state.table_schema = get_table_schema(full_table_name, conn)
                
                if st.session_state.table_data is not None:
                    st.success(f"✅ Connected to {full_table_name} ({len(st.session_state.table_data)} records)")
                    
                    # Show the form interface
                    show_table_form_interface(conn, full_table_name)
                    
            except Exception as e:
                st.error(f"❌ Connection failed: {str(e)}")

def show_table_form_interface(conn, table_name):
    """Show the main form interface for table operations"""
    
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
                            # Create WHERE clause using first column as identifier
                            first_col = list(st.session_state.table_schema.keys())[0]
                            first_val = selected_record[first_col]
                            if isinstance(first_val, str):
                                escaped_val = first_val.replace("'", "''")
                                where_clause = f"{first_col} = '{escaped_val}'"
                            else:
                                where_clause = f"{first_col} = {first_val}"
                            
                            update_record(table_name, form_data, where_clause, conn)
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
                    insert_record(table_name, form_data, conn)
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
                        # Create WHERE clause using first column as identifier
                        first_col = list(st.session_state.table_schema.keys())[0]
                        first_val = selected_record[first_col]
                        if isinstance(first_val, str):
                            escaped_val = first_val.replace("'", "''")
                            where_clause = f"{first_col} = '{escaped_val}'"
                        else:
                            where_clause = f"{first_col} = {first_val}"
                        
                        delete_record(table_name, where_clause, conn)
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
        st.info("Connect to a table in the Form Editor tab to view data here.")

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
    values = []
    
    for val in record_data.values():
        if val is None or val == "":
            values.append("NULL")
        elif isinstance(val, str):
            values.append(f"'{val.replace(\\"'\\", \\"''\\")}'")
        else:
            values.append(str(val))
    
    columns_str = ", ".join(columns)
    values_str = ", ".join(values)
    
    with conn.cursor() as cursor:
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        cursor.execute(query)

# Usage example:
conn = get_connection("/sql/1.0/warehouses/your-warehouse-id")
df = read_table("catalog.schema.table", conn)

# Insert new record
new_record = {"column1": "value1", "column2": 123}
insert_record("catalog.schema.table", new_record, conn)
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