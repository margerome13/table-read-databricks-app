import streamlit as st

st.set_page_config(
    page_title="DQ MDAR Inventory Data Mart Editor",
    page_icon="📊",
    layout="wide"
)

st.logo("assets/logo.svg")
st.title("📊 DQ MDAR Inventory Data Mart Editor")
st.write("Manage your Data Quality MDAR Inventory records with ease")

# Import and run the DQ MDAR form editor directly
exec(open("views/dq_mdar_form_editor.py").read())