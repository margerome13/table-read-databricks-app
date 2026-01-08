import streamlit as st

st.set_page_config(
    page_title="Databricks Table Form Editor",
    page_icon="📝",
    layout="wide"
)

st.logo("assets/logo.svg")
st.title("📝 Databricks Table Form Editor")
st.write("A user-friendly form interface for managing Databricks table records")

# Import and run the form editor directly
exec(open("views/table_form_editor.py").read())