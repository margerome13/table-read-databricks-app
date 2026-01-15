import streamlit as st
from view_groups import groups

st.set_page_config(layout="wide")
st.logo("assets/logo.svg")
st.title(":material/verified: Databricks Apps :material/fact_check:")

pages = {
    group.get("title", ""): [
        st.Page(
            view.get("page"),
            title=view.get("label"),
            icon=view.get("icon"),
        )
        for view in group["views"]
    ]
    for group in groups
}

pg = st.navigation(pages)
pg.run()
