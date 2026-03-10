import streamlit as st

from src.database import get_conn, init_db
from src.theme_service import (
    add_ticker,
    create_theme,
    delete_theme,
    get_theme_members,
    list_themes,
    remove_ticker,
    seed_if_needed,
    update_theme,
)

st.set_page_config(page_title="Theme Registry", layout="wide")
st.title("Theme Registry")

init_db()
with get_conn() as conn:
    seed_if_needed(conn)

st.subheader("Create Theme")
with st.form("create_theme", clear_on_submit=True):
    new_name = st.text_input("Name")
    new_category = st.text_input("Category", value="Custom")
    new_is_active = st.checkbox("Active", value=True)
    create_submitted = st.form_submit_button("Create")

if create_submitted:
    try:
        with get_conn() as conn:
            create_theme(conn, new_name, new_category, new_is_active)
        st.success("Theme created")
        st.rerun()
    except Exception as exc:
        st.error(f"Create failed: {exc}")

with get_conn() as conn:
    themes = list_themes(conn)

if themes.empty:
    st.info("No themes available.")
    st.stop()

labels = {f"{r['name']} [{r['id']}]": int(r["id"]) for _, r in themes.iterrows()}
selected_label = st.selectbox("Select theme to manage", list(labels.keys()))
selected_id = labels[selected_label]
selected = themes[themes["id"] == selected_id].iloc[0]

st.subheader("Edit Theme")
with st.form("edit_theme"):
    edit_name = st.text_input("Theme name", value=selected["name"])
    edit_category = st.text_input("Category", value=selected["category"])
    edit_active = st.checkbox("Active", value=bool(selected["is_active"]))
    c1, c2 = st.columns(2)
    with c1:
        save = st.form_submit_button("Save changes")
    with c2:
        remove = st.form_submit_button("Delete theme")

if save:
    try:
        with get_conn() as conn:
            update_theme(conn, selected_id, edit_name, edit_category, edit_active)
        st.success("Theme updated")
        st.rerun()
    except Exception as exc:
        st.error(f"Update failed: {exc}")

if remove:
    try:
        with get_conn() as conn:
            delete_theme(conn, selected_id)
        st.success("Theme deleted")
        st.rerun()
    except Exception as exc:
        st.error(f"Delete failed: {exc}")

with get_conn() as conn:
    members = get_theme_members(conn, selected_id)

st.subheader("Manage Tickers")
col1, col2 = st.columns(2)

with col1:
    with st.form("add_ticker_form", clear_on_submit=True):
        new_ticker = st.text_input("Add ticker")
        add_submitted = st.form_submit_button("Add ticker")

if add_submitted:
    try:
        with get_conn() as conn:
            add_ticker(conn, selected_id, new_ticker)
        st.success(f"Added {new_ticker.strip().upper()}")
        st.rerun()
    except Exception as exc:
        st.error(f"Add ticker failed: {exc}")

with col2:
    if members.empty:
        st.info("No members to remove.")
        remove_submitted = False
        remove_tkr = None
    else:
        with st.form("remove_ticker_form"):
            remove_tkr = st.selectbox("Remove ticker", members["ticker"].tolist())
            remove_submitted = st.form_submit_button("Remove selected ticker")

if remove_submitted and remove_tkr:
    try:
        with get_conn() as conn:
            remove_ticker(conn, selected_id, remove_tkr)
        st.success(f"Removed {remove_tkr}")
        st.rerun()
    except Exception as exc:
        st.error(f"Remove ticker failed: {exc}")

st.dataframe(members, width="stretch")
