import streamlit as st
import snowflake.snowpark as sp
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Load private key from secrets
p_key = st.secrets["snowflake"]["private_key"]

# Load private key object
private_key_obj = serialization.load_pem_private_key(
    p_key.encode(),
    password=None,
    backend=default_backend()
)

# Convert private key to DER format and base64 encode
pkb = private_key_obj.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Define connection parameters
connection_parameters = {
    "account": st.secrets["snowflake"]["account"],
    "user": st.secrets["snowflake"]["user"],
    "private_key": pkb,
    "role": st.secrets["snowflake"]["role"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"]
}

# Establish session
session = sp.Session.builder.configs(connection_parameters).create()


# ✅ Confirm session identity and access
st.success("🔐 Connected to Snowflake!")
st.write(session.sql("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").collect())

# ✅ Check for the target table
tables = session.sql("SHOW TABLES IN DEMOS.GEDS").collect()
st.write("📋 Tables in DEMOS.GEDS:", tables)


session.sql("USE DATABASE DEMOS").collect()
session.sql("USE SCHEMA GEDS").collect()
st.write("✅ Connected as:", session.sql("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").collect())
st.write("📋 Tables in DEMOS.GEDS:", session.sql("SHOW TABLES IN DEMOS.GEDS").collect())

# === Load data ===
df = session.table("DEMOS.GEDS.GEDS_SHORT").to_pandas()

# === Sidebar Filters ===
st.sidebar.header("Filter Options")

# Reset filters button
if st.sidebar.button("🔄 Reset All Filters"):
    st.rerun()

# Department Acronym
dept_acronyms = ["All"] + sorted(df["DEPARTMENT_ACRONYM"].dropna().unique())
selected_dept_acr = st.sidebar.selectbox("Department Acronym", dept_acronyms)

# Department Name
if selected_dept_acr != "All":
    dept_names = df[df["DEPARTMENT_ACRONYM"] == selected_dept_acr]["DEPARTMENT_NAME_EN"].dropna().unique()
else:
    dept_names = df["DEPARTMENT_NAME_EN"].dropna().unique()

dept_names = ["All"] + sorted(dept_names)
selected_dept = st.sidebar.selectbox("Department Name", dept_names)

# Organization Acronym
if selected_dept != "All":
    org_acronyms = df[df["DEPARTMENT_NAME_EN"] == selected_dept]["ORGANIZATION_ACRONYM"].dropna().unique()
elif selected_dept_acr != "All":
    org_acronyms = df[df["DEPARTMENT_ACRONYM"] == selected_dept_acr]["ORGANIZATION_ACRONYM"].dropna().unique()
else:
    org_acronyms = df["ORGANIZATION_ACRONYM"].dropna().unique()

org_acronyms = ["All"] + sorted(org_acronyms)
selected_org_acr = st.sidebar.selectbox("Organization Acronym", org_acronyms)

# Organization Name
if selected_org_acr != "All":
    org_names = df[df["ORGANIZATION_ACRONYM"] == selected_org_acr]["ORGANIZATION_NAME_EN"].dropna().unique()
elif selected_dept != "All":
    org_names = df[df["DEPARTMENT_NAME_EN"] == selected_dept]["ORGANIZATION_NAME_EN"].dropna().unique()
elif selected_dept_acr != "All":
    org_names = df[df["DEPARTMENT_ACRONYM"] == selected_dept_acr]["ORGANIZATION_NAME_EN"].dropna().unique()
else:
    org_names = df["ORGANIZATION_NAME_EN"].dropna().unique()

org_names = ["All"] + sorted(org_names)
selected_org = st.sidebar.selectbox("Organization Name", org_names)

# Email checkbox
has_email = st.sidebar.checkbox("📧 Only show entries with an email address", value=False)

# Organization Structure Search
st.sidebar.header("🔠 Search Organization Structure")
search1 = st.sidebar.text_input("Search term 1")
search2 = search3 = ""
if search1:
    search2 = st.sidebar.text_input("Search term 2 (optional)")
    if search2:
        search3 = st.sidebar.text_input("Search term 3 (optional)")

# Job Title Search
st.sidebar.header("🔠 Search Job Title (TITLE_EN)")
title_search1 = st.sidebar.text_input("Title search term 1")
title_search2 = title_search3 = ""
if title_search1:
    title_search2 = st.sidebar.text_input("Title search term 2 (optional)")
    if title_search2:
        title_search3 = st.sidebar.text_input("Title search term 3 (optional)")

# Search mode
search_mode = st.sidebar.radio("Search Mode", ["AND", "OR"], horizontal=True)

# Global search
global_search = st.sidebar.text_input("🌐 Global Search (All Columns)")

# === Page title ===
st.title("GEDS Organizational Structure Explorer")

# === Apply filters ===
filter_applied = (
    selected_dept_acr != "All" or selected_dept != "All" or 
    selected_org_acr != "All" or selected_org != "All" or 
    search1 or title_search1 or has_email or global_search
)

if not filter_applied:
    st.warning("Please apply at least one filter to display results.")
else:
    filtered_df = df.copy()

    if selected_dept_acr != "All":
        filtered_df = filtered_df[filtered_df["DEPARTMENT_ACRONYM"] == selected_dept_acr]
    if selected_dept != "All":
        filtered_df = filtered_df[filtered_df["DEPARTMENT_NAME_EN"] == selected_dept]
    if selected_org_acr != "All":
        filtered_df = filtered_df[filtered_df["ORGANIZATION_ACRONYM"] == selected_org_acr]
    if selected_org != "All":
        filtered_df = filtered_df[filtered_df["ORGANIZATION_NAME_EN"] == selected_org]
    if has_email:
        filtered_df = filtered_df[filtered_df["EMAIL"].notna() & (filtered_df["EMAIL"] != "")]

    # Organization Structure Search
    search_terms = [term for term in [search1, search2, search3] if term]
    if search_terms:
        if search_mode == "AND":
            for term in search_terms:
                filtered_df = filtered_df[filtered_df["ORGANIZATION_STRUCTURE_EN"].str.contains(term, case=False, na=False)]
        else:
            pattern = "|".join([re.escape(term) for term in search_terms])
            filtered_df = filtered_df[filtered_df["ORGANIZATION_STRUCTURE_EN"].str.contains(pattern, case=False, na=False, regex=True)]

    # Job Title Search
    title_terms = [term for term in [title_search1, title_search2, title_search3] if term]
    if title_terms:
        if search_mode == "AND":
            for term in title_terms:
                filtered_df = filtered_df[filtered_df["TITLE_EN"].str.contains(term, case=False, na=False)]
        else:
            pattern = "|".join([re.escape(term) for term in title_terms])
            filtered_df = filtered_df[filtered_df["TITLE_EN"].str.contains(pattern, case=False, na=False, regex=True)]

    # Global search
    if global_search:
        pattern = re.escape(global_search)
        mask = filtered_df.apply(lambda row: row.astype(str).str.contains(pattern, case=False, na=False).any(), axis=1)
        filtered_df = filtered_df[mask]

    # Display results
    if len(filtered_df) > 100000:
        st.warning(f"{len(filtered_df)} results found. Please refine your search to fewer than 100,000 rows.")
    elif len(filtered_df) == 0:
        st.info("No matching results found.")
    else:
        st.markdown(f"### {len(filtered_df)} result(s) found")

        # Download and Save options
        csv = filtered_df.to_csv(index=False)
        st.download_button("Download Filtered Results", csv, "geds_filtered.csv", "text/csv")

        with st.expander("💾 Save to Snowflake"):
            table_name = st.text_input("Enter Snowflake table name to save to:")
            if st.button("Save Results") and table_name:
                target_table = f"DEMOS.GEDS.{table_name}"
                existing_tables = [f"{row['name'].upper()}" for row in session.sql("SHOW TABLES IN DEMOS.GEDS").collect()]
                if table_name.upper() in existing_tables:
                    session.write_pandas(filtered_df, target_table, overwrite=False)
                    st.success(f"Data appended to existing table '{target_table}'.")
                else:
                    session.write_pandas(filtered_df, target_table, auto_create_table=True)
                    st.success(f"New table '{target_table}' created in Snowflake.")

        display_df = filtered_df[[
            'GIVENNAME', 'SURNAME', 'TITLE_EN', 'EMAIL',
            'DEPARTMENT_NAME_EN', 'ORGANIZATION_NAME_EN', 'ORGANIZATION_STRUCTURE_EN'
        ]]
        st.dataframe(display_df, use_container_width=True)
