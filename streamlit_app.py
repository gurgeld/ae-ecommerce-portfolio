import duckdb
import streamlit as st
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "olist.duckdb"

st.title("DuckDB Connection Test")

if not DB_PATH.exists():
    st.error(f"Database not found at {DB_PATH}. Run ingestion first.")
else:
    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
        st.success("Connected to DuckDB!")
        tables = con.execute("SHOW TABLES").fetchall()
        if tables:
            st.write("Tables found:")
            st.write([t[0] for t in tables])
            first_table = tables[0][0]
            st.write(f"Showing sample from {first_table}:")
            df = con.execute(f"SELECT * FROM {first_table} LIMIT 5").df()
            st.dataframe(df)
        else:
            st.warning("No tables found in the database.")
    except Exception as e:
        st.error(f"Error connecting to DuckDB: {e}")
