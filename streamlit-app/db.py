import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st

# --- Get DB info from environment or defaults ---
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASSWORD = os.getenv("DB_PASS", "airflow")
DB_NAME = os.getenv("DB_NAME", "airflow")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@st.cache_resource(ttl=60)
def get_engine():
    """
    Connects to the database, retrying on failure.
    Uses st.cache_resource to cache the engine object.
    """
    while True:
        try:
            engine = create_engine(DATABASE_URL, pool_pre_ping=True)
            # Test connection
            with engine.connect() as connection:
                pass  # Connection successful if no exception
            return engine
        except Exception as e:
            # Use st.error for connection issues, make it clear it's retrying
            st.error(f"Database connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# Function to check table existence (not cached itself, check happens before cache lookup)
def _check_table_exists(_engine, table_name: str) -> bool:
    """Checks if a table or view exists in the public schema."""
    try:
        with _engine.connect() as connection:

            query = text("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                )
            """)
            result = connection.execute(query, {"table_name": table_name}).scalar()
            return result or False
    except Exception as e:
        st.error(f"Error checking existence of table/view `{table_name}`: {e}")
        return False

@st.cache_data(ttl=30)
def _internal_load_data(_engine, table_name: str):
    """Internal function to load data, assuming table exists. This part is cached."""
    try:

        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, _engine)
        return df
    except Exception as e:

        raise ConnectionError(f"Error loading data from table `{table_name}`: {e}")


def load_table(table_name: str, _engine):

    if _check_table_exists(_engine, table_name):
        try:
            return _internal_load_data(_engine, table_name)
        except ConnectionError as e:
            st.error(str(e))
            return pd.DataFrame()
        except Exception as e:
            st.error(f"An unexpected error occurred while loading table `{table_name}`: {e}")
            return pd.DataFrame()
    else:
        st.warning(f"The view `{table_name}` does not seem to exist in the database yet.")

        if st.button("Retry Data Load Check", key=f"refresh_btn_{table_name}"):
            st.rerun()
        return pd.DataFrame()
