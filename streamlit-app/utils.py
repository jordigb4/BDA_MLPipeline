import os
import pickle
from io import BytesIO
import streamlit as st

from hdfs import InsecureClient
from hdfs.util import HdfsError

def load_pickle_from_hdfs(hdfs_path):
    client = InsecureClient(os.getenv('HDFS_URL'), user=os.getenv('HDFS_USER'))

    try:
        with client.read(hdfs_path) as reader:
            # Read all bytes and wrap in BytesIO
            buffer = BytesIO(reader.read())
    except HdfsError:
        st.warning(f"File not found in HDFS at: {hdfs_path}")
        return None

    # Load pickle safely
    try:
        return pickle.load(buffer)
    except pickle.UnpicklingError as e:
        raise ValueError("Invalid pickle file") from e
