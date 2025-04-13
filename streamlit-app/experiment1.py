import pandas as pd
import plotly.express as px
import streamlit as st

def plot_exp_1(df):
    stations = df['station'].unique()
    selected_station = st.selectbox("Select a Station", stations)

    # --- Filter Data ---
    filtered_df = df[df['station'] == selected_station]

    # --- Create Scatter Matrix Plot ---
    fig = px.scatter_matrix(
        filtered_df,
        dimensions=['TMAX', 'TMIN', 'PRCP'],
        color='cenergy',
        title=f'Weather vs Consumed Energy for {selected_station}',
        labels={col: col for col in ['TMAX', 'TMIN', 'PRCP', 'cenergy']},
        height=700
    )

    # Update marker size for better visibility
    fig.update_traces(diagonal_visible=False,showupperhalf=False, marker=dict(size=5))

    # --- Display Plot ---
    st.plotly_chart(fig, use_container_width=True)