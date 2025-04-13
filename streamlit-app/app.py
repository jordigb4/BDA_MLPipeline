import streamlit as st # type:ignore
import os
from utils import load_pickle_from_hdfs

# Set up page config
st.set_page_config(
    page_title="Los Angeles Urban Data Dashboard",
    page_icon="🌇",
    layout="wide",
)

# Title and description
st.title("🌇 Los Angeles Urban Data Dashboard")
st.markdown("""
Welcome to the **Los Angeles Urban Data Dashboard** — an interactive analysis of:
- 🌤️ **Weather Patterns**
- 🌫️ **Air Quality Trends**
- 🚗 **Traffic Conditions**
- ⚡ **Energy Consumption**

This dashboard offers a visual exploration of how these factors intertwine in shaping urban life in LA.
""")

# Tabs for each category
tab1, tab2, tab3 = st.tabs(["🌤️️⚡ Weather vs Energy", "🌫️ Air Quality prediction", "🚗 Traffic patterns"])

#HDFS BASE PATH
base_hdfs_path = "/data/data_analysis/"

with tab1:
    st.subheader("Data analysis 1: Weather vs Energy")
    st.write("Weather and Energy consumption patterns in LA.")

    selected_station1 = st.selectbox("Station name:",('reseda','downtown','long_beach'),
        key='station1_selectbox')

    # Load figure
    fig = load_pickle_from_hdfs(base_hdfs_path + f'exp1/{selected_station1}.pkl')

    #Plot figure
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Data analysis 2: Air quality prediction")
    st.write("Air quality prediction based on energy consumption and climate.")

    selected_station2 = st.selectbox("Station name:", ('reseda', 'downtown', 'long_beach'),
        key='station2_selectbox')

    plot_names = ['fig_errors','fig_importance','fig_real_vs_pred']

    for plot_name in plot_names:
        #Plot figure
        fig = load_pickle_from_hdfs(base_hdfs_path + f'exp2/{selected_station2}_{plot_name}.pkl')

        #Load figure
        st.plotly_chart(fig, use_container_width=True)


with tab3:
    st.subheader("Data analysis 3: Traffic patterns")
    st.write("Identification of traffic accident patterns in relation to weather.")
    'date_acc_patterns'

    plot_names = ['feature_importance_fig','results_cm_fig','results_corr_fig','time_series_fig','weather_impact_fig']

    for plot_name in plot_names:

        #Plot figure
        fig = load_pickle_from_hdfs(base_hdfs_path + f'exp3/{plot_name}.pkl')

        #Load figure
        st.plotly_chart(fig, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("📍 LA Open Data")
st.markdown("© 2025 Bases de Dades Avançades")