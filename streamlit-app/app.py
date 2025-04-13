import streamlit as st # type:ignore

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

#Data extraction engine
engine = get_engine()

with tab1:
    st.subheader("Data analysis 1: Weather vs Energy")
    st.write("Weather and Energy consumption patterns in LA.")

    df = load_table("experiment1", engine)

    if not df.empty:
        plot_exp_1(df)
    else:
        st.warning("No data found.")

        st.title("Energy Consumption vs Weather Conditions")

with tab2:
    pass
with tab3:
    pass


# Footer
st.markdown("---")
st.markdown("📍 LA Open Data")
st.markdown("© 2025 Bases de Dades Avançades")