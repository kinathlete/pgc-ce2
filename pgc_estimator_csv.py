# Import required libraries
# OS
import os
# Pandas
import pandas as pd
# Streamlit
import streamlit as st
# Altair
import altair as alt
# App Core Functions
# from core.exivity_export import ExivityExport

# SET PAGE CONTEXT
st.set_page_config(
     page_title="Project Green Cloud - CE2",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This is an *extremely* cool app crafted with Streamlit."
     }
 )

# IMPORT EXIVITY DEMO DATA
def import_cloud_usage_data():
    current_dir = os.path.dirname(__file__)
    path_to_data = os.path.join(current_dir, 'data/exivity_report_depth2.csv')
    cu_df = pd.read_csv(path_to_data)

    return cu_df

# IMPORT REGIONS DATA
def import_regions_data():
    current_dir = os.path.dirname(__file__)
    path_to_data = os.path.join(current_dir, 'data/regions_pue_ci_regions.csv')
    cu_df = pd.read_csv(path_to_data)

    return cu_df

# # IMPORT PUE DATA
# def import_pue_data():
#     current_dir = os.path.dirname(__file__)
#     path_to_data = os.path.join(current_dir, 'data/regions_pue_ci_pue.csv')
#     cu_df = pd.read_csv(path_to_data)

#     return cu_df

# # IMPORT CARBON INTENSITY DATA
# def import_carbon_intensity_data():
#     current_dir = os.path.dirname(__file__)
#     path_to_data = os.path.join(current_dir, 'data/regions_pue_ci_ci.csv')
#     cu_df = pd.read_csv(path_to_data)

#     return cu_df

# BUILD CLEAN CLOUD USAGE DF
def build_clean_cu_pd(raw_pd):
    subset_pd = raw_pd[['rate_id', 'total_cogs', 'account_key', 'account_id','account_name', 'service_key', 'servicecategory_name',\
        'day', 'service_id', 'servicecategory_id', 'instance_value']]
    subset_pd['day'] = pd.to_datetime(subset_pd['day'], format='%Y%m%d')
    # FILTER AWS ROWS
    subset_pd = subset_pd[subset_pd['service_key'].str.contains("BoxUsage")]
    # EXTRACT REGION INTO SEPARATE COLUMN
    subset_pd['data_center'] = subset_pd['service_key'].str.split(' ', 1).str[-1]
    subset_pd['data_center'] = "aws-" + subset_pd['data_center'].astype(str)
    subset_pd['region'] = subset_pd['service_key'].str.split(' ', 1).str[-1].str[:-1]
    subset_pd['region'] = "aws-" + subset_pd['region'].astype(str)
    
    return subset_pd

# BUILD FULL EMISSIONS DF
def build_filtered_emissions_pd(cu_pd, reg_pd):
    filtered_cu_pd = cu_pd[(cu_pd['day'].dt.date >= filter_min_date) &  (cu_pd['day'].dt.date <= filter_max_date)]
    fil_em_pd = pd.merge(filtered_cu_pd, reg_pd, on='region')
    fil_em_pd['co2_e'] = fil_em_pd['total_cogs'] * fil_em_pd['pue'] * fil_em_pd['carbon_intensity']

    return fil_em_pd

# BUILD AGG EMISSIONS DF BY ANY GIVEN DIM
def build_agg_e_by_dim(full_e_pd, dim, agg):
    agg_e_pd_by_dim = full_e_pd[[dim, 'co2_e']].groupby(dim, as_index=False).agg(agg)
    
    return agg_e_pd_by_dim

# BUILD AGG EMISSIONS DF BY MULTIPLE GIVEN DIMS ('day', <some_dim>)
def build_agg_e_by_mult_dims(full_e_pd, dims, agg):
    dims_for_ds = list(('day',) + dims + ('co2_e',))
    dims_for_groupby = list(('day',) + dims)
    agg_e_pd_by_mult_dims = full_e_pd[dims_for_ds].groupby(dims_for_groupby, as_index=False).agg(agg)
    for d in dims:
        agg_e_pd_by_mult_dims[d]=agg_e_pd_by_mult_dims[d].astype(str)
    return agg_e_pd_by_mult_dims

# GET PUE OF DC SELECTED IN SIM FILTERS
def get_sim_dc_pue(reg_pd):
    sim_dc_pue = reg_pd[reg_pd['region'] == sim_improved_dc]['pue'].values

    return sim_dc_pue.item()

# BUILD DATA CENTER REPLACE SCENARIO DF
def build_filtered_sim_1_pd(cu_pd, replaced_dc, replacing_dc, reg_pd):
    fil_sim_1_cu_pd = cu_pd.copy()
    fil_sim_1_cu_pd.loc[fil_sim_1_cu_pd['region'] == replaced_dc, 'region'] = replacing_dc
    fil_sim_1_cu_pd = fil_sim_1_cu_pd[(fil_sim_1_cu_pd['day'].dt.date >= filter_min_date) &  \
        (fil_sim_1_cu_pd['day'].dt.date <= filter_max_date)]
    fil_sim_1_pd = pd.merge(fil_sim_1_cu_pd, reg_pd, on='region')
    fil_sim_1_pd['co2_e'] = fil_sim_1_pd['total_cogs'] * fil_sim_1_pd['pue'] * fil_sim_1_pd['carbon_intensity']

    return fil_sim_1_pd

# BUILD PUE IMPROVE SCENARIO DF
def build_filtered_sim_2_pd(cu_pd, improved_dc, sim_pue, reg_pd):
    fil_sim_2_cu_pd = cu_pd[(cu_pd['day'].dt.date >= filter_min_date) &  (cu_pd['day'].dt.date <= filter_max_date)]
    fil_sim_2_pd = pd.merge(fil_sim_2_cu_pd, reg_pd, on='region')
    fil_sim_2_pd.loc[fil_sim_2_pd['region'] == improved_dc, 'pue'] = sim_pue
    fil_sim_2_pd['co2_e'] = fil_sim_2_pd['total_cogs'] * fil_sim_2_pd['pue'] * fil_sim_2_pd['carbon_intensity']

    return fil_sim_2_pd

# APP INTRO
st.title("PGC - Cloud Emissions Estimator")
st.header("Powered by Exivity & Claudy | Crafted with Streamlit")
st.write("The estimate is derived from cloud-usage-based power consumption, region-based data center efficiency and carbon intensity.\
    Real-world power consumption provided by Exivity, data center efficiency and carbon intensity researched and final app engineered by Claudy.")
with st.expander("Find out more about our approach - click here"):
    st.write("more info soon.")

# RAW DATA
raw_cu_pd = import_cloud_usage_data()
raw_reg_pd = import_regions_data()
clean_cu_pd = build_clean_cu_pd(raw_cu_pd)
co2t_price = 96.5
filter_dimensions = ['service_id', 'data_center', 'region', 'account_name']

# SIDEBAR
## FILTERS
st.sidebar.subheader('FILTERS')
### DATE FILTER ###
st.sidebar.markdown("**Select a date range:** :point_down:")
filter_min_date = st.sidebar.date_input("Start date", clean_cu_pd['day'].min(), clean_cu_pd['day'].min(), clean_cu_pd['day'].max())
filter_max_date = st.sidebar.date_input("End date", clean_cu_pd['day'].max(), clean_cu_pd['day'].min(), clean_cu_pd['day'].max())
### BUILD FULL EMISSIONS DFS WITH FILTERS ###
filtered_em_pd = build_filtered_emissions_pd(clean_cu_pd, raw_reg_pd)

## SCENARIOS
st.sidebar.text('')
st.sidebar.subheader('SCENARIOS')
### SCENARIO SETTER 1 ###
st.sidebar.markdown("**Set your scenario 1:** :point_down:")
sim_replaced_dc = st.sidebar.selectbox(label="Data center region to be replaced",options=filtered_em_pd['region'].unique(),\
    help="Select a data center that you would like to swap out with another data center",)
sim_replacing_dc = st.sidebar.selectbox(label="Replacing data center region",options=raw_reg_pd["region"],\
    help="Select a data center that you would like to replace the above selected data center with",)
st.sidebar.text('')
### SCENARIO SETTER 2 ###
st.sidebar.markdown("**Set your scenario 2:** :point_down:")
sim_improved_dc = st.sidebar.selectbox(label="Data center to be improved",options=filtered_em_pd['region'].unique(),\
    help="Select a data center where you would like to simulate a PUE factor improvement",)
sim_pue = st.sidebar.slider(label="Set a target PUE", min_value=1.01, max_value=1.99, value=get_sim_dc_pue(raw_reg_pd),\
    step=0.01, help="Select the PUE that you want to simulate with")
st.sidebar.markdown("_The current PUE of this data center is {}_".format(get_sim_dc_pue(raw_reg_pd)))
### EXECUTE SCENARIOS ###
sim_run_button = st.sidebar.button('Run Scenarios')
if sim_run_button:
    sim_1_pd = build_filtered_sim_1_pd(clean_cu_pd, sim_replaced_dc, sim_replacing_dc, raw_reg_pd)
    sim_2_pd = build_filtered_sim_2_pd(clean_cu_pd, sim_improved_dc, sim_pue, raw_reg_pd)

with st.container():
    st.subheader("Loaded Dataset (BETA)")
    st.markdown("_Importable via Exivity API | BETA: loaded from CSV_")
    col11, col12, col13, col14 = st.columns(4)
    with col11:
        st.metric(label="Services Used", value=filtered_em_pd['instance_value'].count(),help="Service = distinct server instance used from the AWS Elastic Compute Cloud library")
    with col12:
        st.metric(label="Products Serviced", value=filtered_em_pd['service_id'].nunique(),help="Product = a distinct product/project for which a cloud resource is utilised")
    with col13:
        st.metric(label="Data Centers", value=filtered_em_pd['data_center'].nunique(),help="Data Center = a distinct building stacked with servers (in aws, a data center is \
            determind by the last character in the location name, such as eu-central-1a<--)")
    with col14:
        st.metric(label="Duration", value="{} days".format(filtered_em_pd['day'].nunique()),help="= number of available days in the loaded dataset")
    with st.expander("Click here to visit the input data."):
        st.markdown("**CLOUD USAGE DATA (EXIVITY)**")
        st.dataframe(data=filtered_em_pd, use_container_width=True)
        st.markdown("**REGIONS DATA (CLAUDY)** - limited to AWS.")
        st.dataframe(data=raw_reg_pd, use_container_width=True)

with st.container():
    st.subheader("ACME's Cloud Carbon Footprint")
    col21, col22 = st.columns([1,3])
    with col21:
        st.metric("CO2 in tons", "{}t".format(filtered_em_pd['co2_e'].sum().round(decimals=2)),\
            help="This shows your overall carbon emissions in tons based on the select cloud usage data.")
        st.metric("Current price per ton", "{}€".format(co2t_price),\
            help="This shows the official EU Carbon Permit price per CO2 metric ton as per FEB 13 2023.")
        st.metric("Offset cost total", "{}€".format(filtered_em_pd['co2_e'].sum().round(decimals=2)*co2t_price),\
            help="This shows your total cost to offset your current carbon emissions at the current price per CO2 ton.")
    with col22:
        param_total_e_chart_dim = st.selectbox(label="Drill-down dimension",options=filter_dimensions,\
    help="Select a dimension to drill-down emissions over time in the chart below",)
        total_e_barchart = alt.Chart(build_agg_e_by_mult_dims(filtered_em_pd, (param_total_e_chart_dim,), 'sum'))\
            .mark_bar().encode(
                x='day',
                y='co2_e',
                color=param_total_e_chart_dim
            )
        st.altair_chart(total_e_barchart, use_container_width=True, theme="streamlit")

with st.container():
    col31, col32, col33 = st.columns(3)
    with col31:
        st.subheader("Current State")
        st.metric("CO2 Emissions", "{}t".format(filtered_em_pd['co2_e'].sum().round(decimals=2)))
        st.write('')
        st.write("CO2 by Account")
        st.bar_chart(data=build_agg_e_by_dim(filtered_em_pd, 'account_name', 'sum'), x="account_name", y="co2_e", use_container_width=True)
        st.write("CO2 by Region")
        st.bar_chart(data=build_agg_e_by_dim(filtered_em_pd, 'region', 'sum'), x="region", y="co2_e", use_container_width=True)

    with col32:
        st.subheader("Scenario 1")
        if sim_run_button:
            st.metric("CO2 Emissions", "{}t".format(sim_1_pd['co2_e'].sum().round(decimals=2)),(sim_1_pd['co2_e'].sum()\
                -filtered_em_pd['co2_e'].sum()).round(decimals=2),'inverse')
            st.write("CO2 by Account")
            st.bar_chart(data=build_agg_e_by_dim(sim_1_pd, 'account_name', 'sum'), x="account_name", y="co2_e", use_container_width=True)
            st.write("CO2 by Region")
            st.bar_chart(data=build_agg_e_by_dim(sim_1_pd, 'region', 'sum'), x="region", y="co2_e", use_container_width=True)

    with col33:
        st.subheader("Scenario 2")
        if sim_run_button:
            st.metric("CO2 Emissions", "{}t".format(sim_2_pd['co2_e'].sum().round(decimals=2)),(sim_2_pd['co2_e'].sum()\
                -filtered_em_pd['co2_e'].sum()).round(decimals=2),'inverse')
            st.write("CO2 by Account")
            st.bar_chart(data=build_agg_e_by_dim(sim_2_pd, 'account_name', 'sum'), x="account_name", y="co2_e", use_container_width=True)
            st.write("CO2 by Region")
            st.bar_chart(data=build_agg_e_by_dim(sim_2_pd, 'region', 'sum'), x="region", y="co2_e", use_container_width=True)