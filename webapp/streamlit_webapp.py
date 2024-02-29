import streamlit as st
import pandas as pd
import asyncio
from datetime import datetime
import os
import plotly.express as px
import time
import glob
# import nest_asyncio

#change project path to use fetch_utils.py under webapp folder
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from webapp.fetch_utils import DataFetcher, DataVisualizer

#change the path to the src folder of the project to use config.py
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.config import ELASTICSEARCH_SETTINGS, ES_INDEX_NAME, DATA_PATH, TMP_PATH, TMP_CSV_FILENAME, DATA_FOLDER_PATH
from src.elasticsearch_operations import ElasticsearchService
from src.llm_model import AsyncTextGenerator
from src.etl_utils import ETLUtils
from src.main import sync_sql_to_elasticsearch


def inject_custom_css():
    custom_css = """
        <style>
            /* Apply margin and padding reset more broadly */
            body, .stApp {
                padding: 0 !important;
                margin: 0 !important;
            }
            /* Set the title to be centered */
            .css-1l02zno {
                text-align: center;
            }
        </style>
    """
    st.markdown(custom_css, unsafe_allow_html=True)

def streamlit_main():
    st.set_page_config(layout="wide", page_title="Hotel Bookings", page_icon="📈")
    generator = AsyncTextGenerator()

    tab1, tab2, tab3 = st.tabs(["Dashboard", "Data", "Upload"])

    with tab1:

        inject_custom_css()

        # Set the title of the webapp and center it
        
        st.title("Hotel Booking Reports - Dashboard")


        # st.write("This webapp is a simple demonstration of how to use Streamlit to create a webapp that fetches data from an API and displays it in a table.")
        # st.write("The data is fetched from a FastAPI server that is running on a different machine. The server is connected to a PostgreSQL database and an Elasticsearch instance.")

        fetcher = DataFetcher()
        all_data = asyncio.run(fetcher.fetch_all_data_async())

        data_visualizer = DataVisualizer(all_data)
        
        col1, col2 = st.columns(2)

        with col1:
            data_visualizer.visualize_top_countries()
            data_visualizer.visualize_cancellation_rate()
            data_visualizer.visualize_cancellation_rate_pie()
            data_visualizer.visualize_adr_by_month()
            data_visualizer.visualize_length_of_stay_distribution()
            data_visualizer.visualize_special_requests_impact_on_cancellations()
            data_visualizer.visualize_average_lead_time_by_cancellation_status()
            data_visualizer.visualize_analyze_repeat_guest_bookings()

        with col2:
            data_visualizer.visualize_bookings_distribution_by_room_type()
            data_visualizer.visualize_booking_source_analysis()
            data_visualizer.visualize_booking_trends_over_time()
            data_visualizer.visualize_revenue_analysis_by_room_and_month()
            data_visualizer.visualize_impact_of_lead_time_on_adr()
            data_visualizer.visualize_analyze_booking_composition()
            data_visualizer.visualize_correlate_cancelations_with_factors()
            data_visualizer.visualize_bookings_by_guest_country()

    with tab2:
        st.title("Hotel Booking Reports - Data")

        subtab1, subtab2 = st.tabs(["Search", "Aggregation"])
        
        es_service = ElasticsearchService(config=ELASTICSEARCH_SETTINGS)

        with subtab1:

            st.title('Customizable Elasticsearch Search')

            # User input section
            st.subheader('Search Parameters')

            # Define search parameters
            search_params = {}

            data = pd.read_csv(DATA_PATH)

            #choose latest csv file from data folder
            csv_files = [f for f in glob.glob(DATA_FOLDER_PATH + '*') if os.path.isfile(f)]
            if csv_files:  # Check if there are any csv files
                latest_file = max(csv_files, key=os.path.getctime)
                data_uploaded = pd.read_csv(latest_file)

            #create TMP_PATH if it does not exist
            if not os.path.exists(TMP_PATH):
                os.makedirs(TMP_PATH)


            # Group related fields for better organization
            general_fields = ['hotel', 'is_canceled', 'arrival_date']
            booking_fields = ['country', 'market_segment', 'distribution_channel', 'is_repeated_guest']
            reservation_fields = ['reserved_room_type', 'booking_changes', 'deposit_type', 'customer_type']
            other_fields = ['meal', 'total_of_special_requests', 'reservation_status', 'reservation_status_date']


            # Provide clear instructions for each group
            # with st.expander("General Information"):
            #     for field in general_fields:
            #         unique_values = set(data[field])
            #         if unique_values:
            #             value = st.selectbox(f"{field.capitalize()}", [''] + sorted(unique_values))
            #             if value:
            #                 search_params[field] = value

            with st.expander("General Information"):
                for field in general_fields:
                    if field == 'arrival_date':
                        min_date = pd.to_datetime(data['arrival_date']).min()
                        max_date = pd.to_datetime(data_uploaded['arrival_date']).max()
                        # Set a default date within the range, e.g., the min_date
                        default_date = min_date
                        arrival_date = st.date_input("Arrival Date", value=default_date, min_value=min_date, max_value=max_date)
                        if arrival_date:
                            search_params[field] = arrival_date.strftime('%Y-%m-%d')  # Adjust the format as per your data
                    else:
                        unique_values = set(data[field])
                        if unique_values:
                            value = st.selectbox(f"{field.capitalize()}", [''] + sorted(unique_values))
                            if value:
                                search_params[field] = value


            with st.expander("Booking Details"):
                for field in booking_fields:
                    unique_values = set(data[field])
                    if unique_values:
                        value = st.selectbox(f"{field.capitalize()}", [''] + sorted(unique_values))
                        if value:
                            search_params[field] = value

            with st.expander("Reservation Details"):
                for field in reservation_fields:
                    unique_values = set(data[field])
                    if unique_values:
                        value = st.selectbox(f"{field.capitalize()}", [''] + sorted(unique_values))
                        if value:
                            search_params[field] = value

            with st.expander("Other Information"):
                for field in other_fields:
                    unique_values = set(data[field])
                    if unique_values:
                        value = st.selectbox(f"{field.capitalize()}", [''] + sorted(unique_values))
                        if value:
                            search_params[field] = value

            
            # Execute search query
            if st.button('Search'):
                result = es_service.search_query_command(ES_INDEX_NAME, search_params)
                if result:
                    hits = result['hits']['hits']
                    total_hits = result['hits']['total']['value']
                    st.header("Search Results")
                    st.write(f"Total hits: {total_hits}")

                    # Extracting source data and creating DataFrame
                    source_data = [hit['_source'] for hit in hits]
                    df = pd.DataFrame(source_data)
                    #excluding the last 3 columns
                    df = df.iloc[:, 1:-3]                                    
                    #dataframe should be displayed in a scrollable container for all rows
                    st.dataframe(df)
                    
                    #save as json with timestamp
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    json_filename = f"{TMP_CSV_FILENAME}_{timestamp}.json"
                    json_path = os.path.join(TMP_PATH, json_filename)
                    df.to_json(json_path)

                    
                else:
                    st.write("Search failed or no results found.")
                    
            # check if tmp folder
            if os.listdir(TMP_PATH):
                # get the latest json file in the tmp folder, read and use it to generate report
                generator.generate_report_on_button_click(data=open(max(glob.glob(TMP_PATH + '*'), key=os.path.getctime), 'r').read(), key="df_to_interpret")

            


        with subtab2:
            if 'aggregations' not in st.session_state:
                st.session_state.aggregations = [{"field": "", "agg_type": "avg"}]  # Start with one empty spec
            
            def add_agg_spec():
                st.session_state.aggregations.append({"field": "", "agg_type": "avg"})  # Add a new spec with default values

            def remove_agg_spec(index):
                st.session_state.aggregations.pop(index)  # Remove the spec at the given index

            st.header("Aggregation")

            # Loop over all current aggregation specifications
            for index, agg_spec in enumerate(st.session_state.aggregations):
                with st.container():
                    cols = st.columns([4, 3, 1])
                    agg_spec['field'] = cols[0].text_input(f"Field {index+1}", value=agg_spec['field'], key=f"field{index}", help="Field to aggregate")
                    agg_spec['agg_type'] = cols[1].selectbox("Aggregation Type", ["terms", "avg", "sum", "min", "max"], index=0, key=f"agg_type{index}")
                    remove_btn = cols[2].button("Remove", key=f"remove{index}")
                    if remove_btn:
                        remove_agg_spec(index)
                        st.experimental_rerun()

            # Button to add a new aggregation specification
            st.button("Add Aggregation", on_click=add_agg_spec, help="Add a new aggregation specification")

            # Button to submit all aggregation specifications
            if st.button("Aggregate"):
                if st.session_state.aggregations:
                    agg_params = {"aggregations": st.session_state.aggregations}
                    result = es_service.dynamic_aggregation_query(ES_INDEX_NAME, agg_params)
                    
                    # Assuming this is within the "if st.button('Aggregate'):" block
                    if result and "aggregations" in result:
                        # Create two columns for the visualizations
                        col1, col2 = st.columns(2)
                        
                        # Initialize a flag to control which column to use
                        use_col1 = True
                        
                        for agg in agg_params['aggregations']:
                            field, agg_type = agg['field'], agg['agg_type']
                            agg_key = f"{field}_{agg_type}"
                            if agg_key in result['aggregations']:
                                if agg_type == "terms":
                                    df = pd.DataFrame(result['aggregations'][agg_key]['buckets'])
                                    fig = px.bar(df, x='key', y='doc_count', title=f"Aggregation for {field} ({agg_type})")
                                    
                                    # Decide which column to use based on the flag
                                    if use_col1:
                                        col1.plotly_chart(fig)
                                    else:
                                        col2.plotly_chart(fig)
                                    
                                    # Toggle the flag
                                    use_col1 = not use_col1
                                    
                                else:  # For avg, sum, min, max
                                    value = result['aggregations'][agg_key]['value']
                                    data = {f"{field} ({agg_type})": [value]}
                                    
                                    # Decide which column to use based on the flag
                                    if use_col1:
                                        col1.table(data)
                                    else:
                                        col2.table(data)
                                    
                                    # Toggle the flag
                                    use_col1 = not use_col1

                    # if result and "aggregations" in result:
                    #     for agg in agg_params['aggregations']:
                    #         field, agg_type = agg['field'], agg['agg_type']
                    #         agg_key = f"{field}_{agg_type}"
                    #         if agg_key in result['aggregations']:
                    #             if agg_type == "terms":
                    #                 df = pd.DataFrame(result['aggregations'][agg_key]['buckets'])
                    #                 # st.write(f"Aggregation for {field} ({agg_type}):", df)
                    #                 #Visualize the aggregation result plotly
                    #                 fig = px.bar(df, x='key', y='doc_count', title=f"Aggregation for {field} ({agg_type})")
                    #                 st.plotly_chart(fig)
                    #             else:  # For avg, sum, min, max
                    #                 value = result['aggregations'][agg_key]['value']
                    #                 st.table({f"{field} ({agg_type})": [value]})
                                    
                    else:
                        st.error("No aggregation results found.")
                else:
                    st.error("Please add at least one aggregation.")

    with tab3:
        st.title("Hotel Booking Reports - Upload")
        st.write("Upload your data here")

        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
        if uploaded_file is not None:
            etl_utils = ETLUtils()  # Create an instance of ETLUtils
            _, success_flag = etl_utils.run_etl_flow(uploaded_file)  # Call run_etl_flow on the instance
            if success_flag:
                st.success("Data uploaded successfully")
                #start timer to sync data to elasticsearch
                if st.button("Sync Data to Elasticsearch"):
                    start_time = time.time()
                    with st.spinner("Syncing data to Elasticsearch..."):
                        sync_sql_to_elasticsearch()  # Assume this function is defined elsewhere
                    end_time = time.time()
                    st.success("Data synced to Elasticsearch successfully [Took: {:.2f} sec]".format(end_time - start_time))
            else:
                st.error("Data upload failed")

if __name__ == "__main__":
    streamlit_main()