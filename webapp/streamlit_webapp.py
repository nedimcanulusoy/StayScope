import streamlit as st
import pandas as pd
import asyncio
# import nest_asyncio

#change project path to use fetch_utils.py under webapp folder
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from webapp.fetch_utils import DataFetcher, DataVisualizer

#change the path to the src folder of the project to use config.py
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.config import ELASTICSEARCH_SETTINGS, ES_INDEX_NAME
from src.elasticsearch_operations import ElasticsearchService


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

    tab1, tab2 = st.tabs(["Dashboard", "Data"])

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
            # Full Text Search UI
            st.header("Full Text Search")
            query_string = st.text_input("Enter your search query")
            fields = st.multiselect("Choose fields to search in", ["hotel", "country", "arrival_date_month", "market_segment", "distribution_channel", "reserved_room_type", "assigned_room_type", "deposit_type", "customer_type", "reservation_status", "reservation_status_date"], default="hotel")
            
            if st.button("Search"):
                if query_string and fields:
                    results = es_service.full_text_search_query(ES_INDEX_NAME, {"query_string": query_string, "fields": fields})
                    if results:
                        hits = results['hits']['hits']
                        df = pd.DataFrame([hit["_source"] for hit in hits])
                        df = df.iloc[:, :-3]
                        st.write(df)
                    else:
                        st.error("No results found.")
                else:
                    st.error("Please enter a search query and fields.")

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
                    if result and "aggregations" in result:
                        for agg in agg_params['aggregations']:
                            field, agg_type = agg['field'], agg['agg_type']
                            agg_key = f"{field}_{agg_type}"
                            if agg_key in result['aggregations']:
                                if agg_type == "terms":
                                    df = pd.DataFrame(result['aggregations'][agg_key]['buckets'])
                                    st.write(f"Aggregation for {field} ({agg_type}):", df)
                                else:  # For avg, sum, min, max
                                    value = result['aggregations'][agg_key]['value']
                                    st.write(f"{agg_type.capitalize()} of {field}: {value}")
                    else:
                        st.error("No aggregation results found.")
                else:
                    st.error("Please add at least one aggregation.")

if __name__ == "__main__":
    streamlit_main()