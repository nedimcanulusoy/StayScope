import streamlit as st
import pandas as pd
import httpx
import asyncio
import streamlit as st
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import sys
from pathlib import Path

# Add the parent directory to the Python path.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.config import HOST, ENDPOINTS
from src.llm_model import AsyncTextGenerator

class DataFetcher:
    def __init__(self):
        self.host = HOST
        self.endpoints = ENDPOINTS
        self.report_endpoints = ["".join([self.host, i]) for i in self.endpoints.values()]

    async def fetch_data_async(self, url):
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                st.error(f"Failed to fetch data from {url}.")
                return None

    async def fetch_all_data_async(self):
        tasks = [self.fetch_data_async(url) for url in self.report_endpoints]
        results = await asyncio.gather(*tasks)
        return dict(zip(self.endpoints.keys(), results))

class DataVisualizer:
    def __init__(self, data):
        self.data = data
        self.generator = AsyncTextGenerator()

    def visualize_top_countries(self):
        if "top_countries" in self.data and self.data["top_countries"] is not None:
            top_countries = self.data["top_countries"]
            countries = [item['key'] for item in top_countries]
            booking_counts = [item['doc_count'] for item in top_countries]
            df_countries = pd.DataFrame({'Country': countries, 'Booking Counts': booking_counts})
            #add number of bookings to the plot under the country name and use plotly for a fancier output
            fig = px.bar(df_countries, x='Country', y='Booking Counts', text='Booking Counts', title='Top Countries by Booking Counts',
                        labels={'Booking Counts': 'Number of Bookings', 'Country': 'Country'},
                        color='Booking Counts', color_continuous_scale='Blues')
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
            #update title and set it to center
            fig.update_layout(
                title={
                    'text': 'Top Countries by Booking Counts',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title='Country',
                yaxis_title='Number of Bookings',
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )
            st.plotly_chart(fig)

            #call generator to generate report
            self.generator.generate_report_on_button_click(self.data['top_countries'], key="top_countries_button")
                
                

    def visualize_cancellation_rate(self):
        if "cancellation_rate" in self.data and self.data["cancellation_rate"] is not None:
            
            # Access the 'market_segment' directly from 'cancellation_rate'
            cancellation_data = self.data["cancellation_rate"]["market_segment"]
            market_segments = []
            city_hotel_rates = []
            resort_hotel_rates = []
            
            # Iterate through each market segment
            for segment in cancellation_data['buckets']:
                market_segments.append(segment['key'])
                # Initialize cancellation rates for both hotel types
                city_rate = None
                resort_rate = None
                
                # Extract hotel types and their cancellation rates
                for hotel in segment['hotel_type']['buckets']:
                    if hotel['key'] == 'City Hotel':
                        city_rate = hotel['cancellation_rate']['value']
                    elif hotel['key'] == 'Resort Hotel':
                        resort_rate = hotel['cancellation_rate']['value']
                
                city_hotel_rates.append(city_rate if city_rate is not None else pd.NA)
                resort_hotel_rates.append(resort_rate if resort_rate is not None else pd.NA)
            
            # Create a DataFrame
            # city_hotel_rates = np.array(city_hotel_rates, dtype=np.float64)
            # resort_hotel_rates = np.array(resort_hotel_rates, dtype=np.float64)

            df_cancellation_rate = pd.DataFrame({
                'Market Segment': market_segments,
                'City Hotel Cancellation Rate': city_hotel_rates,
                'Resort Hotel Cancellation Rate': resort_hotel_rates
            }).fillna(0)  # Replace missing values with 0 for visualization purposes

            # Set the index to 'Market Segment' for better visualization
            df_cancellation_rate.set_index('Market Segment', inplace=True)

            #use plotly for a fancier output
            fig = px.bar(df_cancellation_rate, barmode='group', title='Cancellation Rate by Market Segment and Hotel Type',
                        labels={'value': 'Cancellation Rate', 'variable': 'Hotel Type'},
                        color_discrete_sequence=px.colors.sequential.RdBu)
            #update title and set it to center
            fig.update_layout(
                title={
                    'text': 'Cancellation Rate by Market Segment and Hotel Type',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title='Market Segment',
                yaxis_title='Cancellation Rate',
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )
            st.plotly_chart(fig)


            self.generator.generate_report_on_button_click(self.data['cancellation_rate'], key="cancellation_rate_button")


    def visualize_cancellation_rate_pie(self):
        if "cancellation_rate" in self.data and self.data["cancellation_rate"] is not None:
            
            # Access the 'market_segment' directly from 'cancellation_rate'
            cancellation_data = self.data["cancellation_rate"]["market_segment"]
            market_segments = []
            city_hotel_rates = []
            resort_hotel_rates = []
            
            # Iterate through each market segment
            for segment in cancellation_data['buckets']:
                market_segments.append(segment['key'])
                # Initialize cancellation rates for both hotel types
                city_rate = None
                resort_rate = None
                
                # Extract hotel types and their cancellation rates
                for hotel in segment['hotel_type']['buckets']:
                    if hotel['key'] == 'City Hotel':
                        city_rate = hotel['cancellation_rate']['value']
                    elif hotel['key'] == 'Resort Hotel':
                        resort_rate = hotel['cancellation_rate']['value']
                
                city_hotel_rates.append(city_rate if city_rate is not None else pd.NA)
                resort_hotel_rates.append(resort_rate if resort_rate is not None else pd.NA)
            
            # Create a DataFrame
            # city_hotel_rates = np.array(city_hotel_rates, dtype=np.float64)
            # resort_hotel_rates = np.array(resort_hotel_rates, dtype=np.float64)

            df_cancellation_rate = pd.DataFrame({
                'Market Segment': market_segments,
                'City Hotel Cancellation Rate': city_hotel_rates,
                'Resort Hotel Cancellation Rate': resort_hotel_rates
            }).fillna(0)  # Replace missing values with 0 for visualization purposes

            # Set the index to 'Market Segment' for better visualization
            df_cancellation_rate.set_index('Market Segment', inplace=True)

            # Calculate average cancellation rates for City and Resort Hotels
            avg_city_hotel_rate = df_cancellation_rate['City Hotel Cancellation Rate'].mean()
            avg_resort_hotel_rate = df_cancellation_rate['Resort Hotel Cancellation Rate'].mean()

            # Create a DataFrame for the pie chart
            df_pie = pd.DataFrame({
                'Hotel Type': ['City Hotel', 'Resort Hotel'],
                'Average Cancellation Rate': [avg_city_hotel_rate, avg_resort_hotel_rate]
            })

            # Generate a pie chart using Plotly
            fig = px.pie(df_pie, values='Average Cancellation Rate', names='Hotel Type', title='Average Cancellation Rates by Hotel Type',
                        hole=.3, color_discrete_sequence=px.colors.sequential.RdBu)
            
            #update title and set it to center
            fig.update_layout(
                title={
                    'text': 'Average Cancellation Rates by Hotel Type',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )
            
            # Display the pie chart in Streamlit
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(self.data['cancellation_rate'], key="cancellation_rate_pie_button")

    def visualize_adr_by_month(self):
        if "adr_by_month" in self.data and self.data["adr_by_month"] is not None:
            adr_data = self.data["adr_by_month"]["months"]["buckets"]
            months = [bucket["key_as_string"][:7] for bucket in adr_data]

            city_hotel_adr = []
            resort_hotel_adr = []

            for bucket in adr_data:
                city_adr = resort_adr = None
                for hotel in bucket["hotel_type"]["buckets"]:
                    if hotel["key"] == "City Hotel":
                        city_adr = hotel["average_adr"]["value"]
                    elif hotel["key"] == "Resort Hotel":
                        resort_adr = hotel["average_adr"]["value"]
                city_hotel_adr.append(city_adr)
                resort_hotel_adr.append(resort_adr)

            # Create DataFrame
            df_adr_by_month = pd.DataFrame({
                "Month": months,
                "City Hotel ADR": city_hotel_adr,
                "Resort Hotel ADR": resort_hotel_adr
            })

            # use plotly for a fancier output like line chart
            fig = px.line(df_adr_by_month, x='Month', y=['City Hotel ADR', 'Resort Hotel ADR'], title='Average Daily Rate (ADR) by Month',
                        labels={'value': 'Average Daily Rate (ADR)', 'variable': 'Hotel Type'},
                        color_discrete_sequence=px.colors.sequential.RdBu)
            #update title and set it to center
            fig.update_layout(
                title={
                    'text': 'Average Daily Rate (ADR) by Month',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title='Month',
                yaxis_title='Average Daily Rate (ADR)',
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(adr_data, key="adr_by_month_button")
        

    def visualize_length_of_stay_distribution(self):
        if "length_of_stay_distribution" in self.data and self.data["length_of_stay_distribution"] is not None:
            length_of_stay_data = self.data["length_of_stay_distribution"]
            
            # Extract length of stay and counts
            length_of_stay = [int(bucket["key"]) for bucket in length_of_stay_data]
            counts = [bucket["doc_count"] for bucket in length_of_stay_data]

            # Create DataFrame
            df_length_of_stay = pd.DataFrame({
                "Length of Stay": length_of_stay,
                "Counts": counts
            })

            # Sort the DataFrame by 'Length of Stay' numerically
            df_length_of_stay_sorted = df_length_of_stay.sort_values(by="Length of Stay")

            # Visualize with a bar chart using plotly for a fancier output
            fig = px.bar(df_length_of_stay_sorted, x='Length of Stay', y='Counts', title='Length of Stay Distribution',
                        labels={'Counts': 'Number of Bookings', 'Length of Stay': 'Length of Stay'},
                        color='Counts', color_continuous_scale=px.colors.sequential.RdBu)
            
            #update title and set it to center
            fig.update_layout(
                title={
                    'text': 'Length of Stay Distribution',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title='Length of Stay',
                yaxis_title='Number of Bookings',
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(self.data['length_of_stay_distribution'], key="length_of_stay_distribution_button")


    def visualize_special_requests_impact_on_cancellations(self):
        if "special_requests_impact_on_cancellations" in self.data and self.data["special_requests_impact_on_cancellations"] is not None:
            special_requests_data = self.data["special_requests_impact_on_cancellations"]
            special_requests = [bucket["key"] for bucket in special_requests_data]
            counts = [bucket["doc_count"] for bucket in special_requests_data]

            df_special_requests = pd.DataFrame({
                "Special Requests": special_requests,
                "Counts": counts
            })

            df_special_requests = df_special_requests.set_index("Special Requests")
            
            # Visualize with a bar chart using plotly for a fancier output
            fig = px.bar(df_special_requests, x=df_special_requests.index, y='Counts', title='Special Requests Impact on Cancellations',
                        labels={'Counts': 'Number of Bookings', 'variable': 'Special Requests'},
                        color='Counts', color_continuous_scale=px.colors.sequential.RdBu)
            
            #update title and set it to center


            fig.update_layout(
                title={
                    'text': 'Special Requests Impact on Cancellations',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title='Special Requests',
                yaxis_title='Number of Bookings',
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )

            st.plotly_chart(fig)
            
            # st.line_chart(df_special_requests.set_index("Special Requests"))

            self.generator.generate_report_on_button_click(self.data['special_requests_impact_on_cancellations'], key="special_requests_impact_on_cancellations_button")

    def visualize_average_lead_time_by_cancellation_status(self):
        if 'average_lead_time_by_cancellation_status' in self.data and self.data['average_lead_time_by_cancellation_status'] is not None:
            lead_time_data = self.data['average_lead_time_by_cancellation_status']
            cancellation_status = [bucket['key'] for bucket in lead_time_data]
            avg_lead_time = [bucket['average_lead_time']['value'] for bucket in lead_time_data]

            df_lead_time = pd.DataFrame({
                "Cancellation Status": cancellation_status,
                "Average Lead Time": avg_lead_time
            })

            df_lead_time['Cancellation Status'] = df_lead_time['Cancellation Status'].replace({0: 'Not Cancelled', 1: 'Cancelled'})

            pie_chart = px.pie(df_lead_time, values='Average Lead Time', names='Cancellation Status', title='Average Lead Time by Cancellation Status',
                    hole=.3, color_discrete_sequence=px.colors.sequential.Magma)
            #update title and set it to center
            pie_chart.update_layout(
                title={
                    'text': 'Average Lead Time by Cancellation Status',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )
            st.plotly_chart(pie_chart)

            self.generator.generate_report_on_button_click(self.data['average_lead_time_by_cancellation_status'], key="average_lead_time_by_cancellation_status_button")

    def visualize_bookings_distribution_by_room_type(self):
        if "bookings_distribution_by_room_type" in self.data and self.data["bookings_distribution_by_room_type"] is not None:
            bookings_distribution_by_room_type = self.data["bookings_distribution_by_room_type"]
            
            # Convert the data into a DataFrame
            df_room_types = pd.DataFrame(bookings_distribution_by_room_type)
            df_room_types.rename(columns={'key': 'Room Type', 'doc_count': 'Bookings'}, inplace=True)

            # Plotting with Plotly Express for a fancier output
            fig = px.pie(df_room_types, values='Bookings', names='Room Type', title='Bookings Distribution by Room Type',
                        hole=0.3,  # Creates a donut chart appearance
                        color_discrete_sequence=px.colors.sequential.Rainbow,  # Use a colorful theme
                        hover_data=['Room Type'])
            
            #Add number of bookings to the plot under the room type
            fig.update_traces(textinfo="label+value")

            # Customizing chart layout for a fancier look
            fig.update_layout(
                title={
                    'text': 'Bookings Distribution by Room Type',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                legend_title_text='Room Type',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )

            # Display the updated pie chart in Streamlit
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(bookings_distribution_by_room_type, key="bookings_distribution_by_room_type_button")

    def visualize_bookings_by_guest_country(self):
        if "bookings_by_guest_country" in self.data and self.data["bookings_by_guest_country"] is not None:
            bookings_by_guest_country = self.data["bookings_by_guest_country"]
            
            # Convert the data into a DataFrame
            df_guest_countries = pd.DataFrame(bookings_by_guest_country)
            df_guest_countries.rename(columns={'key': 'Country', 'doc_count': 'Bookings'}, inplace=True)

            # Plotting with Plotly Express for a fancier output
            fig = px.treemap(df_guest_countries, path=['Country'], values='Bookings',
                            title='Bookings by Guest Country',
                            color='Bookings', hover_data=['Country'],
                            color_continuous_scale='Blues')
            
            #Add number of bookings to the plot under the country name
            fig.update_traces(textinfo="label+value")

            # Customizing chart layout for a fancier look
            fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))

            #update title and set it to center
            fig.update_layout(
                title={
                    'text': 'Bookings by Guest Country',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                }
            )

            # Display the treemap in Streamlit
            st.plotly_chart(fig)
            
            self.generator.generate_report_on_button_click(bookings_by_guest_country, key="bookings_by_guest_country_button")

    def visualize_booking_source_analysis(self):
        if "booking_source_analysis" in self.data and self.data["booking_source_analysis"] is not None:
            booking_source_analysis = self.data["booking_source_analysis"]
            
            # Convert the data into a DataFrame
            df_booking_sources = pd.DataFrame(booking_source_analysis)
            df_booking_sources.rename(columns={'key': 'Source', 'doc_count': 'Bookings'}, inplace=True)

            # Plotting with Plotly Express for a fancier output
            fig = px.bar(df_booking_sources, x='Source', y='Bookings', text='Bookings',
                        title='Booking Source Analysis',
                        labels={'Bookings': 'Number of Bookings', 'Source': 'Booking Source'},
                        color='Bookings', color_continuous_scale='Blues')
            
            # Customizing chart layout for a fancier look
            fig.update_layout(
                title={
                    'text': 'Booking Source Analysis',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title='Booking Source',
                yaxis_title='Number of Bookings',
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )

            # Display the updated bar chart in Streamlit
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(booking_source_analysis, key="booking_source_analysis_button")

    def visualize_booking_trends_over_time(self):
        if "booking_trends_over_time" in self.data and self.data["booking_trends_over_time"] is not None:
            booking_trends_over_time = self.data["booking_trends_over_time"]
            
            # Convert the data into a DataFrame
            df_booking_trends = pd.DataFrame(booking_trends_over_time)
            df_booking_trends['Date'] = pd.to_datetime(df_booking_trends['key_as_string']).dt.strftime('%Y-%m')

            # Sorting the DataFrame by Date
            df_booking_trends.sort_values('Date', inplace=True)

            # Plotting with Plotly Express
            fig = px.line(df_booking_trends, x='Date', y='doc_count',
                        title='Booking Trends Over Time',
                        markers=True,
                        labels={'doc_count': 'Number of Bookings'},
                        template='plotly_dark')
            
            # Customizing the markers for aesthetics
            fig.update_traces(marker=dict(size=10, opacity=0.8, line=dict(width=2, color='DarkSlateGrey')),
                            mode='lines+markers+text', text=df_booking_trends['doc_count'], textposition='top center')

            # Customizing the layout
            fig.update_layout(
                xaxis=dict(showline=True, showgrid=False, showticklabels=True, linecolor='rgb(204, 204, 204)', linewidth=2, ticks='outside', tickfont=dict(family='Arial', size=12, color='rgb(82, 82, 82)')),
                yaxis=dict(showgrid=False, zeroline=False, showline=False, showticklabels=True),
                autosize=True,
                margin=dict(autoexpand=False, l=100, r=20, t=110),
                showlegend=False,
                #plot_bgcolor='black'
            )

            # Add annotations for the highest points
            for i, point in df_booking_trends.iterrows():
                if point['doc_count'] == df_booking_trends['doc_count'].max():
                    fig.add_annotation(x=point['Date'], y=point['doc_count'],
                                    text="Peak<br>{}".format(point['doc_count']),
                                    showarrow=True,
                                    arrowhead=1,
                                    ax=-50,
                                    ay=-50)
                    
            #Center the title
            fig.update_layout(
                title={
                    'text': 'Booking Trends Over Time',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title='Date',
                yaxis_title='Number of Bookings',
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )

            # Display the line chart in Streamlit
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(booking_trends_over_time, key="booking_trends_over_time_button")

    def visualize_revenue_analysis_by_room_and_month(self):
        if "revenue_analysis_by_room_and_month" in self.data and self.data["revenue_analysis_by_room_and_month"] is not None:
            revenue_analysis_by_room_and_month = self.data["revenue_analysis_by_room_and_month"]

            # Process the data
            data = []
            room_types = sorted([room['key'] for room in revenue_analysis_by_room_and_month])

            for room in revenue_analysis_by_room_and_month:
                room_data = {
                    'type': 'scatter',
                    'name': room['key'],
                    'x': [bucket['key_as_string'] for bucket in room['monthly_revenue']['buckets']],
                    'y': [bucket['revenue']['value'] for bucket in room['monthly_revenue']['buckets']]
                }
                data.append(room_data)

            # Create a Plotly figure
            fig = go.Figure(data)

            # # Customizing the layout
            # fig.update_layout(
            #     title='Revenue Analysis by Room and Month',
            #     xaxis_title='Month',
            #     yaxis_title='Revenue',
            #     template='plotly_dark'
            # )

            #update title and set it to center
            fig.update_layout(
                title={
                    'text': 'Revenue Analysis by Room and Month',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title='Month',
                yaxis_title='Revenue',
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )
            # Display the line chart in Streamlit
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(revenue_analysis_by_room_and_month, key="revenue_analysis_by_room_and_month_button")

    def visualize_impact_of_lead_time_on_adr(self):
        if "impact_of_lead_time_on_adr" in self.data and self.data["impact_of_lead_time_on_adr"] is not None:
            impact_of_lead_time_on_adr = self.data["impact_of_lead_time_on_adr"]

            # Prepare the data for plotting
            lead_times = [item['key'] for item in impact_of_lead_time_on_adr]
            average_adrs = [item['average_adr']['value'] for item in impact_of_lead_time_on_adr]

            # Create a scatter plot
            fig = go.Figure(data=go.Scatter(x=lead_times, y=average_adrs, mode='markers+lines', name='ADR vs. Lead Time'))

            # # Customizing the layout
            # fig.update_layout(
            #     title='Impact of Lead Time on ADR',
            #     xaxis_title='Month',
            #     yaxis_title='Average Daily Rate (ADR)',
            #     template='plotly_dark'
            # )

            #update title and set it to center
            fig.update_layout(
                title={
                    'text': 'Impact of Lead Time on ADR',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                xaxis_title='Lead Time',
                yaxis_title='Average Daily Rate (ADR)',
                margin=dict(t=100, l=0, r=0, b=0)  # Increase top margin to accommodate title
            )

            # Display the line chart in Streamlit
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(impact_of_lead_time_on_adr, key="impact_of_lead_time_on_adr_button")


    def visualize_analyze_booking_composition(self):
        if "analyze_booking_composition" in self.data and self.data["analyze_booking_composition"] is not None:
            analyze_booking_composition = self.data["analyze_booking_composition"]

            # Extracting relevant data
            categories_dirty = [entry['key'] for entry in analyze_booking_composition['booking_composition']['buckets']]
            #run over categories and remove .0 parts from the string
            categories = [category.replace('.0', '') for category in categories_dirty]
            doc_counts = [entry['doc_count'] for entry in analyze_booking_composition['booking_composition']['buckets']]

            # Create bar chart
            fig = go.Figure(data=[go.Bar(x=categories, y=doc_counts)])

            # Update layout for better visualization
            fig.update_layout(title={
                    'text': 'Booking Composition Analysis',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
            },
                            xaxis_title='Booking Composition',
                            yaxis_title='Number of Bookings',
                            xaxis_tickangle=-45)

            # Display the bar chart in Streamlit
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(analyze_booking_composition, key="analyze_booking_composition_button")

    def visualize_correlate_cancelations_with_factors(self):
        if "correlate_cancelations_with_factors" in self.data and self.data["correlate_cancelations_with_factors"] is not None:
            correlate_cancelations_with_factors = self.data["correlate_cancelations_with_factors"]["cancellation_correlation"]["buckets"]

            # Prepare the data
            statuses = ['Not Cancelled', 'Cancelled']  # Based on the "key" 0 and 1
            special_requests_count = [bucket["special_requests_count"]["value"] for bucket in correlate_cancelations_with_factors]
            average_stay_length = [bucket["average_stay_length"]["value"] for bucket in correlate_cancelations_with_factors]
            average_lead_time = [bucket["average_lead_time"]["value"] for bucket in correlate_cancelations_with_factors]

            # Create the Plotly graph
            fig = go.Figure(data=[
                go.Bar(name='Special Requests Count', x=statuses, y=special_requests_count),
                go.Bar(name='Average Stay Length', x=statuses, y=average_stay_length),
                go.Bar(name='Average Lead Time', x=statuses, y=average_lead_time),
            ])
            
            # Change the bar mode to 'group' for grouped bar chart
            fig.update_layout(barmode='group', title={
                    'text': 'Correlation of Cancellations with Hotel Booking Factors',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
            },
                            xaxis_title="Cancellation Status",
                            yaxis_title="Value",
                            legend_title="Factors")

            # Display the figure in Streamlit
            st.plotly_chart(fig)

            self.generator.generate_report_on_button_click(correlate_cancelations_with_factors, key="correlate_cancelations_with_factors_button")

    # def visualize_correlate_adr_with_factors(self): 
    #     if "correlate_adr_with_factors" in self.data and self.data["correlate_adr_with_factors"] is not None:
    #         adr_correlation = self.data["correlate_adr_with_factors"]["adr_correlation"]["buckets"]

    #         # Extracting data
    #         keys = [bucket["key"] for bucket in adr_correlation]
    #         special_requests_counts = [bucket["special_requests_count"]["value"] for bucket in adr_correlation]
    #         cancellation_rates = [bucket["cancellation_rate"]["value"] for bucket in adr_correlation]
    #         average_stay_lengths = [bucket["average_stay_length"]["value"] for bucket in adr_correlation]

    #         # Preparing data for heatmap
    #         # Assuming keys are sorted and unique
    #         x = sorted(list(set(keys)))  # ADR values
    #         y = ['Special Requests Count', 'Cancellation Rate', 'Average Stay Length']  # Factors
    #         z = [
    #             special_requests_counts,  # Intensity based on special requests count
    #             cancellation_rates,  # Intensity based on cancellation rate
    #             average_stay_lengths  # Intensity based on average stay length
    #         ]

    #         # Transposing z to match x and y dimensions
    #         z = np.transpose(z)

    #         # Creating the heatmap
    #         fig = go.Figure(data=go.Heatmap(
    #             z=z,
    #             x=x,
    #             y=y,
    #             colorscale='Viridis',
    #             hoverongaps=False
    #         ))

    #         # Adjusting layout
    #         fig.update_layout(
    #             title='ADR Correlation with Hotel Booking Factors',
    #             xaxis_title='ADR',
    #             yaxis_title='Factors',
    #             xaxis=dict(type='category'),  # Treat ADR values as discrete categories
    #         )
    #         # Display the bubble chart in Streamlit
    #         st.plotly_chart(fig)
    def visualize_analyze_repeat_guest_bookings(self):
        if "analyze_repeat_guest_bookings" in self.data and self.data["analyze_repeat_guest_bookings"] is not None:
            analyze_repeat_guest_bookings = self.data["analyze_repeat_guest_bookings"]["repeat_guests"]["buckets"]

            # Pie chart for hotel type distribution
            labels_hotel_type = []
            values_hotel_type = []
            colors = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA']  # Custom color palette
            for index, guest_type in enumerate(analyze_repeat_guest_bookings):
                repeat_status = 'Repeat Guests' if guest_type["key"] == 1 else 'First-time Guests'
                for hotel_type in guest_type["bookings_by_hotel_type"]["buckets"]:
                    labels_hotel_type.append(f"{repeat_status} - {hotel_type['key']}")
                    values_hotel_type.append(hotel_type["doc_count"])

            # Creating the pie chart with enhancements
            fig_pie = go.Figure(data=[go.Pie(labels=labels_hotel_type, values=values_hotel_type, hole=.4, marker_colors=colors, rotation=90, pull=[0.1, 0, 0.1, 0, 0.1, 0])])
            fig_pie.update_traces(textinfo='percent+label', hoverinfo='label+value', textfont_size=12)
            #realign traces to make it look better
            # fig_pie.update_traces(rotation=90, pull=[0.1, 0, 0.1, 0, 0.1, 0])
            
            fig_pie.update_layout(
                title={
                    'text': 'Bookings by Hotel Type and Guest Type',
                    'y':0.95,  # Adjust the y position of the title
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                # title_text="Bookings by Hotel Type and Guest Type",
                # title_x=0.5,
                # title_font=dict(size=20),
                legend_title="<b>Hotel Types</b>",
                legend_font_size=12,
                legend_title_font_size=14,
                showlegend=True,
                legend_orientation="h",
                legend=dict(x=0, y=-0.1),
                annotations=[dict(text='Hotel<br>Type', x=0.5, y=0.5, font_size=20, showarrow=False)]
            )

            # Display the pie chart visualization
            st.plotly_chart(fig_pie)

            self.generator.generate_report_on_button_click(analyze_repeat_guest_bookings, key="analyze_repeat_guest_bookings_button")