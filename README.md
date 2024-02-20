# StayScope

StayScope is a comprehensive analytics platform designed to transform hotel booking data into actionable insights. It integrates advanced data processing and visualization tools to support decision-making in the hospitality and hotel industry.

## Technologies and Tools

- Python
- FastAPI - REST API
- Elastic Search
- LLM - Ollama (Mistral)
- SQLAlchemy (ORM) - PostgreSQL
- Docker
- Streamlit
- Plotly

## Structure

- **data**: Data - CSV files.
- **scripts**: Python scripts for processing, REST API and Elasticsearch.
- **webapp**: Frontend application with Streamlit.
- **notebooks**: Jupyter notebooks for exploratory data analysis (EDA).

---

1. **Main Application File (`main.py`):** Entry point to your FastAPI application.
2. **Database Models (`models.py`):** Contains SQLAlchemy models and Database operations.
3. ***Elasticsearch Operations (`elasticsearch_operations.py`):** Handles Elasticsearch indexing, searching, and other operations.
5. **Schemas (`schemas.py`):** Pydantic models for request and response validation.
6. **API Routes (`api_routes.py`):** FastAPI route definitions for Elasticsearch operations.
7. **LLM (`llm_model.py`):** Contains LLM model for report interpretation.
8. **Configurations (`config.py`):** Configuration settings (database URLs, Elasticsearch connection details, etc.).
9. **Visualization (`fetch_utils.py`):** Visualization functions for fetched data from REST API.
10. **User Interface - Frontend (`streamlit_webapp.py`):** User Interface (Frontend) for interaction.

---

## Installation

1. Clone the repository
```bash
git clone https://github.com/nedimcanulusoy/StayScope.git
cd StayScope
```

2. Create venv and install requirements.txt

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Set up elastic search via docker

```bash
docker-compose build
```

and

```bash
docker-compose up -d
```

**_NOTE:_** _I assume that you have PostgreSQL running on your computer or server, either locally or through Docker. If you don't, please set one up before proceeding to the next step._

4. Pull LLM model for Ollama
    
**_NOTE:_** _Default model is Mistral and feel free to change with anything you want to use in `config.py` file. LLM model variable is called `LLM_MODEL`_

```bash
ollama pull mistral
```

5. Run REST API and Streamlit User Interface

REST API: 

```bash
cd src

uvicorn main:app --reload
```

Streamlit:

```bash
cd webapp

streamlit run streamlit_webapp.py
```


**_NOTE:_** You need to create a `config.py` file to be able to run the project. Here is a template for it;

```python
DATABASE_URL = 'your-full-postgresql-url'
DATABASE_TABLE_NAME = 'your-database-table-name'
ELASTICSEARCH_SETTINGS = {
    'host': 'es-running-host,
    'port': 'es-running-port',
    'scheme': 'http', #default.
    'auth': ('user', 'secret') #default.
}
ES_INDEX_NAME = 'your-es-index-name'
DATA_PATH = 'your-data-path'

LLM_MODEL = 'mistral' #Can be changed.

#ENDPOINT LIST, SUGGESTED TO KEEP AS IT IS.
ENDPOINTS = {
        "top_countries": "/api/v1/reports/top_countries",
        "cancellation_rate": "/api/v1/reports/cancellation_rate",
        "adr_by_month": "/api/v1/reports/adr_by_month",
        "length_of_stay_distribution": "/api/v1/reports/length_of_stay_distribution",
        "special_requests_impact_on_cancellations": "/api/v1/reports/special_requests_impact_on_cancellations",
        "average_lead_time_by_cancellation_status": "/api/v1/reports/average_lead_time_by_cancellation_status",
        "bookings_distribution_by_room_type": "/api/v1/reports/bookings_distribution_by_room_type",
        "bookings_by_guest_country": "/api/v1/reports/bookings_by_guest_country",
        "booking_source_analysis": "/api/v1/reports/booking_source_analysis",
        "booking_trends_over_time": "/api/v1/reports/booking_trends_over_time",
        "revenue_analysis_by_room_and_month": "/api/v1/reports/revenue_analysis_by_room_and_month",
        "impact_of_lead_time_on_adr": "/api/v1/reports/impact_of_lead_time_on_adr",
        "analyze_booking_composition": "/api/v1/reports/analyze_booking_composition",
        "correlate_cancelations_with_factors": "/api/v1/reports/correlate_cancelations_with_factors",
        "correlate_adr_with_factors": "/api/v1/reports/correlate_adr_with_factors",
        "analyze_repeat_guest_bookings": "/api/v1/reports/analyze_repeat_guest_bookings",
}
```

