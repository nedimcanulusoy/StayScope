from fastapi import FastAPI
from api_routes import router
from logger_setup import Logger
from models import HotelBooking, get_db, insert_data #, is_initial_data_inserted
from sqlalchemy.orm import Session
from apscheduler.schedulers.background import BackgroundScheduler
from elasticsearch_operations import ElasticsearchService
from config import ELASTICSEARCH_SETTINGS, ES_INDEX_NAME, DATA_PATH

# Initialize the logger
log_api, log_db = Logger(__name__, './logs/api.log').get_logger(), Logger(__name__, './logs/db.log').get_logger()


def sync_sql_to_elasticsearch():
    db: Session = next(get_db())
    es_service = ElasticsearchService(ELASTICSEARCH_SETTINGS)

    for instance in db.query(HotelBooking).all():
        #Transform SQL model instance to a dict suitable for Elasticsearch
        instance_dict = instance.__dict__
        instance_dict.pop('_sa_instance_state')
        # instance_dict['country_suggest'] = instance_dict['country']
        # instance_dict['hotel_suggest'] = instance_dict['hotel']
        instance_dict['hotel_suggest'] = {"input": [instance_dict['hotel']]}
        instance_dict['country_suggest'] = {"input": [instance_dict['country']]}


        # Index the document in Elasticsearch
        es_service.index_document(ES_INDEX_NAME, instance_dict)

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(sync_sql_to_elasticsearch, 'interval', hours=1)
    scheduler.start()
        

# Initialize the FastAPI app
try:
    app = FastAPI(title="Hotel Booking API", version="1.0")
    app.include_router(router, prefix="/api/v1")
except Exception as e:
    log_api.error(f'Error initializing FastAPI app: {e}')
    raise e

@app.on_event("startup")
async def startup_event():
    try:
        es_service = ElasticsearchService(ELASTICSEARCH_SETTINGS)
        es_service.create_index(index_name=ES_INDEX_NAME)
        # es_service.insert_bulk_data_from_db(index_name=ES_INDEX_NAME)
        
        start_scheduler()

        flag = True

        if flag:
            db: Session = next(get_db())
            if not db.query(HotelBooking).first():
                log_db.info("Inserting initial data into the database")
                insert_data(data_path=str(DATA_PATH), db=db)
                es_service.insert_bulk_data_from_db(index_name=ES_INDEX_NAME)
                log_db.info("Initial data inserted into the database")
                flag = False
            else:
                log_db.info("Initial data already inserted into the database")

    except Exception as e:
        log_api.error(f'Error during startup: {e}')
        raise e


