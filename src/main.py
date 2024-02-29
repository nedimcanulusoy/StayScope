from fastapi import FastAPI
from api_routes import router
from logger_setup import Logger
from models import HotelBooking, get_db, insert_data #, is_initial_data_inserted
from sqlalchemy.orm import Session
from apscheduler.schedulers.background import BackgroundScheduler
from elasticsearch_operations import ElasticsearchService
from config import ELASTICSEARCH_SETTINGS, ES_INDEX_NAME, DATA_PATH

# Initialize the logger
log_api, log_db, log_es = Logger(__name__, './logs/api.log').get_logger(), Logger(__name__, './logs/db.log').get_logger(), Logger(__name__, './logs/elasticsearch.log').get_logger()

from concurrent.futures import ThreadPoolExecutor, as_completed
import itertools
from models import get_db, HotelBooking
from sqlalchemy.orm import Session

def chunk_data(data, size):
    it = iter(data)
    for i in itertools.count():
        batch = list(itertools.islice(it, size))
        if not batch:
            break
        yield batch

def upsert_chunk(es_service, index_name, chunk):
    # Transform each SQL model instance in the chunk to a dict suitable for Elasticsearch
    documents = [instance_to_dict(instance) for instance in chunk]
    es_service.bulk_upsert(documents, index_name)

def instance_to_dict(instance):
    instance_dict = instance.__dict__
    instance_dict.pop('_sa_instance_state', None)
    return instance_dict

def sync_sql_to_elasticsearch():
    db: Session = next(get_db())
    all_data = db.query(HotelBooking).all()
    chunks = list(chunk_data(all_data, 2500))  #chunk size can be adjusted but I found 2500 as optimal value
    
    es_service = ElasticsearchService(ELASTICSEARCH_SETTINGS)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(upsert_chunk, es_service, ES_INDEX_NAME, chunk) for chunk in chunks]
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Operation failed: {e}")

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


