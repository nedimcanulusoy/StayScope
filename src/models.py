from sqlalchemy import Column, Integer, String, Float, Date, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from config import DATABASE_URL, DATABASE_TABLE_NAME
from logger_setup import Logger
import csv
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import pandas as pd


log = Logger(__name__, './logs/db.log').get_logger()

try:
    Base = declarative_base()
except Exception as e:
    log.error(f'Error creating base: {e}')
    raise e

class HotelBooking(Base):
    __tablename__ = DATABASE_TABLE_NAME
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    hotel = Column(String, nullable=True)
    is_canceled = Column(Integer, nullable=True)
    lead_time = Column(Integer, nullable=True)
    arrival_date = Column(Date, nullable=True)
    arrival_date_year = Column(Integer, nullable=True)
    arrival_date_month = Column(String, nullable=True)
    arrival_date_week_number = Column(Integer, nullable=True)
    arrival_date_day_of_month = Column(Integer, nullable=True)
    stays_in_weekend_nights = Column(Integer, nullable=True)
    stays_in_week_nights = Column(Integer, nullable=True)
    adults = Column(Integer, nullable=True)
    children = Column(Float, nullable=True)
    babies = Column(Integer, nullable=True)
    meal = Column(String, nullable=True)
    country = Column(String, nullable=True)
    market_segment = Column(String, nullable=True)
    distribution_channel = Column(String, nullable=True)
    is_repeated_guest = Column(Integer, nullable=True)
    previous_cancellations = Column(Integer, nullable=True)
    previous_bookings_not_canceled = Column(Integer, nullable=True)
    reserved_room_type = Column(String, nullable=True)
    assigned_room_type = Column(String, nullable=True)
    booking_changes = Column(Integer, nullable=True)
    deposit_type = Column(String, nullable=True)
    agent = Column(Float, nullable=True)
    company = Column(Float, nullable=True)
    days_in_waiting_list = Column(Integer, nullable=True)
    customer_type = Column(String, nullable=True)
    adr = Column(Float, nullable=True)
    required_car_parking_spaces = Column(Integer, nullable=True)
    total_of_special_requests = Column(Integer, nullable=True)
    reservation_status = Column(String, nullable=True)
    reservation_status_date = Column(Date, nullable=True)

try:
    engine = create_engine(DATABASE_URL)
    #drop all tables
    # Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
except Exception as e:
    log.error(f'Error creating engine: {e}')
    raise e


try:
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
except Exception as e:
    log.error(f'Error creating session: {e}')
    raise e

def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        log.error(f'Error getting session: {e}')
        raise e
    finally:
        db.close()

def insert_data(db: Session, data_path: str):
    with open(data_path, 'r') as f:
        reader_ = csv.DictReader(f)
        for row in reader_:
            # Convert data types and handle missing values
            row['arrival_date'] = datetime.strptime(row['arrival_date'], '%Y-%m-%d').date()
            row['arrival_date_year'] = int(row['arrival_date_year'])
            row['arrival_date_week_number'] = int(row['arrival_date_week_number'])
            row['arrival_date_day_of_month'] = int(row['arrival_date_day_of_month'])
            row['stays_in_weekend_nights'] = int(row['stays_in_weekend_nights'])
            row['stays_in_week_nights'] = int(row['stays_in_week_nights'])
            row['adults'] = int(row['adults'])
            row['children'] = float(row['children'])
            row['babies'] = int(row['babies'])
            row['is_repeated_guest'] = int(row['is_repeated_guest'])
            row['previous_cancellations'] = int(row['previous_cancellations'])
            row['previous_bookings_not_canceled'] = int(row['previous_bookings_not_canceled'])
            row['booking_changes'] = int(row['booking_changes'])
            row['agent'] = float(row['agent']) 
            row['company'] = float(row['company']) 
            row['days_in_waiting_list'] = int(row['days_in_waiting_list'])
            row['adr'] = float(row['adr'])
            row['required_car_parking_spaces'] = int(row['required_car_parking_spaces'])
            row['total_of_special_requests'] = int(row['total_of_special_requests'])
            row['reservation_status_date'] = datetime.strptime(row['reservation_status_date'], '%Y-%m-%d').date()

            # Create an instance of the model and add it to the session
            booking = HotelBooking(
                hotel=row['hotel'],
                is_canceled=row['is_canceled'],
                lead_time=row['lead_time'],
                arrival_date=row['arrival_date'],
                arrival_date_year=row['arrival_date_year'],
                arrival_date_month=row['arrival_date_month'],
                arrival_date_week_number=row['arrival_date_week_number'],
                arrival_date_day_of_month=row['arrival_date_day_of_month'],
                stays_in_weekend_nights=row['stays_in_weekend_nights'],
                stays_in_week_nights=row['stays_in_week_nights'],
                adults=row['adults'],
                children=row['children'],
                babies=row['babies'],
                meal=row['meal'],
                country=row['country'],
                market_segment=row['market_segment'],
                distribution_channel=row['distribution_channel'],
                is_repeated_guest=row['is_repeated_guest'],
                previous_cancellations=row['previous_cancellations'],
                previous_bookings_not_canceled=row['previous_bookings_not_canceled'],
                reserved_room_type=row['reserved_room_type'],
                assigned_room_type=row['assigned_room_type'],
                booking_changes=row['booking_changes'],
                deposit_type=row['deposit_type'],
                agent=row['agent'],
                company=row['company'],
                days_in_waiting_list=row['days_in_waiting_list'],
                customer_type=row['customer_type'],
                adr=row['adr'],
                required_car_parking_spaces=row['required_car_parking_spaces'],
                total_of_special_requests=row['total_of_special_requests'],
                reservation_status=row['reservation_status'],
                reservation_status_date=row['reservation_status_date']
            )
            db.add(booking)
    
    # Commit the session to persist the changes
    db.commit()


