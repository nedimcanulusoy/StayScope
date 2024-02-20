from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict

class HotelBookingBase(BaseModel):
    hotel: Optional[str] = None
    is_canceled: Optional[int] = None
    lead_time: Optional[int] = None
    arrival_date_year: Optional[int] = None
    arrival_date_month: Optional[str] = None
    stays_in_weekend_nights: Optional[int] = None
    stays_in_week_nights: Optional[int] = None
    adults: Optional[int] = None
    children: Optional[int] = None
    babies: Optional[int] = None
    country: Optional[str] = None
    adr: Optional[float] = None
    customer_type: Optional[str] = None
    reservation_status: Optional[str] = None
    reservation_status_date: Optional[str] = None

class HotelBookingCreate(HotelBookingBase):
    pass

class HotelBooking(HotelBookingBase):
    id: int

    class Config:
        from_attributes = True

class SearchQueryParams(BaseModel):
    hotel: Optional[str] = None
    is_canceled: Optional[int] = None
    country: Optional[str] = None
    arrival_date_month: Optional[str] = None
    exclude_fields: Optional[Dict[str, Any]] = None  # New field for must_not conditions
    optional_fields: Optional[Dict[str, Any]] = None  # New field for should conditions
    range_fields: Optional[Dict[str, Dict[str, Any]]] = None  # New field for range queries

class AggregationField(BaseModel):
    field: str
    agg_type: str  # "terms", "composite", "significant_terms", etc.

class AggregationQueryParams(BaseModel):
    aggregations: List[AggregationField]

class FullTextSearchParams(BaseModel):
    query_string: str
    fields: List[str]

class SuggestQueryParams(BaseModel):
    text: str
    field: str

class SearchResult(BaseModel):
    hits: List[Dict[str, Any]]
    total: int

class AggregationResult(BaseModel):
    aggregations: Any


class Trend(BaseModel):
    time_bucket: str
    hotel_type: str
    booking_count: int

class BookingTrendsResult(BaseModel):
    trends: List[Trend]