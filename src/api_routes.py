from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from schemas import (
    SearchQueryParams,
    AggregationQueryParams,
    FullTextSearchParams,
    SuggestQueryParams,
    SearchResult,
    AggregationResult
)
from elasticsearch_operations import ElasticsearchService
from config import ELASTICSEARCH_SETTINGS, ES_INDEX_NAME
from logger_setup import Logger

log = Logger(__name__, './logs/api.log').get_logger()

router = APIRouter()
es_service = ElasticsearchService(ELASTICSEARCH_SETTINGS)

@router.get("/")
async def root():
    return {"message": "Hello World"}


@router.post('/search/', tags=['Search'],response_model=SearchResult)
async def search(query_params: SearchQueryParams):
    try:
        # Convert Pydantic model to dict and exclude unset fields
        params = query_params.dict(exclude_unset=True)
        results = es_service.search_query_command(ES_INDEX_NAME, params)
        if results:
            return {"hits": results['hits']['hits'], "total": results['hits']['total']['value']}
        else:
            raise HTTPException(status_code=404, detail="No results found")
    except Exception as e:
        log.error(f'Error searching in Elasticsearch: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/aggregate/", tags=['Aggregate'],response_model=AggregationResult)
async def aggregate(query_params: AggregationQueryParams):
    try:
        result = es_service.dynamic_aggregation_query(ES_INDEX_NAME, query_params.dict())
        if result:
            return result
        else:
            raise HTTPException(status_code=404, detail="No aggregations found")
    except Exception as e:
        log.error(f'Error performing aggregation in Elasticsearch: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/full-text-search/", response_model=SearchResult, tags=['Full Text Search'])
async def full_text_search(query_params: FullTextSearchParams):
    try:
        results = es_service.full_text_search_query(ES_INDEX_NAME, query_params.dict())
        if results:
            return {"hits": results['hits']['hits'], "total": results['hits']['total']['value']}
        else:
            raise HTTPException(status_code=404, detail="No results found")
    except Exception as e:
        log.error(f'Error searching in Elasticsearch: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/suggest/", response_model=List[str], tags=['Suggest'])
async def suggest(query_params: SuggestQueryParams):
    try:
        suggestions = es_service.suggest_query(ES_INDEX_NAME, query_params.text, query_params.field)
        if suggestions:
            return suggestions
        else:
            raise HTTPException(status_code=404, detail="No suggestions found")
    except Exception as e:
        log.error(f'Error suggesting in Elasticsearch: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/cancellation_rate", tags=["Reports"])
async def cancellation_rate_report():
    try:
        results = es_service.get_cancellation_rate_by_segment_and_type(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving cancellation rate report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/adr_by_month", tags=["Reports"])
async def adr_by_month_report():
    try:
        results = es_service.get_adr_by_month_and_type(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving ADR by month report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/top_countries", tags=["Reports"])
async def top_countries_report():
    try:
        results = es_service.get_top_countries_with_most_bookings(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving top countries report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/length_of_stay_distribution", tags=["Reports"])
async def length_of_stay_distribution_simple_report():
    try:
        results = es_service.get_length_of_stay_distribution_simple(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving length of stay distribution report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/booking_trends_over_time", tags=["Reports"])
async def booking_trends_over_time_report():
    try:
        results = es_service.get_booking_trends_over_time(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving booking trends over time report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/special_requests_impact_on_cancellations", tags=["Reports"])
async def special_requests_impact_on_cancellations_report():
    try:
        results = es_service.get_special_requests_impact_on_cancellations(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving special requests impact on cancellations report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/average_lead_time_by_cancellation_status", tags=["Reports"])
async def average_lead_time_by_cancellation_status_report():
    try:
        results = es_service.get_average_lead_time_by_cancellation_status(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving average lead time by cancellation status report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/bookings_distribution_by_room_type", tags=["Reports"])
async def bookings_distribution_by_room_type_report():
    try:
        results = es_service.get_bookings_distribution_by_room_type(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving bookings distribution by room type report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/bookings_by_guest_country", tags=["Reports"])
async def bookings_by_guest_country_report():
    try:
        results = es_service.get_bookings_by_guest_country(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving bookings by guest country report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/booking_source_analysis", tags=["Reports"])
async def booking_source_analysis_report():
    try:
        results = es_service.get_booking_source_analysis(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving booking source analysis report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/revenue_analysis_by_room_and_month", tags=["Reports"])
async def revenue_analysis_by_room_and_month_report():
    try:
        results = es_service.get_revenue_analysis_by_room_and_month(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving revenue analysis by room and month report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/impact_of_lead_time_on_adr", tags=["Reports"])
async def impact_of_lead_time_on_adr_report():
    try:
        results = es_service.get_impact_of_lead_time_on_adr(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving impact of lead time on ADR report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/analyze_repeat_guest_bookings", tags=["Reports"])
async def analyze_repeat_guest_bookings_report():
    try:
        results = es_service.get_analyze_repeat_guest_bookings(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving analyze repeat guest bookings report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/correlate_adr_with_factors", tags=["Reports"])
async def correlate_adr_with_factors_report():
    try:
        results = es_service.get_correlate_adr_with_factors(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving correlate ADR with factors report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

# @router.get("/reports/analyze_booking_trends_by_market_segment", tags=["Reports"])
@router.get("/reports/correlate_cancelations_with_factors", tags=["Reports"])
async def correlate_cancelations_with_factors_report():
    try:
        results = es_service.get_correlate_cancelations_with_factors(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving correlate cancellations with factors report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/reports/analyze_booking_composition", tags=["Reports"])
async def get_analyze_booking_composition_report():
    try:
        results = es_service.get_analyze_booking_composition(ES_INDEX_NAME)
        return results
    except Exception as e:
        log.error(f'Error retrieving analyze booking composition report: {e}')
        raise HTTPException(status_code=500, detail="Internal server error")
