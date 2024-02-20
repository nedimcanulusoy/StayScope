import os

# Reset project path to this file's location
project_path = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_path)

from elasticsearch import Elasticsearch, exceptions
from elasticsearch.helpers import bulk
from logger_setup import Logger
from models import HotelBooking, get_db
from config import ELASTICSEARCH_SETTINGS, ES_INDEX_NAME
from logger_setup import Logger

log = Logger(__name__, './logs/elasticsearch.log').get_logger()

class ElasticsearchService(object):
    def __init__(self, config):
        self.es = Elasticsearch([{
            'host': config['host'],
            'port': config['port'],
            'scheme': config['scheme']
        }], basic_auth=config['auth'])

        self.index_mappings_cache = {}  # Cache for storing index mappings

    def create_index(self, index_name):
        index_mapping = {
            "mappings": {
                "properties": {
                    "hotel": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "hotel_suggest": {
                        "type": "completion"
                    },
                    "is_canceled": {"type": "integer"},
                    "lead_time": {"type": "integer"},
                    "arrival_date": {"type": "date", "format": "yyyy-MM-dd", "fields": {"keyword": {"type": "keyword"}}},
                    "arrival_date_year": {"type": "integer"},
                    "arrival_date_month": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "arrival_date_week_number": {"type": "integer"},
                    "arrival_date_day_of_month": {"type": "integer"},
                    "stays_in_weekend_nights": {"type": "integer"},
                    "stays_in_week_nights": {"type": "integer"},
                    "adults": {"type": "integer"},
                    "children": {"type": "float"},
                    "babies": {"type": "integer"},
                    "meal": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "country": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "country_suggest": {
                        "type": "completion"
                    },
                    "market_segment": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "distribution_channel": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "is_repeated_guest": {"type": "integer"},
                    "previous_cancellations": {"type": "integer"},
                    "previous_bookings_not_canceled": {"type": "integer"},
                    "reserved_room_type": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "assigned_room_type": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "booking_changes": {"type": "integer"},
                    "deposit_type": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "agent": {"type": "float"},
                    "company": {"type": "float"},
                    "days_in_waiting_list": {"type": "integer"},
                    "customer_type": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "adr": {"type": "float"},
                    "required_car_parking_spaces": {"type": "integer"},
                    "total_of_special_requests": {"type": "integer"},
                    "reservation_status": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "reservation_status_suggest": {
                        "type": "completion"
                    },
                    "reservation_status_date": {"type": "date", "format": "yyyy-MM-dd", "fields": {"keyword": {"type": "keyword"}}}
                }
            }
        }
       
        try:
            if not self.es.indices.exists(index=index_name):
                self.es.indices.create(index=index_name, body=index_mapping)
                log.info(f'Index {index_name} created in Elasticsearch')
            else:
                log.info(f'Index {index_name} exists in Elasticsearch')
                
        except Exception as e:
            log.error(f'Error creating index {index_name} in Elasticsearch: {e}')
    

    def index_document(self, index_name, document):
        try:
            # Use the index API to add or update a document.
            response = self.es.index(index=index_name, document=document)
            log.info(f"Document indexed successfully in {index_name}: {response}")
            return response
        except Exception as e:
            log.error(f"Error indexing document in {index_name}: {e}")
            return None

    def get_index_mapping(self, index_name):
        #check if index mapping is already cached
        if index_name not in self.index_mappings_cache:
            # Fetch the mapping from Elasticsearch and store it in the cache
            self.index_mappings_cache[index_name] = self.es.indices.get_mapping(index=index_name)
        return self.index_mappings_cache[index_name]

    def db_to_es_docs(self, session, index_name):
        documents = []
        
        query = session.query(HotelBooking).all()
        for instance in query:
            # Convert instance to dict and prepare the document
            doc = {c.name: getattr(instance, c.name) for c in instance.__table__.columns}
            doc['hotel_suggest'] = {"input": doc['hotel']}
            doc['country_suggest'] = {"input": doc['country']}
            doc['reservation_status_suggest'] = {'input': doc['reservation_status']}

            documents.append({"_index": index_name, "_source": doc})
        return documents
    
    def insert_bulk_data_from_db(self, index_name):
        db_gen = get_db()  # Get the generator for the session
        session = next(db_gen)  # Advance to the first yield to get the session

        documents = self.db_to_es_docs(session, index_name)
        try:
            bulk(self.es, documents)
            log.info(f'Data inserted into {index_name} from database')
        except Exception as e:
            log.error(f'Error inserting data into {index_name} from database: {e}')
        finally:
            next(db_gen, None)
            

    def search_data(self, index_name, query):
        try:
            response = self.es.search(index=index_name, body=query)
            return response
        except Exception as e:
            log.error(f'Error searching in {index_name}: {e}')
            return None

    def search_query_command(self, index_name, params):
        bool_query = {"bool": {"must": [], "should": [], "must_not": []}}
        
        # Handling 'must' conditions based on user input
        for field, value in params.items():
            if value is not None and field not in ['exclude_fields', 'optional_fields', 'range_fields']:
                bool_query["bool"]["must"].append({"match": {field: value}})
        
        # Handling 'exclude_fields' for 'must_not' conditions
        exclude_fields = params.get('exclude_fields', {})
        for field, value in exclude_fields.items():
            bool_query["bool"]["must_not"].append({"match": {field: value}})
        
        # Handling 'optional_fields' for 'should' conditions
        optional_fields = params.get('optional_fields', {})
        for field, value in optional_fields.items():
            bool_query["bool"]["should"].append({"match": {field: value}})
            bool_query["bool"]["minimum_should_match"] = 1  # Ensure at least one 'should' condition matches if present
        
        # Handling 'range_fields' for range conditions
        range_fields = params.get('range_fields', {})
        for field, ranges in range_fields.items():
            range_query = {"range": {field: {}}}
            for range_type, range_value in ranges.items():
                range_query["range"][field][range_type] = range_value
            bool_query["bool"]["must"].append(range_query)
        
        query = {"query": bool_query}
        try:
            response = self.es.search(index=index_name, body=query)
            return response
        except Exception as e:
            log.error(f'Error executing search query in {index_name}: {e}')
            return None



    def dynamic_aggregation_query(self, index_name, agg_params):
        # Use the cached mapping or fetch it if not cached
        # mappings = self.es.indices.get_mapping(index=index_name)
        mappings = self.get_index_mapping(index_name)
        properties = mappings[index_name]['mappings']['properties']

        aggs_body = {"aggs": {}, "size": 0}
        for agg in agg_params['aggregations']:
            field = agg['field']
            agg_type = agg['agg_type']

            # Determine if field is a text field with a keyword sub-field
            if field in properties and 'fields' in properties[field] and 'keyword' in properties[field]['fields']:
                field_name = f"{field}.keyword"
            else:
                field_name = field

            aggs_body["aggs"][f"{field}_{agg_type}"] = {agg_type: {"field": field_name}}

        try:
            response = self.es.search(index=index_name, body=aggs_body)
            if 'aggregations' in response:
                return {"aggregations": response['aggregations']}
            else:
                log.error(f"No aggregations found in response for {index_name}")
                return None
        except Exception as e:
            log.error(f'Error performing dynamic aggregation on {index_name}: {e}')
            return None



    def full_text_search_query(self, index_name, user_input):
        terms = user_input["query_string"].split(' ')  # Split terms by comma
        fields = user_input["fields"]

        must_queries = []
        for term in terms:
            trimmed_term = term.strip()  # Remove any leading/trailing whitespace
            must_queries.append({
                "multi_match": {
                    "query": trimmed_term,
                    "fields": fields,
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            })

        try:
            query = {
                "query": {
                    "bool": {
                        "must": must_queries
                    }
                }
            }
            return self.search_data(index_name, query)
        # try:
        #     query = {
        #         "query": {
        #             "multi_match": {
        #                 "query": user_input["query_string"],
        #                 "fields": user_input["fields"] + ["^2"],
        #                 "fuzziness": "AUTO",
        #                 "type": "cross_fields",
        #             }
        #         }
        #     }
        #     return self.search_data(index_name, query)
        except Exception as e:
            log.error(f'Error performing full-text search on {index_name}: {e}')
            return None

    def suggest_query(self, index_name, text, field):
        suggest_body = {
            "suggest": {
                "text": text,
                field: {
                    "completion": {
                        "field": field,
                        "skip_duplicates": True  # This attempts to skip duplicates, but its effectiveness may vary.
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=suggest_body)
            suggestions = []
            if response and "suggest" in response and field in response["suggest"]:
                seen = set()  # Set to track seen suggestions
                for suggestion in response["suggest"][field][0]["options"]:
                    if suggestion["text"] not in seen:
                        suggestions.append(suggestion["text"])
                        seen.add(suggestion["text"])
            return suggestions
        except Exception as e:
            log.error(f'Error suggesting in {index_name}: {e}')
            return None
        
    def get_cancellation_rate_by_segment_and_type(self, index_name):
        query_body = {
            "size": 0,  # We don't need the actual documents, just the aggregations
            "aggs": {
                "market_segment": {
                    "terms": {"field": "market_segment.keyword"},
                    "aggs": {
                        "hotel_type": {
                            "terms": {"field": "hotel.keyword"},
                            "aggs": {
                                "cancellation_rate": {
                                    "avg": {"field": "is_canceled"}
                                }
                            }
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']
        except Exception as e:
            log.error(f'Error getting cancellation rate by market segment and hotel type: {e}')
            return None
        
    def get_adr_by_month_and_type(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "months": {
                    "date_histogram": {
                        "field": "arrival_date",
                        "calendar_interval": "month",
                        "format": "yyyy-MM-dd"
                    },
                    "aggs": {
                        "hotel_type": {
                            "terms": {"field": "hotel.keyword"},
                            "aggs": {
                                "average_adr": {
                                    "avg": {"field": "adr"}
                                }
                            }
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']
        except Exception as e:
            log.error(f'Error getting ADR by month and hotel type: {e}')
            return None


    def get_top_countries_with_most_bookings(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "top_countries": {
                    "terms": {
                        "field": "country.keyword",
                        "size": 10
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['top_countries']['buckets']
        except Exception as e:
            log.error(f'Error getting top countries with most bookings: {e}')
            return None


    def get_length_of_stay_distribution_simple(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "length_of_stay": {
                    "terms": {
                        "script": {
                            "source": "doc['stays_in_weekend_nights'].value + doc['stays_in_week_nights'].value",
                            "lang": "painless"
                        },
                        "size": 10  # Adjust the size as needed
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['length_of_stay']['buckets']
        except Exception as e:
            log.error(f'Error getting simplified length of stay distribution: {e}')
            return None
        
    def get_booking_trends_over_time(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "bookings_over_time": {
                    "date_histogram": {
                        "field": "arrival_date",
                        "calendar_interval": "month",
                        "format": "yyyy-MM"
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['bookings_over_time']['buckets']
        except Exception as e:
            log.error(f'Error getting booking trends over time: {e}')
            return None


    # def get_booking_trends_over_time(self, index_name):
    #     query_body = {
    #         "size": 0,
    #         "aggs": {
    #             "bookings_over_time": {
    #                 "date_histogram": {
    #                     "field": "arrival_date",
    #                     "calendar_interval": "month",
    #                     "format": "yyyy-MM"
    #                 }
    #             }
    #         }
    #     }
    #     try:
    #         response = self.es.search(index=index_name, body=query_body)
    #         return response['aggregations']['bookings_over_time']['buckets']
    #     except Exception as e:
    #         log.error(f'Error getting booking trends over time: {e}')
    #         return None

    def get_special_requests_impact_on_cancellations(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "special_requests": {
                    "terms": {"field": "total_of_special_requests"},
                    "aggs": {
                        "cancellation_rate": {
                            "avg": {"field": "is_canceled"}
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['special_requests']['buckets']
        except Exception as e:
            log.error(f'Error getting impact of special requests on cancellations: {e}')
            return None


    def get_average_lead_time_by_cancellation_status(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "cancellation_status": {
                    "terms": {"field": "is_canceled"},
                    "aggs": {
                        "average_lead_time": {
                            "avg": {"field": "lead_time"}
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['cancellation_status']['buckets']
        except Exception as e:
            log.error(f'Error getting average lead time by cancellation status: {e}')
            return None

    def get_bookings_distribution_by_room_type(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "room_types": {
                    "terms": {"field": "reserved_room_type.keyword"}
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['room_types']['buckets']
        except Exception as e:
            log.error(f'Error getting bookings distribution by room type: {e}')
            return None
        
    def get_bookings_by_guest_country(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "guest_countries": {
                    "terms": {
                        "field": "country.keyword",
                        "size": 10  # Adjust based on how many top countries you want to analyze
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['guest_countries']['buckets']
        except Exception as e:
            log.error(f'Error getting number of bookings by guest country: {e}')
            return None

    def get_booking_source_analysis(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "booking_sources": {
                    "terms": {
                        "field": "distribution_channel.keyword",
                        "size": 5  # Adjust based on the number of channels you want to analyze
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['booking_sources']['buckets']
        except Exception as e:
            log.error(f'Error getting booking source analysis: {e}')
            return None

    def get_revenue_analysis_by_room_and_month(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "room_types": {
                    "terms": {"field": "reserved_room_type.keyword"},
                    "aggs": {
                        "monthly_revenue": {
                            "date_histogram": {
                                "field": "arrival_date",
                                "calendar_interval": "month",
                                "format": "yyyy-MM",
                                "min_doc_count": 1
                            },
                            "aggs": {
                                "revenue": {
                                    "sum": {
                                        "script": {
                                            "source": "doc['adr'].value * (doc['stays_in_week_nights'].value + doc['stays_in_weekend_nights'].value)",
                                            "lang": "painless"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['room_types']['buckets']
        except Exception as e:
            log.error(f'Error getting revenue analysis by room type and month: {e}')
            return None

    def get_impact_of_lead_time_on_adr(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "lead_time_buckets": {
                    "histogram": {
                        "field": "lead_time",
                        "interval": 10,  # Adjust the interval based on your data distribution
                        "min_doc_count": 1
                    },
                    "aggs": {
                        "average_adr": {
                            "avg": {"field": "adr"}
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']['lead_time_buckets']['buckets']
        except Exception as e:
            log.error(f'Error getting impact of lead time on ADR: {e}')
            return None

    def get_analyze_repeat_guest_bookings(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "repeat_guests": {
                    "terms": {
                        "field": "is_repeated_guest",
                        "size": 2  # 0 for new guests, 1 for repeat guests
                    },
                    "aggs": {
                        "average_lead_time": {
                            "avg": {
                                "field": "lead_time"
                            }
                        },
                        "bookings_by_country": {
                            "terms": {
                                "field": "country.keyword"
                            }
                        },
                        "bookings_by_hotel_type": {
                            "terms": {
                                "field": "hotel.keyword"
                            }
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']
        except Exception as e:
            log.error(f'Error analyzing repeat guest bookings: {e}')
            return None
        

    def get_correlate_adr_with_factors(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "adr_correlation": {
                    "terms": {
                        "field": "adr",
                        "order": {
                            "_key": "asc"  # Order by ADR ascendingly
                        }
                    },
                    "aggs": {
                        "cancellation_rate": {
                            "avg": {
                                "field": "is_canceled"
                            }
                        },
                        "average_stay_length": {
                            "avg": {
                                "script": {
                                    "source": "doc['stays_in_weekend_nights'].value + doc['stays_in_week_nights'].value",
                                    "lang": "painless"
                                }
                            }
                        },
                        "special_requests_count": {
                            "avg": {
                                "field": "total_of_special_requests"
                            }
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']
        except Exception as e:
            log.error(f'Error correlating ADR with booking factors: {e}')
            return None

    def get_correlate_cancelations_with_factors(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "cancellation_correlation": {
                    "terms": {
                        "field": "is_canceled",
                        "size": 2  # 0 for not canceled, 1 for canceled
                    },
                    "aggs": {
                        "average_lead_time": {
                            "avg": {
                                "field": "lead_time"
                            }
                        },
                        "average_stay_length": {
                            "avg": {
                                "script": {
                                    "source": "doc['stays_in_weekend_nights'].value + doc['stays_in_week_nights'].value",
                                    "lang": "painless"
                                }
                            }
                        },
                        "special_requests_count": {
                            "avg": {
                                "field": "total_of_special_requests"
                            }
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']
        except Exception as e:
            log.error(f'Error correlating cancellations with booking factors: {e}')
            return None
        
    def get_analyze_booking_composition(self, index_name):
        query_body = {
            "size": 0,
            "aggs": {
                "booking_composition": {
                    "terms": {
                        "script": {
                            "source": "doc['adults'].value + ' adults, ' + doc['children'].value + ' children, ' + doc['babies'].value + ' babies'",
                            "lang": "painless"
                        }
                    },
                    "aggs": {
                        "average_lead_time": {
                            "avg": {
                                "field": "lead_time"
                            }
                        },
                        "average_stay_length": {
                            "avg": {
                                "script": {
                                    "source": "doc['stays_in_weekend_nights'].value + doc['stays_in_week_nights'].value",
                                    "lang": "painless"
                                }
                            }
                        },
                        "special_requests_count": {
                            "avg": {
                                "field": "total_of_special_requests"
                            }
                        }
                    }
                }
            }
        }
        try:
            response = self.es.search(index=index_name, body=query_body)
            return response['aggregations']
        except Exception as e:
            log.error(f'Error analyzing booking composition: {e}')
            return None