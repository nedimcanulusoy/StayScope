import pandas as pd
from logger_setup import Logger
from io import StringIO
from config import DATA_FOLDER_PATH
from models import get_db, insert_data
from sqlalchemy.orm import Session
from main import sync_sql_to_elasticsearch


log = Logger(__name__, './logs/etl.log').get_logger()

class ETLUtils(object):
    def __init__(self):
        pass 

    def extract(self, uploaded_file):
        if uploaded_file is not None:
            try:
                data = pd.read_csv(uploaded_file)
                log.info('Data loaded successfully')
                return data
            except Exception as e:
                log.error(f'Error loading data: {e}')
                raise e
    
    def transform(self, data):
        #check nulls in data
        missing_values = data.isnull().sum()

        #if all rows missing value isn't 0, drop them
        if missing_values.sum() > 0:
            # Handling missing values for 'children' by replacing them with the median value
            data['children'] = data['children'].fillna(data['children'].median())

            # For 'country', we'll replace missing values with 'Unknown'
            data['country'] = data['country'].fillna('Unknown')

            # For 'agent' and 'company', missing values may indicate bookings without agents or companies,
            # so I'll replace them with 0 to indicate 'No agent' or 'No company' because they can be considered as categorical variables
            data['agent'] = data['agent'].fillna(0)
            data['company'] = data['company'].fillna(0)
            
        #make sure that no nulls are left
        assert data.isnull().sum().sum() == 0

        # It will enable us to take the necessary actions to suppress the outliers in the data set.
        # It is useful if we want to use this dataset with a machine learning model in the future.

        def outlier_thresholds(dataframe, variable):
            quartile1 = dataframe[variable].quantile(0.01)
            quartile3 = dataframe[variable].quantile(0.99)
            interquantile_range = quartile3 - quartile1

            upper_limit = quartile3 + 1.5 * interquantile_range
            lower_limit = quartile1 - 1.5 * interquantile_range
            return lower_limit, upper_limit

        def replace_with_thresholds(dataframe, variable):
            lower_limit, upper_limit = outlier_thresholds(dataframe, variable)
            dataframe.loc[(dataframe[variable] > upper_limit), variable] = round(upper_limit)
            dataframe.loc[(dataframe[variable] < lower_limit), variable] = round(lower_limit)

        # numerical columns
        check_outlier_values = [
            'lead_time',
            'adr',
            'days_in_waiting_list',
            'adults',
            'children',
            'babies',
            'previous_cancellations',
            'previous_bookings_not_canceled'
        ]

        def check_outlier(dataframe, col_name):
            lower_limit, upper_limit = outlier_thresholds(dataframe, col_name)
            if dataframe[((dataframe[col_name] > upper_limit) > upper_limit) | (dataframe[col_name] < lower_limit)].any(axis=None):
                return True
            else:
                return False
            
        for col in check_outlier_values:
            has_outliers = check_outlier(data, col)
            log.info(f"Outliers in {col} column: {has_outliers}")
            if has_outliers:
                replace_with_thresholds(data, col)
                log.info(f"Outliers in {col} column were suppressed.")

        country_names = {
            "PRT": "Portugal",
            "GBR": "United Kingdom",
            "USA": "United States",
            "ESP": "Spain",
            "IRL": "Ireland",
            "FRA": "France",
            "Unknown": "Unknown", # I added this to handle the missing values
            "ROU": "Romania",
            "NOR": "Norway",
            "OMN": "Oman",
            "ARG": "Argentina",
            "POL": "Poland",
            "DEU": "Germany",
            "BEL": "Belgium",
            "CHE": "Switzerland",
            "CN": "Canada", 
            "GRC": "Greece",
            "ITA": "Italy",
            "NLD": "Netherlands",
            "DNK": "Denmark",
            "RUS": "Russia",
            "SWE": "Sweden",
            "AUS": "Australia",
            "EST": "Estonia",
            "CZE": "Czech Republic",
            "BRA": "Brazil",
            "FIN": "Finland",
            "MOZ": "Mozambique",
            "BWA": "Botswana",
            "LUX": "Luxembourg",
            "SVN": "Slovenia",
            "ALB": "Albania",
            "IND": "India",
            "CHN": "China",
            "MEX": "Mexico",
            "MAR": "Morocco",
            "UKR": "Ukraine",
            "SMR": "San Marino",
            "LVA": "Latvia",
            "PRI": "Puerto Rico",
            "SRB": "Serbia",
            "CHL": "Chile",
            "AUT": "Austria",
            "BLR": "Belarus",
            "LTU": "Lithuania",
            "TUR": "Turkey",
            "ZAF": "South Africa",
            "AGO": "Angola",
            "ISR": "Israel",
            "CYM": "Cayman Islands",
            "ZMB": "Zambia",
            "CPV": "Cape Verde",
            "ZWE": "Zimbabwe",
            "DZA": "Algeria",
            "KOR": "South Korea",
            "CRI": "Costa Rica",
            "HUN": "Hungary",
            "ARE": "United Arab Emirates",
            "TUN": "Tunisia",
            "JAM": "Jamaica",
            "HRV": "Croatia",
            "HKG": "Hong Kong",
            "IRN": "Iran",
            "GEO": "Georgia",
            "AND": "Andorra",
            "GIB": "Gibraltar",
            "URY": "Uruguay",
            "JEY": "Jersey",
            "CAF": "Central African Republic",
            "CYP": "Cyprus",
            "COL": "Colombia",
            "GGY": "Guernsey",
            "KWT": "Kuwait",
            "NGA": "Nigeria",
            "MDV": "Maldives",
            "VEN": "Venezuela",
            "SVK": "Slovakia",
            "FJI": "Fiji",
            "KAZ": "Kazakhstan",
            "PAK": "Pakistan",
            "IDN": "Indonesia",
            "LBN": "Lebanon",
            "PHL": "Philippines",
            "SEN": "Senegal",
            "SYC": "Seychelles",
            "AZE": "Azerbaijan",
            "BHR": "Bahrain",
            "NZL": "New Zealand",
            "THA": "Thailand",
            "DOM": "Dominican Republic",
            "MKD": "North Macedonia",
            "MYS": "Malaysia",
            "ARM": "Armenia",
            "JPN": "Japan",
            "LKA": "Sri Lanka",
            "CUB": "Cuba",
            "CMR": "Cameroon",
            "BIH": "Bosnia and Herzegovina",
            "MUS": "Mauritius",
            "COM": "Comoros",
            "SUR": "Suriname",
            "UGA": "Uganda",
            "BGR": "Bulgaria",
            "CIV": "Ivory Coast",
            "JOR": "Jordan",
            "SYR": "Syria",
            "SGP": "Singapore",
            "BDI": "Burundi",
            "SAU": "Saudi Arabia",
            "VNM": "Vietnam",
            "PLW": "Palau",
            "QAT": "Qatar",
            "EGY": "Egypt",
            "PER": "Peru",
            "MLT": "Malta",
            "MWI": "Malawi",
            "ECU": "Ecuador",
            "MDG": "Madagascar",
            "ISL": "Iceland",
            "UZB": "Uzbekistan",
            "NPL": "Nepal",
            "BHS": "Bahamas",
            "MAC": "Macao",
            "TGO": "Togo",
            "TWN": "Taiwan",
            "DJI": "Djibouti",
            "STP": "Sao Tome and Principe",
            "KNA": "Saint Kitts and Nevis",
            "ETH": "Ethiopia",
            "IRQ": "Iraq",
            "HND": "Honduras",
            "RWA": "Rwanda",
            "KHM": "Cambodia",
            "MCO": "Monaco",
            "BGD": "Bangladesh",
            "IMN": "Isle of Man",
            "TJK": "Tajikistan",
            "NIC": "Nicaragua",
            "BEN": "Benin",
            "VGB": "British Virgin Islands",
            "TZA": "Tanzania",
            "GAB": "Gabon",
            "GHA": "Ghana",
            "TMP": "East Timor",
            "GLP": "Guadeloupe",
            "KEN": "Kenya",
            "LIE": "Liechtenstein",
            "GNB": "Guinea-Bissau",
            "MNE": "Montenegro",
            "UMI": "United States Minor Outlying Islands",
            "MYT": "Mayotte",
            "FRO": "Faroe Islands",
            "MMR": "Myanmar",
            "PAN": "Panama",
            "BFA": "Burkina Faso",
            "LBY": "Libya",
            "MLI": "Mali",
            "NAM": "Namibia",
            "BOL": "Bolivia",
            "PRY": "Paraguay",
            "BRB": "Barbados",
            "ABW": "Aruba",
            "AIA": "Anguilla",
            "SLV": "El Salvador",
            "DMA": "Dominica",
            "PYF": "French Polynesia",
            "GUY": "Guyana",
            "LCA": "Saint Lucia",
            "ATA": "Antarctica",
            "GTM": "Guatemala",
            "ASM": "American Samoa",
            "MRT": "Mauritania",
            "NCL": "New Caledonia",
            "KIR": "Kiribati",
            "SDN": "Sudan",
            "ATF": "French Southern Territories",
            "SLE": "Sierra Leone",
            "LAO": "Laos"
        }

        data['country'] = data['country'].map(country_names)

        #split the arrival date into year, month and day
        data['arrival_date'] = pd.to_datetime(data['arrival_date_year'].astype(str) + '-' + data['arrival_date_month'] + '-' + data['arrival_date_day_of_month'].astype(str))

        return data
    
    def load(self, data):
        #save the cleaned data to a csv file with timestamp
        timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
        filename = "".join([DATA_FOLDER_PATH, "hotel_bookings_clean_", timestamp, ".csv"])
        try:
            data.to_csv(filename, index=False)
            log.info(f'Cleaned data saved to {filename}')
            
        except Exception as e:
            log.error(f'Error saving data: {e}')
            raise e

        is_success = False
        
        #insert the cleaned data to the database
        db: Session = next(get_db())
        try:
            insert_data(data_path=filename, db=db)
            log.info(f'Uploaded new data cleaned and inserted into the database')
            is_success = True
            return is_success
        except Exception as e:
            log.error(f'Error inserting new data into the database: {e}')
            raise e
        finally:
            db.close()
            is_success = False          
        
    def run_etl_flow(self, uploaded_file):
        raw_data = self.extract(uploaded_file)
        transformed_data = self.transform(raw_data)
        success_flag = self.load(transformed_data)    
        return transformed_data, success_flag



        