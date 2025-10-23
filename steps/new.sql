-- Set the context (replace if needed, based on your Snowflake environment)
USE ROLE ACCOUNTADMIN; 
USE WAREHOUSE QUICKSTART_WH;
USE DATABASE QUICKSTART_PROD; -- Assuming this is the target database
USE SCHEMA SILVER;             -- Assuming this is the target schema for the objects

-- =============================================================================
-- Create the Python User-Defined Function (UDF) for City-Airport Mapping
-- =============================================================================
CREATE OR REPLACE FUNCTION get_city_for_airport (iata VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
AS $$
from snowflake.snowpark.files import SnowflakeFile
from _snowflake import vectorized
import pandas
import json

@vectorized(input=pandas.DataFrame)
def main(df):
    # Ensure the stage and file exist and are accessible
    try:
        with SnowflakeFile.open("@bronze.raw/airport_list.json", 'r', require_scoped_url = False) as f:
            airport_list = json.loads(f.read())
        # Assuming the structure is [..., city, ..., iata, ...] based on index access
        airports = {airport[3].upper(): airport[1] for airport in airport_list if len(airport) > 3 and airport[3]} 
        return df[0].apply(lambda iata_code: airports.get(str(iata_code).upper()) if iata_code else None)
    except Exception as e:
        # Basic error handling - consider more robust logging/error handling for production
        # Returning None or raising an error might be appropriate depending on desired behavior
        return pandas.Series([None] * len(df))

$$;

-- =============================================================================
-- Create the Views defined in the pipeline
-- =============================================================================

-- View 1: flight_emissions
-- Calculate average CO2 emissions per person per flight
CREATE OR REPLACE VIEW flight_emissions AS
SELECT 
    departure_airport, 
    arrival_airport, 
    -- Calculate avg CO2 per seat in kg
    AVG(estimated_co2_total_tonnes / seats) * 1000 AS co2_emissions_kg_per_person 
FROM oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample
WHERE seats != 0 AND estimated_co2_total_tonnes IS NOT NULL
GROUP BY departure_airport, arrival_airport;

---

-- View 2: flight_punctuality
-- Calculate the percentage of flights arriving on time or early
CREATE OR REPLACE VIEW flight_punctuality AS
SELECT 
    departure_iata_airport_code, 
    arrival_iata_airport_code, 
    -- Calculate percentage of punctual flights
    COUNT(CASE WHEN arrival_actual_ingate_timeliness IN ('OnTime', 'Early') THEN 1 END) * 100.0 / COUNT(*) AS punctual_pct 
FROM oag_flight_status_data_sample.public.flight_status_latest_sample
WHERE arrival_actual_ingate_timeliness IS NOT NULL
GROUP BY departure_iata_airport_code, arrival_iata_airport_code;

---

-- View 3: flights_from_home
-- Join emissions and punctuality, filter for flights from home airport, and add arrival city using UDF
CREATE OR REPLACE VIEW flights_from_home AS
SELECT 
    fe.departure_airport, 
    fe.arrival_airport, 
    -- Use the UDF to get the city for the arrival airport IATA code
    get_city_for_airport(fe.arrival_airport) AS arrival_city,  
    fe.co2_emissions_kg_per_person, 
    fp.punctual_pct
FROM flight_emissions fe
JOIN flight_punctuality fp 
    ON fe.departure_airport = fp.departure_iata_airport_code 
    AND fe.arrival_airport = fp.arrival_iata_airport_code
WHERE fe.departure_airport = (
    -- Read home airport IATA code from JSON file on stage
    SELECT $1:airport 
    FROM @quickstart_common.public.quickstart_repo/branches/main/data/home.json (FILE_FORMAT => bronze.json_format)
    LIMIT 1 -- Ensure only one value is returned if the file has multiple entries
);

---

-- View 4: weather_forecast
-- Calculate average weather forecast metrics per US postal code
CREATE OR REPLACE VIEW weather_forecast AS
SELECT 
    postal_code, 
    AVG(avg_temperature_air_2m_f) AS avg_temperature_air_f, 
    AVG(avg_humidity_relative_2m_pct) AS avg_relative_humidity_pct, 
    AVG(avg_cloud_cover_tot_pct) AS avg_cloud_cover_pct, 
    AVG(probability_of_precipitation_pct) AS precipitation_probability_pct
FROM global_weather__climate_data_for_bi.standard_tile.forecast_day
WHERE country = 'US'
GROUP BY postal_code;

---

-- View 5: major_us_cities
-- Identify US cities with a population over 100k using Snowflake Public Data
CREATE OR REPLACE VIEW major_us_cities AS
SELECT 
    geo.geo_id, 
    geo.geo_name, 
    MAX(ts.value) AS total_population -- Get the latest population figure
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.DATACOMMONS_TIMESERIES ts
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_INDEX geo 
    ON ts.geo_id = geo.geo_id
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS geo_rel 
    ON geo_rel.related_geo_id = geo.geo_id
WHERE ts.variable_name = 'Total Population, census.gov'
    AND ts.date >= '2020-01-01' -- Filter for recent population data
    AND geo.level = 'City'
    AND geo_rel.geo_id = 'country/USA'
GROUP BY geo.geo_id, geo.geo_name
HAVING MAX(ts.value) > 100000 -- Filter for cities with population > 100k
ORDER BY total_population DESC;

---

-- View 6: zip_codes_in_city
-- Map US cities to their constituent ZIP codes using Snowflake Public Data relationships
CREATE OR REPLACE VIEW zip_codes_in_city AS
SELECT 
    city.geo_id AS city_geo_id, 
    city.geo_name AS city_geo_name, 
    city.related_geo_id AS zip_geo_id, 
    city.related_geo_name AS zip_geo_name
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS country
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS city 
    ON country.related_geo_id = city.geo_id
WHERE country.geo_id = 'country/USA'
    AND city.level = 'City'
    AND city.related_level = 'CensusZipCodeTabulationArea';
    -- Removed ORDER BY as it's not typically needed/useful in a view definition

---

-- View 7: weather_joined_with_major_cities
-- Join weather forecast data with major city data via ZIP codes
CREATE OR REPLACE VIEW weather_joined_with_major_cities AS
SELECT 
    city.geo_id, 
    city.geo_name, 
    city.total_population,
    AVG(weather.avg_temperature_air_f) AS avg_temperature_air_f,
    AVG(weather.avg_relative_humidity_pct) AS avg_relative_humidity_pct,
    AVG(weather.avg_cloud_cover_pct) AS avg_cloud_cover_pct,
    AVG(weather.precipitation_probability_pct) AS precipitation_probability_pct
FROM major_us_cities city
JOIN zip_codes_in_city zip 
    ON city.geo_id = zip.city_geo_id
JOIN weather_forecast weather 
    ON zip.zip_geo_name = weather.postal_code
GROUP BY city.geo_id, city.geo_name, city.total_population;

---

-- Placeholder: Add CREATE OR REPLACE VIEW statements for any new views here

-- =============================================================================
-- End of Script
-- =============================================================================

CREATE OR REPLACE VIEW attractions (
    geo_id,
    geo_name,
    aquarium_cnt,
    zoo_cnt,
    korean_restaurant_cnt
) AS
SELECT
    city.geo_id,
    city.geo_name,
    COUNT(CASE WHEN category_main = 'Aquarium' THEN 1 END) AS aquarium_cnt,
    COUNT(CASE WHEN category_main = 'Zoo' THEN 1 END) AS zoo_cnt,
    COUNT(CASE WHEN category_main = 'Korean Restaurant' THEN 1 END) AS korean_restaurant_cnt
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_INDEX poi
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_ADDRESSES_RELATIONSHIPS poi_add 
    ON poi_add.poi_id = poi.poi_id
JOIN SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.US_ADDRESSES address 
    ON address.address_id = poi_add.address_id
JOIN major_us_cities city -- Assumes major_us_cities view exists
    ON city.geo_id = address.id_city
WHERE category_main IN ('Aquarium', 'Zoo', 'Korean Restaurant')
    AND address.id_country = 'country/USA' -- Make sure to qualify id_country
GROUP BY city.geo_id, city.geo_name;