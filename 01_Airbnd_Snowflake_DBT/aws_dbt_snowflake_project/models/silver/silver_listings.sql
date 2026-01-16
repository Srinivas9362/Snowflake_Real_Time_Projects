{{ config (materialized='incremental',unique_key='listing_id')}}

SELECT LISTING_ID, HOST_ID, PROPERTY_TYPE, ROOM_TYPE, CITY, COUNTRY, ACCOMMODATES, BEDROOMS, BATHROOMS, PRICE_PER_NIGHT,
{{tag('price_per_night')}} AS price_per_night_tag , created_At
from {{ref('bronze_listings')}}