{{config(materialized='incremental', unique_key = 'BOOKING_ID')}}


SELECT 
BOOKING_ID, LISTING_ID, BOOKING_DATE,
{{multiply('nights_booked','booking_amount',2)}} as total_amt,
service_fee, cleaning_fee, booking_status, created_at
from {{ref('bronze_bookings')}}