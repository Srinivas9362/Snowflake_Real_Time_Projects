{% set cols = ['NIGHTS_BOOKED','BOOKING_ID','BOOKING_AMOUNT']%}

SELECT 
{% for col in cols%}
{{col}}
{% if loop.last %}{%else%},{% endif %}
{%endfor%}
from {{ref('bronze_bookings')}}