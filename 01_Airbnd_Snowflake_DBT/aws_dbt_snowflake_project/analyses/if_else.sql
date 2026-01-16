-- {% set nights_booked = 6%}

-- SELECT * FROM {{ref('bronze_bookings')}}
-- where nights_booked > {{nights_booked}}


{% set flag=2 %}

select * from {{ref('bronze_bookings')}}
{% if flag==1 %}
where nights_booked > 1
{% else %}
where nights_booked =1
{% endif %}