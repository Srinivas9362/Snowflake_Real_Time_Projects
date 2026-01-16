{{config(materialized= 'view')}}

select * from airbnb.staging.bookings