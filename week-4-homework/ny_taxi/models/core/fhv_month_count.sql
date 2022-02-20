
{{ config(materialized='table') }}


with fact_fhv_trips as (

    select * from {{ref('fact_fhv_trips')}}

)

SELECT (case
 when extract(MONTH from pickup_datetime) = 1 then 'JANUARY' 
 when extract(MONTH from pickup_datetime) = 2 then 'FEBRUARY' 
 when extract(MONTH from pickup_datetime) = 3 then 'MARCH' 
 when extract(MONTH from pickup_datetime) = 4 then 'APRIL' 
 when extract(MONTH from pickup_datetime) = 5 then 'MAY' 
 when extract(MONTH from pickup_datetime) = 6 then 'JUNE' 
 when extract(MONTH from pickup_datetime) = 7 then 'JULY' 
 when extract(MONTH from pickup_datetime) = 8 then 'AUGUST' 
 when extract(MONTH from pickup_datetime) = 9 then 'SEPTEMBER' 
 when extract(MONTH from pickup_datetime) = 10 then 'OCTOBER' 
 when extract(MONTH from pickup_datetime) = 11 then 'NOVEMBER' 
 when extract(MONTH from pickup_datetime) = 12 then 'DECEMBER' 
 end) as month_val
,  count(*) as count 
    from fact_fhv_trips group by month_val order by count desc

