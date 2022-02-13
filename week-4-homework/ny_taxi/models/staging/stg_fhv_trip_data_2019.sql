
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

with fhv_trip_data as (

    select dispatching_base_num, pickup_datetime, dropoff_datetime,
    PULocationId, DOLocationID, SR_Flag from {{ source('staging','external_fhv_trip_data_2019') }}

)

select *
from fhv_trip_data

{% if (var('is_test_run')) %}
    limit 100
{% endif %}

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
