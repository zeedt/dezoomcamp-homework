{{ config(materialized='view') }}

with zone_lookup as (
    select LocationID, Borough, Zone, service_zone from {{ source('staging','taxi_zone_lookup') }}
)

select * from zone_lookup


{% if (var('is_test_run')) %}
    limit 100
{% endif %}