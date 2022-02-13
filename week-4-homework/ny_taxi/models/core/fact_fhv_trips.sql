
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with stg_fhv_trip_data_2019 as (

    select * from {{ref('stg_fhv_trip_data_2019')}}

), stg_zone_lookup as (

    select * from {{ref('stg_zone_lookup')}}

)

Select dispatching_base_num, pickup_datetime, dropoff_datetime,
    PULocationId, DOLocationID, SR_Flag, t1.Zone as pickup_zone, t2.Zone as drop_off_zone from stg_fhv_trip_data_2019 y
 INNER JOIN stg_zone_lookup t1 on t1.LocationID= y.PULocationID inner join stg_zone_lookup t2 on t2.LocationID=y.DOLocationID

