
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with stg_fhv_tripdata as (

    select * from {{ref('stg_fhv_tripdata')}}

), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

Select dispatching_base_num, pickup_datetime, dropoff_datetime,
    PULocationId, DOLocationID, SR_Flag, t1.Zone as pickup_zone, t2.Zone as drop_off_zone from stg_fhv_tripdata y
 INNER JOIN dim_zones t1 on t1.LocationID= y.PULocationID inner join dim_zones t2 on t2.LocationID=y.DOLocationID

