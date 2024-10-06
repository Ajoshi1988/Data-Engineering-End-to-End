
with source as (

  
    select * from {{ source('public', 'city_day_aqi') }}

),

renamed as (

    select
        "index",
        "City", 
        "Date", 
        "year",
        "PM2.5", 
        "PM10",
        "O3",
        "Benzene",
        "Toluene",
        "AQI",
        "AQI_Bucket"

    from source
    Where "City" = 'Delhi'

)

select * from renamed