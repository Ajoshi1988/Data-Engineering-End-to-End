with vkptnm as (

    select * from {{ ref('Vishakapatnam') }}

)

select * from vkptnm
Where "year" = 2016
