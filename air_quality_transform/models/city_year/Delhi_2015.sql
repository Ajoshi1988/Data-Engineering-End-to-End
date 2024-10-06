with delhi as (

    select * from {{ ref('Delhi') }}

)

select * from delhi
Where "year" = 2015
