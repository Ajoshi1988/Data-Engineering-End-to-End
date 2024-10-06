from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import air_quality_transform_dbt_assets, city_day_source, Delhi_MLModel, Vishakapatnam_MLModel
from .project import air_quality_transform_project
from .schedules import schedules

defs = Definitions(
    assets=[city_day_source, air_quality_transform_dbt_assets, Delhi_MLModel, Vishakapatnam_MLModel],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=air_quality_transform_project),
    },
)