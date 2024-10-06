from pathlib import Path

from dagster_dbt import DbtProject

air_quality_transform_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
air_quality_transform_project.prepare_if_dev()