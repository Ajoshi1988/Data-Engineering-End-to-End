from setuptools import find_packages, setup

setup(
    name="air_quality_source_pipeline",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "air_quality_source_pipeline": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-postgres<1.9",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)