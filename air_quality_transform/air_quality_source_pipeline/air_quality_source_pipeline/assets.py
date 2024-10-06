from dagster import AssetExecutionContext
from .project import air_quality_transform_project

import os, warnings, sys, argparse, logging

import pandas as pd
from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets, get_asset_key_for_model

import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet
import mlflow
import mlflow.sklearn
from sklearn import tree







@asset(compute_kind="python")
def city_day_source(context: AssetExecutionContext) -> None:

    print("Extracting the dat, cleaning and storing it in source")

    database='RAW'
    username='postgres'
    password='postgres'
    host='localhost'
    port=5432

    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

    df=pd.read_csv("C:/Users/adity/Desktop/Air_quality/city_day.csv")
    
    
    df_cleaned = df.dropna().reset_index()
    df_cleaned['year']=df_cleaned['Date'].apply(lambda x: x.split("-")[0])
    df_cleaned['year'] = df_cleaned['year'].astype(int)
    
    try:

        df_cleaned.to_sql('city_day_aqi', engine, if_exists='replace', index=True)
    except:
        pass

    engine.dispose()

    # Log some metadata about the table we just wrote. It will show up in the UI.
    context.add_output_metadata({"num_rows": "City Data Mart registered"})




@dbt_assets(manifest=air_quality_transform_project.manifest_path)
def air_quality_transform_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    




@asset(
    compute_kind="python",
    deps=get_asset_key_for_model([air_quality_transform_dbt_assets], "Delhi_2015"),
)
def Delhi_MLModel(context: AssetExecutionContext):

    warnings.filterwarnings("ignore")
    np.random.seed(40)

    db_username = 'postgres'
    db_password = 'postgres'
    db_host = 'localhost'  # e.g., 'localhost' or '127.0.0.1'
    db_port = '5432'  # Default PostgreSQL port
    db_name = 'RAW'

    connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)

    schema_name='transformed'
    table_name='Delhi_2015'
    query = f'SET search_path TO {schema_name}; SELECT * FROM {table_name}'

    df = pd.read_sql_table(table_name, engine, schema=schema_name)
    df=df[['PM2.5', 'PM10', 'O3', 'Benzene', 'Toluene', 'AQI', 'AQI_Bucket']]

    X=df[['PM2.5', 'PM10', 'O3', 'Benzene', 'Toluene', 'AQI']]
    Y=df[['AQI_Bucket']]

    exp = mlflow.set_experiment(experiment_name="AQI_Delhi")

    with mlflow.start_run():
        clf = tree.DecisionTreeClassifier()
        clf = clf.fit(X, Y)

        result=clf.predict([[100, 200, 100, 100, 140, 170]])[0]
        mlflow.log_param("Test result", result)
        mlflow.sklearn.log_model(clf, "mlflow_Delhi_model")


    

@asset(
    compute_kind="python",
    deps=get_asset_key_for_model([air_quality_transform_dbt_assets],  "Vishakapatnam_2016"),
)
def Vishakapatnam_MLModel(context: AssetExecutionContext):

    warnings.filterwarnings("ignore")
    np.random.seed(40)

    db_username = 'postgres'
    db_password = 'postgres'
    db_host = 'localhost'  # e.g., 'localhost' or '127.0.0.1'
    db_port = '5432'  # Default PostgreSQL port
    db_name = 'RAW'

    connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)

    schema_name='transformed'
    table_name='Vishakapatnam_2016'
    query = f'SET search_path TO {schema_name}; SELECT * FROM {table_name}'

    df = pd.read_sql_table(table_name, engine, schema=schema_name)
    df=df[['PM2.5', 'PM10', 'O3', 'Benzene', 'Toluene', 'AQI', 'AQI_Bucket']]

    X=df[['PM2.5', 'PM10', 'O3', 'Benzene', 'Toluene', 'AQI']]
    Y=df[['AQI_Bucket']]

    exp = mlflow.set_experiment(experiment_name="AQI_Vishakapatnam")

    with mlflow.start_run():
        clf = tree.DecisionTreeClassifier()
        clf = clf.fit(X, Y)

        result=clf.predict([[100, 200, 600, 100, 14, 170]])[0]
        mlflow.log_param("Test result", result)
        mlflow.sklearn.log_model(clf, "mlflow_Vishakaptnam_model")


   
