from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,
                               DataQualityOperator,
                               PostgresOperator)

default_args = {
    'owner': 'Ivan Diaz',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2016, 1, 1)
}

with DAG('udac_capstone_dag',
         default_args=default_args,
         catchup=False,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='@once'
         ) as dag:
    start_operator = DummyOperator(task_id='Begin_execution')

    # create_tables = PostgresOperator(
    #     task_id='create_tables',
    #     postgres_conn_id='redshift',
    #     sql='create_tables.sql'
    # )

    load_immigration_table = StageToRedshiftOperator(
        task_id='Load_immi_fact_table',
        redshift_conn_id='redshift',
        table='fact_us_immigration',
        aws_conn_id='aws_credentials',
        s3_bucket='capstone-v01',
        s3_key='sas_clnd_april/immigration.parquet',
        schema='public',
        options=["FORMAT AS PARQUET"]
    )

    load_origin_dimension_table = StageToRedshiftOperator(
        task_id='Load_orig_dim_table',
        redshift_conn_id='redshift',
        table='dim_orig_port',
        aws_conn_id='aws_credentials',
        s3_bucket='capstone-v01',
        s3_key='source/origin.parquet',
        schema='public',
        options=["FORMAT AS PARQUET"]
    )

    load_arrival_dimension_table = StageToRedshiftOperator(
        task_id='Load_arrival_dim_table',
        redshift_conn_id='redshift',
        table='dim_arrival_mode',
        aws_conn_id='aws_credentials',
        s3_bucket='capstone-v01',
        s3_key='source/arrival.parquet',
        schema='public',
        options=["FORMAT AS PARQUET"]
    )

    load_visa_dimension_table = StageToRedshiftOperator(
        task_id='Load_visa_dim_table',
        redshift_conn_id='redshift',
        table='dim_visa',
        aws_conn_id='aws_credentials',
        s3_bucket='capstone-v01',
        s3_key='source/visa.parquet',
        schema='public',
        options=["FORMAT AS PARQUET"]
    )

    load_countries_dimension_table = StageToRedshiftOperator(
        task_id='Load_countries_dim_table',
        redshift_conn_id='redshift',
        table='dim_countries',
        aws_conn_id='aws_credentials',
        s3_bucket='capstone-v01',
        s3_key='source/countries.parquet',
        schema='public',
        options=["FORMAT AS PARQUET"]
    )

    load_states_dimension_table = StageToRedshiftOperator(
        task_id='Load_states_dim_table',
        redshift_conn_id='redshift',
        table='dim_us_states',
        aws_conn_id='aws_credentials',
        s3_bucket='capstone-v01',
        s3_key='source/states.parquet',
        schema='public',
        options=["FORMAT AS PARQUET"]
    )

    load_demographics_dimension_table = StageToRedshiftOperator(
        task_id='Load_demo_dim_table',
        redshift_conn_id='redshift',
        table='dim_us_demographics',
        aws_conn_id='aws_credentials',
        s3_bucket='capstone-v01',
        s3_key='source/demo.parquet',
        schema='public',
        options=["FORMAT AS PARQUET"]
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['dim_us_demographics',
                'fact_us_immigration',
                'dim_countries',
                'dim_us_states',
                'dim_arrival_mode',
                'dim_visa',
                'dim_orig_port']
    )

    end_operator = DummyOperator(task_id='Stop_execution')

start_operator >> load_immigration_table
load_immigration_table >> [load_origin_dimension_table, load_arrival_dimension_table, load_visa_dimension_table,
                           load_countries_dimension_table, load_states_dimension_table,
                           load_demographics_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator

# create_tables >>