from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import os

DEFAULT_ARGS = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

DAG_ID = 'openflights_etl_pipeline'

def _download(**context):
    """Download OpenFlights dataset and return file paths via XCom"""
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import download_datasets as dl
    out_paths = dl.download_openflights()
    print(f"Downloaded datasets: {out_paths}")
    return out_paths

def _transform_routes(**context):
    """Transform routes data - runs in parallel with airports transform"""
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import transform_and_merge as trans
    ti = context['ti']
    paths = ti.xcom_pull(task_ids='download_task')
    routes_path = paths['routes']
    out = trans.transform_routes(routes_path)
    print(f"Transformed routes saved to: {out}")
    return out

def _transform_airports(**context):
    """Transform airports data - runs in parallel with routes transform"""
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import transform_and_merge as trans
    ti = context['ti']
    paths = ti.xcom_pull(task_ids='download_task')
    airports_path = paths['airports']
    out = trans.transform_airports(airports_path)
    print(f"Transformed airports saved to: {out}")
    return out

def _merge_and_load(**context):
    """Merge transformed datasets and load to PostgreSQL"""
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import load_to_postgres as loader
    ti = context['ti']
    # Pull from TaskGroup tasks
    routes_out = ti.xcom_pull(task_ids='transform_group.transform_routes_task')
    airports_out = ti.xcom_pull(task_ids='transform_group.transform_airports_task')
    print(f"Merging: {routes_out} and {airports_out}")
    merged = loader.merge_and_load(routes_out, airports_out)
    return merged

def _analysis(**context):
    """Read from database and perform analysis"""
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import analysis_read_and_plot as analysis
    analysis.run_analysis()
    print("Analysis completed and plot saved")

def _cleanup(**context):
    """Clean up intermediate processed files"""
    processed_dir = '/opt/airflow/data/processed'
    if os.path.exists(processed_dir):
        print(f"Cleaning up intermediate files in {processed_dir}")
        for file in os.listdir(processed_dir):
            file_path = os.path.join(processed_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Removed: {file_path}")
        print("Cleanup completed")
    else:
        print(f"No cleanup needed - {processed_dir} does not exist")

with DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description='OpenFlights ETL pipeline with parallel processing',
    schedule_interval='@daily',  # Runs daily
    start_date=datetime(2025, 11, 4),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'openflights', 'postgres', 'airports']
) as dag:

    # Task 1: Download datasets
    download_task = PythonOperator(
        task_id='download_task',
        python_callable=_download,
        provide_context=True
    )

    # Task 2: Parallel transformation using TaskGroup
    with TaskGroup('transform_group', tooltip='Transform tasks in parallel') as transform_group:
        transform_routes = PythonOperator(
            task_id='transform_routes_task',
            python_callable=_transform_routes,
            provide_context=True
        )

        transform_airports = PythonOperator(
            task_id='transform_airports_task',
            python_callable=_transform_airports,
            provide_context=True
        )

    # Task 3: Merge and load to PostgreSQL
    merge_load_task = PythonOperator(
        task_id='merge_and_load_task',
        python_callable=_merge_and_load,
        provide_context=True
    )

    # Task 4: Analysis
    analysis_task = PythonOperator(
        task_id='analysis_task',
        python_callable=_analysis,
        provide_context=True
    )

    # Task 5: Cleanup intermediate files
    cleanup_task = PythonOperator(
        task_id='cleanup_task',
        python_callable=_cleanup,
        provide_context=True
    )

    # Define task dependencies
    # Download -> Transform (parallel) -> Merge/Load -> Analysis -> Cleanup
    download_task >> transform_group >> merge_load_task >> analysis_task >> cleanup_task