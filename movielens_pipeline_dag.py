from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

DAG_ID = 'movielens_etl_pipeline'

def _download(**context):
    import scripts.download_datasets as dl
    out_paths = dl.download_movielens_100k()
    return out_paths

def _transform_ratings(**context):
    import scripts.transform_and_merge as trans
    ti = context['ti']
    paths = ti.xcom_pull(task_ids='download_task')
    ratings_path = paths['ratings']
    out = trans.transform_ratings(ratings_path)
    return out

def _transform_movies(**context):
    import scripts.transform_and_merge as trans
    ti = context['ti']
    paths = ti.xcom_pull(task_ids='download_task')
    movies_path = paths['movies']
    out = trans.transform_movies(movies_path)
    return out

def _merge_and_load(**context):
    import scripts.load_to_postgres as loader
    ti = context['ti']
    ratings_out = ti.xcom_pull(task_ids='transform_ratings_task')
    movies_out = ti.xcom_pull(task_ids='transform_movies_task')
    merged = loader.merge_and_load(ratings_out, movies_out)
    return merged

def _analysis(**context):
    import scripts.analysis_read_and_plot as analysis
    analysis.run_analysis()

with DAG(DAG_ID,
         default_args=DEFAULT_ARGS,
         description='MovieLens ETL pipeline example',
         schedule_interval='@daily',
         start_date=datetime(2025, 11, 4),
         catchup=False,
         max_active_runs=1) as dag:

    download_task = PythonOperator(
        task_id='download_task',
        python_callable=_download,
        provide_context=True
    )

    with TaskGroup('transform_group') as transform_group:
        transform_ratings = PythonOperator(
            task_id='transform_ratings_task',
            python_callable=_transform_ratings,
            provide_context=True
        )

        transform_movies = PythonOperator(
            task_id='transform_movies_task',
            python_callable=_transform_movies,
            provide_context=True
        )

    merge_load_task = PythonOperator(
        task_id='merge_and_load_task',
        python_callable=_merge_and_load,
        provide_context=True
    )

    analysis_task = PythonOperator(
        task_id='analysis_task',
        python_callable=_analysis,
        provide_context=True
    )

    download_task >> transform_group >> merge_load_task >> analysis_task
