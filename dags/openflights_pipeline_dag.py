from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import os

DEFAULT_ARGS = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 2,  # Increased retries
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30)  # Add timeout
}

DAG_ID = 'openflights_etl_pipeline'

def _download(**context):
    """Download OpenFlights dataset and return file paths via XCom"""
    print("\n" + "="*80)
    print("TASK: DOWNLOAD")
    print("="*80)
    
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import download_datasets as dl
    
    try:
        out_paths = dl.download_openflights()
        print(f"✅ Downloaded datasets: {out_paths}")
        
        # Verify files exist
        from pathlib import Path
        if not Path(out_paths['airports']).exists():
            raise FileNotFoundError(f"Airports file not created: {out_paths['airports']}")
        if not Path(out_paths['routes']).exists():
            raise FileNotFoundError(f"Routes file not created: {out_paths['routes']}")
        
        return out_paths
        
    except Exception as e:
        print(f"❌ DOWNLOAD FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def _transform_routes(**context):
    """Transform routes data - runs in parallel with airports transform"""
    print("\n" + "="*80)
    print("TASK: TRANSFORM ROUTES")
    print("="*80)
    
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import transform_and_merge as trans
    
    try:
        ti = context['ti']
        paths = ti.xcom_pull(task_ids='download_task')
        
        if not paths or 'routes' not in paths:
            raise ValueError("No routes path received from download task")
        
        routes_path = paths['routes']
        print(f"Input: {routes_path}")
        
        out = trans.transform_routes(routes_path)
        print(f"✅ Transformed routes saved to: {out}")
        
        # Verify output
        from pathlib import Path
        if not Path(out).exists():
            raise FileNotFoundError(f"Output file not created: {out}")
        
        return out
        
    except Exception as e:
        print(f"❌ ROUTES TRANSFORM FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def _transform_airports(**context):
    """Transform airports data - runs in parallel with routes transform"""
    print("\n" + "="*80)
    print("TASK: TRANSFORM AIRPORTS")
    print("="*80)
    
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import transform_and_merge as trans
    
    try:
        ti = context['ti']
        paths = ti.xcom_pull(task_ids='download_task')
        
        if not paths or 'airports' not in paths:
            raise ValueError("No airports path received from download task")
        
        airports_path = paths['airports']
        print(f"Input: {airports_path}")
        
        out = trans.transform_airports(airports_path)
        print(f"✅ Transformed airports saved to: {out}")
        
        # Verify output
        from pathlib import Path
        if not Path(out).exists():
            raise FileNotFoundError(f"Output file not created: {out}")
        
        return out
        
    except Exception as e:
        print(f"❌ AIRPORTS TRANSFORM FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def _merge_and_load(**context):
    """Merge transformed datasets and load to PostgreSQL"""
    print("\n" + "="*80)
    print("TASK: MERGE AND LOAD")
    print("="*80)
    
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import load_to_postgres as loader
    
    try:
        ti = context['ti']
        
        # Pull from TaskGroup tasks
        routes_out = ti.xcom_pull(task_ids='transform_group.transform_routes_task')
        airports_out = ti.xcom_pull(task_ids='transform_group.transform_airports_task')
        
        if not routes_out:
            raise ValueError("No routes output received from transform task")
        if not airports_out:
            raise ValueError("No airports output received from transform task")
        
        print(f"Routes: {routes_out}")
        print(f"Airports: {airports_out}")
        
        # Verify files exist before merging
        from pathlib import Path
        if not Path(routes_out).exists():
            raise FileNotFoundError(f"Routes file not found: {routes_out}")
        if not Path(airports_out).exists():
            raise FileNotFoundError(f"Airports file not found: {airports_out}")
        
        success = loader.merge_and_load(routes_out, airports_out)
        
        if not success:
            raise ValueError("Merge and load returned False")
        
        print(f"✅ Merge and load completed successfully")
        return success
        
    except Exception as e:
        print(f"❌ MERGE AND LOAD FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def _analysis(**context):
    """Read from database and perform analysis"""
    print("\n" + "="*80)
    print("TASK: ANALYSIS")
    print("="*80)
    
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    import analysis_read_and_plot as analysis
    
    try:
        result = analysis.run_analysis()
        print(f"✅ Analysis completed! Plot saved to: {result}")
        return result
        
    except Exception as e:
        print(f"❌ ANALYSIS FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def _cleanup(**context):
    """Clean up intermediate processed files"""
    print("\n" + "="*80)
    print("TASK: CLEANUP")
    print("="*80)
    
    processed_dir = '/opt/airflow/data/processed'
    
    try:
        if os.path.exists(processed_dir):
            print(f"Cleaning up intermediate files in {processed_dir}")
            file_count = 0
            for file in os.listdir(processed_dir):
                file_path = os.path.join(processed_dir, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"  Removed: {file}")
                    file_count += 1
            print(f"✅ Cleanup completed - removed {file_count} files")
        else:
            print(f"No cleanup needed - {processed_dir} does not exist")
            
    except Exception as e:
        # Cleanup failures shouldn't fail the entire DAG
        print(f"⚠️  Cleanup warning: {str(e)}")
        print("Pipeline completed successfully despite cleanup issue")

with DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description='OpenFlights ETL pipeline with robust error handling',
    schedule_interval='@daily',
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
        provide_context=True,
        trigger_rule='all_done'  # Run even if previous tasks failed
    )

    # Define task dependencies
    # Download -> Transform (parallel) -> Merge/Load -> Analysis -> Cleanup
    # Each task will fail if previous task didn't complete successfully
    download_task >> transform_group >> merge_load_task >> analysis_task >> cleanup_task