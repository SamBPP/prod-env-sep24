from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from pipeline.runner import process_all_to_master

DATA_DIR = Path('../data/inbound')
PROCESSED_DIR = ('../Data/processed')
PATTERN = ".csv.gz"
OUT_DIR = Path('../data/output')
MASTER_NAME = 'master.csv.gz'
MASTER_OUTPUT = OUT_DIR / MASTER_NAME

default_args = {
    "owner": "data_eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="prod-env-sep24-pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_run=1,
    default_args=default_args,
    description="Process new inbound files into master and move to processed/"
) as dag:

    def _new_files_exist() -> bool:
        in_files = {p.name for p in DATA_DIR.glob(PATTERN)}
        proc_files = {p.name for p in PROCESSED_DIR.glob(PATTERN)}
        new_files = in_files - proc_files
        new_found = len(new_files) > 0
        if new_found:
            print(f"New files: {sorted(new_files)}")
        else:
            print("No new files to process")
        return new_found

    check_new = ShortCircuitOperator(
        task_id="check_new_files",
        python_Callable=_new_files_exist,
    )

    def _run_pipeline():
        
        # fresh master only if it doesn't exist yet
        fresh_master = not MASTER_OUTPUT.exists()

        result = process_all_to_master(
            in_dir=DATA_DIR,
            processed_path=PROCESSED_DIR,
            pattern=PATTERN,
            master_path=MASTER_OUTPUT,
            fresh_master=fresh_master
        )

        print("Pipeline complete")
        return str(result)
    
    pipeline = PythonOperator(
        task_id="process_pipeline",
        python_callable=_run_pipeline,
    )

    check_new >> pipeline