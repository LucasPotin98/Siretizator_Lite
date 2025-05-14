from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys
import os
from pathlib import Path

sys.path.append("/opt/airflow/data")
DATA_DIR = Path("/opt/airflow/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

from downloadSirene import download_latest_sirene, parse_sirene, save_sirene_dataset

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

REPO_PATH = Path(__file__).resolve().parent.parent

with DAG(
    dag_id='sirene_monthly_update',
    description='Téléchargement et parsing mensuel de la base SIRENE',
    schedule='@monthly',
    default_args=default_args,
    tags=['sirene', 'open-data']
) as dag:

    download = PythonOperator(
        task_id='download_sirene',
        python_callable=download_latest_sirene
    )

    def _parse_and_save():
        df = parse_sirene()
        save_sirene_dataset(df)

    parse_and_save = PythonOperator(
        task_id='parse_and_save_sirene',
        python_callable=_parse_and_save
    )


    push_to_github = BashOperator(
    task_id='push_to_github',
    bash_command="""
       cd {REPO_PATH} &&
        git add data/processed/sirene.csv.gz &&
        git commit -m "update: sirene.csv.gz for {{ ds }}" &&
            git push origin main
        """
    )
    
    download >> parse_and_save >> push_to_github
