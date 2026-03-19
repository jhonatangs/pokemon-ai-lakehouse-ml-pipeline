from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Senior Data Engineer: Orchestrating the Pokemon Lakehouse Pipeline
# Following specifications from agent_skills/airflow_orchestration/airflow_orchestration.md

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pokemon_lakehouse_pipeline',
    default_args=default_args,
    description='End-to-end Pokemon Data Lakehouse Pipeline',
    schedule='@daily',
    catchup=False,
    tags=['pokemon', 'lakehouse'],
) as dag:

    run_bronze = BashOperator(
        task_id='run_bronze',
        bash_command='python src/bronze/ingest_pokemon.py',
    )

    run_silver = BashOperator(
        task_id='run_silver',
        bash_command='python src/silver/process_pokemon.py',
    )

    run_gold = BashOperator(
        task_id='run_gold',
        bash_command='python src/gold/create_features.py',
    )

    train_ml = BashOperator(
        task_id='train_ml',
        bash_command='python src/ml/train_model.py',
    )

    # Execution Flow: Bronze -> Silver -> Gold -> ML
    run_bronze >> run_silver >> run_gold >> train_ml
