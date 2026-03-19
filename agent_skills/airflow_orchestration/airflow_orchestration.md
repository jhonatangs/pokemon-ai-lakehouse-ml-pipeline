# Skill: Airflow Orchestration (Astro CLI)

## 1. Objective
Create an Apache Airflow DAG to orchestrate the end-to-end data pipeline: Bronze Ingestion -> Silver Processing -> Gold Feature Store -> ML Model Training.

## 2. DAG Configuration
* **DAG ID:** `pokemon_lakehouse_pipeline`
* **Schedule:** `@daily` (use the `schedule` parameter, NOT `schedule_interval` for Airflow 2.4+)
* **Catchup:** `False`
* **Default Arguments:** Set retries to 1 and retry_delay to 5 minutes.
* **Start Date:** Use a fixed date (e.g., `datetime(2024, 1, 1)`).

## 3. Tasks Definition
Since our pipeline consists of modularized Python scripts in the `src/` directory, use the `BashOperator` to execute them sequentially. Ensure the commands run from the root of the project.

Define the following tasks:
1. `run_bronze`: Executes `python src/bronze/ingest_pokemon.py`
2. `run_silver`: Executes `python src/silver/process_pokemon.py`
3. `run_gold`: Executes `python src/gold/create_features.py`
4. `train_ml`: Executes `python src/ml/train_model.py`

## 4. Dependencies
Set the exact execution order:
`run_bronze >> run_silver >> run_gold >> train_ml`

## 5. Output
Generate ONLY the Python code for the DAG, to be saved at `dags/pokemon_pipeline.py`.