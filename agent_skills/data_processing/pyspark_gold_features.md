# Skill: PySpark Gold Layer Feature Store

## 1. Objective
Read cleaned Pokemon data from the Silver layer, select the relevant features for machine learning, and save the final dataset to the Gold layer. The goal is to prepare a dataset to predict the Pokemon's primary type based on its base stats.

## 2. Data Transformations
* Read data from: s3a://pokemon-lake/silver_pokemon/
* Feature Selection: Select continuous numerical features (e.g., hp, attack, defense, special_attack, special_defense, speed).
* Target Selection: Select the 'primary_type' column as the target label.
* Handle Nulls: Drop any rows with missing values in the selected features or the target column.
* Data Quality: Ensure all feature columns are cast to numeric types (Integer or Double) and the target is a String.

## 3. Destination
* Write the finalized feature dataset to: s3a://pokemon-lake/gold_pokemon_features/
* Format: parquet
* Mode: overwrite