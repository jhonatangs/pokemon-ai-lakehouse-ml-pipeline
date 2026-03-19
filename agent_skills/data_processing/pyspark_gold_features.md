# Skill: PySpark Gold Layer Feature Store

## 1. Objective
Read cleaned Pokemon data from the Silver layer, select the relevant features for machine learning, and save the final dataset to the Gold layer. The goal is to prepare a dataset to predict the Pokemon's primary type based on its base stats and physical characteristics.

## 2. Spark Session Configuration
Same as Silver layer, with critical addition for stability:
* `spark.sql.parquet.enableVectorizedReader`: `false`
* `spark.hadoop.fs.s3a.vectored.read.enabled`: `false`
* `spark.hadoop.parquet.hadoop.vectored.read.enabled`: `false`
  > [!NOTE]
  > These are required to avoid `NoSuchMethodError: VectoredReadUtils` with certain Hadoop/AWS SDK combinations.

## 3. Data Transformations
* Read data from: `s3a://pokemon-lake/silver_pokemon/`
* Feature Selection: Select continuous numerical features: `hp`, `attack`, `defense`, `special_attack`, `special_defense`, `speed`, `height`, `weight`.
* Target Selection: Select the `primary_type` column as the target label.
* Data Quality:
  * Cast all feature columns to `DoubleType`.
  * Cast the target to `StringType`.
  * Handle Nulls: Drop any rows with missing values (`dropna()`) in the selected columns.

## 4. Destination
* Write the finalized feature dataset to: `s3a://pokemon-lake/gold_pokemon_features/`
* Format: parquet
* Mode: overwrite