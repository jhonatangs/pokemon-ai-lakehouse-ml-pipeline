# Skill: PySpark Silver Layer Processing

## 1. Objective
Read raw Pokemon data from the Bronze layer in MinIO, clean the data, extract relevant features from nested structures, and save it to the Silver layer using PySpark.

## 2. Spark Session Configuration for MinIO
The SparkSession must be configured to use the S3A filesystem to communicate with our local MinIO instance.

Required configurations:
* `spark.hadoop.fs.s3a.endpoint`: Read from `.env` (DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL)
* `spark.hadoop.fs.s3a.access.key`: Read from `.env` (DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID)
* `spark.hadoop.fs.s3a.secret.key`: Read from `.env` (DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY)
* `spark.hadoop.fs.s3a.path.style.access`: true
* `spark.hadoop.fs.s3a.impl`: org.apache.hadoop.fs.s3a.S3AFileSystem
* `spark.jars.packages`: `org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.712`
* `spark.driver.extraJavaOptions` / `spark.executor.extraJavaOptions`: Include `--add-opens` flags for Java 17+ compatibility.

## 3. Data Transformations
* Read data from: `s3a://pokemon-lake/bronze_pokemon/pokemon_resource/*.jsonl.gz`
* Extract the Pokemon ID: Use the numeric `id` already present in the detail data.
* Extract Stats: Unpack base stats (`hp`, `attack`, `defense`, `special-attack`, `special-defense`, `speed`) into separate columns (`special_attack`, `special_defense`, etc.).
* Extract Types: Extract the `primary_type` and `secondary_type` safely (using `try_element_at` for the latter).
* Select final columns: `id`, `name`, stats, types, `height`, `weight`.

## 4. Destination
* Write the cleaned DataFrame to: s3a://pokemon-lake/silver_pokemon/
* Format: parquet
* Mode: overwrite