# Skill: PySpark Silver Layer Processing

## 1. Objective
Read raw Pokemon data from the Bronze layer in MinIO, clean the data, extract relevant features from nested structures, and save it to the Silver layer using PySpark.

## 2. Spark Session Configuration for MinIO
The SparkSession must be configured to use the S3A filesystem to communicate with our local MinIO instance.

Required configurations:
* spark.hadoop.fs.s3a.endpoint: http://localhost:9000
* spark.hadoop.fs.s3a.access.key: Read from .env (AWS_ACCESS_KEY_ID)
* spark.hadoop.fs.s3a.secret.key: Read from .env (AWS_SECRET_ACCESS_KEY)
* spark.hadoop.fs.s3a.path.style.access: true
* spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
* spark.jars.packages: org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

## 3. Data Transformations
* Read data from: s3a://pokemon-lake/bronze_pokemon/
* Extract the Pokemon ID: Parse it from the URL column if it exists.
* Extract Stats: Unpack base stats (hp, attack, defense, special_attack, special_defense, speed) into separate columns.
* Extract Types: Extract the primary_type and secondary_type from the nested types array.
* Drop unnecessary raw metadata columns to keep the DataFrame clean.

## 4. Destination
* Write the cleaned DataFrame to: s3a://pokemon-lake/silver_pokemon/
* Format: parquet
* Mode: overwrite