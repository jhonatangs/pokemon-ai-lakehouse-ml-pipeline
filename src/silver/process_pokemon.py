import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

def create_spark_session() -> SparkSession:
    """Creates and configures a SparkSession for MinIO access."""
    load_dotenv()
    
    access_key = os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY")
    endpoint = os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL", "http://localhost:9000")

    java_options = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )

    return (
        SparkSession.builder
        .appName("PokemonSilverProcessing")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.712")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "30000")
        .config("spark.driver.extraJavaOptions", java_options)
        .config("spark.executor.extraJavaOptions", java_options)
        .getOrCreate()
    )

def extract_pokemon_id(df: DataFrame) -> DataFrame:
    """Uses the numeric id already present in the detail data."""
    # Ensure id is int and keep it if it exists
    if "id" in df.columns:
        return df.withColumn("id", F.col("id").cast("int"))
    return df

def extract_stats(df: DataFrame) -> DataFrame:
    """
    Unpacks base stats (hp, attack, defense, speed) from the nested stats array.
    """
    return df.withColumn("hp", F.expr("filter(stats, x -> x.stat.name == 'hp')[0].base_stat")) \
             .withColumn("attack", F.expr("filter(stats, x -> x.stat.name == 'attack')[0].base_stat")) \
             .withColumn("defense", F.expr("filter(stats, x -> x.stat.name == 'defense')[0].base_stat")) \
             .withColumn("speed", F.expr("filter(stats, x -> x.stat.name == 'speed')[0].base_stat"))

def extract_types(df: DataFrame) -> DataFrame:
    """Extracts primary and secondary types safely from the nested types array."""
    return df.withColumn("primary_type", F.col("types")[0]["type"]["name"]) \
             .withColumn("secondary_type", F.expr("try_element_at(types, 2).type.name"))

def process_silver() -> None:
    """Main function to perform Silver layer transformation."""
    spark = create_spark_session()
    
    bronze_path = "s3a://pokemon-lake/bronze_pokemon/pokemon_resource/*.jsonl.gz"
    silver_path = "s3a://pokemon-lake/silver_pokemon/"
    
    # Read raw data
    print(f"Reading data from: {bronze_path}")
    df_raw = spark.read.json(bronze_path)
    
    print("Detected schema:")
    df_raw.printSchema()
    
    # Apply transformations
    df_id = extract_pokemon_id(df_raw)
    df_stats = extract_stats(df_id)
    df_transformed = extract_types(df_stats)
    
    # Select final columns
    df_final = df_transformed.select(
        F.col("id"),
        F.col("name"),
        F.col("hp"),
        F.col("attack"),
        F.col("defense"),
        F.col("speed"),
        F.col("primary_type"),
        F.col("secondary_type"),
        F.col("height"),
        F.col("weight")
    )
    
    # Write to Silver layer
    df_final.write \
        .mode("overwrite") \
        .parquet(silver_path)
    
    spark.stop()

if __name__ == "__main__":
    process_silver()
