import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

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
        .appName("PokemonGoldFeatureCreation")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.712")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "30000")
        # Fix NoSuchMethodError: VectoredReadUtils
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.hadoop.fs.s3a.vectored.read.enabled", "false")
        .config("spark.hadoop.parquet.hadoop.vectored.read.enabled", "false")
        .config("spark.driver.extraJavaOptions", java_options)
        .config("spark.executor.extraJavaOptions", java_options)
        .getOrCreate()
    )

def select_features_and_target(df: DataFrame) -> DataFrame:
    """Selects and casts features and target column."""
    features = ["hp", "attack", "defense", "speed", "height", "weight"]
    target = "primary_type"
    
    # Cast features to Double and target to String
    for feature in features:
        df = df.withColumn(feature, F.col(feature).cast(DoubleType()))
    
    df = df.withColumn(target, F.col(target).cast(StringType()))
    
    return df.select(*features, target)

def handle_missing_values(df: DataFrame) -> DataFrame:
    """Drops rows with missing values in the feature or target columns."""
    return df.dropna()

def create_gold_features() -> None:
    """Main function to create the Gold layer features."""
    spark = create_spark_session()
    
    silver_path = "s3a://pokemon-lake/silver_pokemon/"
    gold_path = "s3a://pokemon-lake/gold_pokemon_features/"
    
    # Read Silver data
    print(f"Reading data from Silver layer: {silver_path}")
    df_silver = spark.read.parquet(silver_path)
    
    # Apply transformations
    df_features = select_features_and_target(df_silver)
    df_clean = handle_missing_values(df_features)
    
    # Display final schema and some rows
    print("Final Gold features schema:")
    df_clean.printSchema()
    
    # Write to Gold layer
    print(f"Writing Gold features to: {gold_path}")
    df_clean.write \
        .mode("overwrite") \
        .parquet(gold_path)
    
    spark.stop()

if __name__ == "__main__":
    create_gold_features()
