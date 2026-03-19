import os
import joblib
import pandas as pd
import s3fs
from typing import Tuple, Dict, Any
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, accuracy_score

def load_data(s3_path: str) -> pd.DataFrame:
    """Reads Parquet data from S3 (MinIO) using Pandas and s3fs.
    
    Args:
        s3_path: The S3 URI to the Parquet dataset.
        
    Returns:
        pd.DataFrame: The loaded dataset.
    """
    load_dotenv()
    
    # Configure environment for s3fs
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID", "admin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY", "password123")
    os.environ["AWS_ENDPOINT_URL_S3"] = os.getenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
    
    # storage_options for s3fs (used by pandas.read_parquet)
    storage_options = {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {"endpoint_url": os.environ["AWS_ENDPOINT_URL_S3"]}
    }
    
    print(f"Reading data from: {s3_path}")
    return pd.read_parquet(s3_path, storage_options=storage_options)

def train_and_evaluate() -> None:
    """Main function to load data, train the model with tuning, and evaluate it."""
    # Data configuration
    gold_path: str = "s3://pokemon-lake/gold_pokemon_features/"
    df: pd.DataFrame = load_data(gold_path)
    
    # Split features and target
    X: pd.DataFrame = df.drop(columns=["primary_type"])
    y: pd.Series = df["primary_type"]
    
    # Train-test split (70/30 split, random_state=42)
    # Using stratify to ensure balanced representation in test set
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )
    
    # Build Scikit-Learn Pipeline with Feature Engineering
    pipeline: Pipeline = Pipeline([
        ("poly", PolynomialFeatures(degree=2, include_bias=False)),
        ("scaler", StandardScaler()),
        ("classifier", HistGradientBoostingClassifier(random_state=42, class_weight='balanced'))
    ])
    
    # Hyperparameter Tuning with GridSearchCV
    param_grid: Dict[str, Any] = {
        "classifier__max_iter": [100, 200],
        "classifier__learning_rate": [0.05, 0.1],
        "classifier__max_leaf_nodes": [31, 63]
    }
    
    print(f"Starting training with features: {list(X.columns)}")
    print("Starting GridSearchCV for HistGradientBoostingClassifier tuning...")
    grid_search = GridSearchCV(
        pipeline, param_grid, cv=5, scoring="accuracy", n_jobs=-1, verbose=1
    )
    
    # Fit the model
    grid_search.fit(X_train, y_train)
    
    best_pipeline = grid_search.best_estimator_
    print(f"\nBest Parameters: {grid_search.best_params_}")
    
    # Evaluate
    print("\nEvaluating the improved model...")
    y_pred = best_pipeline.predict(X_test)
    
    accuracy: float = accuracy_score(y_test, y_pred)
    print(f"\nOverall Accuracy: {accuracy:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, zero_division=0))
    
    # Serialization to MinIO
    print("\nSerializing model to MinIO...")
    
    # Configure s3fs filesystem
    fs = s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"endpoint_url": os.environ["AWS_ENDPOINT_URL_S3"]}
    )
    
    model_output: str = "pokemon-lake/models/pokemon_type_model.pkl"
    with fs.open(model_output, "wb") as f:
        joblib.dump(best_pipeline, f)
        
    print(f"Improved model saved to: s3://{model_output}")

if __name__ == "__main__":
    train_and_evaluate()
