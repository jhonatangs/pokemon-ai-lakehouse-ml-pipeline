import os
from contextlib import asynccontextmanager
from typing import Dict, Any

import joblib
import pandas as pd
import s3fs
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Load environment variables from .env
load_dotenv()

# Configuration from environment variables
AWS_ACCESS_KEY_ID = os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY")
# Default to localhost if running outside docker
AWS_ENDPOINT_URL_S3 = os.getenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000").replace("host.docker.internal", "localhost")
MODEL_PATH = "s3://pokemon-lake/models/pokemon_type_model.pkl"

# Global dictionary to store the loaded model
ml_models: Dict[str, Any] = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for loading the ML model on startup."""
    try:
        # Initialize s3fs
        fs = s3fs.S3FileSystem(
            key=AWS_ACCESS_KEY_ID,
            secret=AWS_SECRET_ACCESS_KEY,
            endpoint_url=AWS_ENDPOINT_URL_S3,
            use_ssl=False  # Typically False for local MinIO
        )
        
        # Load model using joblib and s3fs
        print(f"Loading model from {MODEL_PATH}...")
        with fs.open(MODEL_PATH, "rb") as f:
            ml_models["pokemon_type"] = joblib.load(f)
        print("Model loaded successfully.")
    except Exception as e:
        print(f"Error loading model: {e}")
        # In a real-world scenario, you might want to stop the application
        # if the model cannot be loaded.
        
    yield
    # Clean up
    ml_models.clear()

app = FastAPI(
    title="Pokemon Type Prediction API",
    description="API to predict the primary type of a Pokemon based on its stats.",
    version="1.0.0",
    lifespan=lifespan
)

class PokemonFeatures(BaseModel):
    """Pydantic model for Pokemon features validation."""
    hp: float
    attack: float
    defense: float
    special_attack: float
    special_defense: float
    speed: float
    height: float
    weight: float

@app.get("/")
async def root():
    """Health check endpoint."""
    return {"message": "Pokemon Type Prediction API is running", "status": "healthy"}

@app.post("/predict")
async def predict(features: PokemonFeatures):
    """Endpoint to predict the primary type of a Pokemon."""
    if "pokemon_type" not in ml_models:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        # Convert input features to a DataFrame as expected by the Scikit-Learn pipeline
        input_df = pd.DataFrame([features.dict()])
        
        # Perform prediction
        prediction = ml_models["pokemon_type"].predict(input_df)
        
        # Return the result
        return {
            "prediction": prediction[0],
            "features": features.dict()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
