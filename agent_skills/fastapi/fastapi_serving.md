# Skill: FastAPI Model Serving

## 1. Objective
Create a REST API using FastAPI to serve the trained Scikit-Learn model. The API must load the model directly from the MinIO bucket into memory on startup and expose a prediction endpoint.

## 2. Model Loading (Lifespan)
* Use FastAPI's lifespan context manager to load the model on application startup.
* Connect to MinIO using s3fs and the environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL_S3).
* Read the model from pokemon-lake/models/pokemon_type_model.pkl using joblib.load and store it in a global variable or dictionary accessible to the routes.

## 3. Input Schema (Pydantic)
* Create a Pydantic BaseModel named PokemonFeatures to validate incoming requests.
* The model must include all the features used in the Gold layer/Training: hp, attack, defense, special_attack, special_defense, speed, height, and weight. All should be float or int.

## 4. Prediction Endpoint
* Create a POST endpoint at /predict.
* It should accept the PokemonFeatures schema.
* Convert the input into a Pandas DataFrame (as Scikit-Learn pipelines with feature names expect DataFrames).
* Call the predict() method on the loaded model.
* Return a JSON response with the predicted primary_type.

## 5. Output
Generate the complete Python script to be saved at src/api/main.py.