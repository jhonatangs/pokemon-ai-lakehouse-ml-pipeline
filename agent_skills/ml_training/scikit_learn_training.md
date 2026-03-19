# Skill: Scikit-Learn Model Training

## 1. Objective
Read the feature dataset from the Gold layer in MinIO, train a Machine Learning model to predict the Pokemon's 'primary_type' based on its numerical stats, evaluate its performance, and serialize the model for production.

## 2. Data Loading
* Load data from: s3://pokemon-lake/gold_pokemon_features/
* Use Pandas with `s3fs` to read the Parquet files.
* Ensure Pandas uses the following environment variables to connect to local MinIO:
  * AWS_ACCESS_KEY_ID
  * AWS_SECRET_ACCESS_KEY
  * AWS_ENDPOINT_URL_S3

## 3. Preprocessing and Modeling
* Split the data into features (X) and target (y).
* Split into training and testing sets (70/30 split, random_state=42).
* Build a `scikit-learn` Pipeline with:
  1. `StandardScaler` to normalize the numerical stats.
  2. `RandomForestClassifier` (n_estimators=100, random_state=42) as the estimator.
* Fit the pipeline on the training data.

## 4. Evaluation and Serialization
* Predict on the test set.
* Print the `classification_report` and overall accuracy to the console.
* Create a local directory named `models/` at the root of the project if it doesn't exist.
* Use `joblib` to save the trained pipeline (scaler + model) to `models/pokemon_type_model.pkl`.