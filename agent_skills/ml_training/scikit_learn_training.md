# Skill: Scikit-Learn Model Training

## 1. Objective
Read the feature dataset from the Gold layer in MinIO, train a Machine Learning model to predict the Pokemon's 'primary_type' based on its numerical stats, evaluate its performance, and serialize the model for production.

## 2. Data Loading
* Load data from: s3://pokemon-lake/gold_pokemon_features/
* Use Pandas with `s3fs` to read the Parquet files.
* Ensure Pandas uses the following environment variables to connect to local MinIO:
  * AWS_ACCESS_KEY_ID (often mapped from DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID)
  * AWS_SECRET_ACCESS_KEY (often mapped from DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY)
  * AWS_ENDPOINT_URL_S3

## 3. Preprocessing and Modeling
* Split the data into features (X) and target (y).
* Split into training and testing sets (70/30 split, random_state=42).
  * Use `stratify=y` to maintain class proportions.
* Build a `scikit-learn` Pipeline with:
  1. `PolynomialFeatures` (degree=2) to capture stat interactions.
  2. `StandardScaler` to normalize the features.
  3. `HistGradientBoostingClassifier` (random_state=42, class_weight='balanced') as the estimator.
* Use `GridSearchCV` for hyperparameter tuning (e.g., `learning_rate`, `max_iter`, `max_leaf_nodes`).
* Fit the grid search on the training data and retrieve the `best_estimator_`.

## 4. Evaluation and Serialization
* Predict on the test set using the best pipeline.
* Print the `classification_report` and overall accuracy to the console.
* Create a local directory named `models/` at the root of the project if it doesn't exist.
* Use `joblib` to save the trained pipeline (features + scaler + model) to `models/pokemon_type_model.pkl`.