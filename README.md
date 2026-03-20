# ⚡ Pokémon AI Lakehouse & ML Pipeline ⚡

[![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Spark](https://img.shields.io/badge/Processing-PySpark-E25A1C?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Scikit-Learn](https://img.shields.io/badge/ML-Scikit--Learn-F7931E?logo=scikit-learn&logoColor=white)](https://scikit-learn.org/)
[![FastAPI](https://img.shields.io/badge/Serving-FastAPI-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)

An end-to-end Data Engineering and Machine Learning project that extracts Pokémon data from the PokéAPI, processes it via a **Medallion Architecture**, and trains a state-of-the-art classifier to predict a Pokémon's primary type based on its combat stats and physical characteristics.

Built with an **AI-Assisted Development** workflow using **Google Antigravity**.

---

## 🏗️ Architecture & Tech Stack

This project follows a professional data lakehouse pattern:

```mermaid
graph TD
%% Definindo Estilos para visualização
classDef storage fill:#f9f,stroke:#333,stroke-width:2px,rx:10,ry:10;
classDef compute fill:#dfd,stroke:#333,stroke-width:2px;
classDef orchestrator fill:#dff,stroke:#333,stroke-width:1px,stroke-dasharray: 5 5;
classDef external fill:#ddd,stroke:#333,stroke-width:1px;
classDef serving fill:#fdd,stroke:#333,stroke-width:2px,rx:10,ry:10;

%% Nós Principais
Source(PokéAPI):::external
EndUser(End User):::external

subgraph "Orchestration"
    Airflow(Apache Airflow DAG):::orchestrator
end

subgraph "Compute: Python src/"
    Ingest[dlt Ingestion:<br/>src/bronze]:::compute
    Process[PySpark Normalize:<br/>src/silver]:::compute
    FeatureEng[PySpark Features:<br/>src/gold]:::compute
    Train[scikit-learn Train:<br/>src/ml]:::compute
end

subgraph "Storage: MinIO Data Lakehouse"
    Bronze((s3://pokemon-lake/bronze)):::storage
    Silver((s3://pokemon-lake/silver)):::storage
    Gold((s3://pokemon-lake/gold)):::storage
    Models((s3://pokemon-lake/models)):::storage
end

subgraph "Model Serving API"
    API[FastAPI Server:<br/>src/api]:::serving
end

%% Fluxo de Dados e Dependências
Source --> Ingest
Ingest --> Bronze
Bronze --> Process
Process --> Silver
Silver --> FeatureEng
FeatureEng --> Gold
Gold --> Train
Train --> Models

%% Dependências de Orquestração (linhas tracejadas)
Airflow -.-> Ingest
Airflow -.-> Process
Airflow -.-> FeatureEng
Airflow -.-> Train

%% Fluxo da API
API -- "1. Loads Model" --> Models
EndUser -- "2. Requests Prediction" --> API
API -- "3. Returns Type" --> EndUser
```

1.  **Ingestion (Bronze 🥉):** [**dlt (Data Load Tool)**](https://dlthub.com/) - Extracts full nested JSON details from PokéAPI and loads them as `JSONL` into MinIO.
2.  **Processing (Silver 🥈):** **PySpark** - Normalizes nested structures, extracts stats (`hp`, `attack`, etc.) and types using safe extraction logic.
3.  **Feature Store (Gold 🥇):** **PySpark** - Engineers high-quality features, including `height`, `weight`, and combat stats, ready for modeling.
4.  **Machine Learning:** **scikit-learn** -
    * **Preprocessing:** `PolynomialFeatures` to capture stat interactions.
    * **Model:** `HistGradientBoostingClassifier` (tuned via `GridSearchCV`).
    * **Evaluation:** Detailed metrics via `classification_report`.
5.  **Orchestration:** **Apache Airflow** (via Astro CLI) - Manages the dependency graph and schedules the daily pipeline.
6.  **Serving:** **FastAPI** - Exposes a REST API to serve predictions in real-time.
7.  **Storage:** **MinIO** - S3-compatible object storage running in Docker.

## 📂 Project Structure

    ├── agent_skills/     # 🧠 Context files for AI-Assisted development
    ├── dags/             # ✈️ Airflow DAGs (Pipeline orchestration)
    ├── infra/            # 🐳 Docker Compose for MinIO & Infrastructure
    ├── models/           # 🤖 Serialized ML models (.pkl)
    ├── src/              # 🏗️ Core Python scripts per layer
    │   ├── api/          # FastAPI serving script
    │   ├── bronze/       # Ingestion logic (dlt)
    │   ├── silver/       # PySpark normalization
    │   ├── gold/         # PySpark feature engineering
    │   └── ml/           # Model training and evaluation
    └── requirements.txt  # Project dependencies

## 🚀 How to Run Locally

### 1. Prerequisites
- **Python 3.10+**
- **Docker & Docker Compose**
- **Astro CLI**

### 2. Infrastructure Setup
Start the local MinIO instance:

    cd infra && docker-compose up -d

### 3. Environment Configuration
Create a `.env` file in the root directory (refer to `agent_skills/dlt_ingestion/.env.example` for required keys).

### 4. Setup Environment

    python -m venv venv && source venv/bin/activate
    pip install -r requirements.txt

### 5. Orchestration (Airflow)
Start the Astro environment:

    astro dev start

Go to `http://localhost:8080` (admin/admin) and trigger the `pokemon_lakehouse_pipeline` DAG.

### 6. Model Serving (FastAPI)
With the model trained and securely stored in our Data Lakehouse (MinIO), you can spin up the FastAPI server to make real-time predictions.

Start the local server:

    uvicorn src.api.main:app --reload

Open your browser and navigate to the interactive Swagger UI:
`http://localhost:8000/docs`

You can test the `POST /predict` endpoint by providing the combat stats of a Pokémon. Here is an example payload (representing Pikachu):

    {
      "hp": 35,
      "attack": 55,
      "defense": 40,
      "special_attack": 50,
      "special_defense": 50,
      "speed": 90,
      "height": 4,
      "weight": 60
    }

---
