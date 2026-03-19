# Skill: dlt PokéAPI Ingestion to Local MinIO (Bronze Layer)

## 1. Objective
Extract detailed Pokemon data from the PokéAPI and load it into the Bronze layer using the `dlt` library. The destination is a local MinIO bucket functioning as an S3 filesystem.

## 2. Extraction Logic (PokéAPI)
* **Pagination:** Use a `while` loop to iterate through the list of Pokemon from [https://pokeapi.co/api/v2/pokemon](https://pokeapi.co/api/v2/pokemon).
* **Detail Fetching:** For each Pokemon in the list, perform a secondary request to its specific `url` to fetch the complete details (stats, types, height, weight, etc.).
* **Decorator:** Use `@dlt.resource(write_disposition="replace", max_table_nesting=0)`.
  * `max_table_nesting=0` is critical to prevent `dlt` from flattening the nested JSON into multiple tables, keeping the data in its original format for the lake.

## 3. dlt Destination Configuration (MinIO)
To make `dlt` write to our local MinIO, use the `filesystem` destination.

Pipeline Setup:
Initialize the `dlt` pipeline setting `pipeline_name` to 'pokeapi_pipeline_v2', `destination` to 'filesystem', and `dataset_name` to 'bronze_pokemon'.

Environment Variables Requirements:
The code must assume these variables are set in the `.env` file:
* `DESTINATION__FILESYSTEM__BUCKET_URL`
* `DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID`
* `DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY`
* `DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL`

## 4. Execution
The script should initialize the pipeline, run the extraction resource, and print the load info.