# Skill: dlt PokéAPI Ingestion to Local MinIO (Bronze Layer)

## 1. Objective
Extract Pokemon data (name, URL, and basic details) from the PokéAPI and load it into the Bronze layer using the dlt library. The destination is a local MinIO bucket functioning as an S3 filesystem.

## 2. API Pagination (PokéAPI)
* Endpoint: [https://pokeapi.co/api/v2/pokemon](https://pokeapi.co/api/v2/pokemon)
* Pagination Logic: The API returns a JSON with a results array and a next key containing the URL for the next page.
* Extraction: Use a while loop to yield items from the results array and update the URL to the next value until next is null.
* Decorator: Use @dlt.resource(write_disposition="replace") for the extraction function.

## 3. dlt Destination Configuration (MinIO)
To make dlt write to our local MinIO, the pipeline must be configured to use the filesystem destination, overriding standard AWS S3 endpoints.

Pipeline Setup:
Initialize the dlt pipeline setting pipeline_name to 'pokeapi_pipeline', destination to 'filesystem', and dataset_name to 'bronze_pokemon'.

Environment Variables Requirements:
The code must assume these variables are set in the .env file. The agent should only generate a `.env.example` file with these keys (leave the values empty):
* DESTINATION__FILESYSTEM__BUCKET_URL
* DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID
* DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY
* DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL

## 4. Execution
The script should initialize the pipeline, run the extraction, and print the load info.