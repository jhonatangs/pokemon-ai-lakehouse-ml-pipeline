import dlt
import requests
from typing import Generator, Any
from dotenv import load_dotenv

load_dotenv()

@dlt.resource(write_disposition="replace")
def pokemon_resource() -> Generator[Any, None, None]:
    """
    Extracts Pokemon data from PokéAPI with pagination.
    
    Yields:
        Generator[Any, None, None]: A generator yielding pages of Pokemon data.
    """
    url: str = "https://pokeapi.co/api/v2/pokemon"
    
    while url:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Yield the list of pokemon results
        yield data["results"]
        
        # PokéAPI returns the next page URL in the 'next' key
        url = data.get("next")

def run_pipeline() -> None:
    """
    Initializes and runs the dlt pipeline for the Bronze layer.
    """
    # Initialize the dlt pipeline as specified in the skill file
    pipeline = dlt.pipeline(
        pipeline_name='pokeapi_pipeline',
        destination='filesystem',
        dataset_name='bronze_pokemon',
    )
    
    # Run the extraction and load to MinIO
    load_info = pipeline.run(pokemon_resource())
    
    # Print the load info for verification
    print(load_info)

if __name__ == "__main__":
    run_pipeline()
