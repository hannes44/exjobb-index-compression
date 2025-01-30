from opensearchpy import OpenSearch
import Types
import Utils
import DataParser
from pathlib import Path

def test_search(client: OpenSearch, index_name: str):
    # Search for the document we just indexed
    query = {
        'query': {
            'match': {
                'content': 'Claxton '
            }
        }
    }
    response = client.search(
        index = index_name,
        body = query
    )
    print(response)
    
    return ""

# Creating the client and connecting to the OpenSearch server
# If the server is not running, this will throw an exception
# This will connect to port 9200 on localhost
client = Utils.create_client()
print("Client created! Connected to OpenSearch server.")

# When we are running our complete tests, we first clean the server environment
Utils.delete_all_indices(client)



script_dir = Path(__file__).parent  # Get script directory
folder_path = script_dir / "Datasets"  # Relative folder path

# A list of all the datasets we will be testing
# Each sub folder in the Datasets folder is a dataset
dataset_names = []

# We iterate over all datasets in the folder and index them
# We create a new index for each dataset
for file in folder_path.iterdir():
    dataset_name = file.name
    dataset_names.append(dataset_name)
    
    # Create the index for the dataset
    index_name = DataParser.index_dataset_folder(client, dataset_name)
    
print("Datasets found: " + str(dataset_names))

# Now we create the new indices for all the datasets and measure their times. We will do this for all different compression types 
#index_name = DataParser.insert_dataset_into_server(SearchDatasets.SMALL_UNSTRUCTURED, client)
#index_name = DataParser.insert_dataset_into_server(Types.SearchDatasets.SMALL_UNSTRUCTURED, client)


#print("Index name: " + index_name)


# Extract the time taken from the response
#time_taken_ms = response['took']
#print(f"Request took {time_taken_ms} milliseconds on the server.")
# Query the _tasks API

# Fetch index stats after creation
stats = client.indices.stats(index=index_name)
# Extract the index size in bytes
index_size = stats["_all"]["primaries"]["store"]["size_in_bytes"]
print(f"Total index size for '{index_name}': {index_size / (1024**2):.2f} MB")

#test_search(client, index_name)


