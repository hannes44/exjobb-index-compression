from opensearchpy import OpenSearch
import Types
import Utils
import DataParser
from pathlib import Path
import matplotlib.pyplot as plt

show_plots = False

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



def benchmark_search():
    return 100

def parse_index_stats(stats):
    index_size = stats["_all"]["primaries"]["store"]["size_in_bytes"]
    return index_size

# Creating the client and connecting to the OpenSearch server
# If the server is not running, this will throw an exception
# This will connect to port 9200 on localhost
client = Utils.create_client()
print("Client created! Connected to OpenSearch server.")

# When we are running our complete tests, we first clean the server environment
Utils.delete_all_indices(client)


script_dir = Path(__file__).parent  # Get script directory
folder_path = script_dir / "Datasets"  # Relative folder path
dataset_names = []
# A list of all the datasets we will be testing
# Each sub folder in the Datasets folder is a dataset



#we need to map the dataset names to the index names
dataset_names_to_index_names = {}

# We iterate over all datasets in the folder and index them
# We create a new index for each dataset
for file in folder_path.iterdir():
    dataset_name = file.name
    dataset_names.append(dataset_name)
    
    index_names = []
    
    print(f"Benchmarking dataset '{dataset_name}'")
    
    # We now iterate over all different compression types
    # Starting with finding the index size for each compression type
    for s in Types.CompressionType:
        # Create the index for the dataset
        index_name = DataParser.index_dataset_folder(client, dataset_name, s)
        index_names.append(index_name)
        
        stats = client.indices.stats(index=index_name)
        index_size = parse_index_stats(stats)
        print(f"Total index size for '{index_name}': {index_size / (1024**2):.2f} MB")
        
        # Now we find out the indexing time for each compression type
        # TODO
        print(f"Indexing time for '{index_name}': 0 ms TODO")
        
        search_benchmark = benchmark_search()
        print(f"Search time for '{index_name}': {search_benchmark} ms")
        # Now we find out the search time for each compression type

        
        
#TODO add plotting