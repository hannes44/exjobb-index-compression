from opensearchpy import OpenSearch
import Types
import Utils
import DataParser
from pathlib import Path
import matplotlib.pyplot as plt

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

index_names = []

#we need to map the dataset names to the index names
dataset_names_to_index_names = {}

# We iterate over all datasets in the folder and index them
# We create a new index for each dataset
for file in folder_path.iterdir():
    dataset_name = file.name
    dataset_names.append(dataset_name)
    
    # We now iterate over all different compression types
    for s in Types.CompressionType:
        # Create the index for the dataset
        index_name = DataParser.index_dataset_folder(client, dataset_name, s)
        index_names.append(index_name)
        
        if dataset_name not in dataset_names_to_index_names:
            dataset_names_to_index_names[dataset_name] = []
        dataset_names_to_index_names[dataset_name].append(index_name)
        
print("Datasets found: " + str(dataset_names))



# Now we create the new indices for all the datasets and measure their times. We will do this for all different compression types 
#index_name = DataParser.insert_dataset_into_server(SearchDatasets.SMALL_UNSTRUCTURED, client)
#index_name = DataParser.insert_dataset_into_server(Types.SearchDatasets.SMALL_UNSTRUCTURED, client)


#print("Index name: " + index_name)


# Extract the time taken from the response
#time_taken_ms = response['took']
#print(f"Request took {time_taken_ms} milliseconds on the server.")
# Query the _tasks API


for dataset_name in dataset_names:
    print(f"Dataset: '{dataset_name}'")
    index_names = dataset_names_to_index_names[dataset_name]
    categories = []
    sizes = []
    for index_name in index_names:
        stats = client.indices.stats(index=index_name)
        index_size = parse_index_stats(stats)
        print(f"Total index size for '{index_name}': {index_size / (1024**2):.2f} MB")
        categories.append(index_name)
        sizes.append(index_size)
    
    plt.bar(categories, sizes)
    plt.xlabel("Compression type")
    plt.ylabel("Index size (MB)")
    plt.title(f"Index sizes for dataset '{dataset_name}'")
    
    plt.show()
    



#test_search(client, index_name)



