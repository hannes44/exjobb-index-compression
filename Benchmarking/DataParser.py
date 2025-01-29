import Types
from opensearchpy import OpenSearch

class indexCreationData:
    index_name: str
    timeInNs = 0

# This function is responsible for parsing the datasets and adding them to the OpenSearch server
# Returns the name of the index created
def insert_dataset_into_server(dataset: Types.SearchDatasets, client: OpenSearch) -> str:
    match dataset:
        case Types.SearchDatasets.SMALL_UNSTRUCTURED:
            return insert_small_unstructured_dataset(client)
        case _:
            raise ValueError("Invalid dataset provided")
    return ""
            
def test():
    print("test")
            
def insert_small_unstructured_dataset(client: OpenSearch) -> str:
    index_name = 'python-test-index'
    index_body = {
  'settings': {
    'index': {
      'number_of_shards': 4
    }
  }
}
    
    response = client.indices.create(index_name, body=index_body)
    
    return index_name
