import Types
from opensearchpy import OpenSearch
from pathlib import Path


datasets_folder_path = Path(__file__).parent / "Datasets"

class indexCreationData:
    index_name: str
    timeInNs = 0

# This function is responsible for parsing the datasets and adding them to the OpenSearch server
# Returns the name of the index created
def insert_dataset_into_server(dataset: str, client: OpenSearch) -> str:
    return ""
            

# Indexes all supported files in the given folder
def index_dataset_folder(client: OpenSearch, folder_name: str) -> str:
  # Create the dataset index
  index_name = folder_name.lower()
  index_body = {
    'settings': {
      'index': {
        'number_of_shards': 1,
         "merge.scheduler.max_thread_count": 1,
          "number_of_replicas": 0,
          "codec": "best_compression"
      }
    }
  }

  response = client.indices.create(index_name, body=index_body)
  
  
  folder_path = Path(datasets_folder_path / folder_name)
  for file in folder_path.rglob('*'):  # '*' matches all files and subdirectories
    is_text_file = file.suffix == '.txt' # Currently only supporting .txt files
    if file.is_file() and is_text_file:  # To make sure it's a file and not a directory
      with open(folder_path / file, 'r', encoding="utf-8") as file:
        file_content = file.read()
        response = client.index(
          index = index_name,
          body = {
            'content': file_content
          }
        )
       # print(response)
       # return index_name
    
  return ""


def index_textfile(client: OpenSearch, file_path: str) -> str:
    index_name = 'python-test-index'

    with open(file_path, 'r') as file:
      file_content = file.read()
    
    # Add a document to the index.
    document = {
      'title': 'Moneyball',
      'content': file_content,
    }
    
    id = '1'

    response = client.index(
        index = index_name,
        body = document,
        id = id,
        refresh = True
    )
    return index_name