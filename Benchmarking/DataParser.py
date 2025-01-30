import Types
from opensearchpy import OpenSearch
from pathlib import Path


datasets_folder_path = Path(__file__).parent / "Datasets"

class indexCreationData:
    index_name: str
    timeInNs = 0


# Indexes all supported files in the given folder
def index_dataset_folder(client: OpenSearch, folder_name: str, compression_type: Types.CompressionType) -> str:
  # Create the dataset index
  index_name = folder_name.lower() + "_" + compression_type.value
  index_body = {
    'settings': {
      'index': {
        'number_of_shards': 1,
         "merge.scheduler.max_thread_count": 1,
          "number_of_replicas": 0,
          "codec": compression_type.value,
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
    
  return index_name

