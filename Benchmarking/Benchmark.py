from opensearchpy import OpenSearch
import Types
import Utils
import DataParser


# Creating the client and connecting to the OpenSearch server
# If the server is not running, this will throw an exception
# This will connect to port 9200 on localhost
client = Utils.create_client()

# When we are running our complete tests, we first clean the server environment
Utils.delete_all_indices(client)

# Now we create the new indices for all the datasets and measure their times. We will do this for all different compression types 
#index_name = DataParser.insert_dataset_into_server(SearchDatasets.SMALL_UNSTRUCTURED, client)
index_name = DataParser.insert_dataset_into_server(Types.SearchDatasets.SMALL_UNSTRUCTURED, client)


print("Index name: " + index_name)


# Extract the time taken from the response
#time_taken_ms = response['took']
#print(f"Request took {time_taken_ms} milliseconds on the server.")
# Query the _tasks API

# Fetch index stats after creation
stats = client.indices.stats(index=index_name)
print(stats)

def create_client():
    return OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        use_ssl = False,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False
    )
    
