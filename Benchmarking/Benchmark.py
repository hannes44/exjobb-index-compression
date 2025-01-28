from opensearchpy import OpenSearch

host = 'localhost'
port = 9200

# Create the client with SSL/TLS and hostname verification disabled.
client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    use_ssl = False,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False
)



index_name = 'python-test-indegfsdgfdsfsdfgdgfxsdgf342'
index_body = {
  'settings': {
    'index': {
      'number_of_shards': 1
    }
  }
}

response = client.indices.create(index_name, body=index_body)

#response = client.indices.create(index_name, body=index_body)
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
    

def benchmark
