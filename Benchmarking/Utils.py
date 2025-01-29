from opensearchpy import OpenSearch

def delete_all_indices(client):
    # Get a list of all indexes
    indices = client.indices.get_alias("*").keys()

    # Delete all indexes
    for index in indices:
        client.indices.delete(index=index)
        print(f"Deleted index: {index}")

    print("All indexes have been deleted.")
    
def create_client():
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
    
    return client