import os
import gzip
import shutil

# URL base and range of file numbers
url_base = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2016-44/segments/1476988717783.68/wet/CC-MAIN-20161020183837-"
start_index = 0
end_index = 1000  # Change this to the desired number of iterations

# Destination directory where the files will be saved
destination_dir = "./Datasets/CommonCrawl-2016-44/"

# Ensure the destination directory exists
os.makedirs(destination_dir, exist_ok=True)

# Delete all existing files in the destination directory
for filename in os.listdir(destination_dir):
    file_path = os.path.join(destination_dir, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)  # Delete the file or symbolic link
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)  # Delete the directory and its contents
    except Exception as e:
        print(f"Failed to delete {file_path}. Reason: {e}")

print(f"Cleared all files in {destination_dir}")

# Loop through numbers and download the files
for i in range(start_index, end_index):
    file_num = f"{i:05d}"  # Format the number to 5 digits (e.g., 00001, 00002)
    file_url = f"{url_base}{file_num}-ip-10-171-6-4.ec2.internal.warc.wet.gz"
    
    # Create the full file paths
    gz_file_path = os.path.join(destination_dir, f"CC-MAIN-20161020183837-{file_num}-ip-10-171-6-4.ec2.internal.warc.wet.gz")
    output_file_path = gz_file_path.replace(".gz", "")
    
    # Use curl to download the file to the destination directory
    os.system(f"curl -o {gz_file_path} {file_url}")
    print(f"Downloaded {file_url} to {gz_file_path}")
    
    # Unzip the downloaded .gz file
    with gzip.open(gz_file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Unzipped {gz_file_path} to {output_file_path}")
    
    # Optionally remove the .gz file after extraction
    os.remove(gz_file_path)
    print(f"Removed {gz_file_path}")