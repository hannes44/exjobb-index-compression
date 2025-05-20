import os
import gzip
import shutil
import time
import pathlib
import subprocess

def download_and_unzip(file_url: str, gz_path: pathlib.Path, out_path: pathlib.Path,
                       retries: int = 3, backoff: int = 5):
    """
    Download file_url with curl into gz_path, then gunzip to out_path.
    Retries the download 'retries' times with exponential back‑off if curl
    returns a non‑zero exit status OR if gunzip fails.
    """
    attempt = 0
    while attempt <= retries:
        # ---- download ----------------------------------------------------
        print(f"[{attempt+1}/{retries+1}] downloading {file_url}")
        res = subprocess.run(
            ["curl", "--fail", "--location", "--continue-at", "-",
             "-o", str(gz_path), file_url],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        
        if res.returncode != 0:
            print("curl failed:", res.stdout.decode().strip())
        else:
            # quick sanity‑check: first two bytes of a gzip file are 0x1f 0x8b
            with open(gz_path, "rb") as fh:
                magic = fh.read(2)
            if magic == b"\x1f\x8b":      # looks like a gzip
                try:
                    # ---- unzip ------------------------------------------
                    with gzip.open(gz_path, "rb") as f_in, \
                         open(out_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                    print("✓ downloaded and unzipped OK")
                    return                   # success
                except (gzip.BadGzipFile, OSError) as e:
                    print("gunzip failed:", e)

        # either curl or gunzip failed – retry after a back‑off
        attempt += 1
        if attempt > retries:
            raise RuntimeError(f"Failed after {retries+1} attempts: {file_url}")
        sleep_time = backoff * attempt
        print(f"Retrying in {sleep_time}s …")
        time.sleep(sleep_time)

# URL base and range of file numbers
url_base = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-05/segments/1736703361941.29/wet/CC-MAIN-20250126135402-20250126165402-"
start_index = 0
end_index = 1000  # Change this to the desired number of iterations

# Destination directory where the files will be saved
destination_dir = "./Datasets/CommonCrawl-2025-05/"

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
    file_url = f"{url_base}{file_num}.warc.wet.gz"
    
    # Create the full file paths
    gz_file_path = os.path.join(destination_dir, f"CC-MAIN-20250126135402-20250126165402-{file_num}.warc.wet.gz")
    output_file_path = gz_file_path.replace(".gz", "")
    
    # Download and unzip the file
    ok = download_and_unzip(file_url, pathlib.Path(gz_file_path), pathlib.Path(output_file_path))
    if not ok:
        print(f"Failed to download or unzip {file_url}")
        continue
    print(f"Downloaded and unzipped {file_url} to {output_file_path}")
    
    # Optionally remove the .gz file after extraction
    os.remove(gz_file_path)
    print(f"Removed {gz_file_path}")
