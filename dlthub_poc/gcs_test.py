import os

from google.cloud import storage
from gcp_storage_emulator.server import create_server

HOST = "localhost"
PORT = 9023
BUCKET = "test-bucket"

os.environ["STORAGE_EMULATOR_HOST"] = f"http://{HOST}:{PORT}"
client = storage.Client()

try:
    bucket = client.create_bucket(BUCKET)
except:
    bucket = client.bucket(BUCKET)

breakpoint()

blob = bucket.blob("blob1")
blob.upload_from_string("test1")
print(blob.download_as_bytes())
