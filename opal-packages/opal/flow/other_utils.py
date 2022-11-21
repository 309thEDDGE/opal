import os
import s3fs

# get an s3fs object configured for OPAL minio
def minio_s3fs():
    return s3fs.S3FileSystem(client_kwargs={"endpoint_url":os.environ["S3_ENDPOINT"]})
    