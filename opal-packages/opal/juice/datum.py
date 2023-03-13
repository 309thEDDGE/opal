import json
import os
import time
import tempfile
import opal.flow

def upload_datum(local_dir_path, 
                 upload_directory,
                 unique_id,
                 datum_type, 
                 parent_ids = [],
                 metadata = {}, 
                 label = ''):

    if not os.path.isdir(local_dir_path):
        raise FileNotFoundError(f"'local_dir_path' does not exist: '{local_dir_path}'")

    if not isinstance(upload_directory, str):
        raise ValueError(f"'upload_directory' must be a string: '{upload_directory}'")

    if not isinstance(unique_id, int):
        raise ValueError(f"'unique_id' must be an int: '{unique_id}'")

    if not isinstance(datum_type, str):
        raise ValueError(f"'datum_type' must be a string: '{datum_type}'")

    if not isinstance(parent_ids, list):
        raise ValueError(f"'parent_ids' must be a list of int: '{parent_ids}'")

    if not all(isinstance(x, int) for x in parent_ids):
        raise ValueError(f"'parent_ids' must be a list of int: '{parent_ids}'")

    if not isinstance(metadata, dict):
        raise ValueError(f"'metadata' must be a dictionary: '{metadata}'")

    if not isinstance(label, str):
        raise ValueError(f"'label' must be a string: '{label}'")

    opal_s3fs = opal.flow.minio_s3fs()

    if opal_s3fs.isdir(upload_directory):
        raise FileExistsError(f"'upload_directory' already exists: '{upload_directory}''")

    temp_dir = tempfile.TemporaryDirectory()
    temp_dir_path = temp_dir.name

    datum_json_path = os.path.join(temp_dir_path, 'datum.json')
    metadata_path = os.path.join(temp_dir_path, 'metadata.json')
    datum_json = {}
    datum_json['uuid'] = unique_id
    datum_json['upload_time'] = time.time_ns() // 1000
    datum_json['parent_uuids'] = parent_ids
    datum_json['datum_type'] = datum_type
    datum_json['label'] = label

    with open(datum_json_path, "wb") as outfile:
        json.dump(datum_json, outfile)

    with open(metadata_path, "wb") as outfile:
        json.dump(metadata, outfile)

    upload_path = f"s3://{upload_directory}"
    opal_s3fs.upload(local_dir_path, upload_path, recursive=True)
    opal_s3fs.upload(datum_json_path, os.path.join(upload_path,'datum.json'))
    opal_s3fs.upload(metadata_path, os.path.join(upload_path,'metadata.json'))
    temp_dir.cleanup()
    