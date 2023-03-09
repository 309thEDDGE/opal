import json
import os
import opal.flow
import s3fs
import time 
import uuid

def upload_datum(local_dir_path, 
                 upload_directory, 
                 unique_id,
                 datum_type, 
                 parent_ids = [],
                 metadata = {}, 
                 label = ''):
        
    datum_json_path = f'{local_dir_path}/datum.json'
    metadata_path = f'{local_dir_path}/metadata.json'

    datum_json = {}
    datum_json['uuid'] = unique_id
    datum_json['upload_time'] = time.time_ns() // 1000
    datum_json['parent_uuids'] = parent_ids
    datum_json['datum_type'] = datum_type
    datum_json['label'] = label
    
    with open(datum_json_path, "w") as outfile:
        json.dump(datum_json, outfile)
        
    with open(metadata_path, "w") as outfile:
        json.dump(metadata, outfile)

    upload_path = f"s3://{upload_directory}"
    opal_s3fs = opal.flow.minio_s3fs()
    opal_s3fs.upload(local_dir_path, upload_directory, recursive=True)
    print(upload_path)
    