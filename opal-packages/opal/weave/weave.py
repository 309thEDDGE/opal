import json
import os
import time
import tempfile
import opal.flow

def upload_basket(local_dir_path, 
                 upload_directory,
                 unique_id,
                 basket_type, 
                 parent_ids = [],
                 metadata = {}, 
                 label = ''):
    """
    Upload a directory of data to MinIO. 

    This function takes in a local directory path along with
    taging information and creates a metadata.json file and
    a basket_details.json file. These two files together with 
    the data from local_dir_path are uploaded to the 
    upload_directory path within MinIO as a basket of data. 
    
    basket_details.json contains:
        1) unique_id
        2) list of parent ids
        3) basket type
        4) label
        5) upload date
        
    metadata.json contains:
        1) dictionary passed in through the metadata parameter

    Parameters
    ----------
    local_dir_path : str
        Path to local directory containing data to be 
        uploaded to MinIO.
    upload_directory: str
        MinIO path where basket is to be uploaded.
    unique_id: int
        Unique ID to identify the basket once uploaded.
    basket_type: str
        Type of basket being uploaded
    parent_ids: optional [int]
        List of unique ids associated with the parent baskets
        used to derive the new basket being uploaded
    metadata: optional dict,
        Python dictionary that will be written to metadata.json
        and stored in the basket in MinIO
    label: optional str,
        Optional user friendly label associated with the basket 
    """

    if not os.path.isdir(local_dir_path):
        raise FileNotFoundError(f"'local_dir_path' does not exist: '{local_dir_path}'")

    if not isinstance(upload_directory, str):
        raise ValueError(f"'upload_directory' must be a string: '{upload_directory}'")

    if not isinstance(unique_id, int):
        raise ValueError(f"'unique_id' must be an int: '{unique_id}'")

    if not isinstance(basket_type, str):
        raise ValueError(f"'basket_type' must be a string: '{basket_type}'")

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

    basket_json_path = os.path.join(temp_dir_path, 'basket_details.json')
    metadata_path = os.path.join(temp_dir_path, 'metadata.json')
    basket_json = {}
    basket_json['uuid'] = unique_id
    basket_json['upload_time'] = time.time_ns() // 1000
    basket_json['parent_uuids'] = parent_ids
    basket_json['basket_type'] = basket_type
    basket_json['label'] = label

    with open(basket_json_path, "w") as outfile:
        json.dump(basket_json, outfile)
    
    upload_path = f"s3://{upload_directory}"
    if metadata != {}:
        with open(metadata_path, "w") as outfile:
            json.dump(metadata, outfile)
        opal_s3fs.upload(metadata_path, os.path.join(upload_path,'metadata.json'))

    opal_s3fs.upload(local_dir_path, upload_path, recursive=True)
    opal_s3fs.upload(basket_json_path, os.path.join(upload_path,'basket_details.json'))
    temp_dir.cleanup()
    