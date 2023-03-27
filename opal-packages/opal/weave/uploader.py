import json
import os
import time
import tempfile
import s3fs
import hashlib
import math
from datetime import datetime

def validate_upload_item(upload_item):
    
    if not isinstance(upload_item, dict):
        raise TypeError(f"'upload_item' must be a dictionary: 'upload_item = {upload_item}'")
        
    expected_schema = {'path': str,
                       'stub': bool}
    for key, value in upload_item.items():
        if key not in expected_schema.keys():
            raise KeyError(f"Invalid upload_item key: '{key}'"
                           f"\nExpected keys: {list(expected_schema.keys())}"
                          )
        if not isinstance(value, expected_schema[key]):
            raise TypeError(f"Invalid upload_item type: '{key}: {type(value)}'"
                            f"\nExpected type: {expected_schema[key]}"
                           )
            
    if not os.path.exists(upload_item['path']):
        raise FileExistsError(f"'path' does not exist: '{upload_item['path']}'")
    

def derive_integrity_data(file_path, byte_count = 10**6):
    """
    Derive basic integrity data from a file.

    This function takes in a file path and calculates
    the file checksum, file size, and access date (current time).

    Parameters
    ----------
    file_path : str
        Path to file from which integrity data will be derived
    byte_count: int
        If the file size is greater than 3 * byte_count, the checksum
        will be calculated from the beginning, middle, and end bytes 
        of the file. For example: If the file size is 10 bytes long
        and the byte_count is 2, the checksum will be calculated from bytes
        1, 2 (beginning two bytes), 5, 6 (middle two bytes) and 9, 10 
        (last two bytes). This option is provided to speed up checksum
        calculation for large files.

    Returns
    ----------
    Dictionary  
     {
      'file_size': bytes (int),
      'hash': sha256 hash (string),
      'access_date': current date/time (string)
     }

    """
    if not isinstance(file_path, str):
        raise TypeError(f"'file_path' must be a string: '{file_path}'")

    if not os.path.isfile(file_path):
        raise FileExistsError(f"'file_path' does not exist: '{file_path}'")

    if not isinstance(byte_count, int):
        raise TypeError(f"'byte_count' must be an int: '{byte_count}'")

    if not byte_count > 0:
        raise ValueError(f"'byte_count' must be greater than zero: '{byte_count}'")
        
    max_byte_count = 300 * 10**6
    if byte_count > max_byte_count:
        raise ValueError(f"'byte_count' must be less than or equal to {max_byte_count}"
                         f" bytes: '{byte_count}'")

    file_size = os.path.getsize(file_path)

    if file_size <= byte_count * 3:
        sha256_hash = hashlib.sha256(open(file_path,'rb').read()).hexdigest()
    else:
        hasher = hashlib.sha256()
        midpoint = file_size / 2.0
        midpoint_seek_position = math.floor(midpoint - byte_count/2.0)
        end_seek_position = file_size - byte_count
        with open(file_path, "rb") as file:
            hasher.update(file.read(byte_count))
            file.seek(midpoint_seek_position)
            hasher.update(file.read(byte_count))
            file.seek(end_seek_position)
            hasher.update(file.read(byte_count))
        sha256_hash = hasher.hexdigest()

    return {'file_size': file_size,
            'hash': sha256_hash,
            'access_date': datetime.now().strftime("%m/%d/%Y %H:%M:%S")}

def upload_basket(upload_items,
                 upload_directory,
                 unique_id,
                 basket_type,
                 parent_ids = [],
                 metadata = {},
                 label = '',
                 **kwargs):
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
    
    kwargs_schema = {'test_clean_up': bool}
    for key, value in kwargs.items():
        if key not in kwargs_schema.keys():
            raise KeyError(f"Invalid kwargs argument: '{key}'")
        if not isinstance(value, kwargs_schema[key]):
            raise TypeError(f"Invalid datatype: '{key}: must be type {kwargs_schema[key]}'")

    test_clean_up = kwargs.get("test_clean_up", False)

    if not isinstance(upload_items, list):
        raise TypeError(f"'upload_items' must be a list of dictionaries: '{upload_items}'")
        
    if not all(isinstance(x, dict) for x in upload_items):
        raise TypeError(f"'upload_items' must be a list of dictionaries: '{upload_items}'")
        
    # Validate upload_items
    local_path_basenames = []
    for upload_item in upload_items:
        validate_upload_item(upload_item)
        local_path_basename = os.path.basename(upload_item['path'])
        # Check for Duplicate file/folder names
        if local_path_basename in local_path_basenames:
            raise ValueError(f"'upload_item' folder and file names must be unique:"
                             f" Duplicate Name = {local_path_basename}")
        else:
            local_path_basenames.append(local_path_basename)
        
    if not isinstance(upload_directory, str):
        raise TypeError(f"'upload_directory' must be a string: '{upload_directory}'")

    if not isinstance(unique_id, int):
        raise TypeError(f"'unique_id' must be an int: '{unique_id}'")

    if not isinstance(basket_type, str):
        raise TypeError(f"'basket_type' must be a string: '{basket_type}'")

    if not isinstance(parent_ids, list):
        raise TypeError(f"'parent_ids' must be a list of int: '{parent_ids}'")

    if not all(isinstance(x, int) for x in parent_ids):
        raise TypeError(f"'parent_ids' must be a list of int: '{parent_ids}'")

    if not isinstance(metadata, dict):
        raise TypeError(f"'metadata' must be a dictionary: '{metadata}'")

    if not isinstance(label, str):
        raise TypeError(f"'label' must be a string: '{label}'")

    opal_s3fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]})

    if opal_s3fs.isdir(upload_directory):
        raise FileExistsError(f"'upload_directory' already exists: '{upload_directory}''")

    try:
        temp_dir = tempfile.TemporaryDirectory()
        temp_dir_path = temp_dir.name

        basket_json_path = os.path.join(temp_dir_path, 'basket_manifest.json')
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
        opal_s3fs.upload(local_dir_path, upload_path, recursive=True)
        if metadata != {}:
            with open(metadata_path, "w") as outfile:
                json.dump(metadata, outfile)
            opal_s3fs.upload(metadata_path, os.path.join(upload_path,'metadata.json'))

        opal_s3fs.upload(basket_json_path, os.path.join(upload_path,'basket_manifest.json'))

        if test_clean_up:
            raise Exception('Test Clean Up')
  
    except Exception as e:
        if opal_s3fs.ls(upload_path) != []:
            opal_s3fs.rm(upload_path, recursive = True)
        raise e
  
    finally:
        temp_dir.cleanup()
