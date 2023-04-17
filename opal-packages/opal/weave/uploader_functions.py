import json
import os
import time
import hashlib
import math
from datetime import datetime
from pathlib import Path

def validate_upload_item(upload_item):
    """ Validates an upload_item """
    if not isinstance(upload_item, dict):
        raise TypeError(
            f"'upload_item' must be a dictionary: 'upload_item = {upload_item}'"
        )

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


def derive_integrity_data(file_path, byte_count = 10**8):
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
      'source_path': path to the original source of data (string)
      'byte_count': byte count used for generated checksum (int)
     }

    """
    if not isinstance(file_path, str):
        raise TypeError(f"'file_path' must be a string: '{file_path}'")

    if not os.path.isfile(file_path):
        raise FileExistsError(f"'file_path' does not exist: '{file_path}'")

    if not isinstance(byte_count, int):
        raise TypeError(f"'byte_count' must be an int: '{byte_count}'")

    if not byte_count > 0:
        raise ValueError(
            f"'byte_count' must be greater than zero: '{byte_count}'"
        )

    max_byte_count = 300 * 10**6
    if byte_count > max_byte_count:
        raise ValueError(
            f"'byte_count' must be less than or equal to {max_byte_count}"
            f" bytes: '{byte_count}'"
        )

    file_size = os.path.getsize(file_path)

    # TODO: Read in small chunks of the file at a time to protect from
    #       RAM overload
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
            'access_date': datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
            'source_path': file_path,
            'byte_count': byte_count}

def sanitize_upload_basket_kwargs(**kwargs):
    ''' Sanitizes kwargs for upload_basket '''
    kwargs_schema = {'test_clean_up': bool}
    for key, value in kwargs.items():
        if key not in kwargs_schema.keys():
            raise KeyError(f"Invalid kwargs argument: '{key}'")
        if not isinstance(value, kwargs_schema[key]):
            raise TypeError(
                f"Invalid datatype: '{key}: must be type {kwargs_schema[key]}'"
            )

def sanitize_upload_basket_non_kwargs(upload_items, upload_directory, unique_id,
                                      basket_type, parent_ids, metadata, label,
                                      **kwargs):
    """ Sanitize upload_basket's non kwargs args """
    if not isinstance(upload_items, list):
        raise TypeError(
            f"'upload_items' must be a list of dictionaries: '{upload_items}'"
        )

    if not all(isinstance(x, dict) for x in upload_items):
        raise TypeError(
            f"'upload_items' must be a list of dictionaries: '{upload_items}'"
        )

    # Validate upload_items
    local_path_basenames = []
    unallowed_filenames = [
        'basket_manifest.json', 'basket_metadata.json', 'basket_supplement.json'
    ]
    for upload_item in upload_items:
        validate_upload_item(upload_item)
        local_path_basename = os.path.basename(Path(upload_item['path']))
        if local_path_basename in unallowed_filenames:
            raise ValueError(f"'{local_path_basename}' filename not allowed")
        # Check for duplicate file/folder names
        if local_path_basename in local_path_basenames:
            raise ValueError(
                f"'upload_item' folder and file names must be unique:"
                f" Duplicate Name = {local_path_basename}"
            )
        else:
            local_path_basenames.append(local_path_basename)
        
    if not isinstance(upload_directory, str):
        raise TypeError(
            f"'upload_directory' must be a string: '{upload_directory}'"
        )

    if not isinstance(unique_id, str):
        raise TypeError(
            f"'unique_id' must be a string: '{unique_id}'"
        )

    if not isinstance(basket_type, str):
        raise TypeError(
            f"'basket_type' must be a string: '{basket_type}'"
        )

    if not isinstance(parent_ids, list):
        raise TypeError(
            f"'parent_ids' must be a list of strings: '{parent_ids}'"
        )

    if not all(isinstance(x, str) for x in parent_ids):
        raise TypeError(
            f"'parent_ids' must be a list of strings: '{parent_ids}'"
        )

    if not isinstance(metadata, dict):
        raise TypeError(f"'metadata' must be a dictionary: '{metadata}'")

    if not isinstance(label, str):
        raise TypeError(f"'label' must be a string: '{label}'")

def setup_temp_dir(upload_directory, temp_dir, opal_s3fs, **kwargs):
    """ sets up a temporary dir for usage in upload_basket"""
    upload_path = f"s3://{upload_directory}"
    opal_s3fs.mkdir(upload_path)
    temp_dir_path = temp_dir.name
    return upload_path, temp_dir_path

def upload_files_and_stubs(upload_items, upload_path, opal_s3fs,
                                         **kwargs):
    ''' Returns JSON of supplement data '''

    supplement_data = {}
    supplement_data['upload_items'] = upload_items
    supplement_data['integrity_data'] = []

    for upload_item in upload_items:
        upload_item_path = Path(upload_item['path'])
        if upload_item_path.is_dir():
            for root, dirs, files in os.walk(upload_item_path):
                for name in files:
                    local_path = os.path.join(root, name)
                    file_integrity_data = derive_integrity_data(str(local_path))
                    if upload_item['stub'] == False:
                        file_integrity_data['stub'] = False
                        file_upload_path = os.path.join(
                            upload_path,
                            os.path.relpath(
                                local_path, os.path.split(upload_item_path)[0]
                            )
                        )
                        file_integrity_data['upload_path'] = str(
                            file_upload_path
                        )
                        opal_s3fs.upload(local_path, file_upload_path)
                    else:
                        file_integrity_data['stub'] = True
                        file_integrity_data['upload_path'] = 'stub'
                    supplement_data['integrity_data'].append(
                        file_integrity_data
                    )
        else:
            file_integrity_data = derive_integrity_data(str(upload_item_path))
            if upload_item['stub'] == False:
                file_integrity_data['stub'] = False
                file_upload_path = os.path.join(
                    upload_path,os.path.basename(upload_item_path)
                )
                file_integrity_data['upload_path'] = str(file_upload_path)
                opal_s3fs.upload(str(upload_item_path), file_upload_path)
            else:
                file_integrity_data['stub'] = True
                file_integrity_data['upload_path'] = 'stub'
            supplement_data['integrity_data'].append(file_integrity_data)
    return supplement_data

def dump_basket_json(temp_dir_path, unique_id, parent_ids, basket_type,
                     label, upload_path, opal_s3fs, **kwargs):
    basket_json_path = os.path.join(temp_dir_path, 'basket_manifest.json')
    basket_json = {}
    basket_json['uuid'] = unique_id
    basket_json['upload_time'] = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    basket_json['parent_uuids'] = parent_ids
    basket_json['basket_type'] = basket_type
    basket_json['label'] = label

    with open(basket_json_path, "w") as outfile:
        json.dump(basket_json, outfile)
    opal_s3fs.upload(
        basket_json_path, os.path.join(upload_path,'basket_manifest.json')
    )

def dump_basket_supplement(temp_dir_path, metadata, supplement_data, opal_s3fs,
                           upload_path, **kwargs):
    metadata_path = os.path.join(temp_dir_path, 'basket_metadata.json')
    if metadata != {}:
        with open(metadata_path, "w") as outfile:
            json.dump(metadata, outfile)
        opal_s3fs.upload(
            metadata_path, os.path.join(upload_path,'basket_metadata.json')
        )

    supplement_json_path = os.path.join(temp_dir_path, 'basket_supplement.json')
    with open(supplement_json_path, "w") as outfile:
        json.dump(supplement_data, outfile)
    opal_s3fs.upload(
        supplement_json_path, os.path.join(upload_path,'basket_supplement.json')
    )