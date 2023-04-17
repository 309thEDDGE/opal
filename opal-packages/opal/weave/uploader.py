import os
import tempfile
import s3fs
from .uploader_functions import *

def upload_basket(upload_items, upload_directory, unique_id, basket_type,
                  parent_ids = [], metadata = {}, label = '', **kwargs):
    """
    Upload files and directories to MinIO. 

    This function takes in a list of items to upload along with
    taging information and uploads the data together with three
    json files: basket_manifest.json, basket_metadata.json, and
    supplement.json. The contents of the three files are specified 
    below. These three files together with the data specified by
    upload_items are uploaded to the upload_directory path within 
    MinIO as a basket of data. 

    basket_manifest.json contains:
        1) unique_id
        2) list of parent ids
        3) basket type
        4) label
        5) upload date

    basket_metadata.json contains:
        1) dictionary passed in through the metadata parameter

    basket_supplement.json contains:
        1) the upload_items object that was passed as an input parameter
        2) integrity_data for every file uploaded 
            a) file checksum
            b) upload date
            c) file size (bytes)
            d) source path (original file location) 
            e) stub (true/false)
            f) upload path (path to where the file is uploaded in MinIO)

    Parameters
    ----------
    upload_items : [dict]
        List of python dictionaries with the following schema:
        {
            'path': path to the file or folder being uploaded (string),
            'stub': true/false (bool)
        }
        'path' can be a file or folder to be uploaded. Every filename
        and folder name must be unique. If 'stub' is set to True, integrity data
        will be included without uploading the actual file or folder. Stubs are
        useful when original file source information is desired without
        uploading the data itself. This is especially useful when dealing with
        large files.
    upload_directory: str
        MinIO path where basket is to be uploaded.
    unique_id: str
        Unique ID to identify the basket once uploaded.
    basket_type: str
        Type of basket being uploaded.
    parent_ids: optional [str]
        List of unique ids associated with the parent baskets
        used to derive the new basket being uploaded.
    metadata: optional dict,
        Python dictionary that will be written to metadata.json
        and stored in the basket in MinIO.
    label: optional str,
        Optional user friendly label associated with the basket.
    """
    # Two asterisks on the next line unpacks kwargs to pass to the function
    sanitize_upload_basket_kwargs(**kwargs)

    test_clean_up = kwargs.get("test_clean_up", False)

    sanitize_upload_basket_non_kwargs(**locals())

    opal_s3fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": os.environ["S3_ENDPOINT"]}
    )

    if opal_s3fs.isdir(upload_directory):
        raise FileExistsError(
            f"'upload_directory' already exists: '{upload_directory}''"
        )

    try:
        temp_dir = tempfile.TemporaryDirectory()
        upload_path, temp_dir_path = setup_temp_dir(**locals())
        supplement_data = create_upload_basket_supplement_data(**locals())
        dump_basket_json(**locals())
        dump_basket_supplement(**locals())

        if test_clean_up:
            raise Exception('Test Clean Up')

    except Exception as e:
        if opal_s3fs.exists(upload_path):
            opal_s3fs.rm(upload_path, recursive = True)
        raise e

    finally:
        temp_dir.cleanup()
