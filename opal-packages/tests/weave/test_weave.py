import uuid
import os
import pytest
import tempfile
import json
import botocore

from opal.weave.weave import upload_basket
import opal.flow

# test if local_dir_path exists
def test_upload_basket_local_dirpath_exists():
    local_dir_path = 'n o t a r e a l p a t h'
    unique_id = uuid.uuid1().int
    basket_type = 'test_basket_type'
    upload_path = f"data-store/{basket_type}/{unique_id}"

    with pytest.raises(FileNotFoundError,
                       match = f"'local_dir_path' does not exist: '{local_dir_path}'"):
        upload_basket(local_dir_path, upload_path, unique_id, basket_type)

# check if upload_path is a string
def test_upload_basket_upload_path_is_string():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    basket_type = 'test_basket_type'
    upload_path = 1234

    with pytest.raises(ValueError, match = f"'upload_directory' must be a string: '{upload_path}'"):
        upload_basket(local_dir_path, upload_path, unique_id, basket_type)

# check if unique_id is an int
def test_upload_basket_unique_id_int():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = "fake id"
    basket_type = 'test_basket_type'
    upload_path = f"data-store/{basket_type}/{unique_id}"

    with pytest.raises(ValueError, match = f"'unique_id' must be an int: '{unique_id}'"):
        upload_basket(local_dir_path, upload_path, unique_id, basket_type)

# check if basket_type is a string
def test_upload_basket_type_is_string():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    basket_type = 1234
    upload_path = f"data-store/{basket_type}/{unique_id}"

    with pytest.raises(ValueError, match = f"'basket_type' must be a string: '{basket_type}'"):
        upload_basket(local_dir_path, upload_path, unique_id, basket_type)

# check if parent_ids is a list of ints
def test_upload_basket_parent_ids_list_int():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    basket_type = 'test_basket_type'
    upload_path = f"data-store/{basket_type}/{unique_id}"
    parent_ids_in = ['a', 3]

    with pytest.raises(ValueError, match = f"'parent_ids' must be a list of int:"):
        upload_basket(local_dir_path, upload_path, unique_id, basket_type, parent_ids=parent_ids_in)

<<<<<<< HEAD
# check if parent_ids is a list
=======
# check if parent_ids is a list of ints
>>>>>>> f9f92ec (update name add test for writting metadata)
def test_upload_basket_parent_ids_is_list():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    basket_type = 'test_basket_type'
    upload_path = f"data-store/{basket_type}/{unique_id}"
    parent_ids_in = 56

    with pytest.raises(ValueError, match = f"'parent_ids' must be a list of int:"):
        upload_basket(local_dir_path, upload_path, unique_id, basket_type, parent_ids=parent_ids_in)

# check if metadata is a dictionary
def test_upload_basket_metadata_is_dictionary():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    basket_type = 'test_basket_type'
    upload_path = f"data-store/{basket_type}/{unique_id}"
    metadata_in = 'invalid'

    with pytest.raises(ValueError, match = f"'metadata' must be a dictionary: '{metadata_in}'"):
        upload_basket(local_dir_path, upload_path, unique_id, basket_type, metadata=metadata_in)

# check if label is string
def test_upload_basket_metadata_is_dictionary():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    basket_type = 'test_basket_type'
    upload_path = f"data-store/{basket_type}/{unique_id}"
    label_in = 1234

    with pytest.raises(ValueError, match = f"'label' must be a string: '{label_in}'"):
        upload_basket(local_dir_path, upload_path, unique_id, basket_type, label=label_in)

def test_upload_basket_successful_run():

    opal_s3fs = opal.flow.minio_s3fs()

    # Create basket 
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    json_path = os.path.join(local_dir_path, "sample.json")
    json_data = {'t': [1,2,3]}
    with open(json_path, "w") as outfile:
        json.dump(json_data, outfile)

    original_files = os.listdir(local_dir_path)

    # Run upload_basket
    unique_id = uuid.uuid1().int
    basket_type = 'pytest'
    upload_path = f"tests/{basket_type}/{unique_id}"
    label_in = 'note'
    metadata_in = {'metadata': [1,2,3]}
    parent_ids_in = [5,4,3,2]

    upload_basket(local_dir_path, upload_path, unique_id, 
                 basket_type, parent_ids = parent_ids_in, 
                 metadata = metadata_in, label=label_in)

    # Assert original local path hasn't been altered
    with open(json_path, 'r') as file:
        data = json.load(file)
        assert data == json_data
    assert original_files == os.listdir(local_dir_path)

    # Assert basket.json fields
    with opal_s3fs.open(f's3://{upload_path}/basket_details.json', 'rb') as file:
        basket_json = json.load(file)
        assert basket_json['uuid'] == unique_id
        assert basket_json['parent_uuids'] == parent_ids_in
        assert basket_json['basket_type'] == basket_type
        assert basket_json['label'] == label_in
        assert 'upload_time' in basket_json.keys()

    # Assert metadata.json fields
    with opal_s3fs.open(f's3://{upload_path}/metadata.json', 'rb') as file:
        assert json.load(file) == metadata_in

    # Delete s3 test data
    opal_s3fs.rm(f's3://tests/{basket_type}', recursive = True)
    assert opal_s3fs.ls(f's3://tests/{basket_type}') == []

    # cleanup
    temp_dir.cleanup()
    
def test_upload_basket_no_metadata():

    opal_s3fs = opal.flow.minio_s3fs()

    # Create basket 
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    json_path = os.path.join(local_dir_path, "sample.json")
    json_data = {'t': [1,2,3]}
    with open(json_path, "w") as outfile:
        json.dump(json_data, outfile)

    original_files = os.listdir(local_dir_path)

    # Run upload_basket
    unique_id = uuid.uuid1().int
    basket_type = 'pytest'
    upload_path = f"tests/{basket_type}/{unique_id}"

    upload_basket(local_dir_path, upload_path, unique_id, 
                 basket_type)

    # Assert metadata.json was not written
    assert opal_s3fs.ls(f's3://{upload_path}/metadata.json') == []

    # Delete s3 test data
    opal_s3fs.rm(f's3://tests/{basket_type}', recursive = True)
    assert opal_s3fs.ls(f's3://tests/{basket_type}') == []

    # cleanup
    temp_dir.cleanup()

def test_upload_basket_check_existing_upload_path():

    opal_s3fs = opal.flow.minio_s3fs()

    # Create basket 
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    json_path = os.path.join(local_dir_path, "sample.json")
    json_data = {'t': [1,2,3]}
    with open(json_path, "w") as outfile:
        json.dump(json_data, outfile)

    # Run upload_basket
    unique_id = uuid.uuid1().int
    basket_type = 'pytest'
    upload_path = f"tests/{basket_type}/{unique_id}"

    opal_s3fs.upload(local_dir_path, 
                     f's3://{upload_path}', 
                     recursive = True)
    
    with pytest.raises(FileExistsError,
                       match = f"'upload_directory' already exists: '{upload_path}''"):
        upload_basket(local_dir_path, upload_path, unique_id, basket_type)

    # Delete s3 test data
    opal_s3fs.rm(f's3://tests/{basket_type}', recursive = True)
    assert opal_s3fs.ls(f's3://tests/{basket_type}') == []

    # cleanup
    temp_dir.cleanup()

def test_upload_basket_invalid_upload_path():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    basket_type = 'pytest'
    upload_path = ";invalid_path"

    with pytest.raises(botocore.exceptions.ParamValidationError, match = f"Invalid bucket name"):
        upload_basket(local_dir_path, upload_path,
                     unique_id, basket_type)
