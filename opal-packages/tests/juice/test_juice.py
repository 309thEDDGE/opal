from opal.juice.datum import upload_datum
import opal.flow
import uuid
import pytest
import tempfile
import json
import botocore
import os

# test if local_dir_path exists
def test_upload_datum_local_dirpath_exists():
    local_dir_path = 'n o t a r e a l p a t h'
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"

    with pytest.raises(FileNotFoundError, 
                       match = f"'local_dir_path' does not exist: '{local_dir_path}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type)

# check if upload_path is a string
def test_upload_datum_upload_path_is_string():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = 1234

    with pytest.raises(ValueError, match = f"'upload_directory' must be a string: '{upload_path}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type)

# check if unique_id is an int
def test_upload_datum_unique_id_int():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = "fake id"
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"

    with pytest.raises(ValueError, match = f"'unique_id' must be an int: '{unique_id}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type)

# check if datum_type is a string
def test_upload_datum_type_is_string():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    datum_type = 1234
    upload_path = f"data-store/{datum_type}/{unique_id}"

    with pytest.raises(ValueError, match = f"'datum_type' must be a string: '{datum_type}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type)

# check if parent_ids is a list of ints
def test_upload_datum_parent_ids_list_int():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"
    parent_ids_in = ['a', 3]

    with pytest.raises(ValueError, match = f"'parent_ids' must be a list of int:"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type, parent_ids=parent_ids_in)
        
# check if parent_ids is a list of ints
def test_upload_datum_parent_ids_is_list():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"
    parent_ids_in = 56

    with pytest.raises(ValueError, match = f"'parent_ids' must be a list of int:"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type, parent_ids=parent_ids_in)

# check if metadata is a dictionary
def test_upload_datum_metadata_is_dictionary():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"
    metadata_in = 'invalid'

    with pytest.raises(ValueError, match = f"'metadata' must be a dictionary: '{metadata_in}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type, metadata=metadata_in)

# check if label is string
def test_upload_datum_metadata_is_dictionary():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"
    label_in = 1234

    with pytest.raises(ValueError, match = f"'label' must be a string: '{label_in}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type, label=label_in)

def test_upload_datum_successful_run():

    opal_s3fs = opal.flow.minio_s3fs()

    # Create datum 
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    json_path = os.path.join(local_dir_path, "sample.json")
    json_data = {'t': [1,2,3]}
    with open(json_path, "w") as outfile:
        json.dump(json_data, outfile)

    original_files = os.listdir(local_dir_path)

    # Run upload_datum
    unique_id = uuid.uuid1().int
    datum_type = 'pytest'
    upload_path = f"tests/{datum_type}/{unique_id}"
    label_in = 'note'
    metadata_in = {'metadata': [1,2,3]}
    parent_ids_in = [5,4,3,2]

    upload_datum(local_dir_path, upload_path, unique_id, 
                 datum_type, parent_ids = parent_ids_in, 
                 metadata = metadata_in, label=label_in)

    # Assert original local path hasn't been altered
    with open(json_path, 'r') as f:
        data = json.load(f)
        assert data == json_data
    assert original_files == os.listdir(local_dir_path)

    # Assert datum.json fields
    with opal_s3fs.open(f's3://{upload_path}/datum.json', 'rb') as f:
        datum_json = json.load(f)
        assert datum_json['uuid'] == unique_id
        assert datum_json['parent_uuids'] == parent_ids_in
        assert datum_json['datum_type'] == datum_type
        assert datum_json['label'] == label_in
        assert 'upload_time' in datum_json.keys()

    # Assert metadata.json fields
    with opal_s3fs.open(f's3://{upload_path}/metadata.json', 'rb') as f:
        assert json.load(f) == metadata_in

    # Delete s3 test data
    opal_s3fs.rm(f's3://tests/{datum_type}', recursive = True)
    assert opal_s3fs.ls(f's3://tests/{datum_type}') == []

    # cleanup
    temp_dir.cleanup()

def test_upload_datum_check_existing_upload_path():

    opal_s3fs = opal.flow.minio_s3fs()

    # Create datum 
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    json_path = os.path.join(local_dir_path, "sample.json")
    json_data = {'t': [1,2,3]}
    with open(json_path, "w") as outfile:
        json.dump(json_data, outfile)

    # Run upload_datum
    unique_id = uuid.uuid1().int
    datum_type = 'pytest'
    upload_path = f"tests/{datum_type}/{unique_id}"

    opal_s3fs.upload(local_dir_path, 
                     f's3://{upload_path}', 
                     recursive = True)
    
    with pytest.raises(FileExistsError, match = f"'upload_directory' already exists: '{upload_path}''"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type)

    # Delete s3 test data
    opal_s3fs.rm(f's3://tests/{datum_type}', recursive = True)
    assert opal_s3fs.ls(f's3://tests/{datum_type}') == []

    # cleanup
    temp_dir.cleanup()

def test_upload_datum_invalid_upload_path():
    temp_dir = tempfile.TemporaryDirectory()
    local_dir_path = temp_dir.name
    unique_id = uuid.uuid1().int
    datum_type = 'pytest'
    upload_path = ";invalid_path"

    with pytest.raises(botocore.exceptions.ParamValidationError, match = f"Invalid bucket name"):
        upload_datum(local_dir_path, upload_path,
                     unique_id, datum_type)