from opal.juice.datum import *
import uuid
import pytest

# local_dir_path not writable?
# Delete temporary files afterwards?
# Check to see if upload directory is empty?
# Validate that upload was successful
# Ensure metadata can be written as json
# test all inputs, valid paths, expected ints/strings etc...

# test if local_dir_path exists
def test_upload_datum_local_dirpath_exists():
    local_dir_path = 'n o t a r e a l p a t h'
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"

    with pytest.raises(FileNotFoundError, match = f"'local_dir_path' does not exist: '{local_dir_path}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type)
        
# check if upload_path is a string
def test_upload_datum_upload_path_is_string():
    local_dir_path = "/home/jovyan"
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = 1234

    with pytest.raises(ValueError, match = f"'upload_directory' must be a string: '{upload_path}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type)
        
# check if unique_id is an int
def test_upload_datum_unique_id_int():
    local_dir_path = "/home/jovyan"
    unique_id = "fake id"
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"

    with pytest.raises(ValueError, match = f"'unique_id' must be an int: '{unique_id}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type)
        
# check if datum_type is a string
def test_upload_datum_type_is_string():
    local_dir_path = "/home/jovyan"
    unique_id = uuid.uuid1().int
    datum_type = 1234
    upload_path = f"data-store/{datum_type}/{unique_id}"

    with pytest.raises(ValueError, match = f"'datum_type' must be a string: '{datum_type}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type)
        
# check if parent_ids is a list of ints
def test_upload_datum_parent_ids_list_int():
    local_dir_path = "/home/jovyan"
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"
    parent_ids_in = ['a', 3]

    with pytest.raises(ValueError, match = f"'parent_ids' must be a list of int:"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type, parent_ids=parent_ids_in)
        
# check if parent_ids is a list of ints
def test_upload_datum_parent_ids_is_list():
    local_dir_path = "/home/jovyan"
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"
    parent_ids_in = 56

    with pytest.raises(ValueError, match = f"'parent_ids' must be a list of int:"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type, parent_ids=parent_ids_in)
        
# check if metadata is a dictionary
def test_upload_datum_metadata_is_dictionary():
    local_dir_path = "/home/jovyan"
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"
    metadata_in = 'invalid'

    with pytest.raises(ValueError, match = f"'metadata' must be a dictionary: '{metadata_in}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type, metadata=metadata_in)
        
# check if label is string
def test_upload_datum_metadata_is_dictionary():
    local_dir_path = "/home/jovyan"
    unique_id = uuid.uuid1().int
    datum_type = 'test_datum_type'
    upload_path = f"data-store/{datum_type}/{unique_id}"
    label_in = 1234

    with pytest.raises(ValueError, match = f"'label' must be a string: '{label_in}'"):
        upload_datum(local_dir_path, upload_path, unique_id, datum_type, label=label_in)