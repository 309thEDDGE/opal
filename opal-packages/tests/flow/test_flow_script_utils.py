from opal.flow.flow_script_utils import *

# two levels up from this file
dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_folder_upload_structure_are_files():
    assert os.path.isdir(dir)

    key = os.path.basename(dir)
    upload_struct = get_metaflow_s3_folder_upload_structure(dir, key)

    for k, p in upload_struct:
        assert k.startswith(key)
        assert os.path.isfile(p)


def test_folder_upload_with_key():
    key = "other_key"
    upload_struct = get_metaflow_s3_folder_upload_structure(dir, key)

    assert all([k.startswith(key) for k, _ in upload_struct])
