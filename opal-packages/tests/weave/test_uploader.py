import uuid
import os
import tempfile
import json
import pytest
import botocore
import s3fs
import hashlib
import time
from datetime import datetime

from opal.weave.uploader import upload_basket, derive_integrity_data

class TestDeriveIntegrityData():

    def setup_method(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir.name

    def teardown_method(self):
        self.temp_dir.cleanup()

    def test_derive_integrity_data_file_doesnt_exist(self):
        file_path = 'f a k e f i l e p a t h'
        with pytest.raises(FileExistsError,
                           match = f"'file_path' does not exist: '{file_path}'"):
            derive_integrity_data(file_path)

    def test_derive_integrity_data_path_is_string(self):
        file_path = 10
        with pytest.raises(TypeError, match = f"'file_path' must be a string: '{file_path}'"):
            derive_integrity_data(file_path)

    def test_derive_integrity_data_byte_count_string(self):
        file_path = os.path.join(self.temp_dir_path, 'file.json')
        json_data = {'t': [1,2,3]}
        with open(file_path, "w") as outfile:
            json.dump(json_data, outfile)
        byte_count_in = 'invalid byte count'
        with pytest.raises(TypeError, match = f"'byte_count' must be an int: '{byte_count_in}'"):
            derive_integrity_data(file_path, byte_count = byte_count_in)

    def test_derive_integrity_data_byte_count_float(self):
        file_path = os.path.join(self.temp_dir_path, 'file.json')
        json_data = {'t': [1,2,3]}
        with open(file_path, "w") as outfile:
            json.dump(json_data, outfile)
        byte_count_in = 6.5
        with pytest.raises(TypeError, match = f"'byte_count' must be an int: '{byte_count_in}'"):
            derive_integrity_data(file_path, byte_count = byte_count_in)

    def test_derive_integrity_data_byte_count_0(self):
        file_path = os.path.join(self.temp_dir_path, 'file.json')
        json_data = {'t': [1,2,3]}
        with open(file_path, "w") as outfile:
            json.dump(json_data, outfile)
        byte_count_in = 0
        with pytest.raises(ValueError, match = f"'byte_count' must be greater than zero: '{byte_count_in}'"):
            derive_integrity_data(file_path, byte_count = byte_count_in)

    def test_derive_integrity_data_large_byte_count(self):
        file_path = os.path.join(self.temp_dir_path, 'file.txt')
        file_data = '0123456789'
        with open(file_path, "w") as outfile:
            outfile.write(file_data)

        assert '84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882' == \
        derive_integrity_data(file_path, 10**6)['hash']

    def test_derive_integrity_data_small_byte_count(self):
        file_path = os.path.join(self.temp_dir_path, 'file.txt')
        file_data = '0123456789'
        with open(file_path, "w") as outfile:
            outfile.write(file_data)

        assert 'a2a7cb1d7fc8f79e33b716b328e19bb381c3ec96a2dca02a3d1183e7231413bb' == \
        derive_integrity_data(file_path, 2)['hash']

    def test_derive_integrity_data_file_size(self):
        file_path = os.path.join(self.temp_dir_path, 'file.txt')
        file_data = '0123456789'
        with open(file_path, "w") as outfile:
            outfile.write(file_data)

        assert derive_integrity_data(file_path, 2)['file_size'] == 10

    def test_derive_integrity_data_date(self):
        file_path = os.path.join(self.temp_dir_path, 'file.txt')
        file_data = '0123456789'
        with open(file_path, "w") as outfile:
            outfile.write(file_data)

        access_date = derive_integrity_data(file_path, 2)['access_date']
        access_date = datetime.strptime(access_date, '%m/%d/%Y %H:%M:%S')
        access_date_seconds = access_date.timestamp() 
        now_seconds = time.time_ns() // 10**9
        diff_seconds = abs(access_date_seconds - now_seconds)
        assert diff_seconds < 60 
        
    def test_derive_integrity_data_max_byte_count(self):
        file_path = os.path.join(self.temp_dir_path, 'file.json')
        json_data = {'t': [1,2,3]}
        with open(file_path, "w") as outfile:
            json.dump(json_data, outfile)
        byte_count_in = 300 * 10**6 + 1
        with pytest.raises(ValueError, match = f"'byte_count' must be less "
                         f"than or equal to 300000000 bytes: '{byte_count_in}'"):
            derive_integrity_data(file_path, byte_count = byte_count_in)
        
        derive_integrity_data(file_path, byte_count = (byte_count_in - 1))
        

class TestUploadBasket():
    def setup_class(cls):
        cls.opal_s3fs = s3fs.S3FileSystem(client_kwargs=
                                          {"endpoint_url": os.environ["S3_ENDPOINT"]})
        cls.basket_type = 'test_basket_type'
        cls.test_bucket = f'pytest-{uuid.uuid1().hex}'
        cls.basket_path = f'{cls.test_bucket}/{cls.basket_type}'
        cls.opal_s3fs.mkdir(cls.test_bucket)

    def setup_method(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = self.temp_dir.name
        if self.opal_s3fs.ls(f's3://{self.basket_path}') != []:
            self.opal_s3fs.rm(f's3://{self.basket_path}', recursive = True)

    def teardown_method(self):
        if self.opal_s3fs.ls(f's3://{self.basket_path}') != []:
            self.opal_s3fs.rm(f's3://{self.basket_path}', recursive = True)
        self.temp_dir.cleanup()

    def teardown_class(cls):
        cls.opal_s3fs.rm(cls.test_bucket, recursive = True)

    # test if dir path is
    # test if local_dir_path exists
    def test_upload_basket_local_dirpath_exists(self):
        local_dir_path = 'n o t a r e a l p a t h'
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"

        with pytest.raises(ValueError, match =
                           f"'local_dir_path' must be a valid directory: '{local_dir_path}'"):
            upload_basket(local_dir_path, upload_path, unique_id, self.basket_type)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    def test_upload_basket_local_dirpath_is_file(self):
        # Create basket
        json_path = os.path.join(self.temp_dir_path, "sample.json")
        local_dir_path = json_path
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"
        json_data = {'t': [1,2,3]}
        with open(json_path, "w") as outfile:
            json.dump(json_data, outfile)

        with pytest.raises(ValueError, match =
                           f"'local_dir_path' must be a valid directory: '{local_dir_path}'"):
            upload_basket(local_dir_path, upload_path, unique_id,
                         self.basket_type)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    # check if upload_path is a string
    def test_upload_basket_upload_path_is_string(self):
        local_dir_path = self.temp_dir_path
        unique_id = uuid.uuid1().int
        upload_path = 1234

        with pytest.raises(TypeError, match =
                           f"'upload_directory' must be a string: '{upload_path}'"):
            upload_basket(local_dir_path, upload_path, unique_id, self.basket_type)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    # check if unique_id is an int
    def test_upload_basket_unique_id_int(self):
        local_dir_path = self.temp_dir_path
        unique_id = "fake id"
        upload_path = f"{self.basket_path}/{unique_id}"

        with pytest.raises(TypeError, match = f"'unique_id' must be an int: '{unique_id}'"):
            upload_basket(local_dir_path, upload_path, unique_id, self.basket_type)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    # check if basket_type is a string
    def test_upload_basket_type_is_string(self):
        local_dir_path = self.temp_dir_path
        unique_id = uuid.uuid1().int
        basket_type = 1234
        upload_path = f"{self.test_bucket}/{str(basket_type)}/{unique_id}"

        with pytest.raises(TypeError, match = f"'basket_type' must be a string: '{basket_type}'"):
            upload_basket(local_dir_path, upload_path, unique_id, basket_type)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    # check if parent_ids is a list of ints
    def test_upload_basket_parent_ids_list_int(self):
        local_dir_path = self.temp_dir_path
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"
        parent_ids_in = ['a', 3]

        with pytest.raises(TypeError, match = f"'parent_ids' must be a list of int:"):
            upload_basket(local_dir_path, upload_path, unique_id,
                          self.basket_type, parent_ids=parent_ids_in)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    # check if parent_ids is a list
    def test_upload_basket_parent_ids_is_list(self):
        local_dir_path = self.temp_dir_path
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"
        parent_ids_in = 56

        with pytest.raises(TypeError, match = f"'parent_ids' must be a list of int:"):
            upload_basket(local_dir_path, upload_path, unique_id,
                          self.basket_type, parent_ids=parent_ids_in)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    # check if metadata is a dictionary
    def test_upload_basket_metadata_is_dictionary(self):
        local_dir_path = self.temp_dir_path
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"
        metadata_in = 'invalid'

        with pytest.raises(TypeError, match = f"'metadata' must be a dictionary: '{metadata_in}'"):
            upload_basket(local_dir_path, upload_path, unique_id,
                          self.basket_type, metadata=metadata_in)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    # check if label is string
    def test_upload_basket_label_is_string(self):
        local_dir_path = self.temp_dir_path
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"
        label_in = 1234

        with pytest.raises(TypeError, match = f"'label' must be a string: '{label_in}'"):
            upload_basket(local_dir_path, upload_path, unique_id,
                          self.basket_type, label=label_in)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    def test_upload_basket_successful_run(self):

        # Create basket
        local_dir_path = self.temp_dir_path
        json_path = os.path.join(local_dir_path, "sample.json")
        json_data = {'t': [1,2,3]}
        with open(json_path, "w") as outfile:
            json.dump(json_data, outfile)

        original_files = os.listdir(local_dir_path)

        # Run upload_basket
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"
        label_in = 'note'
        metadata_in = {'metadata': [1,2,3]}
        parent_ids_in = [5,4,3,2]

        upload_basket(local_dir_path, upload_path, unique_id,
                     self.basket_type, parent_ids = parent_ids_in,
                     metadata = metadata_in, label=label_in)

        # Assert original local path hasn't been altered
        with open(json_path, 'r') as file:
            data = json.load(file)
            assert data == json_data
        assert original_files == os.listdir(local_dir_path)

        # Assert basket.json fields
        with self.opal_s3fs.open(f's3://{upload_path}/basket_manifest.json', 'rb') as file:
            basket_json = json.load(file)
            assert basket_json['uuid'] == unique_id
            assert basket_json['parent_uuids'] == parent_ids_in
            assert basket_json['basket_type'] == self.basket_type
            assert basket_json['label'] == label_in
            assert 'upload_time' in basket_json.keys()

        # Assert metadata.json fields
        with self.opal_s3fs.open(f's3://{upload_path}/metadata.json', 'rb') as file:
            assert json.load(file) == metadata_in

        # Assert sample.json
        with self.opal_s3fs.open(f's3://{upload_path}/sample.json', 'rb') as file:
            assert json.load(file) == json_data

    def test_upload_basket_no_metadata(self):
        # Create basket
        local_dir_path = self.temp_dir_path
        json_path = os.path.join(local_dir_path, "sample.json")
        json_data = {'t': [1,2,3]}
        with open(json_path, "w") as outfile:
            json.dump(json_data, outfile)

        # Run upload_basket
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"

        upload_basket(local_dir_path, upload_path, unique_id,
                     self.basket_type)

        # Assert metadata.json was not written
        assert self.opal_s3fs.ls(f's3://{upload_path}/metadata.json') == []

    def test_upload_basket_check_existing_upload_path(self):
        # Create basket
        local_dir_path = self.temp_dir_path
        json_path = os.path.join(local_dir_path, "sample.json")
        json_data = {'t': [1,2,3]}
        with open(json_path, "w") as outfile:
            json.dump(json_data, outfile)

        # Run upload_basket
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"

        self.opal_s3fs.upload(local_dir_path,
                         f's3://{upload_path}', 
                         recursive = True)

        with pytest.raises(FileExistsError,
                           match = f"'upload_directory' already exists: '{upload_path}''"):
            upload_basket(local_dir_path, upload_path, unique_id, self.basket_type)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == [upload_path]

    def test_upload_basket_invalid_upload_path(self):
        local_dir_path = self.temp_dir_path
        unique_id = uuid.uuid1().int
        upload_path = ";invalid_path"

        with pytest.raises(botocore.exceptions.ParamValidationError,
                           match = f"Invalid bucket name"):
            upload_basket(local_dir_path, upload_path,
                         unique_id, self.basket_type)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    def test_upload_basket_clean_up_on_error(self):
        local_dir_path = self.temp_dir_path
        json_path = os.path.join(local_dir_path, "sample.json")
        json_data = {'t': [1,2,3]}
        with open(json_path, "w") as outfile:
            json.dump(json_data, outfile)

        # Run upload_basket
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"

        with pytest.raises(Exception,
                           match = "Test Clean Up"):
            upload_basket(local_dir_path, upload_path,
                          unique_id, self.basket_type,
                         test_clean_up = True)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []
        
    def test_upload_basket_invalid_optional_argument(self):
        local_dir_path = self.temp_dir_path
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"
        invalid_arg = 'invalid_arg'
        
        with pytest.raises(KeyError,
                           match = "Invalid kwargs argument: 'junk'"):
            upload_basket(local_dir_path, upload_path,
                         unique_id, self.basket_type,
                         junk = True)

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []

    def test_upload_basket_invalid_test_clean_up_datatype(self):
        local_dir_path = self.temp_dir_path
        unique_id = uuid.uuid1().int
        upload_path = f"{self.basket_path}/{unique_id}"
        invalid_arg = 'invalid_arg'

        with pytest.raises(TypeError,
                           match = f"Invalid datatype: 'test_clean_up: must be type <class \'bool\'>'"):
            upload_basket(local_dir_path, upload_path,
                         unique_id, self.basket_type,
                         test_clean_up = 'a')

        assert self.opal_s3fs.ls(f's3://{self.basket_path}') == []
