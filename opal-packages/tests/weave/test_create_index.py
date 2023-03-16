from opal.weave.create_index import create_index_from_s3
from opal.weave.weave import upload_basket
import pytest
import s3fs
import tempfile
import os

class TestCreateIndex():
    def setup_class(cls):
        #create file locally, upload basket, delete file locally
        cls.opal_s3fs = s3fs.S3FileSystem(client_kwargs=
                                          {"endpoint_url": os.environ["S3_ENDPOINT"]})
        
        cls.basket_type = 'test_basket_type'
        cls.test_bucket = 'index-testing-bucket'
        
        temp_dir = tempfile.TemporaryDirectory()
        local_dir_path = temp_dir.name
        print(local_dir_path)
        file_path = os.path.join(local_dir_path, "sample.txt")
        with open(file_path, "w") as outfile:
            outfile.write('this is a test file')
        
        upload_basket(local_dir_path, f'{cls.test_bucket}/{cls.basket_type}/1234', 1234, cls.basket_type, [1111,2222], label = 'my label')
        
        temp_dir.cleanup()
        
        
    def teardown_class(cls):
        #remove baskets from s3
        cls.opal_s3fs.rm(f's3://{cls.test_bucket}', recursive = True)
        
    def test_correct_index(self):
        assert True
        #just use the data uploaded and create and index and check that it's right
        
    def test_create_index_with_wrong_keys(self):
        assert True
        #upload a basket with a basket_details.json with incorrect keys. check that correct error is thrown. delete said basket from s3
        
    def test_root_dir_does_not_exist(self):
        assert True
        #try to create an index in a bucket that doesn't exist, check that it throws an exception
        
    def test_schema_path_does_not_exist(self):
        assert True
        #run create index with an invalid schema path, check that it throws an exception
        
    