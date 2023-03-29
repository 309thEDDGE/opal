import pytest
import s3fs
import tempfile
import os
import json
import pandas as pd
from opal.weave.create_index import create_index_from_s3
from opal.weave.uploader import upload_basket

class TestCreateIndex():
    """
    A class for to test functions in create_index.py
    """
    def setup_class(self):
        '''
        create file locally, upload basket, delete file locally
        '''
        self.opal_s3fs = s3fs.S3FileSystem(client_kwargs=
                                          {"endpoint_url": os.environ["S3_ENDPOINT"]})

        self.basket_type = 'test_basket_type'
        self.test_bucket = 'index-testing-bucket'

        #make sure minio bucket doesn't exist. if it does, delete it.
        if self.opal_s3fs.exists(f's3://{self.test_bucket}'):
            self.opal_s3fs.rm(f's3://{self.test_bucket}', recursive = True)

        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name

        #make something to put in basket
        file_path = os.path.join(self.local_dir_path, "sample.txt")
        with open(file_path, "w") as f:
            f.write('this is a test file')

        upload_basket(self.local_dir_path, f'{self.test_bucket}/{self.basket_type}/1234', 1234,
                      self.basket_type, [1111,2222], label = 'my label')

        #make schema.json for testing purposes
        self.schema_path = os.path.join(self.local_dir_path, 'schema.json')
        with open(self.schema_path, 'w') as f:
            json.dump(['uuid', 'upload_time', 'parent_uuids', 'basket_type', 'label'], f)



    def teardown_class(self):
        '''
        remove baskets from s3
        '''
        self.opal_s3fs.rm(f's3://{self.test_bucket}', recursive = True)
        self.temp_dir.cleanup()
        
    def test_root_dir_is_string(self):
        with pytest.raises(TypeError, match =
                           f"'root_dir' must be a string"):
            create_index_from_s3(765,self.schema_path)
            
    def test_schema_path_is_string(self):
        with pytest.raises(TypeError, match =
                           f"'schema_path' must be a string"):
            create_index_from_s3(f'{self.test_bucket}',4321)

    def test_correct_index(self):
        '''
        just use the data uploaded and create and index and check that it's right
        '''
        truth_index_dict = {'uuid': '1234',
                 'upload_time': 1679335295759652,
                 'parent_uuids': [[1111, 2222]],
                 'basket_type': 'test_basket_type',
                 'label': 'my label',
                 'address': 's3://index-testing-bucket/test_basket_type/1234/basket_manifest.json',
                 'storage_type': 's3'}
        truth_index = pd.DataFrame(truth_index_dict)

        minio_index = create_index_from_s3(f'{self.test_bucket}',self.schema_path)

        #check that the indexes match, ignoring 'upload_time'
        assert (truth_index == minio_index).drop(columns = ['upload_time']).all().all()


    def test_create_index_with_wrong_keys(self):
        '''
        upload a basket with a basket_details.json with incorrect keys.
        check that correct error is thrown. delete said basket from s3
        '''

        #make something to put in basket
        file_path = os.path.join(self.local_dir_path, "sample.txt")
        with open(file_path, "w") as f:
            f.write('this is another test file')
    
        upload_basket(self.local_dir_path, f'{self.test_bucket}/{self.basket_type}/5678',
                      5678, self.basket_type, [3333], label = 'my label')

        #change a key in this basket_manifest.json
        basket_dict = {}
        with self.opal_s3fs.open(f'{self.test_bucket}/{self.basket_type}/5678/basket_manifest.json', 'rb') as f:
            basket_dict = json.load(f)
            basket_dict.pop('uuid')
        basket_path = os.path.join(self.local_dir_path, 'basket_manifest.json')
        with open(basket_path, 'w') as f:
            json.dump(basket_dict, f)
        self.opal_s3fs.upload(basket_path, f'{self.test_bucket}/{self.basket_type}/5678/basket_manifest.json')

        with pytest.raises(ValueError, match = 'basket found at'):
            minio_index = create_index_from_s3(f'{self.test_bucket}',self.schema_path)

    def test_root_dir_does_not_exist(self):
        '''
        try to create an index in a bucket that doesn't exist, check that it throws an exception
        '''
        with pytest.raises(FileNotFoundError, match = 'The specified bucket does not exist'):
            minio_index = create_index_from_s3('NOT-A-BUCKET',self.schema_path)

    def test_schema_path_does_not_exist(self):
        '''
        run create index with an invalid schema path, check that it throws an exception
        '''
        with pytest.raises(FileNotFoundError, match = 'No such file or directory'):
            minio_index = create_index_from_s3(f'{self.test_bucket}','NOT-A-SCHEMA')
