from opal.weave.create_index import create_index_from_s3
from opal.weave.uploader import upload_basket
import pytest
import s3fs
import tempfile
import os
import pandas as pd
import json

class TestCreateIndex():
    def setup_class(cls):
        #create file locally, upload basket, delete file locally
        cls.opal_s3fs = s3fs.S3FileSystem(client_kwargs=
                                          {"endpoint_url": os.environ["S3_ENDPOINT"]})
        
        cls.basket_type = 'test_basket_type'
        cls.test_bucket = 'index-testing-bucket'
        
        #make sure minio bucket doesn't exist. if it does, delete it. 
        if cls.opal_s3fs.exists(f's3://{cls.test_bucket}'):
            cls.opal_s3fs.rm(f's3://{cls.test_bucket}', recursive = True)
        
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.local_dir_path = cls.temp_dir.name
        
        #make something to put in basket
        file_path = os.path.join(cls.local_dir_path, "sample.txt")
        with open(file_path, "w") as f:
            f.write('this is a test file')
        
        upload_basket(cls.local_dir_path, f'{cls.test_bucket}/{cls.basket_type}/1234', 1234, cls.basket_type, [1111,2222], label = 'my label')
        
        #make schema.json for testing purposes
        cls.schema_path = os.path.join(cls.local_dir_path, 'schema.json')
        with open(cls.schema_path, 'w') as f:
            json.dump(['uuid', 'upload_time', 'parent_uuids', 'basket_type', 'label'], f)
        
        
        
    def teardown_class(cls):
        #remove baskets from s3
        cls.opal_s3fs.rm(f's3://{cls.test_bucket}', recursive = True)
        cls.temp_dir.cleanup()
        
    def test_correct_index(self):
        #just use the data uploaded and create and index and check that it's right
        truth_index_dict = {'uuid': ['1234'],
                             'upload_time': [1679335295759652],
                             'parent_uuids': [[1111, 2222]],
                             'basket_type': ['test_basket_type'],
                             'label': ['my label'],
                             'address': ['s3://index-testing-bucket/test_basket_type/1234/basket_manifest.json'],
                             'storage_type': ['s3']}
        truth_index = pd.DataFrame(truth_index_dict)
        
        minio_index = create_index_from_s3(f'{self.test_bucket}',self.schema_path)
        
        #check that the indexes match, ignoring 'upload_time'
        assert (truth_index == minio_index).drop(columns = ['upload_time']).all().all()
        
        
    def test_create_index_with_wrong_keys(self):
        #upload a basket with a basket_details.json with incorrect keys. check that correct error is thrown. delete said basket from s3
        
        #make something to put in basket
        file_path = os.path.join(self.local_dir_path, "sample.txt")
        with open(file_path, "w") as f:
            f.write('this is another test file')
            
        upload_basket(self.local_dir_path, f'{self.test_bucket}/{self.basket_type}/5678', 5678, self.basket_type, [3333], label = 'my label')
        
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
        #try to create an index in a bucket that doesn't exist, check that it throws an exception
        with pytest.raises(FileNotFoundError, match = 'The specified bucket does not exist'):
            minio_index = create_index_from_s3('NOT-A-BUCKET',self.schema_path)
        
    def test_schema_path_does_not_exist(self):
        #run create index with an invalid schema path, check that it throws an exception
        with pytest.raises(FileNotFoundError, match = 'No such file or directory'):
            minio_index = create_index_from_s3(f'{self.test_bucket}','NOT-A-SCHEMA')
        
    