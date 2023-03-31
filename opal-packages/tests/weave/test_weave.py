from opal.weave.weave import upload

class TestUpload():
    def setup_class(self):
        self.opal_s3fs = s3fs.S3FileSystem(client_kwargs=
                                          {"endpoint_url": os.environ["S3_ENDPOINT"]})
        self.basket_type = 'test_basket_type'
        self.bucket_name = 'weave_test_bucket'
        
        #make sure minio bucket doesn't exist. if it does, delete it.
        if self.opal_s3fs.exists(f's3://{self.bucket_name}'):
            self.opal_s3fs.rm(f's3://{self.bucket_name}', recursive = True)
        
        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name
        self.data_file_path = os.path.join(self.local_dir_path, 'test.txt')
        with open(self.data_file_path, 'w') as f:
            f.write('0123456789')
            
        self.upload_items = [{
                                'path': self.data_file_path,
                                'stub': False
        }]
        
    def teardown_class(self):
        self.temp_dir.cleanup()
        self.opal_s3fs.rm(f's3://{self.bucket_name}', recursive = True)
        
    def test_upload_successful_run():
        pass
    
    def test_upload_bucket_name_is_string():
        bucket_name = 7
        
        with pytest.raises(TypeError, match = f"'bucket_name' must be a string: '{bucket_name}'"):
            upload(self.upload_items, bucket_name, self.basket_type)
    
    
        