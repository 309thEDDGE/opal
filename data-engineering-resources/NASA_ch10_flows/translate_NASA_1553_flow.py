import os
import s3fs
import subprocess
import yaml
import pandas as pd
import tempfile
import metaflow
import json
import shutil
from metaflow import step, card
import opal.flow
from opal.weave.create_index import create_index_from_s3

class TranslateNASA1553Flow(opal.flow.OpalFlowSpec):
    '''Defines a flow to translate parsed NASA ch10 files.'''

    n = metaflow.Parameter(
        "n",
        help="Number of ch10_parsed baskets to translate and upload. "
             "Default is to translate all parsed baskets.",
        required=False,
        default=None,
        type=int
    )

    bucket_name = metaflow.Parameter(
        "bucket_name",
        help="Name of the s3 bucket where data will be uploaded.",
        required=False,
        default='basket-data'
    )

    def extract_metadata(self):
        '''Gather metadata from translation.'''
        translate_metadata = {}
        meta_file = os.path.join(self.local_translate_path, "parsed_data_translated", "_metadata.yaml")
        
        if not os.path.exists(meta_file):
            raise FileNotFoundError(f'Translate metadata not found: {meta_file}')
                                    
        with open(meta_file) as f:
            metadata = yaml.safe_load(f)
            translate_metadata['translate_metadata'] = metadata
            
        return translate_metadata
    
    def translate_basket(self, basket):
        '''Translate parsed 1553 data and extract metadata.
        
        Given a parsed 1553 basket, check that there exists only one 1553.parquet
        in that basket. Then download the basket to a temporary directory, and translate
        said parsed data. Extract and return the generated metadata.
        
        Parameters
        ----------
        basket (str): path to a basket of type ch10_parsed in s3.
        
        Returns
        -------
        translate_metadata (dict): metadata generated during tip_translate.
        '''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = {'endpoint_url': os.environ['S3_ENDPOINT']})
        
        basket_contents = opal_s3fs.ls(basket)

        #check that there is one parsed path and get the path to it
        s3_parsed_path = [x for x in basket_contents if '1553' in x and x.endswith('.parquet')]
        if len(s3_parsed_path) != 1: 
            raise Exception(f'Basket does not have parsed data, skipping: {basket}')

        s3_parsed_path = s3_parsed_path[0]

        self.local_parsed_dir = os.path.join(self.local_dir_path, 'parsed_data.parquet')
        os.mkdir(self.local_parsed_dir)

        opal_s3fs.get(s3_parsed_path, self.local_parsed_dir, recursive = True)
        self.local_translate_path = os.path.join(self.local_dir_path, 'translated_output')
        os.mkdir(self.local_translate_path)

        # run tip translate
        subprocess.run(
            [
                "tip_translate_1553",
                self.local_parsed_dir,
                self.local_dts_path,
                "-L",
                "off",
                "--output_path",
                self.local_translate_path
            ]
        )
        
        return self.extract_metadata()
    
    def upload_translate_basket(self, basket, translate_metadata):
        '''Upload translated data to MinIO.
        
        From s3, get the name of the ch10 and uuid of the parent basket. Then 
        Upload the basket with the below information.
        
        Parameters
        ----------
        basket (str): path to a basket of type ch10_parsed in s3.
        translate_metadata (dict): all metadata generated during tip_translate.
                             
        Returns
        -------
        basket_upload_path (str): path to where the basket was uploaded,
                                  returned from self.metaflow_upload_basket
        '''  
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = {'endpoint_url': os.environ['S3_ENDPOINT']})
        
        manifest_data = {}
        manifest_path = os.path.join(basket, 'basket_manifest.json')
        with opal_s3fs.open(manifest_path, 'rb') as file:
            manifest_data = json.load(file)

        self.parsed_id = manifest_data['uuid']
        parent_ids = [self.parsed_id, self.dts_id]

        parsed_metadata = {}
        metadata_path = os.path.join(basket, 'basket_metadata.json')
        with opal_s3fs.open(metadata_path, 'rb') as file:
            parsed_metadata = json.load(file)

        ch10_name = parsed_metadata['ch10name']
        
        translate_metadata['ch10name'] = ch10_name

        #build upload_dicts
        upload_dicts = []
        for f in os.scandir(self.local_translate_path):
            upload_dicts.append({'path':f.path,'stub':False})

        return self.metaflow_upload_basket( upload_dicts,
                                            self.bucket_name,
                                           'ch10_translated_1553',
                                            label = ch10_name,
                                            metadata = translate_metadata)

    @step
    def start(self):
        '''Create empty temporary directory for parsed and translated data.'''
        #create temporary directory to put data files locally
        self.local_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.local_dir.name
        self.dts_folder = os.path.join(self.local_dir_path, 'local_dts_folder')
        os.mkdir(self.dts_folder)

        self.next(self.get_dts_file)
        
    @step
    def get_dts_file(self):
        '''Get latest DTS file from S3 and save locally for translation.'''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = {'endpoint_url': os.environ['S3_ENDPOINT']})
        
        index = create_index_from_s3(self.bucket_name, 'schema.json')
        dts_index = index[index['basket_type'] == 'DTS_1553_NASA'].copy()
        dts_index['time'] = pd.to_datetime(dts_index['upload_time'], format='%m/%d/%Y %H:%M:%S')
        dts_basket_address = dts_index.loc[dts_index['time'].idxmax()]['address']
        
        basket_contents = opal_s3fs.ls(dts_basket_address)
        
        dts_file_path = [x for x in basket_contents if x.endswith('.yaml')]
        if len(dts_file_path) != 1: 
            raise ValueError(f'Could not find DTS file: {dts_basket_address}')

        self.s3_dts_file_path = dts_file_path[0]
        self.local_dts_path = os.path.join(self.dts_folder, os.path.basename(self.s3_dts_file_path))
        
        opal_s3fs.get(self.s3_dts_file_path, self.local_dts_path)
        
        manifest_data = {}
        manifest_path = os.path.join(dts_basket_address, 'basket_manifest.json')
        with opal_s3fs.open(manifest_path, 'rb') as file:
            manifest_data = json.load(file)
            self.dts_id = manifest_data['uuid']
                
        self.next(self.translate_parsed_1553)
        
    @step
    def translate_parsed_1553(self):
        '''Translate ch10_parsed data from S3 and upload translated data as a ch10_translated_1553 basket'''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = {'endpoint_url': os.environ['S3_ENDPOINT']})
        
        if not opal_s3fs.exists(self.bucket_name):
            raise FileNotFoundError(f"Specified Bucket Not Found: {self.bucket_name}")
        
        index = create_index_from_s3(self.bucket_name, 'schema.json')
        ch10_index = index[index['basket_type'] == 'ch10_parsed']
        self.ch10_parsed_baskets = ch10_index['address']

        if self.n is not None:
            self.ch10_parsed_baskets = self.ch10_parsed_baskets[:self.n]

        num_baskets = len(self.ch10_parsed_baskets)
        count = 1
        for i, basket in enumerate(self.ch10_parsed_baskets):
            try:
                print(f'-- translating {i} of {num_baskets}: {basket}')
                
                translate_metadata = self.translate_basket(basket)

                basket_upload_path = self.upload_translate_basket(basket, translate_metadata)
                        
                print(f'basket successfully parsed and uploaded: {basket_upload_path}')

            except Exception as e:
                print(e)
                print(f'translation failed: {basket})')
                
            finally:
                if os.path.exists(self.local_translate_path):
                    shutil.rmtree(self.local_translate_path)
                if os.path.exists(self.local_parsed_dir):
                    shutil.rmtree(self.local_parsed_dir)

        self.next(self.end)

    @card
    @step
    def end(self):
        '''Cleanup temporary directory and satisfy metaflow'''
        self.local_dir.cleanup()
        print("All Done")

if __name__ == "__main__":
    TranslateNASA1553Flow()
