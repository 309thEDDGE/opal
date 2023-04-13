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
    '''Defines a flow to upload NASA ch10 files.'''

    n = metaflow.Parameter(
        "n",
        help="Number of Parsed 1553 items to translate and upload.",
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
        translate_metadata = {}
        meta_file = os.path.join(self.local_translate_path, "parsed_data_translated", "_metadata.yaml")
        
        if not os.path.exists(meta_file):
            raise FileNotFoundError(f'Translate metadata not found: {meta_file}')
                                    
        with open(meta_file) as f:
            metadata = yaml.safe_load(f)
            translate_metadata['translate_metadata'] = metadata
            
        return translate_metadata

    @step
    def start(self):
        """Create empty temporary directory for parsed and translated data.
        
        """
        #create temporary directory to put data files locally
        self.local_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.local_dir.name
        self.dts_folder = os.path.join(self.local_dir_path, 'local_dts_folder')
        os.mkdir(self.dts_folder)

        self.next(self.get_dts_file)
        
    @step
    def get_dts_file(self):
        
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
        '''
        '''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = {'endpoint_url': os.environ['S3_ENDPOINT']})
        if not opal_s3fs.exists(self.bucket_name):
            raise FileNotFoundError(f"Specified Bucket Not Found: {self.bucket_name}")
        
        index = create_index_from_s3(self.bucket_name, 'schema.json')
        ch10_index = index[index['basket_type'] == 'ch10_parsed']
        self.ch10_parsed_baskets = ch10_index['address']

        if self.n is not None:
            self.ch10_baskets = self.ch10_parsed_baskets[:self.n]

        num_baskets = len(self.ch10_parsed_baskets)
        count = 1
        for basket in self.ch10_parsed_baskets:            
            basket_contents = opal_s3fs.ls(basket)
            
            #check that there is one parsed path and get the path to it
            s3_parsed_path = [x for x in basket_contents if '1553' in x and x.endswith('.parquet')]
            if len(s3_parsed_path) != 1: 
                print(f'Basket does not have parsed data, skipping: {basket}')
                continue
                
            s3_parsed_path = s3_parsed_path[0]
            
            local_parsed_dir = os.path.join(self.local_dir_path, 'parsed_data.parquet')
            os.mkdir(local_parsed_dir)
            
            opal_s3fs.get(s3_parsed_path, local_parsed_dir, recursive = True)
            self.local_translate_path = os.path.join(self.local_dir_path, 'translated_output')
            os.mkdir(self.local_translate_path)
            
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
            
            print(f'-- translating {count} of {num_baskets}: {ch10_name}')
            count += 1
            
            # run tip translate
            subprocess.run(
                [
                    "tip_translate_1553",
                    local_parsed_dir,
                    self.local_dts_path,
                    "-L",
                    "off",
                    "--output_path",
                    self.local_translate_path
                ]
            )
            
            translate_metadata = self.extract_metadata()
            translate_metadata['ch10name'] = ch10_name

            #build upload_dicts
            upload_dicts = []
            for f in os.scandir(self.local_translate_path):
                upload_dicts.append({'path':f.path,'stub':False})
            
            self.metaflow_upload_basket(upload_dicts,
                                        self.bucket_name,
                                       'ch10_translated',
                                        label = ch10_name,
                                        metadata = translate_metadata)

            shutil.rmtree(self.local_translate_path)
            shutil.rmtree(local_parsed_dir)

        self.next(self.end)

    @card
    @step
    def end(self):
        '''Cleanup temporary directory and satisfy metaflow'''
        self.local_dir.cleanup()
        print("All Done")

if __name__ == "__main__":
    TranslateNASA1553Flow()
