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

class NASAch10TranslateFlow(opal.flow.OpalFlowSpec):
    '''Defines a flow to translate parsed NASA ch10 files.''' 
    
    ''' Translate options provide the tip executable to be used 
        to translate (first item in the list) and the end part of 
        the parsed parquet file to look for in the basket to 
        be used for translation (second item in the list).    
    '''
    translate_options = {
        "MILSTD1553": ["tip_translate_1553", "_MILSTD1553_F1.parquet"],
        "ARINC429": ["tip_translate_arinc429", "_ARINC429_F0.parquet"],
    }
    
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
        help="Name of the s3 bucket where parsed data is stored." 
             " Translated data will also be uploaded to this same bucket",
        required=False,
        default='basket-data'
    )
    
    data_type = metaflow.Parameter(
        "data_type",
        help="Type of data to be translated. 'MILSTD1553' or 'ARINC429'",
        required=False,
        default='MILSTD1553',
        type=str
    )

    def extract_metadata(self):
        '''Gather metadata from translation.'''
        translate_metadata = {}
        meta_file = os.path.join(self.local_translate_path, 
                                 "parsed_data_translated", 
                                 "_metadata.yaml")
        
        if not os.path.exists(meta_file):
            raise FileNotFoundError(f'Translate metadata not found: {meta_file}')
                                    
        with open(meta_file) as f:
            metadata = yaml.safe_load(f)
            translate_metadata['translate_metadata'] = metadata
            
        return translate_metadata
    
    def translate_basket(self, basket):
        '''Translate parsed data and extract metadata.
        
        Given a parsed basket of the type given from the <type> parameter, 
        check that <type>.parquet exists. Then download the basket to a 
        temporary directory, and translate said parsed data. Extract and 
        return the generated metadata.
        
        Parameters
        ----------
        basket (str): path to a basket of type ch10_parsed in s3.
        
        Returns
        -------
        translate_metadata (dict): metadata generated during tip_translate.
        '''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = 
                                      {'endpoint_url': os.environ['S3_ENDPOINT']})
        
        parsed_metadata = {}
        metadata_path = os.path.join(basket, 'basket_metadata.json')
        with opal_s3fs.open(metadata_path, 'rb') as file:
            parsed_metadata = json.load(file)

        self.ch10_name = parsed_metadata['ch10name']
        
        s3_parsed_path = f"{basket}/{self.ch10_name}" \
                         f"{self.translate_options[self.data_type][1]}"
        if not opal_s3fs.exists(s3_parsed_path):
            raise Exception(f'Parsed data does not exist {s3_parsed_path}' \
                            f', skipping: {basket}')

        self.local_parsed_dir = os.path.join(self.local_dir_path, 
                                             'parsed_data.parquet')
        os.mkdir(self.local_parsed_dir)

        opal_s3fs.get(s3_parsed_path, self.local_parsed_dir, recursive = True)
        self.local_translate_path = os.path.join(self.local_dir_path, 
                                                 'translated_output')
        os.mkdir(self.local_translate_path)

        tip_exec = self.translate_options[self.data_type][0]
        
        # run tip translate
        # "-L off" turns off std out logs
        # "--thread_count" sets the translator to run in parallel
        # "--output_path" specifies the path for translated output
        subprocess.run(
            [
                tip_exec,
                self.local_parsed_dir,
                self.local_dts_path,
                "-L",
                "off",
                "--thread_count",
                "3",
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
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = 
                                      {'endpoint_url': os.environ['S3_ENDPOINT']})
        
        manifest_data = {}
        manifest_path = os.path.join(basket, 'basket_manifest.json')
        with opal_s3fs.open(manifest_path, 'rb') as file:
            manifest_data = json.load(file)

        self.parsed_id = manifest_data['uuid']
        parent_ids = [self.parsed_id, self.dts_id]
        
        translate_metadata['ch10name'] = self.ch10_name

        #build upload_dicts
        upload_dicts = []
        for f in os.scandir(self.local_translate_path):
            upload_dicts.append({'path':f.path,'stub':False})

        return self.metaflow_upload_basket( upload_dicts,
                                            self.bucket_name,
                                            f'ch10_translated_{self.data_type}',
                                            label = self.ch10_name,
                                            parent_ids = parent_ids,
                                            metadata = translate_metadata)

    @step
    def start(self):
        '''Sanitize inputs and create temporary directories.'''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = 
                                      {'endpoint_url': os.environ['S3_ENDPOINT']})
        
        # sanitize inputs
        if self.n is not None:
            if self.n < 0:
                raise ValueError(f'n is required to be > 0: n = {self.n}')
                
        if self.data_type not in self.translate_options.keys():
            raise ValueError(f"data_type must be any of {self.data_type}, " \
                             f"data_type = {self.data_type}")
                
        if not opal_s3fs.exists(self.bucket_name):
            raise FileNotFoundError(f"bucket_name does not exist: " \
                                    f"bucket_name = {self.bucket_name}")
                
        # create temporary directory to put data files locally
        self.local_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.local_dir.name
        self.dts_folder = os.path.join(self.local_dir_path, 'local_dts_folder')
        os.mkdir(self.dts_folder)
        self.basket_index = create_index_from_s3(self.bucket_name, 'schema.json')
        self.next(self.get_dts_file)
        
    @step
    def get_dts_file(self):
        '''Get latest DTS file from S3 and save locally for translation.'''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = 
                                      {'endpoint_url': os.environ['S3_ENDPOINT']})
        
        dts_index = self.basket_index[self.basket_index['basket_type'] == 
                                      f'NASA_{self.data_type}_DTS'].copy()
        
        dts_index['time'] = pd.to_datetime(dts_index['upload_time'], 
                                           format='%m/%d/%Y %H:%M:%S')
        
        dts_basket_address = dts_index.loc[dts_index['time'].idxmax()]['address']
        
        s3_dts_path = f'{dts_basket_address}/NASA_{self.data_type}_DTS.yaml'
        if not opal_s3fs.exists(s3_dts_path):
            raise Exception(f'DTS file does not exist {s3_dts_path}')

        self.s3_dts_path = s3_dts_path
        self.local_dts_path = os.path.join(self.dts_folder, 
                                           os.path.basename(self.s3_dts_path))
        
        opal_s3fs.get(self.s3_dts_path, self.local_dts_path)
        
        manifest_data = {}
        manifest_path = os.path.join(dts_basket_address, 'basket_manifest.json')
        with opal_s3fs.open(manifest_path, 'rb') as file:
            manifest_data = json.load(file)
            self.dts_id = manifest_data['uuid']
                
        self.next(self.translate_parsed)
        
    @step
    def translate_parsed(self):
        '''Translate ch10_parsed data from S3.
        
            Download ch10_parsed data from S3, then translate
            and upload translated data as a ch10_translated_<type> 
            basket.
        '''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = 
                                      {'endpoint_url': os.environ['S3_ENDPOINT']})
        
        if not opal_s3fs.exists(self.bucket_name):
            raise FileNotFoundError(f"Specified Bucket Not Found: " \
                                    f"{self.bucket_name}")        
        ch10_index = self.basket_index[self.basket_index['basket_type'] == 
                                       'ch10_parsed']
        self.ch10_parsed_baskets = ch10_index['address']

        if self.n is not None:
            self.ch10_parsed_baskets = self.ch10_parsed_baskets[:self.n]

        num_baskets = len(self.ch10_parsed_baskets)
        for i, basket in enumerate(self.ch10_parsed_baskets):
            try:
                print(f'-- translating {i + 1} of {num_baskets}: {basket}')
                
                translate_metadata = self.translate_basket(basket)

                basket_upload_path = self.upload_translate_basket(basket, 
                                                                  translate_metadata)
                        
                print(f"basket successfully translated and uploaded: " \
                      f" {basket_upload_path}")

            except Exception as e:
                print(e)
                print(f'translation failed: {basket})')
                
            finally:
                if hasattr(self, 'local_translate_path'):
                    if os.path.exists(self.local_translate_path):
                        shutil.rmtree(self.local_translate_path)
                if hasattr(self, 'local_parsed_dir'):
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
    NASAch10TranslateFlow()
