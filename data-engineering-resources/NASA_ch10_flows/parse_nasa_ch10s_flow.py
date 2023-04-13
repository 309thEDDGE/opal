import os
import shutil
import s3fs
import tempfile
import subprocess
import yaml
import json
import metaflow
from metaflow import step, card
import opal.flow
from opal.weave.create_index import create_index_from_s3

class NASAch10ParseFlow(opal.flow.OpalFlowSpec):
    '''Defines a flow to parse NASA ch10 files.'''

    n = metaflow.Parameter(
        "n",
        help="Number of ch10s to parse.",
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
        """Gather tip metadata into the metaflow artifact object."""
        # scan for metadata files, save the output as a dict in tip_metadata
        tip_metadata = {}
        for de in os.scandir(self.local_dir_path):
            # look for _metadata.yaml under *.parquet folders
            if de.is_dir() and de.path.endswith(".parquet"):
                meta_file = os.path.join(de.path, "_metadata.yaml")
                if os.path.exists(meta_file):
                    # load the yaml and save it in metaflow
                    with open(meta_file) as f:
                        metadata = yaml.safe_load(f)
                        tip_metadata[metadata["type"]] = metadata

        # we should find at least one
        if not tip_metadata:
            raise Exception("No tip metadata file found. Tip might be broken.")
            
        return tip_metadata

    @step
    def start(self):
        """Create empty temporary directory for ch10 storage."""
        #create temporary directory to put data files locally
        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name

        self.next(self.parse_ch10s)

    @step
    def parse_ch10s(self):
        '''Download ch10, parse it, upload output of tip_parse'''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = {'endpoint_url': os.environ['S3_ENDPOINT']})
        if not opal_s3fs.exists(self.bucket_name):
            raise FileNotFoundError(f"Specified Bucket Not Found: {self.bucket_name}")
        
        #create an index, get the address column, and get the first n
        index = create_index_from_s3(self.bucket_name, 'schema.json')
        ch10_index = index[index['basket_type'] == 'ch10']
        self.ch10_baskets = ch10_index['address']
        if self.n is not None and self.n < len(self.ch10_baskets):
            self.ch10_baskets = self.ch10_baskets[:self.n]

        num_ch10s = len(self.ch10_baskets)
        for i, basket in enumerate(self.ch10_baskets):
            basket_contents = opal_s3fs.ls(basket)
            
            #check that there is one ch10 and get the path to it
            rch10_path = [x for x in basket_contents if x.endswith('ch10')]
            if len(rch10_path) != 1:
                print(f'there are {len(rch10_path)} ch10s in basket {basket}. skipping.')
                continue
            rch10_path = rch10_path[0]
            
            print(f'{i+1}/{num_ch10s}: {rch10_path}')

            ch10_filename = os.path.basename(rch10_path)
            ch10_path = os.path.join(self.local_dir_path, ch10_filename)
            opal_s3fs.get(rch10_path, ch10_path)
            
            #run tip parse
            subprocess.run(
                [
                    "tip_parse",
                    ch10_path,
                    "-L",
                    "off",
                    "-o",
                    self.local_dir_path,
                    "-t",
                    "4",
                ]
            )
            
            os.remove(ch10_path)
            
            tip_metadata = self.extract_metadata()
            
            #get ch10_name from basket_metadata
            with opal_s3fs.open(os.path.join(basket, 'basket_metadata.json'), 'rb') as file:
                basket_metadata = json.load(file)
                ch10_name = basket_metadata['ch10name']
            
            tip_metadata['ch10name'] = ch10_name
            
            #get parent_uuid from basket_manifest
            parent_uuids = []
            with opal_s3fs.open(os.path.join(basket, 'basket_manifest.json'), 'rb') as file:
                basket_manifest = json.load(file)
                parent_uuids.append(basket_manifest['uuid'])

            #build upload_dicts
            upload_dicts = []
            for f in os.scandir(self.local_dir_path):
                upload_dicts.append({'path':f.path,'stub':False})
            
            self.metaflow_upload_basket(upload_dicts,
                                        self.bucket_name,
                                       'ch10_parsed',
                                        label = ch10_name,
                                        parent_ids = parent_uuids,
                                        metadata = tip_metadata)

            # clean out temp_dir
            for f in os.scandir(self.local_dir_path):
                if f.is_dir():
                    shutil.rmtree(f.path)
                else:
                    os.remove(f.path)

        self.next(self.end)

    @card
    @step
    def end(self):
        '''Cleanup temporary directory and satisfy metaflow'''
        self.temp_dir.cleanup()
        print("All Done")

if __name__ == "__main__":
    NASAch10ParseFlow()
