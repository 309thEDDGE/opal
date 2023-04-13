import os
import s3fs
import tempfile
import subprocess
import yaml
import metaflow
from metaflow import step, card
import opal.flow
from opal.weave.create_index import create_index_from_s3

class NASAc10UploadFlow(opal.flow.OpalFlowSpec):
    '''Defines a flow to upload NASA ch10 files.'''

    n = metaflow.Parameter(
        "n",
        help="Number of ch10s to upload.",
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
        for de in os.scandir(self.temp_dir):
            # look for _metadata.yaml under *.parquet folders
            if de.is_dir() and de.path.endswith(".parquet"):
                meta_file = os.path.join(de.path, "_metadata.yaml")
                if os.path.exists(meta_file):
                    # load the yaml and save it in metaflow
                    with open(meta_file) as f:
                        metadata = yaml.safe_load(f)
                        print(f"Found metadata for {metadata['type']}")
                        tip_metadata[metadata["type"]] = tip_metadata

                        # copy some chapter 10 metadata for convenience
                        # resources = tip_metadata["provenance"]["resource"]
                        # ch10_resource = [r for r in resources if r["type"] == "CH10"]
                        # self.ch10_metadata = ch10_resource[0] if ch10_resource else {}

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
        '''
        '''
        opal_s3fs = s3fs.S3FileSystem(client_kwargs = {'endpoint_url': os.environ['S3_ENDPOINT']})
        if not opal_s3fs.exists(ch10_bucket):
            raise FileNotFoundError(f"Specified Bucket Not Found: {self.bucket_name}")
        
        index = create_index_from_s3(self.bucket_name, 'schema.json')
        ch10_index = index[index['basket_type'] == 'ch10']
        self.ch10_baskets = ch10_index['address']

        if self.n is not None:
            self.ch10_baskets = self.ch10_baskets[:self.n]

        for basket in self.ch10_baskets:
            basket_contents = opal_s3fs.ls(path)
            parent_uuid = os.path.basename(basket)
            
            #check that there is one ch10 and get the path to it
            ch10_path = [x for x in basket_contents if x.endswith('ch10')]
            if len(ch10_path) != 1: print(
            
            ch10_name = os.path.splitext(ch10_filename)[0]
            ch10_path = os.path.join(self.local_dir_path, ch10_filename)
            opal_s3fs.get(name, ch10_path)
            
            #run tip parse
            subprocess.run(
                [
                    "tip_parse",
                    ch10_path,
                    "-L",
                    "off",
                    "-o",
                    self.temp_dir,
                    "-t",
                    "4",
                ]
            )
            
            os.remove(ch10_path)
            
            tip_metadata = self.extract_metadata()

            #build upload_dicts
            upload_dicts = []
            for f in os.scandir(self.temp_dir):
                upload_dicts.append({'path':f.path,'stub':False})
            
            self.metaflow_upload_basket(upload_dicts,
                                        self.bucket_name,
                                       'parsed_ch10',
                                        label = ch10_name,
                                        metadata = {'ch10name': ch10_name})

            os.remove(ch10_path)

        self.next(self.end)

    @card
    @step
    def end(self):
        '''Cleanup temporary directory and satisfy metaflow'''
        self.temp_dir.cleanup()
        print("All Done")

if __name__ == "__main__":
    NASAc10UploadFlow()
