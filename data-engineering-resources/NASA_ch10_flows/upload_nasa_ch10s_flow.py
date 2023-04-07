import metaflow
import os
import s3fs
import tempfile
import sys
from tqdm import tqdm
from metaflow import step, card
import opal.flow

class NASAc10UploadFlow(opal.flow.OpalFlowSpec):
    ch10_directory_date = metaflow.Parameter(
        "ch10_directory_date", help="Date of NASA ch10 upload (YYYY_MM_DD).", 
        required=False,
        default = "2023_03_20"
    )

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
    
    @step
    def start(self):
        """
        Create empty temporary directory for ch10 storage
        """
        #create temporary directory to put data files locally
        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name
    
        self.next(self.upload_ch10s)
        
    @step
    def upload_ch10s(self):
        opal_data = s3fs.S3FileSystem(anon = True, client_kwargs = {'region_name':'us-gov-west-1'})
        
        self.ch10_source_path = f's3://opal-data/nasa_ch10s_{self.ch10_directory_date}/'
        
        if not opal_data.exists(self.ch10_source_path):
            raise FileNotFoundError(f"Ch10 Source Directory Not Found: {self.ch10_source_path}")
        if self.n == None:
            self.ch10_names = opal_data.ls(self.ch10_source_path)
        else:
            self.ch10_names = opal_data.ls(self.ch10_source_path)[:self.n]

        for name in tqdm(self.ch10_names):
            ch10_filename = os.path.basename(name)
            ch10_path = os.path.join(self.local_dir_path, ch10_filename)
            opal_data.get(name, ch10_path)

            upload_dict = [{'path':ch10_path,'stub':False}]
            self.metaflow_upload_basket(upload_dict, 
                                        self.bucket_name, 
                                       'ch10')

            os.remove(ch10_path)
    
        self.next(self.end)
        
    @card
    @step
    def end(self):
        self.temp_dir.cleanup()
        print("All Done")

if __name__ == "__main__":
    NASAc10UploadFlow()
