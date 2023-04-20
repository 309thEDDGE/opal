import os
import s3fs
import tempfile
import metaflow
from metaflow import step, card
import opal.flow

class NASAc10UploadFlow(opal.flow.OpalFlowSpec):
    '''Defines a flow to upload NASA ch10 files.'''
    
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
    
    old = metaflow.Parameter(
        "old",
        help="Pass this parameter if you want the old ch10s from govcloud."
             "This will override anything passed in to ch10_directory_date.",
        required=False,
        default=None,
        type = bool
    )

    @step
    def start(self):
        """Create empty temporary directory for ch10 storage."""
        #create temporary directory to put data files locally
        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir_path = self.temp_dir.name

        self.next(self.upload_ch10s)

    @step
    def upload_ch10s(self):
        '''Upload NASA ch10s from govcloud datastore.
        
        get all the NASA ch10 files from a govcloud datastore,
        and upload them one at a time to the OPAL datastore
        '''
        opal_data = s3fs.S3FileSystem(anon = True, client_kwargs = {'region_name':'us-gov-west-1'})

        
        if self.old is not None:
            self.ch10_source_path = f's3://opal-data/nasa_ch10s_{self.ch10_directory_date}/'
        else:
            self.ch10_source_path = 's3://opal-data/nasa-ch10/chapter10'

        if not opal_data.exists(self.ch10_source_path):
            raise FileNotFoundError(f"Ch10 Source Directory Not Found: {self.ch10_source_path}")
        if self.n is None:
            self.ch10_names = [x for x in opal_data.ls(self.ch10_source_path) if x.endswith('ch10')]
        else:
            self.ch10_names = [x for x in opal_data.ls(self.ch10_source_path) if x.endswith('ch10')][:self.n]

        num_ch10s = len(self.ch10_names)
        for i, name in enumerate(self.ch10_names):
            print(f'{i+1}/{num_ch10s}: {name}')
            ch10_filename = os.path.basename(name)
            ch10_name = os.path.splitext(ch10_filename)[0]
            ch10_path = os.path.join(self.local_dir_path, ch10_filename)
            opal_data.get(name, ch10_path)

            upload_dict = [{'path':ch10_path,'stub':False}]
            basket_upload_path = self.metaflow_upload_basket(upload_dict,
                                        self.bucket_name,
                                       'ch10',
                                        label = ch10_name,
                                        metadata = {'ch10name': ch10_name})
            
            print(f'basket successfully uploaded: {basket_upload_path}')

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
