import metaflow
import os
from .flow_script_utils import publish_run, get_metaflow_s3_folder_upload_structure
from metaflow import FlowSpec, current
from weave import upload as weave_upload

# OPAL-specific subclass of metaflow's FlowSpec base class
# provides a simplified interface for uploading and a direct
# connection to the catalog via the publish method.
class OpalFlowSpec(FlowSpec):

    # uploads a file or folder, and puts the s3 path of the
    # uploaded object into `self.data_files`
    def upload(self, path, key=None):
        if not hasattr(self, "data_files"):
            self.data_files = {}

        # use file or folder name if key is not provided
        base = os.path.basename(path)
        if not key:
            key = base

        with metaflow.S3(run=self) as s3:
            # make sure we save the S3 root path
            self.s3root = s3._s3root

            # remove s3:// - causes problems with pd.read_parquet
            # on a partitioned parquet directory
            if self.s3root.startswith("s3://"):
                cut_out = len("s3://")
                self.s3root = self.s3root[cut_out:]

            # get file or directory upload structure for metaflow's S3 thing
            if os.path.isdir(path):
                upload = get_metaflow_s3_folder_upload_structure(path, key)
                self.data_files[key] = os.path.join(self.s3root, key)
            elif os.path.isfile(path):
                upload = [(base, path)]
                self.data_files[key] = os.path.join(self.s3root, base)

            s3.put_files(upload)

    def metaflow_upload_basket(self, 
                               upload_dict, 
                               basket_type,
                               bucket_name = 'basket-data', 
                               label = '',
                               parent_ids = [],
                               metadata = {}):
        '''A wrapper for metaflow to use weave.upload and track ids.'''

        if not hasattr(self, "basket_uploads"):
            self.basket_uploads = []

        metadata['metaflow_manifest'] = {'run_id': current.run_id,
                                         'flow_name': current.flow_name}

        basket_upload_path = weave_upload(upload_dict,
                                          basket_type,
                                          bucket_name,
                                          label = label,
                                          parent_ids = parent_ids,
                                          metadata = metadata)

        self.basket_uploads.append(basket_upload_path)
        
        return basket_upload_path

    # simple wrapper for flow_script_utils.publish_run
    def publish(self, **kwargs):
        publish_run(self, **kwargs)
