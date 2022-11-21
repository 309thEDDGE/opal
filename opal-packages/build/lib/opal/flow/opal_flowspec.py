import metaflow
from .flow_script_utils import publish_run, get_metaflow_s3_folder_upload_structure
from metaflow import FlowSpec
import os

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

    # simple wrapper for flow_script_utils.publish_run
    def publish(self, **kwargs):
        publish_run(self, **kwargs)
