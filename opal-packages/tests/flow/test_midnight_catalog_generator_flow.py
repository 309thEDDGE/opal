import subprocess
import metaflow
from opal.flow.flow_script_utils import *

singleuser_python_path = '/opt/conda/envs/singleuser/bin/python'
torch_python_path = '/opt/conda/envs/torch/bin/python'
midnight_cat_flow_path ='/home/jovyan/opal/data-engineering-resources/tip_midnight_catalog_generator.py'
test_artifacts_path = '/home/jovyan/opal/opal-packages/tests/flow/check_artifacts.py'
      
    
def test_midnight_flow_torch_create_singleuser_read():
    # Run the flow from torch, read back from single user
    subprocess.run([torch_python_path, midnight_cat_flow_path, '--no-pylint', 'run', '--tag', 'test'])
    run = subprocess.run([singleuser_python_path, test_artifacts_path, 'TipMidnightCatalog'], capture_output=True)
    run.check_returncode()  
    
    # clean up
    opal.flow.flow_script_utils.delete_run_data(list(metaflow.Flow('TipMidnightCatalog').runs('test'))[0])

def test_midnight_flow_singleuser_create_torch_read():
    # Run the flow from singleuser, read back from torch
    subprocess.run([singleuser_python_path, midnight_cat_flow_path, '--no-pylint', 'run', '--tag', 'test'])
    run = subprocess.run([torch_python_path, test_artifacts_path, 'TipMidnightCatalog'], capture_output=True)
    run.check_returncode() 
    
    # clean up
    opal.flow.flow_script_utils.delete_run_data(list(metaflow.Flow('TipMidnightCatalog').runs('test'))[0])