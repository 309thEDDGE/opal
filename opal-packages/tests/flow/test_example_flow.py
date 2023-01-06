import subprocess
import metaflow
import os
from opal.flow.flow_script_utils import *

singleuser_python_path = '/opt/conda/envs/singleuser/bin/python'
torch_python_path = '/opt/conda/envs/torch/bin/python'
example_flow_path ='/home/jovyan/opal/test-notebooks/example_flow.py'
test_artifacts_path = '/home/jovyan/opal/opal-packages/tests/flow/check_artifacts.py'

def test_example_flow_torch_create_singleuser_read():
    # Run the flow from torch, read back from single user
    subprocess.run([torch_python_path, example_flow_path, '--no-pylint', 'run', '--n', '9', '--tag', 'test'])
    run = subprocess.run([singleuser_python_path, test_artifacts_path, 'ExampleFlow'], capture_output=True)
    if run.returncode != 0:
        print( 'exit status:', run.returncode )
        print( 'stdout:', run.stdout.decode() )
        print( 'stderr:', run.stderr.decode() )
    run.check_returncode()
    
    # clean up
    opal.flow.flow_script_utils.delete_run_data(list(metaflow.Flow('ExampleFlow').runs('test'))[0])

def test_example_flow_singleuser_create_torch_read():
    # Run the flow from singleuser, read back from torch
    subprocess.run([singleuser_python_path, example_flow_path, '--no-pylint', 'run', '--n', '9', '--tag', 'test'])
    run = subprocess.run([torch_python_path, test_artifacts_path, 'ExampleFlow'], capture_output=True)
    if run.returncode != 0:
        print( 'exit status:', run.returncode )
        print( 'stdout:', run.stdout.decode() )
        print( 'stderr:', run.stderr.decode() )
    run.check_returncode()
    
    # clean up
    opal.flow.flow_script_utils.delete_run_data(list(metaflow.Flow('ExampleFlow').runs('test'))[0])