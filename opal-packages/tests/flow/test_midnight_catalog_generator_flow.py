import subprocess
import metaflow
from opal.flow.flow_script_utils import *

midnight_cat_pickle_util_str = """import metaflow;
run = list(metaflow.Flow('TipMidnightCatalog').runs('test'))[0];
for key, val in run.data.__dict__['_artifacts'].items():
    if key == 'generator':
        continue
    temp = val.data
"""

def test_midnight_flow_torch_create_singleuser_read():
    # Run the flow from torch, read back from single user
    subprocess.run(['/opt/conda/envs/torch/bin/python', '/home/jovyan/opal/data-engineering-resources/tip_midnight_catalog_generator.py', '--no-pylint', 'run', '--tag', 'test'])
    run = subprocess.run(['/opt/conda/envs/singleuser/bin/python', '-c', midnight_cat_pickle_util_str], capture_output=True)
    print( 'exit status:', run.returncode )
    print( 'stdout:', run.stdout.decode() )
    print( 'stderr:', run.stderr.decode() )
    run.check_returncode()
    
    # clean up
    opal.flow.flow_script_utils.delete_run_data(list(metaflow.Flow('TipMidnightCatalog').runs('test'))[0])

def test_midnight_flow_singleuser_create_torch_read():
    # Run the flow from singleuser, read back from torch
    subprocess.run(['/opt/conda/envs/singleuser/bin/python', '/home/jovyan/opal/data-engineering-resources/tip_midnight_catalog_generator.py', '--no-pylint', 'run', '--tag', 'test'])
    run = subprocess.run(['/opt/conda/envs/torch/bin/python', '-c', midnight_cat_pickle_util_str], capture_output=True)
    print( 'exit status:', run.returncode )
    print( 'stdout:', run.stdout.decode() )
    print( 'stderr:', run.stderr.decode() )
    run.check_returncode()
    
    # clean up
    opal.flow.flow_script_utils.delete_run_data(list(metaflow.Flow('TipMidnightCatalog').runs('test'))[0])