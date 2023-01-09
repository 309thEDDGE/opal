import sys
import metaflow

flow_name = sys.argv[1]

# The TipMidnightCatalog flow saves an instance of the 
# MidnightCatalogGenertor class as an artifact. When unpickling
# a run of TipMidnightCatalog, the MidnightCatalogGenertor class 
# needs to have been imported so that the MidnightCatalogGenerator object
# can be unpickled. 
if flow_name == 'TipMidnightCatalog':
    sys.path.append('/home/jovyan/opal/data-engineering-resources')
    from midnight_catalog_utils import MidnightCatalogGenerator

# Read back the latest 'test' flow run associated with the
# 'flow_name' passed to the script as the first argument.
run = list(metaflow.Flow(flow_name).runs('test'))[0]

# Iterate over all the artifacts and make sure they
# can be assigned to a variable.
# This loop essentially tests to make sure
# all of the artifacts can be unpickled.
for key, val in run.data.__dict__['_artifacts'].items():
    temp = val.data