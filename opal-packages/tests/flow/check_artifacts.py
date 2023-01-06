import sys
import metaflow

flow_name = sys.argv[1]
if flow_name == 'TipMidnightCatalog':
    sys.path.append('/home/jovyan/opal/data-engineering-resources')
    from midnight_catalog_utils import MidnightCatalogGenerator

run = list(metaflow.Flow(flow_name).runs('test'))[0]
for key, val in run.data.__dict__['_artifacts'].items():
    temp = val.data