import sys
import metaflow
sys.path.append('/home/jovyan/opal/data-engineering-resources')

from midnight_catalog_utils import MidnightCatalogGenerator

run = list(metaflow.Flow('TipMidnightCatalog').runs('test'))[0]
for key, val in run.data.__dict__['_artifacts'].items():
    temp = val.data