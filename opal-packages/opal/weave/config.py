'''
config.py provides configuration settings used by weave.
'''

def index_schema():
    '''
    Return the keys expected from the manifest.json file.
    '''
    return ["uuid", "upload_time", "parent_uuids", "basket_type", "label"]
    