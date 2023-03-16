'''
USAGE:
python create_index.py <root_dir> <schema_path>
root_dir: the root directory of s3 you wish to build your index off of
schema_path: the path to a local json file that specifies datum keys
'''

import json
import pandas as pd
import opal.flow
import argparse

#validate datum keys and value data types on read in

def validate_datum(datum_dict, schema, datum_address):
    if list(datum_dict.keys()) != schema:
        #How can I get the address to work here? what's the best way to do it?
        raise ValueError(f'datum found at {datum_address} has invalid schema')
        
    ########## validate types for each key
    
    return datum_dict

def create_index_from_s3(root_dir, schema_path):
    opal_s3fs = opal.flow.minio_s3fs()

    datum_files = [x for x in opal_s3fs.find(root_dir) if x.endswith('basket_details.json')]

    index_dict = {}
    with open(schema_path) as f:
        schema = json.load(f)
    for key in schema:
        index_dict[key] = []
    index_dict['address'] = []
    index_dict['storage_type'] = []
    

    for datum_file in datum_files:
        datum_address = f's3://{datum_file}'
        with opal_s3fs.open(datum_address, 'rb') as file:
            datum_dict = json.load(file)
            validate_datum(datum_dict, schema, datum_address)
            for field in datum_dict.keys():
                index_dict[field].append(datum_dict[field])
            #index_dict['uuid'] = index_dict['uuid'].astype(int64)
            index_dict['address'].append(datum_address)
            index_dict['storage_type'].append('s3')
            
    index = pd.DataFrame(index_dict) 
    index['uuid'] = index['uuid'].astype(str)
    return index

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        description="Save a local index of an s3 bucket built off of datums."
    )
    argparser.add_argument(
        "root_dir",
        metavar="<root_dir>",
        type=str,
        help="the root directory of s3 you wish to build your index off of",
    )
    argparser.add_argument(
        "schema_path",
        metavar="<schema_path>",
        type=str,
        help="the path to a local json file that specifies datum keys",
    )
    
    args = argparser.parse_args()
    
    create_index_from_s3(args.root_dir, args.schema_path).to_parquet('index.parquet')