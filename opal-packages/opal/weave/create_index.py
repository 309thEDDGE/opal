'''
USAGE:
python create_index.py <root_dir> <schema_path>
root_dir: the root directory of s3 you wish to build your index off of
<<<<<<< HEAD
schema_path: the path to a local json file that specifies basket keys
=======
schema_path: the path to a local json file that specifies datum keys
>>>>>>> 01b3a38 (save changes)
'''

import json
import pandas as pd
import opal.flow
import argparse

#validate basket keys and value data types on read in

def validate_basket_json(basket_dict, schema, basket_address):
    if list(basket_dict.keys()) != schema:
        raise ValueError(f'basket found at {basket_address} has invalid schema')
        
    ########## validate types for each key
    
    return basket_dict

def create_index_from_s3(root_dir, schema_path):
    opal_s3fs = opal.flow.minio_s3fs()

    basket_jsons = [x for x in opal_s3fs.find(root_dir) if x.endswith('basket_details.json')]

    index_dict = {}
    with open(schema_path) as f:
        schema = json.load(f)
    for key in schema:
        index_dict[key] = []
    index_dict['address'] = []
    index_dict['storage_type'] = []
    
    for basket_json in basket_jsons:
        basket_address = f's3://{basket_json}'
        with opal_s3fs.open(basket_address, 'rb') as file:
            basket_dict = json.load(file)
            validate_basket(basket_dict, schema, basket_address)
            for field in basket_dict.keys():
                index_dict[field].append(basket_dict[field])
            #index_dict['uuid'] = index_dict['uuid'].astype(int64)
            index_dict['address'].append(basket_address)
            index_dict['storage_type'].append('s3')
            
    index = pd.DataFrame(index_dict) 
    index['uuid'] = index['uuid'].astype(str)
    return index

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        description="Save a local index of an s3 bucket built off of basket_details.json found within said bucket."
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
        help="the path to a local json file that specifies basket keys",
    )
    
    args = argparser.parse_args()
    
    create_index_from_s3(args.root_dir, args.schema_path).to_parquet('index.parquet')