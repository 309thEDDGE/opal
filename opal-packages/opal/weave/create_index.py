'''
USAGE:
python create_index.py <root_dir> <schema_path>
    root_dir: the root directory of s3 you wish to build your index off of
    schema_path: the path to a local json file that specifies basket keys.
                Currently the contents of this file just contain an array
                of the keys found in basket_manifest.json, such as 
                ["uuid", "upload_time", "parent_uuids", "basket_type", "label"]
'''

import json
import argparse
import pandas as pd
import opal.flow


#validate basket keys and value data types on read in
def validate_basket_dict(basket_dict, schema, basket_address):
    """
    validate the basket_manifest.json has the correct structure
    
    Parameters:
        basket_dict: dictionary read in from basket_manifest.json in minio
        schema: loaded from schema_path passed to create_index_from_s3
        basket_address: basket in question. Passed here to create better error message
    """
    if list(basket_dict.keys()) != schema:
        raise ValueError(f'basket found at {basket_address} has invalid schema')

    #TODO: validate types for each key

def create_index_from_s3(root_dir, schema_path):
    """
    Recursively parse an s3 bucket and create an index using basket_manifest.json found therein
    
    Parameters:
        root_dir: path to s3 bucket
        schema_path: path to json file that specifies structure of basket_manifest.json
                    Currently the contents of this file just contain an array
                    of the keys found in basket_manifest.json, such as 
                    ["uuid", "upload_time", "parent_uuids", "basket_type", "label"]
        
    Returns:
        index: a pandas DataFrame with columns
                ["uuid", "upload_time", "parent_uuids", "basket_type", "label", "address", "storage_type"]
                and where each row corresponds to a single basket_manifest.json
                found recursively under specified root_dir
    """
    
    #check parameter data types
    if not isinstance(root_dir, str):
        raise TypeError(f"'root_dir' must be a string: '{root_dir}'")
        
    if not isinstance(schema_path, str):
        raise TypeError(f"'schema_path' must be a string: '{schema_path}'")
    
    opal_s3fs = opal.flow.minio_s3fs()

    basket_jsons = [x for x in opal_s3fs.find(root_dir) if x.endswith('basket_manifest.json')]

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
            validate_basket_dict(basket_dict, schema, basket_address)
            for field in basket_dict.keys():
                index_dict[field].append(basket_dict[field])
            index_dict['address'].append(basket_address)
            index_dict['storage_type'].append('s3')

    index = pd.DataFrame(index_dict)
    index['uuid'] = index['uuid'].astype(str)
    return index

if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        description="""Save a local index of an s3 bucket built off of 
                        basket_details.json found within said bucket."""
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