import json
import pandas as pd
import opal.flow

#storage type always s3
#generalize index dict
#validate datum keys and value data types on read in
#create external schema

def validate_datum(datum_dict, schema, datum_address):
    if list(datum_dict.keys()) != schema:
        #How can I get the address to work here? what's the best way to do it?
        raise ValueError(f'datum found at {datum_address} has invalid schema')
        
    ########## validate types for each key
    
    return datum_dict

def create_index_from_s3(root_dir, schema_path):
    opal_s3fs = opal.flow.minio_s3fs()

    datum_files = [x for x in opal_s3fs.find(root_dir) if x.endswith('datum.json')]

    index_dict = {}
    schema = json.load(schema_path)
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
            index_dict['address'].append(datum_address)
            index_dict['storage_type'].append('s3')
            
    return pd.DataFrame(index_dict)

if __name__ == "__main__":
    create_index_from_s3().to_parquet('index.parquet')
