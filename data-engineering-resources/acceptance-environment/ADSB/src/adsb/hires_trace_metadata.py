import datetime
import adsb.validate as validate

field_data = {'icao': '', 'r': '', 't': '', 'desc': '', 
    'dbFlags': 'bitfield', 'timestamp': 'epoch'}

def _has_valid_fields(data: dict) -> bool:
    keys = list(data.keys())
    for req_key in field_data.keys():
        if not req_key in keys:
            return False
    return True

def get_metadata(data: dict) -> dict:
    result = {}
    if not _has_valid_fields(data):
        return result 

    for field in field_data.keys():
        result[field] = data[field]

    ts = validate._convert_timestamp(result['timestamp'])
    result['timestamp'] = ts
    return result


