import datetime

def _is_bitfield(bits) -> bool:
    if isinstance(bits, str):
        try:
            bitval = int(bits)
        except ValueError as e:
            print(f"_is_bitfield(): {e}")
            return False
    elif isinstance(bits, float):
        return False
    return True

def _convert_timestamp(timestamp: float) -> datetime.datetime:
    ts = datetime.datetime.fromtimestamp(timestamp, 
        tz=datetime.timezone.utc)
    return ts

def _validate_field_by_type(field_value, field_type):
    if field_type == 'bitfield':
        return _is_bitfield(field_value)
    elif field_type == 'epoch':
        return _convert_timestamp(field_value)
    return True

    