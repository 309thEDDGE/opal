import requests
import json
import os

# for serializing
import pandas as pd
import numpy as np

def publish_serializer(obj):
    if isinstance(obj, pd.DataFrame):
        return json.loads(obj.to_json())
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, frozenset):
        return list(obj)
    else:
        return str(obj)

def publish(
    kind_id: str,
    kind_type: str,
    kind_metadata: dict,
    api_url=os.environ.get("CATALOG_BACKEND_URL"),
    token=os.environ.get("JUPYTERHUB_API_URL"),
):

    json_data = json.dumps({
        "kind_metadata":kind_metadata, 
        "kind_type":kind_type,
        "kind_id":kind_id
    },  default=publish_serializer)

    res = requests.post(
        f"{api_url}/instance",
        # json=dict(kind_id=kind_id, kind_type=kind_type, kind_metadata=kind_metadata),
        data=json_data,
        headers={"content-type": "application/json", "Authorization": "token %s" % token}
    )

    return (True, res.status_code) if res.status_code == 201 else (False, res.status_code)

# DELETE BY KIND ID
def delete(
    kind_id:str, 
    api_url=os.environ.get("CATALOG_BACKEND_URL"),
    token=os.environ.get("JUPYTERHUB_API_URL"),
):
    res = requests.delete(
        f"{api_url}/instance", 
        headers={"Authorization": "token %s" % token}, 
        data=json.dumps({
            "kind_id": kind_id
        })
    )

    return (res.status_code == 204, res.status_code)