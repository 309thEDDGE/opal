from opal import kinds
import json
import os
import requests

# get our parsed and translated kind types
kinds = kinds.load()
kp_label = "1553_parsed"
kt_label = "1553_translated"
kp = kinds.lookup(kp_label)
kt = kinds.lookup(kt_label)

# get the locations of parsed/translated datasets
def get_parsed_translated(shallow_cat_path):
    parsed = os.path.join(shallow_cat_path, parsed_fname)
    translated = os.path.join(shallow_cat_path, translated_fname)
    return parsed, translated

def upload_a_dataset(dataset_path, user_meta, inplace=False):
    p, t = get_parsed_translated(dataset_path)
    
    # manually check if a dataset at this location was already uploaded
    kp_search = kp.metadata_search("source_path", p)
    kt_search = kt.metadata_search("source_path", t)
    
    if len(kp_search) == 0: # an instance uploaded from this location doesn't exist
        print(f"Uploading {p} as parsed")
        kp_meta_out = kp.upload(p, user_meta, inplace=inplace)
        kp_id = kp_meta_out["instance_id"]
    else:
        print(f"Instance from {p} already exists")
        _, kp_id = kp_search[0]
    print(kp_id)
    
    if len(kt_search) == 0:
        print(f"Uploading {t} as translated")
        kt_meta_out = kt.upload(t, user_meta, inplace=inplace, parent_instance=kp_id)
    else:
        print(f"Instance from {t} already exists")
        _, kt_id = kt_search[0]
    print(kt_id)
        
    return kp_id, kt_id
        
def publish_instance(
    kind_id: str, kind_type: str, s3_path: str = None
):
    kind = kinds.lookup(kind_type)
    kind_metadata = kind.read_instance_metadata(kind_id)
    
    # CATALOG_API_URL = os.environ.get("CATALOG_API_URL")
    back_end_url = "http://opalcatalog-be:9001/services/opal-catalog"
    CATALOG_API_URL = "http://opalcatalog-be:9001/services/opal-catalog"
    assert back_end_url == CATALOG_API_URL
    JUPTYERHUB_API_TOKEN = os.environ.get("JUPTYERHUB_API_TOKEN")
    publish_endpoint = f"{CATALOG_API_URL}/instance"

    publish_instance_payload = dict(
        kind_id=kind_id,
        kind_type=kind_type,
        kind_metadata=kind_metadata,
        s3_path="",
    )

    d = requests.post(
        publish_endpoint,
        data=json.dumps(
            publish_instance_payload,
        ),
        headers=dict(Authorization=f"token {JUPTYERHUB_API_TOKEN}"),
        # ),
    )
    
# DELETE BY KIND ID
def delete_by_kind_id(kind_id, error_ok=False):
    back_end_url = "http://opalcatalog-be:9001/services/opal-catalog"
    d = requests.delete(f"{back_end_url}/instance", headers={"Authorization": "token %s" % os.environ.get("JUPYTERHUB_API_TOKEN")}, data=json.dumps({
       "kind_id": kind_id
    }))
    assert d.status_code == 204 or error_ok
    
if __name__ == "__main__":
    import os
    import sys
    import glob

    # search s3 for this string
    glob_str = sys.argv[1] if len(sys.argv) >= 2 else "testbucket/Shallow_Cat_*"
    print(f"Uploading from {glob_str}")
    datasets = glob.glob(glob_str)
    parsed_fname = "test_1553.parquet"
    translated_fname = "test_1553_translated"

    for pth in datasets:
        kp_id, kt_id = upload_a_dataset(pth, { "source_search": glob_str })
        
        kp.strong_validate_instance(kp_id)
        kt.strong_validate_instance(kt_id)
        
        publish_instance(kp_id, kp_label, "")
        publish_instance(kt_id, kt_label, "")
        
    print("Done")