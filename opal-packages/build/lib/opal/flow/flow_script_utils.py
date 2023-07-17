import metaflow
import os
import pathlib as pl
import json
import opal.publish
from .other_utils import minio_s3fs

# metaflow put_files needs a list of (key, path)
# where key is some string (may be path-like)
# and path is the path to a *file*. This function
# constructs that list for a directory.
# Details here:
# https://docs.metaflow.org/metaflow/data#store-multiple-objects-or-files
def get_metaflow_s3_folder_upload_structure(folder, key):
    folder = os.path.abspath(os.path.expanduser(folder))
    root = os.path.basename(folder)
    out = []

    # recursively over the entire directory:
    for r, _, fs in os.walk(folder):
        for f in fs:
            p = pl.Path(r, f)
            # get the path to `f` just after `root`
            idx = p.parts.index(root)
            rpath = pl.Path(key, *p.parts[idx + 1 :])
            # key to this file `f` is `key` + (`f` path up to `root`)
            out.append((str(rpath), str(p)))

    return out


# generic upload interace for files or directories
def upload(run, path, key=None):
    if not key:
        # use folder name as key if not set
        key = os.path.basename(path)

    if os.path.isdir(path):
        upload = get_metaflow_s3_folder_upload_structure(path, key)
        with metaflow.S3(run=run) as s3:
            return s3.put_files(upload)
    elif os.path.isfile(path):
        with metaflow.S3(run=run) as s3:
            return s3.put_files([(key, path)])
    else:
        raise Exception(f"Argument is not a file or a directory: {path}")


# simple enough - should this run be published?
def should_publish_run(run):
    return run.successful and not "no_data" in run.tags


# publish a metaflow run to the catalog.
# force=True will ignore results of should_publish_run
def publish_run(run, force=False):
    if not should_publish_run(run) and not force:
        return False

    # get the data in run
    run_data = {k: v.data for k, v in run.data._artifacts.items()}

    # elevate some metaflow metadata because it's useful
    run_data["created_at"] = str(run._created_at)
    run_data["tags"] = run._tags
    run_data["id"] = run.id
    run_data["successful"] = run.successful
    run_data["flow_id"] = run.parent.id

    # publish to catalog
    return opal.publish.publish(run_data["id"], run_data["flow_id"], run_data)


def delete_run_data(run):
    # tag as "no data"
    run.add_tag("no_data")

    # delete files if there are any
    try:
        files = run.data.data_files
        s3fs = minio_s3fs()
        for _, file in files.items():
            s3fs.rm(file, recursive=True)
    except BaseException:
        pass

    # delete from catalog
    opal.publish.delete(run.id)
