import metaflow
import os
import subprocess
import sys
import json
import tempfile
import fsspec

from tip_utils import already_parsed_hashes, tip_hash_ch10

# runs the tip parse flow on this chapter 10 file
def parse_one(ch10_name):
    subprocess.run(
        ["python", "tip_parse_flow.py", "--no-pylint", "run", "--c10", ch10_name]
    )


# filter out chapter 10s that are already parsed when using a directory
def from_files(ch10_inputs):
    to_parse = []

    parsed_hashes = already_parsed_hashes()

    for ch10_file in ch10_inputs:
        ch10_hash = tip_hash_ch10(ch10_file)
        if ch10_hash in parsed_hashes:
            print(
                f"This chapter 10 has already been parsed "
                f"and will not be parsed again: {ch10_hash}"
            )
        else:
            print(f"New chapter 10 {ch10_file} {ch10_hash}")
            yield ch10_file

    return to_parse


# filter out chapter 10s that are already parsed when using a Chapter10Catalog
def from_ch10_catalog(ch10_cat_run):
    parsed_hashes = already_parsed_hashes()
    newly_parsed_hashes = set()

    n = len(ch10_cat_run.data.data_dict)

    # skip a chapter 10 if its hash exists in parsed_hashes or newly_parsed_hashes
    for i, (path, data) in enumerate(ch10_cat_run.data.data_dict.items()):
        file = os.path.basename(path)
        prefix = f"{i+1}/{n}\t"
        if data["hash"] in parsed_hashes or data["hash"] in newly_parsed_hashes:
            print(
                f"{prefix}This chapter 10 has already been parsed "
                f"and will not be parsed again: {file}"
            )
        else:
            print(f"{prefix}New chapter 10 {file} (hash = {data['hash']})")
            yield data | {"path": path}
            newly_parsed_hashes.add(data["hash"])


if __name__ == "__main__":
    ch10_cat_run = metaflow.Flow("Chapter10Catalog").latest_successful_run
    ch10s = from_ch10_catalog(ch10_cat_run)

    for ch10_info in ch10s:
        fsspec_dict = ch10_cat_run.data.sources[ch10_info["source"]]["fsspec"]
        if fsspec_dict["protocol"] != "file":
            fs = fsspec.AbstractFileSystem.from_json(json.dumps(fsspec_dict))
            file = os.path.join(
                tempfile.gettempdir(), os.path.basename(ch10_info["path"])
            )
            fs.get(ch10_info["path"], file)
        else:
            file = ch10_info["path"]

        parse_one(file)
