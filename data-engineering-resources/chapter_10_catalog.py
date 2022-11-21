import os
from pathlib import Path
import tempfile

import metaflow
from metaflow import Parameter, step, card, IncludeFile
import opal.flow
import pandas as pd
from tip_utils import tip_hash_ch10
import json
import fsspec
import itertools


class Chapter10Catalog(opal.flow.OpalFlowSpec):
    base_dir = metaflow.Parameter(
        "dir",
        help="Directory to search for chapter 10s in. One of 'dir' and 'sources' must be set",
        default=None,
    )

    sources_file = metaflow.IncludeFile(
        "sources",
        help="JSON description of locations and sources to search for chapter 10s",
    )

    cache = metaflow.Parameter(
        "cache", help="Use latest successful run as cache", default=True
    )

    @step
    def start(self):
        # load sources description
        if self.sources_file:
            self.sources = json.loads(self.sources_file)
        else:
            self.sources = {}

        # fill sources if base_dir is used
        # note - this is untested
        if self.base_dir is not None:
            if not os.path.exists(self.base_dir):
                raise Exception(f"Directory does not exist: {self.base_dir}")

            self.sources["local_path"] = {
                "fsspec": {"protocol": "file"},
                "urls": [f"{self.base_dir}/**.ch10"],
            }

        # can and should we use previous run as cache?
        if self.cache:
            try:
                # get latest run of this flow to use as base if it exists
                ch10_cat_flow = metaflow.Flow("Chapter10Catalog")
                self.cached_run = ch10_cat_flow.latest_successful_run
                if not self.cached_run:
                    raise metaflow.exception.MetaflowNotFound("")
                print(f"Using cache from {self.cached_run.id}")
                self.use_cache = True
            except metaflow.exception.MetaflowNotFound:
                # otherwise don't use cache
                print("Could not find a cached run to use")
                print("Continuing anyways")
                self.use_cache = False
        else:
            self.use_cache = False

        self.next(self.get_ch10_data)

    @step
    def get_ch10_data(self):
        self.data_dict = {}

        cached_dict = self.cached_run.data.data_dict if self.use_cache else {}

        for source_name, source_spec in self.sources.items():
            log_prefix = f"({source_name})"
            # get the file system object for this source
            fs = fsspec.AbstractFileSystem.from_json(json.dumps(source_spec["fsspec"]))

            # get all chapter 10 paths by glob
            ch10_paths = itertools.chain.from_iterable(
                (fs.glob(glob_url) for glob_url in source_spec["urls"])
            )

            for ch10_p in ch10_paths:
                # some basic information about the file
                ch10_name = os.path.basename(ch10_p)
                ch10_url = f"{source_spec['fsspec']['protocol']}://{ch10_p}"
                ch10_size = fs.size(ch10_p)

                # filter out ch10s in the same spot with the same size.
                # We assume they will have the same hash, and therefore
                # are the same file.
                # note - if you have multiple chapter 10s with the same urls
                #        across different sources, they will interfere with
                #        each other here.
                if ch10_url in cached_dict:
                    cached_record = cached_dict[ch10_url]
                    if (
                        ch10_size == cached_record["size"]
                        and source_name == cached_record["source"]
                    ):
                        print(
                            f"{log_prefix} Found {ch10_name} in cache ({ch10_size} b)"
                        )
                        self.data_dict[ch10_url] = cached_dict[ch10_url]
                        continue
                    else:
                        print(
                            f"{log_prefix} Found {ch10_name} in cache, "
                            f"but it's a different size "
                            f"({ch10_size} != {cached_record['ch10_size']}). "
                            "Assuming this is a new chapter 10 file."
                        )
                else:
                    print(f"{log_prefix} Found new ch10 {ch10_name}")

                self.data_dict[ch10_url] = {
                    "size": ch10_size,
                    "source": source_name,
                    "hash": "",
                }

        self.next(self.hash_ch10s)

    @step
    def hash_ch10s(self):
        n = len(self.data_dict)
        for i, k in enumerate(self.data_dict.keys()):
            # for printing progress
            prefix = f"{i+1}/{n}"
            ch10_info = self.data_dict[k]
            name = os.path.basename(k)

            # hash the chapter 10 if no hash exists
            if ch10_info["hash"] == "":
                print(f"{prefix} Hashing {name}")
                fsspec_dict = self.sources[ch10_info["source"]]["fsspec"]
                fs = fsspec.AbstractFileSystem.from_json(json.dumps(fsspec_dict))
                with fs.open(k) as f:
                    ch10_info["hash"] = tip_hash_ch10(f)
            else:
                print(f"{prefix} Using cache for {name} ({ch10_info['hash']})")

        self.next(self.end)

    #     @step
    #     def to_parquet(self):
    #         self.table = pd.DataFrame(self.data_dict).transpose()

    #         # save as parquet to a temp folder for upload
    #         temp_path = os.path.join(tempfile.gettempdir(), "ch10_catalog.parquet")
    #         temp_path = str(temp_path)
    #         self.table.to_parquet(temp_path)
    #         self.upload(temp_path, key="parquet_table")

    #         self.next(self.end)

    @card
    @step
    def end(self):
        print("Done")


if __name__ == "__main__":
    Chapter10Catalog()
