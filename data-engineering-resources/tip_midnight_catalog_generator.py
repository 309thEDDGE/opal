import metaflow
import os
import pandas as pd
import tempfile
from midnight_catalog_utils import MidnightCatalogGenerator
from metaflow import Parameter, step, card, current
import re

import opal.flow
import opal.publish

fs = opal.flow.minio_s3fs()


class TipMidnightCatalog(opal.flow.OpalFlowSpec):
    @step
    def start(self):
        """
        make sure there is the required data to catalog
        """
        # get the necessary flows
        if 'test' in metaflow.current.tags:
            self.test = True
        else:
            self.test = False
        self.ch10_run = metaflow.Flow("Chapter10Catalog").latest_successful_run
        self.parse_flow = metaflow.Flow("TipParseFlow")
        self.translate_flow = metaflow.Flow("TipTranslateFlow")
        self.generator = MidnightCatalogGenerator()
        self.next(self.add_ch10_cat_data)

    @step
    def add_ch10_cat_data(self):
        """
        Add data from the Chapter10Catalog flow
        """
        for path, data in self.ch10_run.data.data_dict.items():
            name = os.path.splitext(os.path.basename(path))[0]
            self.generator.add(
                [
                    {
                        "column": "ch10_name",
                        "value": name,
                    },
                    {
                        "column": "ch10_path",
                        "value": path,
                    },
                    {
                        "column": "source",
                        "value": data["source"],
                    },
                    {"column": "ch10_tip_hash", "value": data["hash"]},
                ],
                name,
            )
            # If testing, only iterate once
            if self.test:
                print('Testing')
                break

        self.next(self.add_parse_data)

    @step
    def add_parse_data(self):
        """
        Add data from the tip_parse flow
        """

        for run in self.parse_flow:
            if not run.successful or "no_data" in run.tags:
                continue
            # tip parsed metadata
            self.generator.add(
                [
                    {
                        "column": "parse_run_id",
                        "value": run.id,
                    }
                ],
                run.data.ch10_name,
            )

            # tip parsed datasets - this is a bit complicated
            # get all the parquet files
            glob = os.path.join(run.data.data_files["parsed_data"], "*.parquet")
            parse_data_files = fs.glob(glob)
            # match the file path against this regex to pull out the dataset name
            parsed_dataset_re = f"(.*)/{run.data.ch10_name}_(?P<dataset>.*)\\.parquet"

            for file in parse_data_files:
                m = re.match(parsed_dataset_re, file)
                # add the parquet file path under its dataset name
                # dataset name found from the regex above
                if m:
                    self.generator.add(
                        [{"column": m["dataset"], "value": file}], run.data.ch10_name
                    )
                    
            if self.test:
                print('Testing')
                break

        self.next(self.add_translate_data)

    @step
    def add_translate_data(self):
        """
        Add data from the tip_translate flow
        """
        # mostly the same as catalog_parse_data
        for run in self.translate_flow:
            if not run.successful or "no_data" in run.tags:
                continue

            # if this run id under its translated type
            self.generator.add(
                [
                    {
                        "column": f"translate_run_id_{run.data.translate_type}",
                        "value": run.id,
                    }
                ],
                run.data.parse_run.data.ch10_name,
            )

            # see similar block in catalog_parse_data
            glob = os.path.join(run.data.data_files["translated_data"], "*/*.parquet")
            translate_data_files = fs.glob(glob)
            translated_dataset_re = f"(.*)/(?P<dataset>.*)\\.parquet"
            for file in translate_data_files:
                m = re.match(translated_dataset_re, file)
                if m:
                    self.generator.add(
                        [{"column": m["dataset"], "value": file}],
                        run.data.parse_run.data.ch10_name,
                    )
                    
            if self.test:
                print('Testing')
                break

        self.next(self.upload_table)

    @step
    def upload_table(self):
        """
        Save the table as parquet and upload to S3
        """
        temp_df = self.generator.to_dataframe().set_index("ch10_name")
        temp_path = os.path.join(tempfile.gettempdir(), "tip_catalog.parquet")
        temp_path = str(temp_path)
        temp_df.to_parquet(temp_path)
        self.upload(temp_path, key="parquet_table")
        self.next(self.end)

    @card
    @step
    def end(self):
        print("Done")


if __name__ == "__main__":
    TipMidnightCatalog()
