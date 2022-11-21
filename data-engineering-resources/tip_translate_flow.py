import metaflow
import os

import opal.flow
from metaflow import step, card
import shutil
import tempfile
import subprocess
import yaml


class TipTranslateFlow(opal.flow.OpalFlowSpec):
    parse_pointer = metaflow.Parameter(
        "parsed", help="Metaflow run ID of the parsed data to translate", required=True
    )

    # TODO: replace default with tempfile.mkdtemp()?
    temp_dir = metaflow.Parameter(
        "temp-dir", default=os.path.expanduser("~/translated_data")
    )

    dts = metaflow.Parameter("dts", help="ICD to translate the parsed data with")

    # datasets that can be translated
    # title : (tip_cli_program, parsed_dataset_name)
    translate_options = {
        "MILSTD1553": ("tip_translate_1553", "MILSTD1553_F1"),
        "ARINC429": ("tip_translate_arinc429", "ARINC429_F0"),
    }
    translate_type = metaflow.Parameter(
        "type",
        help=f"What type of data to translate ({'|'.join(translate_options.keys())})",
        default="MILSTD1553",
    )

    @step
    def start(self):
        """
        Verify some stuff, preliminary setup
        """

        parse_flow = metaflow.Flow("TipParseFlow")
        self.parse_run = parse_flow[self.parse_pointer]

        assert self.translate_type in self.translate_options

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        os.mkdir(self.temp_dir)

        self.next(self.download_parsed)

    @step
    def download_parsed(self):
        """
        Download the parsed data, because tip only runs locally
        """
        self.dtemp = tempfile.mkdtemp()
        self.dtemp = os.path.join(self.dtemp, "parse_data.parquet")

        s3fs = opal.flow.minio_s3fs()

        # construct the S3 path to the parquet file
        _, parse_dataset_name = self.translate_options[self.translate_type]
        self.parse_data_s3_path = os.path.join(
            self.parse_run.data.data_files["parsed_data"],
            f"{self.parse_run.data.ch10_name}_{parse_dataset_name}.parquet",
        )

        # get it
        s3fs.get(self.parse_data_s3_path, self.dtemp, recursive=True)

        self.next(self.translate)

    # TODO: additional tip arguments from command line
    @step
    def translate(self):
        """
        Run tip_translate
        """
        tip_command, file_name = self.translate_options[self.translate_type]
        subprocess.run(
            [tip_command, self.dtemp, self.dts, "-o", self.temp_dir, "-L", "off"]
        )

        self.next(self.extract_metadata)

    @step
    def extract_metadata(self):
        """
        Gather tip metadata into the metaflow artifact object
        """
        self.tip_metadata = {}

        # scan for an "_metadata.yaml" file, save in tip_metadata
        for dirpath, dirnames, fnames in os.walk(self.temp_dir):
            if "_metadata.yaml" in fnames:
                with open(os.path.join(dirpath, "_metadata.yaml")) as f:
                    data = yaml.safe_load(f)
                    print(f"Found metadata for {data['type']}")
                    self.tip_metadata[data["type"]] = data

        # copy over some metadata from the parse run
        self.ch10_metadata = self.parse_run.data.ch10_metadata
        # we should find at least one
        if not self.tip_metadata:
            raise Exception("No tip metadata file found. Tip might be broken.")

        self.next(self.upload_data)

    @step
    def upload_data(self):
        """
        Move the translated files up to S3
        """
        self.upload(self.temp_dir, key="translated_data")
        self.next(self.end)

    @card
    @step
    def end(self):
        print("Deleting temp dir")
        shutil.rmtree(self.dtemp)


if __name__ == "__main__":
    TipTranslateFlow()
