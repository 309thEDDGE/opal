import metaflow
import os
from metaflow import step, card
import os
import shutil
import subprocess
import opal.flow
import yaml


class TipParseFlow(opal.flow.OpalFlowSpec):
    chapter_10_file = metaflow.Parameter(
        "c10", help="path to the chapter 10 file to parse", required=True
    )

    # TODO: replace default with tempfile.mkdtemp()?
    temp_dir = metaflow.Parameter(
        "parse-dir",
        help="directory to temporarily save parsed output to",
        default=os.path.expanduser("~/parsed_data"),
    )

    @step
    def start(self):
        """
        Create empty temporary output location, get basic information about the input CH10
        """

        self.ch10_name = ".".join(
            os.path.basename(self.chapter_10_file).split(".")[:-1]
        )

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        os.mkdir(self.temp_dir)

        self.ch10_size = os.path.getsize(self.chapter_10_file)

        self.next(self.parse)

    # TODO: additional tip arguments from the command line
    @step
    def parse(self):
        """
        Use tip_parse to parse the chapter 10 file
        """
        subprocess.run(
            [
                "tip_parse",
                self.chapter_10_file,
                "-L",
                "off",
                "-o",
                self.temp_dir,
                "-t",
                "4",
            ]
        )

        self.next(self.extract_metadata)

    @step
    def extract_metadata(self):
        """
        Gather tip metadata into the metaflow artifact object
        """
        # scan for metadata files, save the output as a dict in tip_metadata
        self.tip_metadata = {}
        for de in os.scandir(self.temp_dir):
            # look for _metadata.yaml under *.parquet folders
            if de.is_dir() and de.path.endswith(".parquet"):
                meta_file = os.path.join(de.path, "_metadata.yaml")
                if os.path.exists(meta_file):
                    # load the yaml and save it in metaflow
                    with open(meta_file) as f:
                        tip_metadata = yaml.safe_load(f)
                        print(f"Found metadata for {tip_metadata['type']}")
                        self.tip_metadata[tip_metadata["type"]] = tip_metadata

        # we should find at least one
        if not self.tip_metadata:
            raise Exception("No tip metadata file found. Tip might be broken.")

        self.next(self.upload_data)

    @step
    def upload_data(self):
        """
        Move the parsed files up to S3
        """
        self.upload(self.temp_dir, key="parsed_data")
        self.next(self.end)

    @card
    @step
    def end(self):
        print("All done")


if __name__ == "__main__":
    TipParseFlow()
