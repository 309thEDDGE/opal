import metaflow
from metaflow import FlowSpec, step, card, S3
import pandas as pd
import tempfile
import os
from opal.flow import OpalFlowSpec

# There is a ticket in to change the names of these environment variables
# This SHOULD only be temporary
os.environ["AWS_ACCESS_KEY_ID"] = os.environ["S3_KEY"]
os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["S3_SECRET"]
os.environ["AWS_SESSION_TOKEN"] = os.environ["S3_SESSION"]
os.environ["USERNAME"] = "jovyan"

# https://docs.metaflow.org/metaflow/basics

# This flow generates a multiplication table and saves it to parquet
# Note: Flow files must pass pylint
class ExampleFlow(OpalFlowSpec):
    # number to go up to
    n = metaflow.Parameter("n", help="Number to make the table up to", required=True)

    @step  # the '@step' decorator is required
    def start(self):  # 'start' and 'end' steps are required
        # get the number parameter and convert it to an int
        self.n_int = int(self.n)

        # self.next points to next step
        self.next(self.save_table)

    @step
    def save_table(self):
        """
        Make the multiplication table and
        save the table to minio 
        """
        # https://docs.metaflow.org/metaflow/data

        s = pd.Series(range(1, self.n_int)).to_frame()
        df = s.merge(s, how="cross")
        df["prod"] = df["0_x"] * df["0_y"]

        df = df.set_index(["0_x", "0_y"])
        
        # the s3 variable here isn't S3FS, so it doesn't support just opening a file
        # and pandas can't save directly to this.
        # This is one of the things we can add a workaround for.
        d = tempfile.gettempdir()
        path = os.path.join(d, f"mult_table_{self.n_int}.parquet")
        df.to_parquet(path)

        # doesn't have to be 'run=self', you can set a different root path - see website above
        # We could also skip this entirely and save files wherever/however we want with S3FS,
        # and put the S3 paths to those files in the metaflow data under 'self'
        self.upload(path, "parquet_table")

        self.next(self.end)

    @card  # the card decorator saves data to generate a nice HTML file
    @step
    def end(self):
        print("All done!")


# This part is necessary to run the flow from the command line
if __name__ == "__main__":
    ExampleFlow()
