import metaflow
from metaflow import S3, FlowSpec, step
import os
import pathlib as pl
from opal.flow import OpalFlowSpec


class UploadFlow(OpalFlowSpec):
    d = metaflow.Parameter("dir")

    @step
    def start(self):
        self.upload(self.d, "file")
        self.next(self.end)

    @step
    def end(self):
        print("e")


if __name__ == "__main__":
    UploadFlow()
