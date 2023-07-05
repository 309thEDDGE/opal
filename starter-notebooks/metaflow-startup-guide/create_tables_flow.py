import metaflow
from metaflow import step, card
import pandas as pd
import tempfile
import os
from opal.flow import OpalFlowSpec

# This flow generates two tables and saves them to the MinIO datastore
class CreateTablesFlow(OpalFlowSpec):
    # table_length is an input parameter passed when running this flow
    table_length_1 = metaflow.Parameter(
        "table_length_1",
        help="Length Of Table 1 to Create",
        required=True
    )

    table_length_2 = metaflow.Parameter(
        "table_length_2",
        help="Length Of Table 2 to Create",
        required=True
    )

    @step # the '@step' decorator is required
    def start(self): # 'start' and 'end' steps are required
        # Comments like the one below are attached to their function
        # and are visible from cards, cli tools, etc
        """
        Get the table_length parameters and convert them to integers
        """
        self.n_int_1 = int(self.table_length_1)
        self.n_int_2 = int(self.table_length_2)

        # self.next points to the next step
        self.next(self.create_tables)

    @step
    def create_tables(self):
        """
        Make tables
        """
        df1 = pd.DataFrame({'data': list(range(self.n_int_1))})
        df2 = pd.DataFrame({'data': list(range(self.n_int_2))})
        temp_file_1 = tempfile.gettempdir()
        temp_file_2 = tempfile.gettempdir()
        self.path1 = os.path.join(temp_file_1, f"table_1.parquet")
        self.path2 = os.path.join(temp_file_2, f"table_2.parquet")
        df1.to_parquet(self.path1)
        df2.to_parquet(self.path2)

        self.next(self.upload_tables)

    @step
    def upload_tables(self):
        """
        Upload the tables to MinIO
        """
        # self.upload(path, reference)
        #    path: The local path to the file being uploaded
        #    reference: A tag that will be used to reference the Data
        self.upload(self.path1, "table1")
        self.upload(self.path2, "table2") 
        self.next(self.save_metadata)

    @step
    def save_metadata(self):
        """
        Save Metadata
        """
        # All items saved to self will
        # be saved with the Flow Run
        self.metadata_1 = 1
        self.metadata_dictionary = {'key': 1}
        self.next(self.end)

    @card # The card decorator saves data to generate a nice HTML file
    @step
    def end(self):
        """
        Print All Done
        """
        print("All done!")

if __name__ == "__main__":
    CreateTablesFlow()