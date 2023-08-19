import os
import pytest

import metaflow
import opal.flow


@pytest.fixture
def opal_flow_and_run():

    s3 = opal.flow.minio_s3fs()

    ex_flow = metaflow.Flow("ExampleFlow")
    up_flow = metaflow.Flow("UploadFlow")
    ex_run = ex_flow.latest_successful_run
    up_run = up_flow.latest_successful_run

    yield s3, ex_run, up_run


def test_local_shared_metadata_is_configured_correctly(opal_flow_and_run):
    assert metaflow.get_metadata() == "local@/opt/opal/metaflow-metadata"


def test_files_in_data_files_actually_exist(opal_flow_and_run):
    s3, ex_run, up_run = opal_flow_and_run

    assert s3.ls(up_run.data.data_files["file"])
    assert s3.exists(ex_run.data.data_files["parquet_table"])


def test_tagged_as_no_data(opal_flow_and_run):
    up_run = opal_flow_and_run[2]

    assert "no_data" not in up_run.tags


def test_data_files_deleted(opal_flow_and_run):
    s3 = opal_flow_and_run[0]
    up_run = opal_flow_and_run[2]

    assert s3.ls(up_run.data.data_files["file"])


def test_frozenset_up_run_tags(opal_flow_and_run):
    up_run = opal_flow_and_run[2]

    assert type(up_run.tags) == frozenset


def test_size_of_ex_run_data_files(opal_flow_and_run):
    import pandas as pd

    s3 = opal_flow_and_run[0]
    ex_run = opal_flow_and_run[1]

    ex_run_df = pd.read_parquet(ex_run.data.data_files["parquet_table"],
                                filesystem=s3)
    ex_run_df_shape = ex_run_df.shape

    assert ex_run_df_shape == (64, 1)


def test_pip_install_metaflow():
    exit_status = os.system("pip list | grep metaflow")
    import metaflow
    # An exit status of 0 indicates a successful execution
    assert exit_status == 0


def test_pip_install_panel():
    exit_status = os.system("pip list | grep panel")
    import panel
    # An exit status of 0 indicates a successful execution
    assert exit_status == 0


def test_pip_install_holoviews():
    exit_status = os.system("pip list | grep holoviews")
    import holoviews
    # An exit status of 0 indicates a successful execution
    assert exit_status == 0


def test_pip_install_param():
    exit_status = os.system("pip list | grep param")
    import param
    # An exit status of 0 indicates a successful exectuion
    assert exit_status == 0


def test_pip_install_plotly():
    exit_status = os.system("pip list | grep plotly")
    import plotly
    # An exit status of 0 indicates a successful execution
    assert exit_status == 0
