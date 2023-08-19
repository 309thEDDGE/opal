import os
import pytest

import metaflow
import opal.flow


@pytest.fixture
def opal_flow_and_run():
    """Pytest fixture to set up the tests."""

    s3 = opal.flow.minio_s3fs()

    ex_flow = metaflow.Flow("ExampleFlow")
    up_flow = metaflow.Flow("UploadFlow")
    ex_run = ex_flow.latest_successful_run
    up_run = up_flow.latest_successful_run

    yield s3, ex_run, up_run


def test_local_shared_metadata_is_configured_correctly(opal_flow_and_run):
    """Test that we collect metadata from metaflow."""
    assert metaflow.get_metadata() == "local@/opt/opal/metaflow-metadata"


def test_files_in_data_files_actually_exist(opal_flow_and_run):
    # """Test that are our s3 file system exists."""
    s3, ex_run, up_run = opal_flow_and_run

    assert s3.ls(up_run.data.data_files["file"])
    assert s3.exists(ex_run.data.data_files["parquet_table"])


def test_tagged_as_no_data(opal_flow_and_run):
    """Test that all of the tags in up_run are unique"""
    up_run = opal_flow_and_run[2]

    assert "no_data" not in up_run.tags


def test_data_files_deleted(opal_flow_and_run):
    """Test that that up_run contains data_files"""
    s3 = opal_flow_and_run[0]
    up_run = opal_flow_and_run[2]

    assert s3.ls(up_run.data.data_files["file"])


def test_frozenset_up_run_tags(opal_flow_and_run):
    """Test that up_run is a frozenset type"""
    up_run = opal_flow_and_run[2]

    assert type(up_run.tags) == frozenset


def test_size_of_ex_run_data_files(opal_flow_and_run):
    """Test that the size of ex_run is what we expect"""
    import pandas as pd

    s3 = opal_flow_and_run[0]
    ex_run = opal_flow_and_run[1]

    ex_run_df = pd.read_parquet(ex_run.data.data_files["parquet_table"],
                                filesystem=s3)
    ex_run_df_shape = ex_run_df.shape

    assert ex_run_df_shape == (64, 1)


def test_pip_install_metaflow():
    """Test that metaflow exists in the pip list"""
    exit_status = os.system("pip list | grep metaflow")
    # We simply want to see if the import statment does not
    # raise an error
    # pylint: disable-next=unused-import
    import metaflow
    # An exit status of 0 indicates a successful execution
    assert exit_status == 0


def test_pip_install_panel():
    """Test that panel exists in the pip list"""
    exit_status = os.system("pip list | grep panel")
    # We simply want to see if the import statment does not
    # raise an error
    # pylint: disable-next=unused-import
    import panel
    # An exit status of 0 indicates a successful execution
    assert exit_status == 0


def test_pip_install_holoviews():
    """Test that holoviews exists in the pip list"""
    exit_status = os.system("pip list | grep holoviews")
    # We simply want to see if the import statment does not
    # raise an error
    # pylint: disable-next=unused-import
    import holoviews
    # An exit status of 0 indicates a successful execution
    assert exit_status == 0


def test_pip_install_param():
    """Test that param exists in the pip list"""
    exit_status = os.system("pip list | grep param")
    # We simply want to see if the import statment does not
    # raise an error
    # pylint: disable-next=unused-import
    import param
    # An exit status of 0 indicates a successful exectuion
    assert exit_status == 0


def test_pip_install_plotly():
    """Test that plotly exists in the pip list"""
    exit_status = os.system("pip list | grep plotly")
    # We simply want to see if the import statment does not
    # raise an error
    # pylint: disable-next=unused-import
    import plotly
    # An exit status of 0 indicates a successful execution
    assert exit_status == 0
