import metaflow
import opal.flow
import pytest


@pytest.fixture
def opal_flow_and_run():
    
    s3 = opal.flow.minio_s3fs()

    ex_flow = metaflow.Flow("ExampleFlow")
    up_flow = metaflow.Flow("UploadFlow")
    ex_run = ex_flow.latest_successful_run
    up_run = up_flow.latest_successful_run
    
    yield s3, ex_flow, up_flow, ex_run, up_run
    

def test_local_shared_metadata_is_configured_correctly(opal_flow_and_run):
    assert metaflow.get_metadata() == "local@/opt/opal/metaflow-metadata"
    
    
def test_files_in_data_files_actually_exist(opal_flow_and_run):
    s3, ex_flow, up_flow, ex_run, up_run = opal_flow_and_run
    assert s3.ls(up_run.data.data_files['file'])
    assert s3.exists(ex_run.data.data_files['parquet_table'])
    

def test_tagged_as_no_data(opal_flow_and_run):
    s3, ex_flow, up_flow, ex_run, up_run = opal_flow_and_run
    assert 'no_data' not in up_run.tags
    
    
def test_data_files_deleted(opal_flow_and_run):
    s3, ex_flow, up_flow, ex_run, up_run = opal_flow_and_run
    assert s3.ls(up_run.data.data_files['file'])

