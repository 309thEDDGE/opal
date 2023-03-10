from setuptools import setup, find_namespace_packages

setup(
    name="opal-packages",
    version="0.1",
    packages=["opal.flow", "opal.publish", "opal.query", "opal.juice"],
    install_requires=["metaflow", "numpy", "pandas", "s3fs", "requests"],
    package_data={"opal.flow_utils": ["resources/flow_script_upload.py"]},
)
