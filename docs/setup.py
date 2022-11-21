from pathlib import Path
from setuptools import find_packages, setup


current_dir = Path(__file__).parent

setup(
    name="opal",
    version="0.0.1",
    packages=find_packages(exclude=("tests", "dts_utils")),
    install_requires=["pyyaml"],
    setup_requires=["wheel"],
    python_requires=">=3.6",
)
