from setuptools import find_packages
from setuptools import setup

setup(
    name="saturn_engine",
    version="0.2.0dev55", # Also in `pyproject.toml`
    packages=["saturn_engine"],
    package_dir={"":"src"},
    install_requires=[],
)
