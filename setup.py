from setuptools import find_packages
from setuptools import setup
import os.path
import re

def get_version():
    with open(os.path.join(os.path.dirname(__file__), "pyproject.toml"), "r") as f:
        for l in f:
            if m := re.search(r'^version *= *"([^"]+)" *$', l):
                return m.group(1)


setup(
    name="saturn_engine",
    version=get_version(),
    packages=["saturn_engine"],
    package_dir={"":"src"},
    install_requires=[],
)
