import setuptools
import json

with open("readme.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name="db_utils",
    version='0.1',
    author="Jeff Disbrow",
    author_email="disbr007@umn.edu",
    description="Tools for interacting with Postgres databases.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/disbr007/db_utils",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)