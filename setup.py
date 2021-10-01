import setuptools
import json

with open("readme.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

with open("requirements.txt", "r") as fh:
    DEPENDENCIES = fh.readlines()
    

setuptools.setup(
    name="dbtools",
    version='0.1',
    author="Jeff Disbrow",
    author_email="disbr007@umn.edu",
    description="Tools for interacting with Postgres databases.",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/disbr007/dbtools",
    package_dir={"":"."},
    packages=setuptools.find_packages(),
    py_modules=['pg'],
    requires=DEPENDENCIES,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)