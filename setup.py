#!/usr/bin/env python

import os
from setuptools import setup, find_packages
import distutils.dir_util


def list_files(directory):
    paths = []
    for path, directories, filenames in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join(path, filename))
    return paths


setup(
    name="hdcutil",
    version="0.1.7",
    author="attapon.th",
    maintainer="attapon.th",
    maintainer_email="attapon.4work@gmial.com",
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    # install_requires=open("requirements.txt").readlines(),
    packages=find_packages(),
    python_requires=">=3.8",
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    # entry_points={
    #     "console_scripts": ["mercury=mercury.mercury:main"],
    # },
    # package_data={"hdcutil": list_files("hdcutil")},
    # # package_data={"hdcutil": list_files("hdcutil") + ["requirements.txt"]},
    # include_package_data=True,
)
