#!/usr/bin/env python3

from setuptools import setup
import versioneer

setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    name='aws-helpers',
    description='Helpers for AWS services using Boto3',
    author='Celogeek',
    author_email='me@celogeek.com',
    url='https://github.com/celogeek/aws-helpers',
    packages=["aws_helpers"],
    test_suite='nose.collector'
)
