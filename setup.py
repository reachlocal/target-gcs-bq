#!/usr/bin/env python

from setuptools import setup

setup(name='target-gcs-bq',
      version='0.3.0',
      description='Singer.io target for writing CSV files to GCS',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_gcs_bq'],
      install_requires=[
          'jsonschema==2.6.0',
          'singer-python==2.1.4',
          'google-api-python-client==1.10.0',
          'google-auth==1.21.1',
          'google-cloud-bigquery==1.26.1',
          'google-cloud-storage==1.30.0'
      ],
      entry_points='''
          [console_scripts]
          target-gcs-bq=target_gcs_bq:main
      ''',
)
