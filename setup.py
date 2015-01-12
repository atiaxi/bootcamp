#!/usr/bin/env python

from distutils.core import setup

setup(name='Capstone_KARJ',
      version='1.0',
      description='Food recommendation project',
      author='KARJ',
      author_email='roger.ostrander@datastax.com',
      url='http://http://54.68.239.187//',
      py_modules=['foodreview'],
      install_requires=['flask', 'cassandra-driver', 'beautifulsoup4'],
     )
