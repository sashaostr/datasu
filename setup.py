#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
from setuptools.command.test import test as TestCommand


from distutils.core import setup


exec(compile(open('datasu/__init__.py').read(),
                  'datasu/__init__.py', 'exec'))

setup(name='datasu',
      version=__version__,
      description='utils for data science',
      maintainer='sashaostr (Alexander Ostrikov)',
      maintainer_email='alexander.ostrikov@gmail.com',
      url='https://github.com/sashaostr/datasu.git',
      packages=['datasu'],
      keywords=['data science', 'utils', 'pandas', 'sklearn'],
      install_requires=[
          'pandas>=0.11.0',
          'matplotlib',
          'sklearn',
          'scipy',
          'gzip',
          'joblib'



      ],
      # tests_require=['pytest', 'mock'],
      # cmdclass={'test': PyTest},
      )