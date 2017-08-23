#!/usr/bin/env python3

import os
from setuptools import setup
from pip.req import parse_requirements

requirements = parse_requirements(os.path.join(os.path.dirname(__file__), 'requirements.txt'), session='dummy')

setup(name='mtv_dl',
      version='0.1',
      description='MediathekView Downloader',
      long_description='Command line tool to download videos from sources available through MediathekView.',
      author='Frank Epperlein',
      author_email='frank+mtv_dl@epperle.in',
      url='https://github.com/efenka/mtv_dl',
      py_modules=['mtv_dl'],
      entry_points={
            'console_scripts': [
                  'mtv_dl = mtv_dl:main',
            ]
      },
      classifiers=[
            'Topic :: Multimedia :: Video',
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'License :: Public Domain',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
      ],
      python_requires='>=3.5',
      install_requires=[str(ir.req) for ir in requirements])
