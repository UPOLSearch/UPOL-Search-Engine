#!/usr/bin/env python

# from distutils.core import setup
from setuptools import find_packages, setup

setup(name='UPOL-Crawler',
      version='0.4-dev',
      description='Web crawler for UPOL Search, part of Master thesis on Department of Computer Science UPOL',
      author='Tomas Mikula',
      author_email='mail@tomasmikula.cz',
      license='MIT',
      url='https://github.com/UPOLSearch/UPOL-Crawler',
      packages=find_packages(),
      package_data={
          'upol_crawler': ['config-default.ini'],
      },
      install_requires=[
          'beautifulsoup4',
          'celery',
          'lxml',
          'pymongo',
          'pytest',
          'reppy',
          'requests',
          'w3lib',
          'langdetect'
      ],
      entry_points={
          'console_scripts': [
              'upol_crawler = upol_crawler.__main__:main'
          ]
      }
      )
