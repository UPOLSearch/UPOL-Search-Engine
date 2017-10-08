#!/usr/bin/env python

from setuptools import find_packages, setup

setup(name='UPOL-Search-Engine',
      version='0.6-dev',
      description='UPOL Search engine is search engine for upol.cz domain, \
      topic of Master thesis on Department of Computer Science UPOL',
      author='Tomas Mikula',
      author_email='mail@tomasmikula.cz',
      license='MIT',
      url='https://github.com/UPOLSearch/UPOL-Search-Engine',
      packages=find_packages(),
      package_data={
          'upol_search_engine': ['config-default.ini'],
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
          'langdetect',
          'networkx',
          'psycopg2'
      ],
      entry_points={
          'console_scripts': [
              'upol_search_engine = upol_search_engine.__main__:main'
          ]
      }
      )
