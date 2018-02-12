#!/usr/bin/env python

from setuptools import find_packages, setup

setup(name='UPOL-Search-Engine',
      version='0.7-dev',
      description='UPOL Search engine is search engine for upol.cz domain, \
      topic of Master thesis on Department of Computer Science UPOL',
      author='Tomas Mikula',
      author_email='mail@tomasmikula.cz',
      license='MIT',
      url='https://github.com/UPOLSearch/UPOL-Search-Engine',
      packages=find_packages(),
      package_data={
          'upol_search_engine': ['config-default.ini',
                                 'upol_search_engine/templates/search/*',
                                 'upol_search_engine/templates/info/*',
                                 'upol_search_engine/templates/error/*',
                                 'upol_search_engine/static/css/*',
                                 'upol_search_engine/static/js/*',
                                 'upol_search_engine/static/fonts/*',
                                 'upol_search_engine/static/images/*.png'],
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
          'psycopg2-binary',
          'flask',
          'html5lib',
          'pdfminer.six',
          'backports.pbkdf2',
          'timeout-decorator'
      ],
      entry_points={
          'console_scripts': [
              'upol_search_engine = upol_search_engine.__main__:main'
          ]
      }
      )
