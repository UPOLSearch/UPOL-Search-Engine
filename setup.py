#!/usr/bin/env python

from distutils.core import setup

from upol_crawler import settings

setup(name='UPOL-Crawler',
      version=settings.CONFIG.get('Info', 'version'),
      description='Web crawler for UPOL Search, part of Master thesis on Department of Computer Science UPOL',
      author='Tomas Mikula',
      author_email='mail@tomasmikula.cz',
      license='MIT',
      url='https://github.com/UPOLSearch/UPOL-Crawler',
      packages=['upol_crawler'],
      entry_points={
          'console_scripts': [
              'upol_crawler = upol_crawler.__main__:main'
          ]
      }
      )
