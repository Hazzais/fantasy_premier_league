import os
from setuptools import setup, find_packages

setup(name='fpl_etl',
      version='0.0.1',
      description='Tools to extract, transform, and then load data from the '
                  'official Premier League Fantasy Football API',
      long_description=open('README.md').read(),
      long_description_content_type='text/markdown',
      url='https://github.com/Hazzais/fantasy_premier_league',
      author='Harry Firth',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
      ],
      install_requires=['boto3',
                        'numpy',
                        'pandas',
                        'psycopg2',
                        'sqlalchemy'],
      python_requires='>=3.6',
      packages=find_packages(exclude=['tests'])
      )
