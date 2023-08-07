from setuptools import setup, find_packages

import src
import bigdatatoolbox

setup(
  name='bigdatatoolbox',
  version=src.__version__,
  author=src.__author__,
  url='https://databricks.com',
  author_email='nacho.soto@thebridgeschool.es',
  description='my test wheel',
  packages=find_packages(include=['src', "bigdatatoolbox"]),
  entry_points={
    'src': 'run=__main__:main',
  },
  install_requires=[
    'setuptools'
  ]
)
