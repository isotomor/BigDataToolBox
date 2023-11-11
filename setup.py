from setuptools import setup, find_packages
import atexit
import sys
import src

setup(
  name='bigdatatoolbox',
  version=src.__version__,
  author=src.__author__,
  url='https://databricks.com',
  author_email='nacho.soto@thebridgeschool.es',
  description='Python Wheel para ',
  packages=find_packages(),
  entry_points={
    'group_1': 'run=src.__main__:main'
  },
  install_requires=[
    'setuptools'
  ]
)
