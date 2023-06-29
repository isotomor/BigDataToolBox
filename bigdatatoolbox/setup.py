from distutils.core import setup
from setuptools import find_packages

setup(
  name='bigdatatoolbox',         # The name that pip will use for your package
  packages=find_packages(include=["bigdatatoolbox"]),   # Chose the same as "name"
  version='0.1',      # Start with a small number and increase it with every change you make
  description='Paquete de funciones auxiliares',   # Give a short description about your library
  author='Ignacio',                   # Type in your name
  author_email='i.sotomoreno@gmail.com',      # Type in your E-Mail
  url='https://github.com/isotomor/BigDataToolBox', # Provide either the link to your github or to your website
  setup_requires=['pytest-runner', 'flake8', 'wheel'], # Esto no se instala en el paquete. Flake8 comprueba formato.
  tests_require=['pytest'],
  package_data={'edh_functions': ['config.yaml']},
  include_package_data=True,
  classifiers=['Development Status :: 4 - Beta',
               'Intended Audience :: Developers'],
  test_suite="test"
)


"""
Para poder ejecutar una funcion concreta desde linea de comandos
entry_points={
      'console_scripts': ['my-command=exampleproject.example:main']
  }
  
  classifiers=[
  'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
  'Intended Audience :: Developers',      # Define that your audience are developers
  'Topic :: Software Development :: Build Tools',
  'License :: OSI Approved :: MIT License',   # Again, pick a license
  'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
  'Programming Language :: Python :: 3.4',
  'Programming Language :: Python :: 3.5',
  'Programming Language :: Python :: 3.6',
]
"""
