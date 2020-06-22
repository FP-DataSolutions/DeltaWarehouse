from setuptools import setup
from setuptools import find_packages

setup(name='etl_sparkdw_tools',
      version='0.1.1',
      description='ETL Spark Delta Warehouse tools',
      packages=find_packages(),
      author='tomasz.k.krawczyk@gmail.com',
      package_data={'': ['model.pb']},
      install_requires=['pyspark', 'numpy', 'pandas'],
      include_package_data=True)

