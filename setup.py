from setuptools import setup, find_packages

from redisent import __version__  as redisent_version

setup(
    name='redisent',
    version=redisent_version,
    packages=find_packages(where='.'),
    url='https://github.com/jhannah01/redisent',
    license='',
    author='Jon Hannah',
    author_email='jon@synistree.com',
    description='Async-enabled Redis serialization library',
    include_package_data=True
)
