import os.path
import setuptools

from redisent import __version__ as redisent_version

req_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
with open(req_path, 'rt') as f:
    install_reqs = [req.rstrip('\n') for req in f.readlines()]


setuptools.setup(
    name='redisent',
    version=redisent_version,
    packages=setuptools.find_packages(where='.'),
    url='https://github.com/jhannah01/redisent',
    license='',
    author='Jon Hannah',
    author_email='jon@synistree.com',
    description='Python library for reading and storing entries of dataclasses in Redis',
    include_package_data=True,
    package_data={
        'redisent': ['py.typed']
    },
    install_requires=install_reqs
)
