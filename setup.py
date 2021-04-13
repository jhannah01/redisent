import os.path
import setuptools

with open('README.md', 'rt') as f:
    readme_contents = f.read()

with open('requirements.txt', 'rt') as f:
    install_requirements = [req.rstrip('\n') for req in f.readlines()]

with open('docs/requirements.txt', 'rt') as f:
    doc_requirements = [req.rstrip('\n') for req in f.readlines()]

with open('testing/requirements.txt', 'rt') as f:
    test_requirements = [req.rstrip('\n') for req in f.readlines()]


setuptools.setup(
    name='redisent',
    author='Jon Hannah',
    author_email='jon@synistree.com',
    description='Python library for reading and storing entries of dataclasses in Redis',
    keywords='redis, serialization',
    long_description=readme_contents,
    long_description_content_type='text/markdown',
    url='https://github.com/synistree/redisent',
    project_urls={
        'Documentation': 'https://redisent.readthedocs.io/en/latest/',
        'Source Code': 'https://github.com/synistree/redisent'},
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src'),
    python_requires='>=3.8',
    zip_safe=False,
    extras_require={
        'dev': ['IPython', 'jupyterlab'],
        'test': test_requirements,
        'docs': doc_requirements
    },
    include_package_data=True,
    package_data={
        'redisent': ['py.typed', '*.pyi']
    },
    install_requires=install_requirements
)
