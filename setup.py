import dblink
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='dblink',
    version=dblink.__version__,
    description='DBLink tools makes operation on exists table easier.',
    author='skyduy',
    author_email='cuteuy@gmail.com',
    url='https://github.com/skyduy/dblink',
    license='MIT',
    keywords='sql orm dblink',
    test_suite="tests",
    packages=find_packages(exclude=['tests']),
    data_files=[
        ('./', ['README.md'])
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
