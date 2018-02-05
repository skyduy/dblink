from setuptools import setup, find_packages


def read(fn):
    try:
        import pypandoc
        long_description = pypandoc.convert_file(fn, 'rst')
    except(IOError, ImportError):
        long_description = open(fn).read()
    return long_description


setup(
    name='dblink',
    version='0.1.2',
    description='DBLink tools makes operation on exists table easier.',
    long_description=read('README.md') + '\n\n' + read('HISTORY.md'),
    author='skyduy',
    author_email='cuteuy@gmail.com',
    url='https://github.com/skyduy/dblink',
    license='MIT',
    keywords='sql orm dblink',
    test_suite="tests",
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'sqlalchemy==1.2.2',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
