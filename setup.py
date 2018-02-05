from setuptools import setup, find_packages


setup(
    name='dblink',
    version='0.1.0',
    description='DBLink tools makes operation on exists table easier.',
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
        'Development Status :: Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
