from setuptools import setup, find_packages


def get_long_description():
    def read(fn):
        try:
            import pypandoc
            long_description = pypandoc.convert_file(fn, 'rst')
        except(IOError, ImportError):
            try:
                long_description = open(fn).read()
            except Exception:
                long_description = ''
        except Exception:
            long_description = ''
        return long_description
    return '\n\n'.join(read(f) for f in ('README.md', 'HISTORY.md'))


setup(
    name='dblink',
    version='0.2.1',
    description='DBLink tools makes operation on exists table easier.',
    author='skyduy',
    author_email='cuteuy@gmail.com',
    url='https://github.com/skyduy/dblink',
    license='MIT',
    keywords='sql orm dblink',
    test_suite="tests",
    packages=find_packages(exclude=['tests']),
    data_files=[
        ('./', ['HISTORY.md', 'README.md'])
    ],
    long_description=get_long_description(),
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
