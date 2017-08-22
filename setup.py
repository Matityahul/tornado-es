# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

VERSION = '3.0.0'

setup(
    name='tornadoes-ext',
    version=VERSION,
    description="A tornado-powered python library that provides asynchronous access to elasticsearch. Extends the tornadoes package.",
    author='Matityahul',
    author_email='lior.mati@gmail.com',
    url='http://github.com/Matityahul/tornado-es-ext',
    download_url='http://github.com/Matityahul/tornado-es-ext',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'testes']),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Operating System :: OS Independent',
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'tornado>=3.0.0',
        'six>=1.7.3',
    ],
    tests_require=[
        'unittest2',
        'nose'
    ],
    dependency_links=[],
)
