#!/usr/bin/env python
# coding: utf-8

"""Packaging for Isilon Hadoop Tools"""

import setuptools

with open('README.md') as readme_file:
    README = readme_file.read()

setuptools.setup(
    name='isilon_hadoop_tools',
    use_scm_version=True,
    description='Tools for Using Hadoop with OneFS',
    long_description=README,
    long_description_content_type='text/markdown',
    license='MIT',
    url='https://github.com/isilon/isilon_hadoop_tools',
    maintainer='Isilon',
    maintainer_email='support@isilon.com',
    package_dir={'': 'src'},
    packages=setuptools.find_packages('src'),
    include_package_data=True,
    python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*',
    setup_requires=['setuptools_scm ~= 3.3'],
    install_requires=[
        'enum34 >= 1.1.6; python_version<"3.4"',
        'future >= 0.16.0',
        'isi-sdk-7-2 ~= 0.2.7',
        'isi-sdk-8-0 ~= 0.2.7',
        'isi-sdk-8-0-1 ~= 0.2.7',
        'isi-sdk-8-1-0 ~= 0.2.7',
        'isi-sdk-8-1-1 ~= 0.2.7',
        'isi-sdk-8-2-0 ~= 0.2.7',
        'isi-sdk-8-2-1 ~= 0.2.7',
        'isi-sdk-8-2-2 ~= 0.2.7',
        'requests >= 2.20.0',
        'setuptools >= 41.0.0',
    ],
    entry_points={
        'console_scripts': [
            'isilon_create_directories = isilon_hadoop_tools._scripts:isilon_create_directories',
            'isilon_create_users = isilon_hadoop_tools._scripts:isilon_create_users',
        ],
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
