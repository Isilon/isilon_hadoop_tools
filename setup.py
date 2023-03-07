"""Packaging for Isilon Hadoop Tools"""

import setuptools

with open("README.md", encoding="utf-8") as readme_file:
    README = readme_file.read()

setuptools.setup(
    name="isilon_hadoop_tools",
    description="Tools for Using Hadoop with OneFS",
    long_description=README,
    long_description_content_type="text/markdown",
    license="MIT",
    url="https://github.com/isilon/isilon_hadoop_tools",
    maintainer="Isilon",
    maintainer_email="support@isilon.com",
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    include_package_data=True,
    python_requires=">=3.7",
    install_requires=[
        "isi-sdk-7-2 ~= 0.2.11",
        "isi-sdk-8-0 ~= 0.2.11",
        "isi-sdk-8-0-1 ~= 0.2.11",
        "isi-sdk-8-1-0 ~= 0.2.11",
        "isi-sdk-8-1-1 ~= 0.2.11",
        "isi-sdk-8-2-0 ~= 0.2.11",
        "isi-sdk-8-2-1 ~= 0.2.11",
        "isi-sdk-8-2-2 ~= 0.2.11",
        "requests >= 2.20.0",
        "setuptools >= 41.0.0",
        "urllib3 >= 1.22.0",
    ],
    entry_points={
        "console_scripts": [
            "isilon_create_directories = isilon_hadoop_tools._scripts:isilon_create_directories",
            "isilon_create_users = isilon_hadoop_tools._scripts:isilon_create_users",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
