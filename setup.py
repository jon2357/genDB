from setuptools import setup, find_packages

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = ["pyodbc>=4"]

setup(
    name="gendb",
    version="0.0.1",
    author="Jon Strunk",
    author_email="jon@jonstrunk.com",
    description="A package to provide a generic SQL Querying Base Class",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/jon2357/genDB",
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">=3.5, <4",
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
)