from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

VERSION = '0.1.3'
DESCRIPTION = 'Easy download and export EBAS data'
LONG_DESCRIPTION = 'Python package for an easy-access to open-source air pollutant data from EBAS database via FTP links.'


# Setting up
setup(
    name="pyebas",
    version=VERSION,
    author="Chuanlong Zhou",
    author_email="<zhouchuanlong1988@gmail.com>",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(),
    
    install_requires=["numpy","pandas","xarray","pycountry","bs4","wget","tqdm"],
    url = 'https://github.com/defve1988/pyebas',
    entry_points={
        'console_scripts': [
            'pyebas = pyebas.main:main'
        ]
    },
    keywords=['python', 'ebas', 'air pollution', 'ftp'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
