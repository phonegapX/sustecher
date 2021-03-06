from setuptools import setup, find_packages
from jaqs import __version__ as ver
import codecs


def read_install_requires():
    with codecs.open('requirements.txt', 'r', encoding='utf-8') as f:
        res = f.readlines()
    res = list(map(lambda s: s.replace('\n', ''), res))
    return res

setup(
    # Install data files specified in MANIFEST.in file.
    include_package_data=True,
    #package_data={'': ['*.json', '*.css', '*.html']},
    # Package Information
    name='jaqs',
    url='https://github.com/phonegapx/sustecher',
    version=ver,
        license='Apache 2.0',
    # information
    description='Open source quantitative research&trading framework.',
    keywords="quantiatitive trading research finance",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: Chinese (Simplified)",
        "Natural Language :: English",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: Unix",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
    ],
    # install
    install_requires=read_install_requires(),
    packages=find_packages(),
    # author
    author='moses'
)
