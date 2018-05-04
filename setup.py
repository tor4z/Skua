from setuptools import setup, find_packages


PACKAGE      = "skua"
NAME         = PACKAGE
DESCRIPTION  = "MySQL, SQLite, MongoDB wrapper for Python3"
AUTHOR       = "tor4z"
AUTHOR_EMAIL = "vwenjie@hotmail.com"
URL          = "https://github.com/tor4z/Skua"
LICENSE      = "MIT License"
VERSION      = 0.01
INSTALL_REQUIRES = [pymongo, PyMySQL]

setup(name=NAME,
      version=VERSION,
      description=DESCRIPTION,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      license=LICENSE,
      url=URL,
      install_requires=INSTALL_REQUIRES
      packages = find_packages(exclude=["tests"]))