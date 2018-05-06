from setuptools import setup, find_packages
import unittest


PACKAGE      = "skua"
NAME         = PACKAGE
DESCRIPTION  = "MySQL, SQLite, MongoDB wrapper for Python3"
AUTHOR       = "tor4z"
AUTHOR_EMAIL = "vwenjie@hotmail.com"
URL          = "https://github.com/tor4z/Skua"
LICENSE      = "MIT License"
VERSION      = 0.1
INSTALL_REQUIRES = ["pymongo", "PyMySQL"]

def test_suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover("tests", pattern="test_*.py")
    return test_suite

setup(name=NAME,
      version=VERSION,
      description=DESCRIPTION,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      license=LICENSE,
      url=URL,
      install_requires=INSTALL_REQUIRES,
      test_suite="setup.test_suite",
      packages = find_packages(exclude=["tests"]))