@set PYTHONPATH=%PYTHONPATH%;%cd%
@cd tests
@python -m unittest discover --pattern=test_*.py -v
@cd ..