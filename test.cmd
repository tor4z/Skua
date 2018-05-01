@set PYTHONPATH=%PYTHONPATH%;%cd%
@cd test
@python -m unittest discover --pattern=test_*.py -v
@cd ..