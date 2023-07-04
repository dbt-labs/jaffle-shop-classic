
unit-test:
	python -m pytest --cov-report term-missing
	python -m code_coverage
	coverage-badge -o coverage-python.svg -f
