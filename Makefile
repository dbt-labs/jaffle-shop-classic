
unit-test:
	python -m pytest --cov-report term-missing
	coverage-badge -o coverage-python.svg -f
