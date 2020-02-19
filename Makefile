.DEFAULT_GOAL := all

.PHONY: install
install:
	pip install -U setuptools pip
	pip install -U -r requirements-test.txt
	pip install -e .[test]

.PHONY: format
format:
	isort -rc cubedash integration_tests
	black cubedash integration_tests

.PHONY: lint
lint:
	python setup.py check -rms
	flake8 cubedash/ integration_tests/
	black --check cubedash integration_tests

.PHONY: weblint
weblint:
	stylelint $(find . -iname '*.html') $(find . -iname '*.sass')

.PHONY: style
style: cubedash/static/base.css

cubedash/static/base.css: cubedash/static/base.sass
	sass -t compact --no-cache $< $@

.PHONY: test
test:
	pytest -r sx --durations=5

.PHONY: testcov
testcov:
	pytest --cov=cubedash
	@echo "building coverage html"
	@coverage html

.PHONY: all
all: testcov lint 

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -rf .cache
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf htmlcov
	rm -rf *.egg-info
	rm -f .coverage
	rm -f .coverage.*
	rm -rf build
	# python setup.py clean

