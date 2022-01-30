SCRIPT_DIR='scripts'

# lists all available targets
list:
	@sh -c "$(MAKE) -p no_targets__ | \
		awk -F':' '/^[a-zA-Z0-9][^\$$#\/\\t=]*:([^=]|$$)/ {\
			split(\$$1,A,/ /);for(i in A)print A[i]\
		}' | grep -v '__\$$' | grep -v 'make\[1\]' | grep -v 'Makefile' | sort"
# required for list
no_targets__:

clean:
	@rm -rf build dist .eggs *.egg-info
	@rm -rf .coverage
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

format:
	@poetry run black dagma/ tests/ scripts/

check:
	@poetry run flake8 dagma tests scripts
	@poetry run pyright dagma tests scripts

setup:
	@poetry install

build:
	@poetry build

publish:
	@poetry publish

test:
	@poetry run pytest --cov=dagma --cov-config .coveragerc tests/

coverage_missing:
	@poetry run pytest --cov=dagma --cov-report term-missing --cov-config .coveragerc tests/

test_watch:
	@poetry run ptw -- -rP
