all: deps lint test

deps:
	@python3 -m pip install --upgrade pip && pip3 install -r requirements-dev.txt

black:
	@black --line-length 120 aiopg_listen tests

isort:
	@isort --line-length 120 --use-parentheses --multi-line 3 --combine-as --trailing-comma aiopg_listen tests

mypy:
	@mypy --strict --ignore-missing-imports aiopg_listen

flake8:
	@flake8 --max-line-length 120 --ignore C901,C812,E203 --extend-ignore W503 aiopg_listen tests

lint: black isort flake8 mypy

test:
	@python3 -m pytest -vv --rootdir tests .

pyenv:
	echo aiopg-listen > .python-version && pyenv install -s 3.11.2 && pyenv virtualenv -f 3.11.2 aiopg-listen

pyenv-delete:
	pyenv virtualenv-delete -f aiopg-listen
