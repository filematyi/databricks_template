install:
	poetry install

build:
	poetry shell
	poetry build

upload:
	twine upload -r ZEISS_databricks_example dist/*