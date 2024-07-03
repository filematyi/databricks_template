install:
	poetry install

build:
	poetry build

upload:
	twine upload -r ZEISS_databricks_example dist/*