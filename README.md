# Databricks-Local development

### Description
## Purpose of the repository

The purpose of this repository is to provide a scalable framework for Databricks and local development. Writing production-ready applications in Databricks notebooks can be challenging. The goal of this repository is to demonstrate a way to work in a local environment with IDE support and the ability to perform unit and integration tests.

## Prerequisites

Before using this repository, make sure you have the following prerequisites:

- A Databricks instance
- Unity Catalog enabled
- Storage Credentials created for External Location
- External Locations created

## Capabilities

This repository offers the following capabilities:

- Define and create new Catalog
- Define and create new Schema (Database)
- Define and create new Table
- Define and create new Volume
- Simplified logic implementation
- Test framework

## Installation
```shell
poetry install
poetry shell
pytest
```

## Build wheel file
```shell
poetry build
ls dist/
```