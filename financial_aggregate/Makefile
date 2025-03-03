SHELL := /bin/bash

ENV_DIR := .venv
VERSION := $(shell cat version)
DATEREF := $(shell date +%Y%m%d)

all:

config: $(ENV_DIR) install
	@echo "Configuration done!"

$(ENV_DIR):
	python -m virtualenv --python="/usr/bin/python3.12" $(ENV_DIR)

install:
	source .venv/bin/activate; pip install -r requirements.txt

clean:
	rm -rf .venv

zip:
	zip -r ../cf_financial_transform_v$(VERSION)_$(DATEREF).zip *.py Makefile README.md requirements.txt version
