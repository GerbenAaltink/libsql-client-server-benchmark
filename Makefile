all: install test

install:
	-@rm -rf venv
	python3 -m venv venv
	pip install -r requirements.txt

test:
	./venv/bin/python test1.py

zip:
	zip libsql-test.zip Makefile bench.gif README.md test1.py requirements.txt 