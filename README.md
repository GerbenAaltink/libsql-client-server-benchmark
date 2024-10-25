# Benchmark libsql-client

This project benchmarks the python libsql-client to determine performance differences between local file database and the libsql database server.

## Requirements

1. The libsql server running on default port 8080.
2. A python environment. You can create this using `python3 -m venv venv`

## Usage 

```
source venv/bin/activate
pip install -r requirements.txt
make
```

## Screen recording
Can take some time to load. It's more than a mb. 
![Screen recording](bench.gif)
