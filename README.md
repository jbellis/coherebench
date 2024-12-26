Benchmarking program that throws data from Cohere's public Wikipedia dataset at a local Cassandra node.  This gives us a more realistic dataset than random vectors would.

# Installation

1. edit config.properties dataset_location to where you want the (360GB) dataset
1. edit config.properties nodetool_path
1. `pip install datasets`
1. python download.py

# Running

## Insert Data
To insert data, use:

```bash
# Insert 1 million rows, skipping first 1000000
CB_CMD=insert CB_INSERT_ROWS=1000000 CB_SKIP=1000000 mvn compile exec:exec@run

# Default: CB_INSERT_ROWS=10000000, CB_SKIP=0
```

## Query Data
To run queries, use:

```bash
# Run simple ANN queries
CB_CMD=query CB_QUERY_TYPE=simple mvn compile exec:exec@run

# Run restrictive ANN queries (language='sq') corresponding to 1% of data
CB_CMD=query CB_QUERY_TYPE=restrictive mvn compile exec:exec@run

# Run unrestrictive ANN queries (language='en') corresponding to 99% of data
CB_CMD=query CB_QUERY_TYPE=unrestrictive mvn compile exec:exec@run

# Default: CB_QUERY_TYPE=simple, CB_QUERIES=10000 (number of queries to run)
```

# Comparing to postgresql

There's been some bitrot here, PG flavor needs to be updated to match C*.
