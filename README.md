# entitygraph

## Functionality

A library for turning data entities into a graph.  The entities
can be files or RDBMS, depending on which connectors we're supporting
at the time you read this.  To start, we're supporting local flat files,
Snowflake, and PostgreSQL.

## Motivation

Data discovery is an increasingly hard problem, requiring domain
experts, good documentation, and somewhat reliable data.  This becomes
an even larger problem when onboarding new team members, and gets
exacerbated by the data warehouse / lake pattern of shoving all an org's
data into a single location.  The idea is to automate some of the discovery
by turning relational portions of data into graphs, which can be navigated
visually and programmatically.  

## Usage
```
from entitygraph.sources import PostgresSource
source = PostgresSource(
    host=os.getenv('MY_HOST'),
    user=os.getenv('MY_USER'),
    pw=os.getenv('MY_PASSWORD'),
    port=5432,
    database='MY_DATABASE',
    databases=['MY_DATABASE'],
    schemas=['SCHEMA1', 'SCHEMA2']
)

import entitygraph.graph

g = graph.EntityGraph(source)
g.build_graph()

len(g)
1632
len(g.edges)
2450
```


## Future work
* More sources
* closer integration with compute
* substituting `networkx` for Knowledge graphs (probably `kglab` https://github.com/derwenai/kglab)
* consolidate data types, most likely by leveraging good Open source work out of `pyarrow` https://arrow.apache.org/docs/python/api/datatypes.html
