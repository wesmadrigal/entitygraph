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
