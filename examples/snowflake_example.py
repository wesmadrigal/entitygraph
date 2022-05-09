#!/usr/bin/env python
import os
import sys
import json

import networkx as nx
import snowflake

def get_snowflake_connection():
    from snowflake import connector
    conn = connector.Connect(
            user=os.getenv('SNOWFLAKE_USERNAME'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT')
            )

    cs = conn.cursor()


def populate_nodes(G, dbs, connection):
    '''
    Given a graph and some databases populate
    the graph with nodes (tables)

    graph: networkx.Graph
    dbs : list of Snowflake database strings
    connection : Snowflake connection
    '''
    for db in dbs:
        tables = connection.execute("show tables in database %s" % db).fetchall()
        primary_keys = connection.execute("show primary keys in database %s" % db).fetchall()
        database_columns = connection.execute("show columns in database %s" % db).fetchall()
        for t in tables:
            node_identifier = '{0}.{1}.{2}'.format(
                t['database_name'],
                t['schema_name'],
                t['name']
            )
            num_rows = t['rows']
            # find the primary key, if it exists
            primary_key = list(filter(lambda x: x['table_name'] == t['name']
                                     and x['schema_name'] == t['schema_name']
                                     and x['database_name'] == t['database_name'], primary_keys))
            if len(primary_key):
                primary_key = primary_key[0]['column_name']
            else:
                primary_key = None

            # columns
            table_columns = [
                c for c in database_columns if c['table_name'] == t['name']
                and c['schema_name'] == t['schema_name']
                and c['database_name'] == t['database_name']
            ]
            table_columns = list(map(lambda c: {'name': c['column_name'], 'type' : c['data_type']}, table_columns))
            graph.add_node(node_identifier,
                       num_rows=num_rows,
                       primary_key=primary_key,
                       columns=table_columns)

def extract_date_cols(G):
    for node in G.nodes:
        G.nodes[node]['date_columns'] = [
                x for x in G.nodes[node]['columns']
                                    if 'DATE' in x['type']
                                    or 'TIMESTAMP' in x['type']
                                    or 'DATETIME' in x['type']
                                    or 'TIME' in x['type']
                                    or 'TIMESTAMP_LTZ' in x['type']
                                    or 'TIMESTAMP_NTZ' in x['type']
                                    or 'TIMESTAMP_TZ' in x['type']
                                    ]


def build_warehouse_network(G):
    '''
    Nested for loop iteration through nodes exhaustively
    comparing each node with every other node (table)
    in the warehous and identifying common edges (keys)
    between them

    Runtime: O(n**2)
    '''
    # this assumes some basic data about the graph is already built
    for db, attrs in list(G.nodes.items()):
        for c in G.nodes[db]['columns']:
            cname = c['name']
            if cname.endswith('ID'):
               # primary_key = G.nodes[db]['primary_key']
                try:
                    # for every column in a database we'll see if it's in ANY other table
                    for db2, attrs2 in list(G.nodes.items()):
                        if db2 != db:
                            if cname in [x['name'] for x in G.nodes[db2]['columns']]:
                                if G.has_edge(db, db2):
                                    # get the edge data
                                    links = G.get_edge_data(db, db2)['link_on']
                                    if cname not in links:
                                        links.append(cname)
                                        G.add_edge(db, db2, link_on=links)
                                    else:
                                         pass
                                else:
                                    G.add_edge(db, db2, link_on=[cname])
                except Exception as e:
                    pass
    return G


def optimize_paths_distance_hops(G, start, end):
    """
    Something like a Dijkstra's implementation for shortest path

    Usage: `optimize_paths_distance_hops(G, 'table1', 'table2')
    -> ['table1', 'tableX', 'table2']

    Parameters
    ----------
    start : str node to start with
    end : str node to end with
    Returns
    ---------
    paths : list of paths to take
    """
    nodes_to_traverse = []
    paths = {}
    for n in G.neighbors(start):
        nodes_to_traverse.append( n )
        try:
            paths[n] = {
                'path' : [start, n],
                'paths' : []
                }
        except Exception as e:
            print("Failed on node: {0}".format(n))
    while len(nodes_to_traverse) > 0:
        n = nodes_to_traverse[0]
        del nodes_to_traverse[0]
        for n1 in G.neighbors(n):
            if not paths.get(n1):
                paths[n1] = {
                        'path' : paths[n]['path'] + [n1],
                        'paths' : [ paths[n]['path'] + [n1] ]
                        }
                nodes_to_traverse.append(n1)
            else:
                paths[n1]['paths'].append(paths[n]['path'] + [n1])
                if len(paths[n1]['path']) >= len(paths[n]['path'] + [n1]):
                    paths[n1]['path'] = paths[n]['path'] + [n1]
    return paths[end]





