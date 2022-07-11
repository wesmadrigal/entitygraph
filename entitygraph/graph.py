#!/usr/bin/env python

# python standard libraries
import json
import pathlib
import traceback

# third party libraries
import networkx as nx
import pyvis

# internal libs
from entitygraph.cardinality import RelationalCardinality
from entitygraph.entity import Entity
from entitygraph.sources import PostgresSource, FileSource


class EntityGraph(nx.Graph):
    def __init__(self,
            source
            ):
        self.source = source
        self._graph_built = False
        super(EntityGraph, self).__init__()


    def get_defined_edges(self) -> list:
        """
Gets predefined edges, which we don't do inference on, we just use
These are things like, in RDBMS world, Foreign Keys
In RDF this would be the predicate https://www.w3.org/TR/rdf-concepts/#dfn-predicate
        """
        return self.source.get_defined_edges()


    def build_graph_relational(self):
        """
Build graph implementation for relational tables
The assumption for schema is:
    database, schema, table

With a database, schema, and table structure we can generate
the entity identifiers with those metadata and apply some
additional edge inference heuristics to build the `EntityGraph`
        """
        entities = self.source.get_entities()
        # add all nodes to the graph if not already added
        if not self._graph_built:
            for ent in entities:
                if not self.has_node(ent):
                    self.add_node(ent)

            # start with already defined edges
            for n1, n2, key1 in self.get_defined_edges():
                self.add_edge(n1, n2, attr={
                    f'{n1.identifier}_key' : key1,
                    f'{n2.identifier}_key' : 'id',
                    'from_schema' : True
                })

            for node in self.nodes():
                db, schema, table = node.identifier.split('.')
                if table.endswith('s'):
                    # strip the s at the end of the table name (e.g. customers_id becomes customer_id)
                    fkname1 = '{0}_id'.format(table[:-1])
                else:
                    fkname1 = f'{table}_id'
                fkname2 = None
                fkname2 = '_'.join(f'{table}_id'.split('_')[1:]) if len(f'{table}_id'.split('_'))>2 else None
                for node2 in self.nodes():
                    if node != node2:
                        db2, schema2, table2 = node2.identifier.split('.')
                        for column in node2.columns:
                            if column == fkname1:
                                if not self.has_edge(node, node2):
                                    self.add_edge(node, node2, attr={
                                        f'{node.identifier}_key' : 'id',
                                        f'{node2.identifier}_key' : column,
                                        'from_schema' : False
                                    })
                                # already has an edge
                                elif self.has_edge(node, node2):
                                    edge_data = self[node][node2]
                                    edge_data.update({
                                        f'{node.identifier}_key' : 'id',
                                        f'{node2.identifier}_key' : column,
                                        'from_schema' : False
                                        })
                                    self.add_edge(node, node2, attr=edge_data)
            self._graph_built = True
        pass

    def build_graph_filesystem(self):
        """
Build graph implementation for filesystem (local, s3, blob, gcfs)
The assumption for schema is:

    root path, relative path, filename, and storage format

With these parts parameterized in the dependency `Source` instance
we apply this algorithm
        """
        if not self._graph_built:
            entities = self.source.get_entities()
            if not self._graph_built:
                for ent in entities:
                    if not self.has_node(ent):
                        self.add_node(ent)
            # this is a weak heuristic tries to find 
            # columns referencing a foreign table from
            # the assumption that a table being referenced
            # in a foreign table will take the name:
            # `table_name` -> `table_name_id`
            for n1 in self.nodes():
                for n2 in self.nodes():
                    if n1 == n2:
                        continue
                    else:
                        for cname in n1.columns:
                            try:
                                if '_'.join(cname.split('_')[:-1]) in n2.identifier:
                                    self.add_edge(n1, n2,attr={
                                        f'{n1.identifier}_key' : 'id',
                                        f'{n2.identifier}_key' : column,
                                        'from_schema' : False 
                                        })
                            except Exception as e:
                                pass
            self._graph_built = True
        pass


    def build_graph_warehouse(self):
        pass

    def build_graph_custom(self):
        pass
  
    def build_graph(self):
        """
Build the entity graph from our underlying source
        """
        if isinstance(self.source, FileSource):
            self.build_graph_filesystem()
        elif isinstance(self.source, PostgresSource):
            self.build_graph_relational()
        else:
            self.build_graph_custom()

    def string_nodes(self):
        """
Turns the nodes into strings for visualization packages like `pyvis`
        """
        Gstring = nx.Graph()
        for node in self.nodes():
            Gstring.add_node(node.identifier)
        for edge in self.edges():
            Gstring.add_edge(edge[0].identifier, edge[1].identifier)
        return Gstring

    def plot_graph(self, fname : str = 'entity_graph.html'):
        """
Plot an entity graph with `pyvis`
        """
        gstring = self.string_nodes()
        nt = pyvis.network.Network('500px', '500px')
        nt.from_nx(gstring)
        nt.show(fname)

    def include_domain_expertise(self):
        """
Iterates through the graph and encodes custom domain expertise
        """
        pass

    def infer_edge(self, n1, n2):
        """
Attempts to infer an edge between two nodes (connected or disconnected) in the graph

This method should wrap other more specific heuristics used to infer edges between nodes
`self.infer_edge_nlp`
`self.infer_edge_dtypes`
`self.infer_edge_composite`
        """
        self.infer_edge_nlp(n1, n2)
        self.infer_edge_dtypes(n1, n2)
        self.infer_edge_composite(n1, n2)

    def infer_edge_nlp(self, n1, n2):
        """
Use NLP approaches to inferring an edge between two entities
        """
        pass

    def infer_edge_dtypes(self, n1, n2):
        """
User datatypes and distributions to infer an edge between two entities
        """
        pass

    def infer_edge_composite(self, n1, n2):
        """
To start we may brute force the problem of inferring edges between entities through composite attributes
        """
        pass

    def infer_cardinality(
            self, 
            n1: Entity, 
            n2: Entity) -> RelationalCardinality:
        """
Attempt to infer the cardinality between two nodes
        """
        # which keys are in the edge to use here?
        #n1_samp = n1.get_sample()
        #n2_samp = n2.get_sample()
        pass


    def optimize_paths_distance_hops(start, end):
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

