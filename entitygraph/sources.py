#!/usr/env/bin/ python

"""
Data sources for an entitygraph to infer from
"""

import os
import sys

import psycopg2
import pandas as pd
import networkx as nx

from base_source import BaseSource
from entity import Entity

import logging
import sys


root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)



class PostgresSource(BaseSource):
    def __init__(self, host, user, pw, port, database,
            databases = [],
            schemas = [],
            tables = [],
            columns = { }
            ):
        self.host = host
        self.user = user
        self.pw = pw
        self.port = port
        self.database = database

        self._conn = None

        # if not empty, only select from these databases
        self.databases = databases
        self.dbs_sql = "SELECT datname FROM pg_database"
        self._dbs_df = None

        # if not empty, only select from these schemas
        self.schemas = schemas
        self.schema_sql = "SELECT schema_name FROM information_schema.schemata"
        self._schemas_df = None

        # if not empty, only seelct from these tables
        self.tables = tables
        self.tables_sql = """
            SELECT * FROM information_schema.tables;
        """
        self._tables_df = None
        #TODO: make this able to be filtered on
        self.columns_sql = """
        SELECT	* FROM
        information_schema.columns
        """
        self._tables_and_columns_df = None
        
        # store the entities in this list
        self._entities = []

        self._graph_built = False 
        self.graph = nx.Graph()


    def __repr__(self):
        return f'<PostgresSource(host={self.host})>'

    def get_connection(self):
        if self._conn:
            return self._conn
        self._conn = psycopg2.connect(
                    host=self.host,
                    user=self.user,
                    password=self.pw,
                    port=self.port,
                    database=self.database
                    )
        return self._conn

    def get_databases(self) -> pd.DataFrame:
        con = self.get_connection()
        if not self._dbs_df:
            dbs_df = pd.read_sql_query(self.dbs_sql, con)
            self._dbs_df = dbs_df
        return self._dbs_df

    def get_schemas(self) -> pd.DataFrame:
        con = self.get_connection()
        if not self._schemas_df:
            schemas_df = pd.read_sql_query(self.schema_sql, con)
            self._schemas_df = schemas_df
        return self._schemas_df

    def get_entities(self) -> list:
        """
List the entities in this source
Since this is an RDBMS source we know we could have
multiple databases in this connection, so we'll only
source the ones in the database provided for now 
        """
        con = self.get_connection()
        if not isinstance(self._tables_and_columns_df, pd.DataFrame):
            tables_and_columns_df = pd.read_sql_query(self.columns_sql, con)
            self._tables_and_columns_df = tables_and_columns_df

        if not len(self._entities):
            # postgres specific
            for ix, row in self._tables_and_columns_df.groupby(
                    ['table_catalog', 'table_schema', 'table_name']).agg({'column_name':'count'}).reset_index().iterrows():
                these_cols = self._tables_and_columns_df[
                        (self._tables_and_columns_df['table_catalog']==row['table_catalog'])
                        &
                        (self._tables_and_columns_df['table_schema']==row['table_schema'])
                        &
                        (self._tables_and_columns_df['table_name']==row['table_name'])
                        ]
                identifier = '{0}.{1}.{2}'.format(row['table_catalog'],row['table_schema'],row['table_name'])
                entity_instance = Entity(
                        source=self,
                        identifier=identifier,
                        columns=list(these_cols.column_name.unique()),
                        column_type_map={},
                        column_df=these_cols)
                if entity_instance not in self._entities:
                    self._entities.append(entity_instance)

        return self._entities

    def list_entities(self):
        entities = self.get_entities()
        for ix, row in entities.iterrows():
            logging.debug(row)

    
    def get_sample(self, identifier: str, n: int = 100):
        """
Get a sample of the parameterized identifier
        """
        con = self.get_connection()
        df = pd.read_sql_query(f"""
            SELECT * FROM {identifier}
            LIMIT {n}
        """, con)
        return df

    def build_entity_graph(self) -> nx.Graph:
        entities = self.get_entities()
# add all nodes to the graph if not already added
        if not self._graph_built:
            for ent in entities:
                if not self.graph.has_node(ent):
                    self.graph.add_node(ent)
            for node in self.graph.nodes():
                db, schema, table = node.identifier.split('.')
                if table.endswith('s'):
# strip the s at the end of the table name (e.g. customers_id becomes customer_id)
                    fkname1 = '{0}_id'.format(table[:-1])
                else:
                    fkname1 = f'{table}_id'
                fkname2 = None
                fkname2 = '_'.join(f'{table}_id'.split('_')[1:]) if len(f'{table}_id'.split('_'))>2 else None
                for node2 in self.graph.nodes():
                    if node != node2:
                        db2, schema2, table2 = node2.identifier.split('.')
                        for column in node2.columns:
                            if column == fkname1:
                                if not self.graph.has_edge(node, node2):
                                    self.graph.add_edge(node, node2, attr={
                                        f'{node.identifier}_key' : 'id',
                                        f'{node2.identifier}_key' : column
                                    })
            self._graph_built = True
        return self.graph 


class FileSource(BaseSource):
    def __init__(self,
            path_root: str,
            storage_type: str = 'parquet'):
        self.path_root = path_root
        self.storage_type = storage_type

    def get_connection(self, path: str):
        """
Gets a reference to the root directory in
the FileSystem where the data files exist
        """
        if os.path.isdir(path):
            return os.scandir(path)
        else:
            raise Exception(f'FileSource connection path must be a dir, got {path}')
        return

    def list_entities(self):
        """
List entities within this source
        """
        raise NotImplementedError("`list_entities` not yet implemented")

    def get_sample(self, identifier, n: int = 100):
        """
Get a sample of parameterized identifier's data
        """
        raise NotImplementedError("`get_sample` not yet implemented")


class S3Source(BaseSource):
    def __init__(self, bucket: str, prefix: str = ''):
        self.bucket = bucket
        self.prefix = prefix

    def get_connection(self):
        raise NotImplementedError("not yet implemented")

    def list_entities(self):
        raise NotImplementedError("not yet implemented")

