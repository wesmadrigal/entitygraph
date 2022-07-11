#!/usr/env/bin/ python

"""
Data sources for an entitygraph to infer from
"""

import os
import re
import sys
import typing
import logging

import psycopg2
import pandas as pd
import networkx as nx
import boto3
import pyarrow
from pyarrow import dataset as ds
from pyarrow import fs


from entitygraph.base_source import BaseSource
from entitygraph.entity import Entity
from entitygraph.enums import FileProvider, StorageFormat


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
                # filter out rows that aren't relevant and belong to `pg_catalog` and `information_schema`
                if row['table_schema'] in ['information_schema', 'pg_catalog']:
                    continue
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


    def get_defined_edges(self) -> list:
        """
Defined edges in RDBMS world are FOREIGN KEYS
        """
        q1 = """
        WITH unnested_confkey AS (
        SELECT oid, unnest(confkey) as confkey
            FROM pg_constraint
        ),
        unnested_conkey AS (
            SELECT oid, unnest(conkey) as conkey
            FROM pg_constraint
            )
        select
        c.conname                   AS constraint_name,
        c.contype                   AS constraint_type,
        tbl.relname                 AS constraint_table,
        col.attname                 AS constraint_column,
        referenced_tbl.relname      AS referenced_table,
        referenced_field.attname    AS referenced_column,
        pg_get_constraintdef(c.oid) AS definition
        FROM pg_constraint c
        LEFT JOIN unnested_conkey con ON c.oid = con.oid
        LEFT JOIN pg_class tbl ON tbl.oid = c.conrelid
        LEFT JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = con.conkey)
        LEFT JOIN pg_class referenced_tbl ON c.confrelid = referenced_tbl.oid
        LEFT JOIN unnested_confkey conf ON c.oid = conf.oid
        LEFT JOIN pg_attribute referenced_field ON (referenced_field.attrelid = c.confrelid AND referenced_field.attnum = conf.confkey)
        WHERE c.contype = 'f';
        """
        if not self._entities:
            self.get_entities()

        conn = self.get_connection()
        fks = pd.read_sql_query(q1, conn)
        edges_to_add = []
        for ix, row in fks.iterrows():
            try:
                constraint_table = row['constraint_table']
                constraint_column = row['constraint_column']
                referenced_table = row['referenced_table']
                referenced_column = row['referenced_column']
                # get the constraint table and column pertinent entities
                constraint_entity = list(filter(lambda x:
                    x.identifier.split('.')[-1] == constraint_table
                    and 
                    constraint_column in x.columns,
                    self._entities))[0]
                referenced_entity = list(filter(lambda x:
                    x.identifier.split('.')[-1] == referenced_table,
                    self._entities))[0]

                edges_to_add.append( (constraint_entity, referenced_entity, constraint_column) )
            except Exception as e:
                continue
        return edges_to_add

    
    def get_sample(self,
            entity : Entity,
            n : int = 100
            ) -> pd.DataFrame:
        """
Get a sample of the parameterized identifier
        """
        con = self.get_connection()
        identifier = entity.identifier
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
            provider : FileProvider = FileProvider.local,
            storage_format: StorageFormat = StorageFormat.parquet,
            prefix : typing.Optional[str] = None,
            regex_filter : typing.Optional[str] = None,
            entities_are_partitioned : bool = False
        ):
        """
We are using the pyarrow.fs.FileSystem so for more information
please refer to the pyarrow docs: https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html
        """
        self.provider = provider
        # in the case of an object store this would be a bucket
        # s3: bucket, blob: bucket, GCS: bucket
        # in the case of local filesystem this would be
        # the root directory to search from
        self.path_root = path_root
        if sum([1 if getattr(FileProvider, x).value in self.path_root else 0
               for x in [a for a in dir(FileProvider) if not a.startswith('_')]
               ]):
            raise Exception(f"Path root cannot have provider prefix {self.provider.value}")

        self.storage_format = storage_format
        # prefix is not necessary if the path root is the
        self.prefix = prefix
        self.regex_filter = re.compile(regex_filter) if isinstance(regex_filter, str) else regex_filter
        # entities are stored in partitions
        self.entities_are_partitioned = entities_are_partitioned

        self._fs = None
        self._source_path = None
        # pyarrow's relative path from a call to `pyarrow.fs.FileSystem.from_uri`
        self._relpath = None

        self._entities = []


    def get_source_path(self) -> str:
        """
Builds the full path of the source
        """
        if not self._source_path:
            source_path = self.provider.value + self.path_root
            if self.prefix:
                source_path = source_path + '/' + self.prefix
            self._source_path = source_path
        return self._source_path


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


    def get_entities(self):
        """
List entities within this source
        """
        if not self._entities:
            if not self._fs or not self._relpath:
                this_fs, path = fs.FileSystem.from_uri(self.get_source_path())
                self._fs = this_fs
                self._relpath = path
            raw_entities = self._fs.get_file_info(fs.FileSelector(self._relpath, recursive=True))
            # filter if we have a regex pattern
            if self.regex_filter and isinstance(self.regex_filter, re.Pattern):
                raw_entities = [
                    e for e in raw_entities
                    if not len(re.findall(self.regex_filter, e.path))
                ]
            # filter entities of the pertinent storage format
            filtered_entities = [
                e for e in raw_entities
                if e.path.endswith(self.storage_format.value)
            ]

            # check if the entities are stored in directories
            # an example of this is how Spark and Dask partition
            # files:
            # table entity_a would be stored as:
            # entity_a/part1.parquet, entity_a/part2.parquet, etc.
            if self.entities_are_partitioned:
                filtered_entities = [
                    e for e in filtered_entities
                    if not e.is_file
                ]

            entity_objects = []
            for obj in filtered_entities:
                if obj.path.startswith(self.provider.value):
                    identifier = obj.path.split(self.provider.value)[1]
                else:
                    identifier = obj.path
                ent = Entity(
                        source=self,
                        identifier=identifier
                        )
                # in order to get columns
                # we need a sample of data
                ent_samp = self.get_sample(ent, n=100)
                ent.columns = ent_samp.columns
                entity_objects.append(ent)
            self._entities = entity_objects
        return self._entities


    def build_entity_graph(self):
        """
Interface for building the entity graph for this source
        """
        pass


    def get_defined_edges(self):
        """
Predefined edges that are either available from schema
or encoded from user input
        """
        pass


    def get_sample(self,
            entity : Entity,
            n : int = 100
            ) -> pd.DataFrame:
        """
Get a sample of parameterized identifier's data
        """
        entity_dataset = ds.dataset(source=entity.identifier, filesystem=self._fs)
        # grab the first batch of data for the sample
        databatch = None
        for batch in entity_dataset.to_batches():
            databatch = batch
            break
        samp = databatch.slice(offset=0, length=n).to_pandas()
        return samp
