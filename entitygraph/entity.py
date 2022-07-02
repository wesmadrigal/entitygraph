#!/usr/bin/env python

# python standard libraries
import pathlib
import json
import typing


class Entity:
    def __init__(self,
            source,
            identifier : str,
            columns : typing.Optional[typing.List[str]] = [],
            column_type_map : typing.Optional[dict] = {},
            column_df = None
            ):
        """
Constructor for an `Entity` object
        """
        self.source = source
        self.identifier = identifier
        self.columns = columns
        self.column_type_map = column_type_map

        # primary key candidates
        self._pk_candidates = []
        # elected primary key
        self.pk = None
        # date keys
        self.dks = []
        #TODO: add the nx.Graph instance?

    def __repr__(self):
        return f'<Entity (identifier={self.identifier}, source={self.source.__repr__()})>'


    def _extract_pk_candidates(self):
        """
Extracts the candidates for this `Entity` instance
        """
        if not self.pk:
            for col in self.columns:
                self.check_if_pk(col)
        

    def _elect_pk(self):
        """
Extracts the primary key 
        """
        pass

    def get_columns(self) -> list:
        return self.columns

    def get_type_map(self) -> dict:
        return self.column_map

    def get_sample(self, n=100):
        """
Gets a sample of `n` records of this entity instance's underlying data by
leveraging the associated source
        """
        if not self._sample:
            sample = self.source.get_sample(identifier=self.identifier, n=n)
            self._sample = sample
        return self._sample

    def extract_date_keys(self):
        """
Extracts the date keys for this instance
        """
        raise NotImplementedError("`extract_date_keys` not yet implemented")
