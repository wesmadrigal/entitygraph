#!/usr/bin/env python
import abc

class BaseSource(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_connection(self):
        raise NotImplementedError('`get_connection` method must be implemented')

    @abc.abstractmethod
    def list_entities(self):
        raise NotImplementedError('`list_entities` method must be implemented')

    @abc.abstractmethod
    def build_entity_graph(self):
        raise NotImplementedError('`build_entity_graph` must be implemented')
