#!/usr/bin/env python
import enum

class RelationalCardinality(enum.Enum):
    one_to_one = 'one_to_one'
    one_to_many = 'one_to_many'
    many_to_one = 'many_to_one'
    many_to_many = 'many_to_many'
