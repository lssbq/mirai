"""
Value type not match type defined in schema
"""
class TypeError(Exception):
    def __init__(self, key: str, should: str, got: str, schema: str):
        self.schema = schema
        self.should = should
        self.got = got
        self.key = key

    def __str__(self):
        return '%s: \'%s\' should be \'%s\' -> got \'%s\''%(self.schema, self.key, self.should, self.got)

class SchemaNotFound(Exception):
    def __init__(self, name: str):
        self.name = name

    def __str__(self):
        return 'SCHEMA \'%s\' not found, please check name or defined in schema.json.'%(self.name)