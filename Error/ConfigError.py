"""
Error occured while read the configuration from json file
    Key not found
        or
    Key type error
"""

class ConfigError(Exception):
    def __init__(self, key: str, msg: str):
        self.key = key
        self.msg = msg

    def __str__(self):
        return "Error occured in configuration key: \n    %s -> %s"%(self.key, self.msg)