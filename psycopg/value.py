# -*- coding:utf-8 -*-
"""
Validator to validate and pre-process the data type.
Currently supported data type:
    int - integer
    bool- boolean
    str - text
    double
    datetime - date
    uuid - uuid

Return 'NULL' (P_ostgreSQL) if field is None etc.
"""
import re
import uuid
import datetime as dt

def value(_val, _type: str):
    if _val is None:
        return 'NULL'
    elif re.match(r'^varchar', _type):
        return ' \'' + _varchar(_val, _type) + '\''
    elif _type == 'float8':
        return ' \'' + str(_val) + '\''
    elif _type == 'integer':
        return ' \'' + str(int(float(_val))) + '\''
    elif _type == 'boolean':
        return ' \'' + str(bool(_val)) + '\''
    elif _type == 'uuid':
        return _uuid(_val)
    elif _type == 'date':
        return _date(_val)
    else:
        return ' \'' + _val + '\''
    

def _date(_val):
    try:
        if _val and isinstance(_val, (dt.date, dt.datetime)):
            return ' \'' + _val.strftime(r'%Y%m%d') + '\''
        elif _val and isinstance(_val, str):
            if re.match(r'\d{8}', _val) or re.match(r'\d{4}-\d{2}-\d{2}', _val):
                return  ' \'' + _val + '\''
            else:
                raise ValueError('Incorrect value format date: %s'%_val)
        elif _val == 0 or _val == None:
            return 'NULL'
        else:
            return ' \'' + str(_val) + '\''
    except Exception:
        raise ValueError('Incorrect value format date: %s'%_val)

def _uuid(_val):
    if _val and isinstance(_val, uuid.UUID):
        return ' \'' + _val.__str__() + '\''
    elif _val and isinstance(_val, str):
        if re.match(r'^[0-9a-z]{8}(-[0-9a-z]{4}){3}-[0-9a-z]{12}', _val):
            return ' \'' + _val + '\''
    raise ValueError('Incorrect value format uuid: %s'%_val)

def _varchar(_val, _type):
    _len = re.search(r'varchar\((\d*)\)', _type).group(1)
    if len(_val) > int(_len):
        raise ValueError('Value is toooooo long!')
    else:
        return _val

if __name__ == '__main__':
    uu = uuid.uuid4()
    td = dt.date.today()

    print(value('cf52cbc2-126a-4fdc-ab15-dc52f3d266d9', 'uuid'))
    print(value('129999340404', 'varchar(20)'))
    