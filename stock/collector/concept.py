# -*- coding:utf-8 -*-
import re
import uuid
import pandas as pd
import psycopg.schema as schema
from Logger.Logger import Logger

logger = Logger('lib')
db_o = schema.get_schema('CONCEPT')

logger.info('Start filter concept...')
persistence = db_o.select().execute().fetch()
origin = set()
for i in persistence:
    origin.add(i['name'])

def do(hs):
    hs = hs.reindex(['code','name','concept'], axis=1)
    concepts = set()
    for item in list(hs['concept']):
        if item != '':
            split = item.split(',')
            for sub in split:
                concepts.add(sub)
    
    for item in concepts:
        group = hs[hs['concept'].apply(lambda i: (','+item in i) or (item+',' in i))].reindex(['name','code'], axis=1)
        concept_d = list(group.to_dict('index').values())
        concept_str = str(concept_d).replace('\'', '"')

        if item in origin:
            logger.info('Update existing concept \'%s\''%item)
            old_item = list(filter(lambda it: item == it['name'], persistence))[0]
            guid = old_item['guid']
            db_o.update(uuid=guid, list=concept_str).execute()
        else:
            logger.info('Creating new concept \'%s\''%item)
            uid = uuid.uuid4()
            db_o.insert(**{'guid':uid, 'name':item, 'list':concept_str}).execute()
    db_o.commit()