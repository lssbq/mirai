# -*- coding:utf-8 -*-
import uuid
import pandas as pd
import psycopg.schema as schema
from Logger.Logger import Logger

logger = Logger('lib')
db_o = schema.get_schema('INDUSTRY')

logger.info('Start filter industry...')
persistence = db_o.select().execute().fetch()
origin = set()
for i in persistence:
    origin.add(i['name'])

def do(hs):
    hs = hs.reindex(['name','code','industry'], axis=1)
    industries = set(hs['industry'])
    for item in industries:
        group = hs[hs['industry']==item].reindex(['code','name'],axis=1)
        industry_d = list(group.to_dict('index').values())
        industry_str = str(industry_d).replace('\'', '"')

        if item in origin:
            logger.info('Update existing industry \'%s\''%item)
            old_item = list(filter(lambda it: item == it['name'], persistence))[0]
            guid = old_item['guid']
            db_o.update(uuid=guid, list=industry_str).execute()
        else:
            logger.info('Creating new industry \'%s\''%item)
            uid = uuid.uuid4()
            db_o.insert(**{'guid':uid, 'name':item, 'list':industry_str}).execute()
    db_o.commit()

# Select from database:
#   Summary all stocks from industries:
#       SELECT sum(jsonb_array_length(list)) FROM meta.industry;
#   Summary stocks for each industry:
#       SELECT jsonb_array_length(list) AS len FROM meta.industry;