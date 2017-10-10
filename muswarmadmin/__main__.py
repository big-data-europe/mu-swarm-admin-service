from aiohttp import web
from os import environ as ENV
from muswarmadmin.main import app
from time import sleep

import requests
import logging
import asyncio

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()


def pollAccumulate(count=0):
    """
    Poll the database until it is ready to answer queries
    """
    payload = {'query': 'select distinct ?c where {[] a ?c } LIMIT 1'}
    url = ENV['MU_SPARQL_ENDPOINT']
    try:
        r = requests.get(url, params=payload)
    except requests.RequestException:
        logger.warn('SPARQL endpoint not yet ready')
        sleep(2)
        if count == 10:
            return False
        else:
            return pollAccumulate(count+1)
    logger.info('SPARQL endpoint is ready')        
    return True 

try:
    while not pollAccumulate():
        continue
    web.run_app(app, port=(int(ENV['PORT']) if 'PORT' in ENV else None),
                loop=loop)
except (SystemExit, Exception):
    loop.run_until_complete(app.cleanup())
    exit(1)
finally:
    loop.close()
