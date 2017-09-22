from aiohttp import web
from os import environ as ENV
from muswarmadmin.main import app
from time import sleep

import requests
import logging
import asyncio

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()

try:
    payload = {'query': 'select distinct ?c where {[] a ?c } LIMIT 1'}
    url = ENV['MU_SPARQL_ENDPOINT']
    getout = False
    while not getout:
        r = requests.get(url, params=payload)
        if r.ok:
            logger.info('SPARQL endpoint is ready')
            getout = True
        else:
            logger.warn('SPARQL endpoint not yet ready')
        sleep(2)

    web.run_app(app, port=(int(ENV['PORT']) if 'PORT' in ENV else None),
                loop=loop)
except (SystemExit, Exception):
    loop.run_until_complete(app.cleanup())
    exit(1)
finally:
    loop.close()
