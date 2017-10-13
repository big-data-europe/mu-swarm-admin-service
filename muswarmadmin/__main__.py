import requests
import logging
import asyncio

from aiohttp import web
from os import environ as ENV
from muswarmadmin.main import app
from time import sleep

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()


def pollAccumulate(count=0):
    """
    Poll the database until it is ready to answer queries
    """
    if count >= int(ENV['POLL_RETRIES']):
        return False
    payload = {'query': 'select distinct ?c where {[] a ?c } LIMIT 1'}
    url = ENV['MU_SPARQL_ENDPOINT']
    try:
        requests.get(url, params=payload)
    except requests.RequestException:
        logger.warn('SPARQL endpoint not yet ready')
        sleep(1)
        return pollAccumulate(count+1)
    logger.info('SPARQL endpoint is ready')
    return True


if not pollAccumulate():
    exit(1)

try:
    web.run_app(app, port=(int(ENV['PORT']) if 'PORT' in ENV else None),
                loop=loop)
except (SystemExit, Exception):
    loop.run_until_complete(app.cleanup())
    exit(1)
finally:
    loop.close()
