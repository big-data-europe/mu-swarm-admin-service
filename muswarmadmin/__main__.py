import asyncio
import logging

from aiohttp import web
from aiosparql.client import SPARQLClient
from aiohttp.client_exceptions import ClientConnectionError
from aiosparql.syntax import IRI
from os import environ as ENV
from muswarmadmin.main import app
from time import sleep
from random import randint

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()


async def pollAccumulate(count=0, client=None):
    """
    Poll the database until it is ready to answer queries
    """
    if client is None:
        client = SPARQLClient(ENV['MU_SPARQL_ENDPOINT'],
                              graph=IRI(ENV['MU_APPLICATION_GRAPH']))
    if count >= int(ENV['POLL_RETRIES']):
        await client.close()
        return False
    try:
        result = await client.query("""
            ASK
            FROM {{graph}}
            WHERE {
                ?s ?p ?o
            }
            """)
        if not result:
            logger.warn('SPARQL endpoint not yet ready')
            sleep(randint(1, 5))
            return pollAccumulate(count+1, client)
        else:
            logger.info('SPARQL endpoint is ready')
            await client.close()
            return True
    except ClientConnectionError:
        sleep(randint(1, 5))
        return pollAccumulate(count+1, client)


if not loop.run_until_complete(pollAccumulate()):
    exit(1)

try:
    web.run_app(app, port=(int(ENV['PORT']) if 'PORT' in ENV else None),
                loop=loop)
except (SystemExit, Exception):
    loop.run_until_complete(app.cleanup())
    exit(1)
finally:
    loop.close()
