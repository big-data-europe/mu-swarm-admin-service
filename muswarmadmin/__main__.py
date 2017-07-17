from aiohttp import web
from os import environ as ENV

from muswarmadmin.main import app


import asyncio
loop = asyncio.get_event_loop()
try:
    web.run_app(app, port=(int(ENV['PORT']) if 'PORT' in ENV else None),
                loop=loop)
except (SystemExit, Exception):
    loop.run_until_complete(app.cleanup())
    exit(1)
finally:
    loop.close()
