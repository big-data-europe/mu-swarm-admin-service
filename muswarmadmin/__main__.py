from aiohttp import web
from os import environ as ENV

from muswarmadmin.main import app


try:
    web.run_app(app, port=(int(ENV['PORT']) if 'PORT' in ENV else None))
except (SystemExit, KeyboardInterrupt):
    exit(0)
