from aiohttp import web
from os import environ as ENV

from muswarmadmin.main import app


try:
    web.run_app(app, port=int(ENV.get("PORT")))
except (SystemExit, KeyboardInterrupt):
    exit(0)
