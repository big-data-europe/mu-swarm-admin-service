#!/bin/bash

[ -n "$1" ] && exec "$@"

if [ "${ENV:0:3}" == dev ]; then
	pip install aiohttp-devtools
	exec adev runserver -p $PORT --no-pre-check /src/muswarmadmin
fi

exec /src/run.py
