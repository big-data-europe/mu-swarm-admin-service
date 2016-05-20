#!/bin/bash
if [ $ENV == "dev" ]; then
	exec ./server.py
else
	sed -i 's/user www-data/user root/' /etc/nginx/nginx.conf
	nginx && exec uwsgi --ini /app/app.ini
fi
