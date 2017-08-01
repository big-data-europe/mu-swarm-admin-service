#!/bin/bash

set -e

cd `dirname $0`
cd ..

if [ -z "$1" ]; then
	commands=(tox)
else
	commands=("$@")
fi

exec docker run -it --rm \
	--net muswarmadmin_test --net-alias swarm-admin \
	-e TOX=true \
	-e MU_APPLICATION_GRAPH=http://mu.semte.ch/test \
	-e MU_SPARQL_ENDPOINT=http://delta:8890/sparql \
	-v $PWD:/src \
	-v /var/run/docker.sock:/var/run/docker.sock \
	-l com.docker.compose.project=APPSWARMUI \
	bde2020/mu-swarm-admin-service:latest "${commands[@]}"
