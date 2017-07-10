#!/bin/bash

set -e

cd `dirname $0`
cd ..

exec docker run -it --rm \
	--net muswarmadmin_test --net-alias swarm-admin \
	-e TOX=true \
	-e MU_APPLICATION_GRAPH=http://mu.semte.ch/test \
	-e MU_SPARQL_ENDPOINT=http://delta:8890/sparql \
	-v $PWD:/src \
	-v /var/run/docker.sock:/var/run/docker.sock \
	bde2020/mu-swarm-admin-service:latest /bin/bash -c "tox && pip install codecov && codecov --token=${CODECOV_TOKEN?}"
