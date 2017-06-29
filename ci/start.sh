#!/bin/bash

set -e

cd `dirname $0`

docker network create muswarmadmin_test

docker run -d --name=muswarmadmin_test_db \
	--net muswarmadmin_test \
	--network-alias database \
	-e SPARQL_UPDATE=true \
	-e DEFAULT_GRAPH=http://mu.semte.ch/test \
	-p 8890:8890 \
	tenforce/virtuoso:1.2.0-virtuoso7.2.2

docker run -d --name=muswarmadmin_test_delta \
	--net muswarmadmin_test \
	--network-alias delta \
	-e CONFIGFILE=/config/config.properties \
	-e SUBSCRIBERSFILE=/config/subscribers.json \
	-v "$PWD"/delta:/config \
	-p 8891:8890 \
	semtech/mu-delta-service:beta-0.9
