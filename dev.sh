#!/bin/bash

docker run -it --rm \
    --network appbdiide_default  \
    -p 1234:80 -v "$PWD":/src \
    --link database:appbdiide_database_1 \
    --name mu-docker-compose-handler  \
    mu-swarm-admin-service