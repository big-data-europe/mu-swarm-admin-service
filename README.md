[![Build Status](https://travis-ci.org/big-data-europe/mu-swarm-admin-service.svg?branch=master)](https://travis-ci.org/big-data-europe/mu-swarm-admin-service)
[![codecov](https://codecov.io/gh/big-data-europe/mu-swarm-admin-service/branch/master/graph/badge.svg)](https://codecov.io/gh/big-data-europe/mu-swarm-admin-service)

Mu Swarm Admin Service
======================

Admin service for Docker Swarm.

Quick Start
-----------

```
docker run -it --rm \
    --link database:some_container \
    -v /var/run/docker.sock:/var/run/docker.sock \
    bde2020/mu-swarm-admin-service
```

### Overrides

 *  The default graph can be overridden by passing the environment variable
    `MU_APPLICATION_GRAPH` to the container.
 *  The default SPARQL endpoint can be overridden by passing the environment
    variable `MU_SPARQL_ENDPOINT` to the container.

Example on Docker Swarm
-----------------------

Simply use the same environment variables that you would use for the Docker
client:

```
docker run -it --rm \
    --link database:some_container \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e DOCKER_TLS_VERIFY=1 \
    -e DOCKER_HOST="tcp://192.168.99.100:3376" \
    -v /path/to/certs:/certs \
    -e DOCKER_CERT_PATH=/certs \
    -e DOCKER_MACHINE_NAME="mhs-demo0" \
    bde2020/mu-swarm-admin-service
```

Development
-----------

### Running the integration tests

```
# initialize the related services
$ ./ci/start.sh
dc6d9dd71176820f2a39f72cfacedf789a75104caae7a757ff755872e17a2148
e6ff59908790637416499b3546312e138b83a4a66da7ff3b06a83de30d923942
aa3d6290658baf4605e480173642004462a20a1a5fee812a2bb11055fb0d9d73

# run the test once
$ ./ci/run-tests.sh
[...]
  py36: commands succeeded
  flake8: commands succeeded
  congratulations :)

# run bast in the test container
$ ./ci/run-tests.sh bash
root@b536da333fc1:/src# tox -e py36,flake8 -- -x tests/integration

# stop & cleanup the related services
$ ./ci/stop.sh
```
