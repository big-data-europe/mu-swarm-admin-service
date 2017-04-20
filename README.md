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
