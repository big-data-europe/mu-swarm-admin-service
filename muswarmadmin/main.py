import aiodockerpy.api.client
from aiohttp import web
from aiosparql.client import SPARQLClient
from aiosparql.syntax import escape_string, IRI
import asyncio
from compose import config
from compose.config.environment import Environment
import docker.utils.utils
import logging
from os import environ as ENV
import re
import subprocess

from muswarmadmin.delta import update
from muswarmadmin.services import logs


logger = logging.getLogger(__name__)


if ENV.get("ENV", "prod").startswith("dev"):
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


class Application(web.Application):
    run_command_timeout = 600
    base_resource = IRI("http://swarm-ui.big-data-europe.eu/resources/")

    @property
    def sparql(self):
        if not hasattr(self, '_sparql'):
            self._sparql = SPARQLClient(loop=self.loop)
        return self._sparql

    @property
    def docker(self):
        if not hasattr(self, '_docker'):
            docker_args = docker.utils.utils.kwargs_from_env()
            self._docker = aiodockerpy.api.client.APIClient(
                loop=self.loop, **docker_args)
        return self._docker

    @property
    async def container(self):
        if not hasattr(self, '_container'):
            regex = re.compile(r"/docker[/-]([a-f0-9]{64})(\.scope)?$")
            with open("/proc/self/cgroup") as fh:
                for line in fh.readlines():
                    matches = regex.search(line)
                    if matches:
                        container_id = matches.group(1)
                        break
                else:
                    raise Exception("Could not find container ID")
            self._container = await self.docker.inspect_container(container_id)
        return self._container

    @property
    async def network(self):
        if not hasattr(self, '_network'):
            try:
                container = await self.container
                self._network = next(iter(
                    container['NetworkSettings']['Networks'].keys()))
            except StopIteration:
                raise Exception("No network found")
        return self._network

    @property
    async def labels(self):
        if not hasattr(self, '_labels'):
            container = await self.container
            self._labels = container['Config']['Labels']
        return self._labels

    @property
    async def project(self):
        if not hasattr(self, '_project'):
            self._project = (await self.labels)['com.docker.compose.project']
        return self._project

    async def join_public_network(self, project_id):
        network = await self.network
        for container in await self.docker.containers(
                filters={
                    'label': "com.docker.compose.project=" + project_id.lower()
                }):
            container = await self.docker.inspect_container(container)
            env = dict([
                x.split('=', 1)
                for x in container['Config']['Env']
            ])
            if 'VIRTUAL_HOST' not in env:
                continue
            if network in container['NetworkSettings']['Networks']:
                continue
            logger.debug("Connecting container %s to network %s...",
                         container['Id'], network)
            await self.docker.connect_container_to_network(container['Id'],
                                                           network)

    async def restart_proxy(self):
        for container in await self.docker.containers(
                all=True,
                filters={
                    'label': [
                        "com.docker.compose.project=" + (await self.project),
                        "com.docker.compose.service=proxy",
                    ]
                }):
            logger.debug("Restarting proxy %s..." % container['Id'])
            await self.docker.restart(container['Id'])

    async def get_resource_id(self, subject):
        result = await self.sparql.query("""
            SELECT ?o
            FROM {{graph}}
            WHERE
            {
                {{}} mu:uuid ?o .
            }
            """, subject)
        return result['results']['bindings'][0]['o']['value']

    def open_compose_data(self, project_id):
        project_dir = '/data/%s' % project_id
        config_files = config.config.get_default_config_files(project_dir)
        environment = Environment.from_env_file(project_dir)
        config_details = config.find(project_dir, config_files, environment)
        return config.load(config_details)

    async def update_state(self, uuid, state):
        await self.sparql.update("""
            WITH {{graph}}
            DELETE {
                ?s swarmui:status ?oldstate .
            }
            INSERT {
                ?s swarmui:status {{new_state}} .
            }
            WHERE {
                ?s mu:uuid {{uuid}} .
                OPTIONAL { ?s swarmui:status ?oldstate } .
            }
            """, uuid=escape_string(uuid), new_state=state)

    async def reset_restart_requested(self, uuid):
        await self.sparql.update("""
            WITH {{graph}}
            DELETE {
                ?s swarmui:restartRequested ?oldvalue .
            }
            INSERT {
                ?s swarmui:restartRequested "false" .
            }
            WHERE {
                ?s mu:uuid {{uuid}} .
                OPTIONAL { ?s swarmui:restartRequested ?oldvalue } .
            }
            """, uuid=escape_string(uuid))

    async def get_dct_title(self, uuid):
        result = await self.sparql.query("""
            SELECT ?title
            FROM {{graph}}
            WHERE
            {
                ?s mu:uuid {{uuid}} ;
                  dct:title ?title .
            }
            """, uuid=escape_string(uuid))
        if not result['results']['bindings'] or \
                not result['results']['bindings'][0]:
            raise KeyError("resource %r not found" % uuid)
        return result['results']['bindings'][0]['title']['value']

    async def get_service_pipeline(self, service_id):
        result = await self.sparql.query("""
            SELECT ?uuid
            FROM {{graph}}
            WHERE
            {
                ?service a swarmui:Service ;
                  mu:uuid {{uuid}} .

                ?pipeline a swarmui:Pipeline ;
                  swarmui:services ?service ;
                  mu:uuid ?uuid .
            }
            """, uuid=escape_string(service_id))
        if not result['results']['bindings'] or \
                not result['results']['bindings'][0]:
            raise KeyError("service %r not found" % service_id)
        return result['results']['bindings'][0]['uuid']['value']

    async def run_command(self, *args, logging=True, **kwargs):
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        try:
            if logging:
                await asyncio.wait_for(self._log_process_output(proc),
                                       self.run_command_timeout)
            else:
                await asyncio.wait_for(proc.wait(),
                                       self.run_command_timeout)
        except asyncio.TimeoutError:
            logger.warn(
                "Child process %d awaited for too long, terminating...",
                proc.pid)
            try:
                proc.terminate()
            except Exception:
                pass
        await proc.wait()
        return proc

    _control_char_re = re.compile(r'[\x00-\x1f\x7f-\x9f]')

    async def _log_streamreader(self, reader):
        while True:
            line = await reader.readline()
            if not line:
                break
            line = self._control_char_re.sub("", line.decode())
            if line:
                logger.info(line)

    async def _log_process_output(self, proc):
        await asyncio.gather(self._log_streamreader(proc.stdout),
                             self._log_streamreader(proc.stderr))


app = Application()
app.router.add_post("/update", update)
app.router.add_get("/services/{id}/logs", logs)
