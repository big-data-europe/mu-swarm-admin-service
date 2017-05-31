from aiohttp import web
from aiosparql.client import SPARQLClient
from aiosparql.syntax import escape_string, IRI
import asyncio
from compose import config
from compose.config.environment import Environment
from compose.project import Project
import logging
from os import environ as ENV, path
import re
import subprocess

from muswarmadmin.delta import update
from muswarmadmin.services import logs


CONFIG_FILES = ['docker-compose.yml', 'docker-compose.prod.yml']
logger = logging.getLogger(__name__)


if ENV.get("ENV", "prod").startswith("dev"):
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


class FakeDockerClient:
    def __getattr__(self, attr):
        raise Exception("This method must never be called")


class Application(web.Application):
    run_command_timeout = 600
    base_resource = IRI("http://swarm-ui.big-data-europe.eu/resources/")

    @property
    def sparql(self):
        if not hasattr(self, '_sparql'):
            self._sparql = SPARQLClient(loop=self.loop)
        return self._sparql

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

    def open_project(self, project_id):
        project_dir = '/data/%s' % project_id
        config_files = filter(
            lambda x: path.exists(path.join(project_dir, x)),
            CONFIG_FILES)
        environment = Environment.from_env_file(project_dir)
        config_details = config.find(project_dir, config_files, environment)
        config_data = config.load(config_details)
        return Project.from_config(project_id, config_data, FakeDockerClient())

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
