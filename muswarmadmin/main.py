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

from muswarmadmin import delta, eventmonitor, services
from muswarmadmin.actionscheduler import ActionScheduler, OneActionScheduler
from muswarmadmin.prefixes import SwarmUI


logger = logging.getLogger(__name__)


if ENV.get("ENV", "prod").startswith("dev"):
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


class Application(web.Application):
    # NOTE: timeout allowed to a docker-compose command to proceed. If the time
    #       exceeds. If the time exceed, the application is killed.
    run_command_timeout = 600
    # NOTE: timeout allowed to docker-compose up command to proceed. If the
    #       time exceeds, the application is killed.
    compose_up_timeout = 1800
    # NOTE: base IRI used for all the resources managed by this service.
    base_resource = IRI("http://swarm-ui.big-data-europe.eu/resources/")

    @property
    def sparql(self):
        """
        The SPARQL client
        """
        if not hasattr(self, '_sparql'):
            self._sparql = SPARQLClient(loop=self.loop)
        return self._sparql

    @property
    def docker(self):
        """
        The Docker client
        """
        if not hasattr(self, '_docker'):
            docker_args = docker.utils.utils.kwargs_from_env()
            self._docker = aiodockerpy.api.client.APIClient(
                loop=self.loop, **docker_args)
        return self._docker

    @property
    async def container(self):
        """
        The container running this application. Returns a dict which is the
        result of a docker inspect.
        """
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
        """
        The first network used by the container running this application
        """
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
        """
        The labels of the container running this application
        """
        if not hasattr(self, '_labels'):
            container = await self.container
            self._labels = container['Config']['Labels']
        return self._labels

    @property
    async def project(self):
        """
        The Docker Compose project of the container running this application
        """
        if not hasattr(self, '_project'):
            self._project = (await self.labels)['com.docker.compose.project']
        return self._project

    async def join_public_network(self, container_id):
        """
        An helper method that makes a container join the application's own
        network
        """
        network = await self.network
        container = await self.docker.inspect_container(container_id)
        env = dict([
            x.split('=', 1)
            for x in container['Config']['Env']
        ])
        if 'PIPELINE_HOST' not in env:
            return False
        if network in container['NetworkSettings']['Networks']:
            return False
        logger.debug("Connecting container %s to network %s...",
                     container_id, network)
        await self.docker.connect_container_to_network(container_id,
                                                       network)
        return True

    async def restart_proxy(self):
        """
        An helper method that restarts the service "proxy" of the running
        application
        """
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
        """
        Get the mu:uuid of a subject IRI
        """
        result = await self.sparql.query("""
            SELECT ?o
            FROM {{graph}}
            WHERE
            {
                {{}} mu:uuid ?o .
            }
            """, subject)
        return result['results']['bindings'][0]['o']['value']

    async def ensure_resource_id_exists(self, resource_id):
        """
        Return True if the resource ID given in parameter exists in the
        database. Otherwise return False.
        """
        result = await self.sparql.query("""
            ASK FROM {{graph}} WHERE { ?s mu:uuid {{}} }
            """, escape_string(resource_id))
        return result['boolean']

    def open_compose_data(self, project_id):
        """
        Use Docker Compose to load the data of a project given in parameter.
        Return a Docker Compose data object.
        """
        project_dir = '/data/%s' % project_id
        config_files = config.config.get_default_config_files(project_dir)
        environment = Environment.from_env_file(project_dir)
        config_details = config.find(project_dir, config_files, environment)
        return config.load(config_details)

    async def update_state(self, uuid, state):
        """
        Helper that update the swarmui:status of a node given in parameter
        """
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

    async def reset_status_requested(self, uuid):
        """
        Helper that removes any swarmui:requestedStatus triple of a node
        given in parameter
        """
        await self.sparql.update("""
            WITH {{graph}}
            DELETE {
                ?s swarmui:requestedStatus ?oldvalue .
            }
            WHERE {
                ?s mu:uuid {{uuid}} ;
                  swarmui:requestedStatus ?oldvalue .
            }
            """, uuid=escape_string(uuid))

    async def reset_restart_requested(self, uuid):
        """
        Helper that removes any swarmui:restartRequested triple of a node
        given in parameter
        """
        await self.sparql.update("""
            WITH {{graph}}
            DELETE {
                ?s swarmui:restartRequested ?oldvalue .
            }
            WHERE {
                ?s mu:uuid {{uuid}} ;
                  swarmui:restartRequested ?oldvalue .
            }
            """, uuid=escape_string(uuid))

    async def get_dct_title(self, uuid):
        """
        Get the dct:title of a node
        """
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
        """
        Get the pipeline ID of a service given in paramater
        """
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

    async def run_command(self, *args, logging=True, timeout=None, **kwargs):
        """
        Run a subprocess, log the output, wait for its execution to complete,
        timeout eventually. Return a process object.
        """
        if timeout is None:
            timeout = self.run_command_timeout
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        try:
            if logging:
                await asyncio.wait_for(self._log_process_output(proc), timeout)
            else:
                await asyncio.wait_for(proc.wait(), timeout)
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
        """
        Helper method that the output of a StreamReader object to the logger
        """
        while True:
            line = await reader.readline()
            if not line:
                break
            line = self._control_char_re.sub("", line.decode())
            if line:
                logger.info(line)

    async def _log_process_output(self, proc):
        """
        Gather two futures that will log the STDOUT and STDERR of a subprocess
        """
        await asyncio.gather(self._log_streamreader(proc.stdout),
                             self._log_streamreader(proc.stderr))

    async def enqueue_action(self, key, action, args):
        """
        Enqueue an action in the queue of an ActionScheduler. The
        ActionScheduler is determined by the key. The action will be executed
        serially after all previous actions have finished.
        """
        await ActionScheduler.execute(key, action, args, loop=self.loop)

    async def enqueue_one_action(self, key, action, args):
        """
        Enqueue an action in the queue of a OneActionScheduler. The
        OneActionScheduler is determined by the key. The action will be
        executed serially after any running action has completed. If an action
        already exists in the queue, the new action will be discarded.
        """
        await OneActionScheduler.execute(key, action, args, loop=self.loop)

    async def event_container(self, event):
        """
        This method receive the event from the Docker client
        """
        if event["Action"] == "start":
            await self.event_container_started(event)
        elif event["Action"] == "die":
            await self.event_container_died(event)

    async def event_container_started(self, event):
        """
        Watch for container "starting" event
        """
        container_id = event["Actor"]["ID"]
        attr = event["Actor"]["Attributes"]
        project_name = attr.get("com.docker.compose.project")
        service_name = attr.get("com.docker.compose.service")
        container_number = int(attr.get("com.docker.compose.container-number",
                                        0))
        if not (project_name and service_name):
            return
        project_id = project_name.upper()
        if not await self.ensure_resource_id_exists(project_id):
            return
        await self.sparql.update(
            """
            WITH {{graph}}
            DELETE {
                ?pipeline swarmui:status ?oldpipelinestate .
                ?service swarmui:status ?oldservicestate ;
                  swarmui:scaling ?oldscaling .
            }
            INSERT {
                ?pipeline swarmui:status swarmui:Started .
                ?service swarmui:status swarmui:Started ;
                  swarmui:scaling ?newscaling .
            }
            WHERE {
                ?pipeline a swarmui:Pipeline ;
                  mu:uuid {{project_id}} ;
                  swarmui:status ?oldpipelinestate ;
                  swarmui:services ?service .

                ?service a swarmui:Service ;
                  dct:title {{service_name}} ;
                  swarmui:scaling ?oldscaling ;
                  swarmui:status ?oldservicestate .

                BIND(IF(?oldscaling > {{scaling}},
                  ?oldscaling, {{scaling}}) AS ?newscaling) .
            }
            """, project_id=escape_string(project_id),
            service_name=escape_string(service_name),
            scaling=container_number)
        if await self.join_public_network(container_id):
            await self.enqueue_one_action("proxy", self.restart_proxy, [])

    async def event_container_died(self, event):
        """
        Watch for container "dying" event
        """
        attr = event["Actor"]["Attributes"]
        project_name = attr.get("com.docker.compose.project")
        service_name = attr.get("com.docker.compose.service")
        container_number = int(attr.get("com.docker.compose.container-number",
                                        0))
        if not (project_name and service_name):
            return
        project_id = project_name.upper()
        if not await self.ensure_resource_id_exists(project_id):
            return
        service_status = (
            SwarmUI.Started if container_number > 1 else SwarmUI.Stopped
        )
        await self.sparql.update(
            """
            WITH {{graph}}
            DELETE {
                ?pipeline swarmui:status ?oldpipelinestate .
                ?service swarmui:status ?oldservicestate ;
                  swarmui:scaling ?oldscaling .
            }
            INSERT {
                ?pipeline swarmui:status ?newstate .
                ?service swarmui:status {{service_status}} ;
                  swarmui:scaling ?newscaling .
            }
            WHERE {
                ?pipeline a swarmui:Pipeline ;
                  mu:uuid {{project_id}} ;
                  swarmui:status ?oldpipelinestate ;
                  swarmui:services ?service .

                ?service a swarmui:Service ;
                  dct:title {{service_name}} ;
                  swarmui:scaling ?oldscaling ;
                  swarmui:status ?oldservicestate .

                BIND(IF(?oldscaling < {{scaling}},
                  ?oldscaling, {{scaling}}) AS ?newscaling) .

                OPTIONAL {
                    ?pipeline swarmui:services ?otherservice .
                    ?otherservice swarmui:status swarmui:Started ;
                      dct:title ?otherservicetitle .
                    FILTER ( ?otherservicetitle != {{service_name}} )
                } .

                BIND(IF(BOUND(?otherservice), swarmui:Started, swarmui:Stopped)
                  AS ?newstate) .
            }
            """,
            project_id=escape_string(project_id),
            service_name=escape_string(service_name),
            scaling=(container_number - 1),
            service_status=service_status)


async def stop_cleanup(app):
    """
    Properly close SPARQL and Docker client
    """
    app.sparql.close()
    app.docker.close()


async def stop_action_schedulers(app):
    """
    Stop all action schedulers. This will wait for all actions to be completed
    before ending. All the actions in all ActionSchedulers queue will be
    executed and awaited before leaving.
    """
    await ActionScheduler.graceful_cancel()
    await OneActionScheduler.graceful_cancel()


async def start_event_monitor(app):
    """
    Start the Docker event monitor
    """
    app['event_monitor'] = app.loop.create_task(
        eventmonitor.watch(app.docker, {"container": [app.event_container]}))


async def stop_event_monitor(app):
    """
    Cancel event monitor
    """
    app['event_monitor'].cancel()
    await app['event_monitor']


app = Application()
app.on_cleanup.append(stop_action_schedulers)
app.on_startup.append(start_event_monitor)
app.on_cleanup.append(stop_event_monitor)
app.on_cleanup.append(stop_cleanup)
app.on_startup.append(delta.startup)
app.on_startup.append(eventmonitor.startup)
app.router.add_post("/update", delta.update)
app.router.add_get("/services/{id}/logs", services.logs)
