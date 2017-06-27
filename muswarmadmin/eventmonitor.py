import asyncio
import logging


logger = logging.getLogger(__name__)


async def watch(docker, handlers):
    try:
        logger.debug("Event monitor started")
        async for event in docker.events(decode=True):
            if event["Type"] in handlers:
                for handler in handlers[event["Type"]]:
                    try:
                        await handler(event)
                    except Exception:
                        logger.exception("Event handler %r failed", handler)
    except asyncio.CancelledError:
        pass
    except:
        # NOTE: gracefully exit the application
        logger.exception("Event monitor exception")
        raise KeyboardInterrupt()
    finally:
        logger.debug("Event monitor stopped")


async def startup(app):
    """
    Hook on the startup of the application that will call the Docker API to
    check for existing running containers and call for container started event
    on the application. When the application starts up, it will automatically
    update the database based on actual running containers. It will also check
    the information in the database and ensure that the containers exist and
    are running. If they don't exist, call for container die event on the
    application will be made.
    """
    running_services = {}
    for container in await app.docker.containers():
        project_name = container['Labels'].get("com.docker.compose.project")
        service_name = container['Labels'].get("com.docker.compose.service")
        if not (project_name and service_name):
            continue
        project_id = project_name.upper()
        container_number = int(
            container['Labels'].get("com.docker.compose.container-number", 0))
        await app.event_container_started(
            container['Id'], project_id, service_name, container_number)
        if project_name and service_name:
            running_services[(project_name, service_name)] = max(
                running_services.get((project_name, service_name), 0),
                container_number)

    result = await app.sparql.query(
        """
        SELECT *
        FROM {{graph}}
        WHERE {
            ?service a swarmui:Service ;
              mu:uuid ?uuid ;
              dct:title ?name ;
              swarmui:status swarmui:Started ;
              swarmui:scaling ?scaling .

            ?pipeline a swarmui:Pipeline ;
              swarmui:services ?service ;
              mu:uuid ?projectid .
        }
        """)
    for data in result['results']['bindings']:
        project_id = data['projectid']['value']
        service_name = data['name']['value']
        project_name = project_id.lower()
        scaling = int(data['scaling']['value'])
        actual_scaling = running_services.get((project_name, service_name), 0)
        for i in range(scaling, actual_scaling, -1):
            await app.event_container_died(project_id, service_name, i)
