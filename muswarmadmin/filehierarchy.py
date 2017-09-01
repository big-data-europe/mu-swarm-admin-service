import os
from aiosparql.syntax import IRI
from base64 import b64decode

from muswarmadmin.prefixes import SwarmUI


async def deploy_file_hierarchy(sparql, subject, path):
    result = await sparql.query("""
        SELECT *
        FROM {{graph}}
        WHERE {
            {{}} swarmui:fileHierarchy ?o
        }
        """, subject)
    if not result['results']['bindings'] or \
            not result['results']['bindings'][0]:
        return
    file_iri = IRI(result['results']['bindings'][0]['o']['value'])
    await create_file(sparql, file_iri, path, [], True)


async def create_file(sparql, file_iri, path, parents, root):
    if file_iri in parents:
        raise Exception("File hierarchy loop detected in %s, parents are: "
                        % (file_iri, parents))
    result = await sparql.query("DESCRIBE {{}} FROM {{graph}}", file_iri)
    if not result or not result[file_iri]:
        return
    name = result[file_iri][SwarmUI.fileName][0]['value']
    is_directory = (
        result[file_iri].get(SwarmUI.isDirectory,
                             [{'value': "false"}])[0]['value'] == "true")
    is_file = (
        result[file_iri].get(SwarmUI.isFile,
                             [{'value': "false"}])[0]['value'] == "true")
    if is_file:
        content = result[file_iri][SwarmUI.fileContent][0]['value']
        with open(os.path.join(path, name), "wb") as fh:
            fh.write(b64decode(content))
    elif is_directory:
        parents = parents + [file_iri]
        if root:
            sub_path = path
        else:
            sub_path = os.path.join(path, name)
            os.mkdir(sub_path)
        for sub_file in result[file_iri].get(SwarmUI.files, []):
            await create_file(sparql, IRI(sub_file['value']), sub_path,
                              parents, False)
    else:
        raise Exception("Unknown file type for node %s" % file_iri)
