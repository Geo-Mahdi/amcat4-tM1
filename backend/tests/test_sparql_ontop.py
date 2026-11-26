"""
SPARQL through Ontop (deploy/ontop) on the test database: Ontop 5.5.0 (the CLI, downloaded with
deploy/ontop/fetch-ontop-cli.sh) is started as an endpoint with the mapping and the read-only role of role.sql.
Skipped unless AMCAT4_ONTOP_CLI_DIR points to the downloaded CLI (the directory with lib/), and java is on the PATH.
"""

import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlencode

import pytest
from psycopg.conninfo import conninfo_to_dict

from amcat4.config import get_settings
from amcat4.connections import db_schema
from tests.sparql_tools import (
    BASE_IRI,
    MAPPING,
    PRIVATE,
    PROPERTIES_EXAMPLE,
    PUBLIC,
    SECRET_WORD,
    create_role,
    drop_projects,
    drop_role,
    make_projects,
)
from tests.tools import get_json

CLI_DIR = os.environ.get("AMCAT4_ONTOP_CLI_DIR")

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.ontop,
    pytest.mark.skipif(
        not CLI_DIR or not (Path(CLI_DIR) / "lib").is_dir() or not shutil.which("java"),
        reason="set AMCAT4_ONTOP_CLI_DIR to the Ontop CLI (deploy/ontop/fetch-ontop-cli.sh) to run these tests",
    ),
]

PREFIXES = """
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX schema: <https://schema.org/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX amcat: <https://amcat.nl/ns/amcat4#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""
PASSWORD = "ontop-test-password"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _properties(role: str) -> str:
    """ontop.properties.example with the test database and the test role"""
    info = conninfo_to_dict(get_settings().postgres_url)
    host = info.get("host") or "localhost"
    if host.startswith("/"):  # (a unix socket: the JDBC driver needs tcp)
        host = "localhost"
    url = (
        f"jdbc:postgresql://{host}:{info.get('port') or 5432}/{info['dbname']}"
        f"?currentSchema={db_schema()}&options={quote('-c statement_timeout=10s')}"
    )
    lines = []
    for line in PROPERTIES_EXAMPLE.read_text().splitlines():
        key = line.split("=", 1)[0]
        values = {"jdbc.url": url, "jdbc.user": role, "jdbc.password": PASSWORD}
        lines.append(f"{key}={values[key]}" if key in values else line)
    return "\n".join(lines) + "\n"


@pytest.fixture(scope="module")
async def endpoint(tmp_path_factory):
    await make_projects()
    role = create_role(password=PASSWORD)
    directory = tmp_path_factory.mktemp("ontop")
    properties = directory / "ontop.properties"
    properties.write_text(_properties(role))
    port = _free_port()
    log = open(directory / "ontop.log", "w")
    process = subprocess.Popen(
        [
            "java",
            "-cp",
            f"{CLI_DIR}/lib/*",
            "it.unibz.inf.ontop.cli.Ontop",
            "endpoint",
            "--mapping",
            str(MAPPING),
            "--properties",
            str(properties),
            "--port",
            str(port),
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    url = f"http://127.0.0.1:{port}/sparql"
    try:
        deadline = time.monotonic() + 180
        while True:
            if process.poll() is not None:
                raise RuntimeError(f"Ontop stopped: {(directory / 'ontop.log').read_text()[-3000:]}")
            try:
                if _post(url, "ASK {}", timeout=5)[0] == 200:
                    break
            except OSError:  # (not listening yet)
                pass
            if time.monotonic() > deadline:
                raise RuntimeError(f"Ontop did not start: {(directory / 'ontop.log').read_text()[-3000:]}")
            time.sleep(1)
        yield {"url": url, "properties": properties, "role": role}
    finally:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
        log.close()
        await drop_role(role)
        await drop_projects()


def _post(url: str, query: str, timeout: float = 30) -> tuple[int, str]:
    """POST a SPARQL query (with urllib: httpx is mocked in the tests, see conftest.mock_middlecat)"""
    request = urllib.request.Request(
        url,
        data=urlencode({"query": query}).encode(),
        headers={"Accept": "application/sparql-results+json", "Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def sparql(endpoint, query: str) -> list[dict]:
    """The rows of a SELECT query, as {variable: value}"""
    status, body = _post(endpoint["url"], PREFIXES + query)
    assert status == 200, body
    return [{k: v["value"] for k, v in b.items()} for b in json.loads(body)["results"]["bindings"]]


def iri(number: int, project: str = PUBLIC) -> str:
    return f"{BASE_IRI}/index/{project}/releases/{number}"


async def test_releases_of_a_public_project(endpoint):
    rows = sparql(
        endpoint,
        f"""SELECT ?release ?version ?issued ?n WHERE {{
              ?project a dcat:DatasetSeries ; dct:identifier "{PUBLIC}" ; dct:title ?title .
              ?release a dcat:Dataset ; dcat:inSeries ?project ; dcat:version ?version ; dct:issued ?issued ;
                       amcat:documentCount ?n .
            }} ORDER BY ?version""",
    )
    assert [(r["release"], r["version"], r["n"]) for r in rows] == [(iri(1), "1", "12"), (iri(2), "2", "15")]
    # the private project is not in the graph at all
    assert sparql(endpoint, f'SELECT ?p WHERE {{ ?p dct:identifier "{PRIVATE}" }}') == []
    assert sparql(endpoint, "SELECT ?p WHERE { ?p a dcat:DatasetSeries }") == [{"p": f"{BASE_IRI}/index/{PUBLIC}"}]


async def test_fields_of_a_release(endpoint):
    rows = sparql(
        endpoint,
        f"""SELECT ?name ?type ?n WHERE {{
              <{iri(1)}> schema:variableMeasured ?f .
              ?f a schema:PropertyValue ; schema:name ?name ; amcat:fieldType ?type ; amcat:documentCount ?n .
            }} ORDER BY ?name""",
    )
    assert rows == [
        {"name": "date", "type": "date", "n": "12"},
        {"name": "kw", "type": "keyword", "n": "12"},
        {"name": "tags", "type": "tag", "n": "6"},
    ]


async def test_top_values_of_a_field(endpoint):
    rows = sparql(
        endpoint,
        f"""SELECT ?value ?n WHERE {{
              <{iri(1)}> schema:variableMeasured ?f . ?f schema:name "kw" ;
                  schema:valueReference [ schema:value ?value ; amcat:documentCount ?n ; schema:position ?rank ] .
            }} ORDER BY ?rank""",
    )
    assert rows == [{"value": f"cat{k}", "n": "4"} for k in range(3)]


async def test_counts_equal_the_release_api(endpoint, client, admin):
    for number in (1, 2):
        release = await get_json(client, f"/index/{PUBLIC}/releases/{number}", user=admin)
        manifest = release["manifest"]
        rows = sparql(endpoint, f"SELECT ?n WHERE {{ <{iri(number)}> amcat:documentCount ?n }}")
        assert rows == [{"n": str(manifest["documents"])}]
        rows = sparql(
            endpoint,
            f"""SELECT ?name ?n WHERE {{
                  <{iri(number)}> schema:variableMeasured [ schema:name ?name ; amcat:documentCount ?n ] }}""",
        )
        assert {r["name"]: int(r["n"]) for r in rows} == {
            name: manifest["fields"][name]["documents"] for name in ("date", "kw", "tags")
        }


async def test_nothing_text_like(endpoint):
    # no text field, no field that metareaders cannot see
    assert sparql(endpoint, 'SELECT ?f WHERE { ?f amcat:fieldType "text" }') == []
    rows = sparql(endpoint, "SELECT DISTINCT ?name WHERE { ?f a schema:PropertyValue ; schema:name ?name }")
    names = {r["name"] for r in rows}
    assert names == {"date", "kw", "tags"}
    # the whole graph: no literal from a text, a hidden field or the withdrawal reason
    rows = sparql(endpoint, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
    assert rows and not [r for r in rows if SECRET_WORD in r["o"] or SECRET_WORD in r["s"]]
    assert not [r for r in rows if PRIVATE in r["s"] or PRIVATE in r["o"]]
    predicates = {r["p"] for r in rows}
    assert not [p for p in predicates if "text" in p.lower() or "content" in p.lower() or "body" in p.lower()]


async def test_withdrawn_and_doi(endpoint):
    rows = sparql(
        endpoint,
        """SELECT ?release ?at ?doi ?same WHERE {
              ?release amcat:withdrawn true ; prov:invalidatedAtTime ?at ; dct:identifier ?doi ; schema:sameAs ?same .
            }""",
    )
    assert len(rows) == 1
    at = datetime.fromisoformat(rows[0].pop("at"))
    assert at == datetime(2026, 11, 1, 12, tzinfo=timezone.utc)
    assert rows == [{"release": iri(2), "doi": "10.5555/amcat.test.2", "same": "https://doi.org/10.5555/amcat.test.2"}]
    assert sparql(endpoint, f"SELECT ?s WHERE {{ <{iri(1)}> prov:invalidatedAtTime ?s }}") == []
    assert sparql(endpoint, f"SELECT ?st WHERE {{ <{iri(2)}> amcat:doiStatus ?st }}") == [{"st": "findable"}]


async def test_query_timeout_setting(endpoint):
    properties = endpoint["properties"].read_text()
    assert "ontop.query.defaultTimeout=10" in properties
    assert "statement_timeout" in properties  # (the second limit, in postgres, through the JDBC url)
