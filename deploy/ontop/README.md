# SPARQL endpoint (Ontop)

AmCAT can answer SPARQL queries about its **public release metadata**: which projects have published releases,
their numbers, dates, DOIs, document counts, whether a release was withdrawn, and which fields (variables) a release
has with how many documents have a value.

It uses [Ontop](https://ontop-vkg.org), a *virtual* knowledge graph: Ontop translates each SPARQL query into SQL on
the AmCAT database. Nothing is copied, so the answers are always current. Ontop reads only three views
(`sparql_projects`, `sparql_releases`, `sparql_release_fields`, `sparql_field_values`, made by migration 0007), as a
database role that can read nothing else.

It is **off by default**.

## What is never exposed

- Documents, their texts and their values, tags and annotations. The views do not read the tables of documents or
  labels at all. The only values shown are the top 10 values (with counts) of keyword and tag fields that metareaders
  may read, as the golden queries of the release froze them.
- Users, roles, API keys, requests, settings.
- Releases whose metadata is not public (`metadata_public` is false), and releases of deleted projects.
- Fields of type text, and fields that metareaders may not see (metareader access other than `read`).
- The reason a release was withdrawn (free text). The time it was withdrawn is shown.

## Files

| file | what |
| --- | --- |
| `mapping.ttl` | R2RML mapping of the views to RDF (DCAT, Dublin Core, schema.org, PROV-O, a few `amcat:` terms) |
| `ontop.properties.example` | database connection, query timeout; copy to `ontop.properties` |
| `role.sql` | creates the database role `amcat_sparql` that can read only the views |
| `fetch-ontop-cli.sh` | downloads the Ontop command line tool from Maven Central, for tests without Docker |

## Switch it on (docker compose)

From the `deploy` directory, after AMCAT has run its migrations:

1. Create the role (as the database superuser; for the compose database that is `amcat`):

   ```sh
   docker compose exec -T db psql -U amcat -d amcat -v schema=amcat -v password='a long random password' \
       -v base_iri=https://amcat.example.org < ontop/role.sql
   ```

   `base_iri` is the start of the IRIs of projects and releases: use the value of `AMCAT4_RELEASE_ID_BASE` (or the
   host), so a release has the same IRI as the `@id` of its JSON-LD, e.g. `https://amcat.example.org/index/x/releases/1`.
   Run the script again after an upgrade that adds a `sparql_*` view.
2. `cp ontop/ontop.properties.example ontop/ontop.properties` and set `jdbc.url` (database name and
   `currentSchema` = your `AMCAT4_POSTGRES_SCHEMA`, default `amcat`). Put the password there, or in `.env` as
   `SPARQL_DB_PASSWORD`. `ontop.properties` is in `.gitignore`.
3. In `.env`: `AMCAT4_SPARQL=on` (so Caddy sends `/sparql` to Ontop), then
   `docker compose --profile deploy --profile sparql up -d`.

The endpoint is then at `https://<your server>/sparql` (GET or POST, standard SPARQL 1.1 protocol).

Without `AMCAT4_SPARQL=on`, `/sparql` is an ordinary page of the web interface. With another web server, send
`/sparql` to port 8080 of the `ontop` container.

If the `ontop/ontop` image cannot find the PostgreSQL JDBC driver, put `postgresql-42.7.4.jar` (from Maven Central)
in `ontop/jdbc/` and add the volume `./ontop/jdbc:/opt/ontop/jdbc:ro` to the `ontop` service.

## Run it without Docker

```sh
deploy/ontop/fetch-ontop-cli.sh ~/ontop-cli        # needs Java 11+ and Maven
java -cp "$HOME/ontop-cli/lib/*" it.unibz.inf.ontop.cli.Ontop endpoint \
    --mapping deploy/ontop/mapping.ttl --properties deploy/ontop/ontop.properties --port 8080
curl -H 'Accept: application/sparql-results+json' --data-urlencode \
    'query=SELECT ?r ?n WHERE { ?r <https://amcat.nl/ns/amcat4#documentCount> ?n }' http://localhost:8080/sparql
```

## Example query

```sparql
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX schema: <https://schema.org/>
PREFIX amcat: <https://amcat.nl/ns/amcat4#>
SELECT ?release ?version ?documents ?field ?type WHERE {
  ?project dct:identifier "my_project" .
  ?release dcat:inSeries ?project ; dcat:version ?version ; amcat:documentCount ?documents ;
           schema:variableMeasured [ schema:name ?field ; amcat:fieldType ?type ] .
}
```

## Limits

- Each query stops after 10 seconds (`ontop.query.defaultTimeout`), and postgres stops every statement of the role
  after 10 seconds (`statement_timeout`, set by `role.sql` and in the JDBC url).
- The role is read only and has at most 20 connections.
- Ontop has no setting for a maximum number of results. The graph is small (a few triples per release and field),
  so a full dump is cheap; if needed, limit the response size in the web server.
- Counts per value only for the top 10 values of keyword and tag fields (what the release froze).
- Not tested with the Docker image here (only with the Ontop 5.5.0 command line tool, see
  `backend/tests/test_sparql_ontop.py`).
