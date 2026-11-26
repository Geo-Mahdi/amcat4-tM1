-- The database role of the SPARQL endpoint (Ontop): it can read the sparql_* views of migration 0007 and nothing else.
-- Run it as a superuser (or a role with CREATEROLE that owns the AmCAT schema), after the migrations, and again after
-- a migration that adds a sparql_* view:
--
--   psql "$DATABASE_URL" -v schema=amcat -v password="$SPARQL_DB_PASSWORD" \
--        -v base_iri=https://amcat.example.org -f deploy/ontop/role.sql
--
-- Variables:
--   schema    the schema of the AmCAT tables (AMCAT4_POSTGRES_SCHEMA), default amcat
--   password  the password of the role. Without it the role is NOLOGIN (it exists, but nobody can log in with it)
--   base_iri  the start of the IRIs of projects and releases: AMCAT4_RELEASE_ID_BASE (or the host of AmCAT), so a
--             release has the IRI of the @id of its JSON-LD (<base>/index/<project>/releases/<n>).
--             Default http://localhost:5000
--   role      the name of the role, default amcat_sparql
--
-- The role gets: LOGIN (with the password), USAGE on the schema, SELECT on the sparql_* views. Everything else on the
-- tables, sequences and functions of the schema is revoked. Its sessions are read-only, with a statement timeout of
-- 10 seconds. (Like every role it can read the system catalogs, so it can see the names of tables and columns, not
-- their contents. The views run with the rights of their owner, so the role needs no rights on releases or projects.)

\set ON_ERROR_STOP on
\if :{?schema}
\else
  \set schema amcat
\endif
\if :{?role}
\else
  \set role amcat_sparql
\endif
\if :{?base_iri}
\else
  \set base_iri http://localhost:5000
\endif

BEGIN;

SELECT format('CREATE ROLE %I NOLOGIN', :'role')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role') \gexec

\if :{?password}
  SELECT format('ALTER ROLE %I LOGIN PASSWORD %L CONNECTION LIMIT 20', :'role', :'password') \gexec
\else
  SELECT format('ALTER ROLE %I NOLOGIN', :'role') \gexec
\endif

-- not a superuser, cannot create anything, cannot get the rights of other roles
SELECT format('ALTER ROLE %I NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT', :'role') \gexec
SELECT format('ALTER ROLE %I SET default_transaction_read_only = on', :'role') \gexec
SELECT format('ALTER ROLE %I SET statement_timeout = %L', :'role', '10s') \gexec
SELECT format('ALTER ROLE %I SET search_path = %I', :'role', :'schema') \gexec
SELECT format('ALTER ROLE %I SET amcat4.sparql_base_iri = %L', :'role', :'base_iri') \gexec

-- first take everything away, then give the views
SELECT format('REVOKE ALL ON ALL TABLES IN SCHEMA %I FROM %I', :'schema', :'role') \gexec
SELECT format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA %I FROM %I', :'schema', :'role') \gexec
SELECT format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %I FROM %I', :'schema', :'role') \gexec
SELECT format('REVOKE ALL ON SCHEMA %I FROM %I', :'schema', :'role') \gexec
SELECT format('REVOKE CREATE ON SCHEMA public FROM %I', :'role') \gexec
SELECT format('GRANT USAGE ON SCHEMA %I TO %I', :'schema', :'role') \gexec
SELECT format('GRANT SELECT ON %I.%I TO %I', n.nspname, c.relname, :'role')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = :'schema' AND c.relkind = 'v' AND c.relname LIKE 'sparql\_%' \gexec

COMMIT;
