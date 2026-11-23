[![Unit tests](https://github.com/ccs-amsterdam/amcat4/actions/workflows/unittests.yml/badge.svg)](https://github.com/ccs-amsterdam/amcat4/actions/workflows/unittests.yml)
[![Flake8 & Mypy linting](https://github.com/ccs-amsterdam/amcat4/actions/workflows/linting.yml/badge.svg)](https://github.com/ccs-amsterdam/amcat4/actions/workflows/linting.yml)
[![pip version](https://badge.fury.io/py/amcat4.svg)](https://pypi.org/project/amcat4/)
![Python](https://img.shields.io/badge/python-3.10,3.11,3.12,3.13-blue.svg)
![Elasticsearch](https://img.shields.io/badge/elasticsearch-8.17-green)

# AmCAT4

## In this fork: branch `postgres-fast-v2`, faster read paths for the PostgreSQL version

This fork has an extra branch, [`postgres-fast-v2`](https://github.com/Geo-Mahdi/amcat4-tM1/tree/postgres-fast-v2). The `master` branch you are looking at is unchanged AmCAT 4.2.11 (Elasticsearch), apart from this notice. The branch builds on [`postgres-migration`](https://github.com/Geo-Mahdi/amcat4-tM1/tree/postgres-migration)
(commit `bb35e43a`) and keeps its design: one documents table, values in JSON columns under stable field keys,
projects sharing partitions, AmCAT's own query parser and field-level access. It changes how queries reach the
pg_search index, and adds a few index and maintenance details. It is experimental work in a personal fork, prepared
with the help of Claude, and has not been reviewed by the AmCAT maintainers.

[All changes compared with `postgres-migration`](https://github.com/Geo-Mahdi/amcat4-tM1/compare/postgres-migration...postgres-fast-v2)
· overview with reasons, measurements and switches:
[`backend/POSTGRES_FAST_PATHS.md`](https://github.com/Geo-Mahdi/amcat4-tM1/blob/postgres-fast-v2/backend/POSTGRES_FAST_PATHS.md)
· the full reasoning behind every change, with alternatives and measurements:
[`docs/postgres-fast-v2/RATIONALE.md`](https://github.com/Geo-Mahdi/amcat4-tM1/blob/postgres-fast-v2/docs/postgres-fast-v2/RATIONALE.md)

### Try this branch

You need [Docker with docker compose](https://docs.docker.com/compose/install/). Ready-made images of this branch
are published in this fork's container registry (`ghcr.io/geo-mahdi/amcat4-tm1-backend` and `-frontend`), built
from every push to the branch.

**With the ready-made images** (no build, a few minutes):

```sh
mkdir amcat4-fast-v2 && cd amcat4-fast-v2
B=https://raw.githubusercontent.com/Geo-Mahdi/amcat4-tM1/postgres-fast-v2/deploy
curl -O $B/docker-compose.yml -O $B/docker-compose.fast-v2.yml -O $B/.env.example
cp .env.example .env      # edit if needed, see below
docker compose -f docker-compose.yml -f docker-compose.fast-v2.yml up -d
```

Then open http://localhost. Stop it with the same command and `down` instead of `up -d`; add `-v` to also delete
the data.

**Or build the images yourself** from a clone:

```sh
git clone -b postgres-fast-v2 https://github.com/Geo-Mahdi/amcat4-tM1 && cd amcat4-tM1/deploy
cp .env.example .env
docker compose -f docker-compose.yml -f docker-compose.build.yml up -d --build
```

Before you start:

- The example settings use `amcat4_auth=no_auth`: **everybody who can reach the server is an administrator**, and
  the web server listens on ports 80 and 443 of all network interfaces. Use it only on your own computer or a
  trusted network, or set up authentication and a real `AMCAT4_COOKIE_SECRET` in `.env` first.
- Ports 80 and 443 must be free.
- Upgrading an existing database of `postgres-migration` runs migrations `0002` and `0003`, which rebuild the
  search index once (minutes per few million documents; it needs about as much free disk as the index takes).
  Run `docker compose run --rm api amcat4 migrate` first, so the API is not down while it runs.

### What changed

| Area | Change |
|---|---|
| Searches | Read queries go to the project's partition table, so pg_search can count and aggregate inside its index. The project condition and filters are unscored (`const_score`), so only the search words are scored. No parallel workers for API queries. One count per search, run next to the page. |
| Aggregations | Done inside the index with `pdb.agg` (keywords, tags, dates, nested axes, metrics), with the original SQL as fallback. |
| Sorting | Five more sort slots (`fast_sort`), next to `date` and `source`: three keyword, one number, one date. Sorting on a slot runs inside the index. |
| Downloads | New sort key `"sort": "_seq"`: pages by internal id with a cursor, at the same cost for every page. |
| Listing | Listing a project without a query uses a normal B-tree index. |
| Maintenance | pg_search 0.27.0, VACUUM after large changes, an optional move of a large project to its own partition. |
| Fixes | Trailing wildcards find all matching words, aggregations handle empty values, concurrent uploads of the same documents no longer fail. |
| Schema | Migrations `0002` (sort slot columns, id index, the search index rebuilt once) and `0003` (project moves). `0001` is unchanged. |

Every change has a switch that restores the original behaviour (see `deploy/.env.example`). The test suite went from
125 to 257 tests and passes on pg_search 0.26.0 and 0.27.0.

### Short test, 22 November 2026

One laptop, one run per version, the same 2.68 million Dutch news articles, requests through the AmCAT API.
The two PostgreSQL versions used the same database (PostgreSQL 18, pg_search 0.27.0), one after the other.

| Median response time (ms) | Elasticsearch (`master` 4.2.11) | `postgres-migration` | `postgres-fast-v2` |
|---|---:|---:|---:|
| Rare word | 6.0 | 16.2 | 7.6 |
| Medium word | 8.8 | 27.3 | 8.9 |
| Common word | 8.8 | 108.1 | 11.8 |
| Very common word (`de`, `het`) | 8.3 | 490.1 | 16.1 |
| Phrase | 8.7 | 29.1 | 11.8 |
| Boolean | 6.5 | 24.8 | 12.2 |
| Broad wildcard | 8.3 | 22.7 | 12.1 |
| Search with filters | 6.9 | 26.6 | 13.2 |
| No query, filters only | 7.2 | 99.8 | 13.2 |
| Sorted results | 8.0 | 477.2 | 12.4 |
| Snippets | 14.1 | 50.4 | 21.1 |
| Aggregations (15 kinds) | 28.9 | 237.8 | 22.1 |

| Other measures | Elasticsearch | `postgres-migration` | `postgres-fast-v2` |
|---|---:|---:|---:|
| Requests per second, 32 clients, mixed searches | 912 | 133 | 370 |
| Download, documents per second (best paging mode) | 7,645 | 4,779 | 13,217 |
| Same counts as `postgres-migration` on 56 searches | | | 56 of 56 |
| Aggregations identical to `postgres-migration` (15) | | | 15 of 15 |
| Wildcard counts equal to Elasticsearch (7 tested) | | 1 of 7 | 7 of 7 |
| Bulk upload, documents per second (batches of 1,000) | 9,741 | 1,342 | 8,605 |
| Four clients upload the same documents at once | all accepted | 3 of 4 get HTTP 500 (data correct) | all accepted |
| A reader sees a half-finished upload | yes | no | no |
| Upload interrupted by a crash | part of it is kept | all or nothing | all or nothing |

Notes on reading these numbers:

- One run on one laptop with no authentication, so differences of a few milliseconds mean little.
- `postgres-migration` is pinned to pg_search 0.26.0 and was run here on 0.27.0. It also had no sort slot for the
  fields used in the sorted searches, which is part of what this branch adds.
- Elasticsearch caches repeated aggregation requests, so its aggregation times are partly cache hits.
- In `postgres-fast-v2`, two fields were put in sort slots before the test. Filling a slot rewrites every document
  of the project (more than 10 minutes for two slots here, before a later improvement that fills several slots in
  one pass), so it is a maintenance step.
- Not tested in this short run: larger collections, cold starts, fixed-rate load, queries over many projects.

The original README of `master` follows.

---

Server for document management and automatic text analysis, developed as part of [OPTED](https://opted.eu).
[Learn more](https://opted.eu/fileadmin/user_upload/k_opted/OPTED_deliverable_D7.1.pdf) See also the [AmCAT book (in progress)](https://amcat.nl/book/02._getting-started).

# Installing AmCAT

The recommended way to install AmCAT either for local use or in a production environment is through docker:

0. [Install docker compose](https://docs.docker.com/compose/install/) if needed
1. Download the [amcat4-deploy.zip](https://github.com/ccs-amsterdam/amcat4/releases/latest/download/amcat4-deploy.zip)
2. Unzip the the archive, creating the amcat4-deploy folder on your computer
3. If needed, edit the .env file (which is based on [.env.example](deploy/.env.example)) for local configuration. For private use, this step can be skipped. For setting up a shared server you certainly want to set up authentication, change the cookie secret, add a https address etc.
4. Run `docker compose up -d`

On linux-like systems, you can also use the commands below to go through these steps:

```{sh}
wget https://github.com/ccs-amsterdam/amcat4/releases/latest/download/amcat4-deploy.zip
unzip amcat4-deploy.zip
cd amcat4-deploy
editor .env   # if needed; replace 'editor' by an editor of your choice
docker compose up -d
```

## General code overview

AmCAT4 is a 'monorepo' containing the code for both the [python FastAPI backend API](backend/) and the [vite react frontend](frontend/).
The root folder contains a [package.json](package.json) script which contains a number of commands to manage both frontend and backend.

In production, the front-end is built and statically served. In development, the front-end is dynamically served using vite, which can be started with the various `pnpm` commands listed in the next section.

The backend uses a ElasticSearch document storage as a database. Each project in AmCAT is represented as a single index within elastic. In addition, a number of 'system indices' represent server and project metadata including users, roles, and fields.

A number of important configuration options for both AmCAT and Elastic are set using a `.env` file, with reasonable defaults for single-user. See [deploy/.env.example](deploy/.env.example) for an overview of options.

## Development

### Before starting, ensure you have the following installed:

* [Python 3.11+](https://www.python.org/downloads/)
* [Node.js 20+](https://nodejs.org/en)

Once Node.js is installed, run the following command to install pnpm globally:

```
npm install -g pnpm
```

### Install and run AmCAT4 in development mode

The easiest way to install AmCAT4 for local development mode is to use the following commands:

```
# Clone the repo
git clone https://github.com/ccs-amsterdam/amcat4

# Install the monorepo
pnpm install

# install AmCAT4 in development mode
pnpm dev:install

# TUI for creating AmCAT4 .env file
pnpm dev:config
```

Then we need an ElasticSearch instance, and optionally a SeaweedFS instance. An easy way to fire this up is to use the [deploy/docker-compose.yml](deploy/docker-compose.yml) file with the `dev` profile. The `start:db` command runs this from the root folder, ensuring the root `.env` file is used to configure the elastic:

```
pnpm start:db
```

Then to start the development servers for both the frontend and backend, run

```
pnpm dev
```

When you stop the dev server, the docker containers will keep running.
To shut them down, run:

```
pnpm stop:db
```

## Releasing a new version

1. Update the version number in [backend/pyproject.toml](backend/pyproject.toml), [frontend/package.json](frontend/package.json), and [deploy/.env.example](deploy/.env.example) to the new version (e.g. `4.3.0`).
2. Commit the version bump and push to `master`.
3. Create and push a git tag matching the version:
   ```sh
   git tag v4.3.0
   git push origin v4.3.0
   ```

Pushing the tag triggers the [release workflow](.github/workflows/release.yml), which:
- Builds and pushes Docker images for the frontend and backend to Docker Hub (tagged with both the version and `latest`).
- Packages the deploy files into `amcat4-deploy.zip` and attaches it to a new GitHub release with auto-generated release notes.
