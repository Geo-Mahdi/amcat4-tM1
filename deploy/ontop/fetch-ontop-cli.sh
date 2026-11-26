#!/usr/bin/env bash
# Download the Ontop command line tool (and the PostgreSQL JDBC driver) from Maven Central, for local tests of the
# SPARQL endpoint without Docker. Needs Java 11+ and Maven (mvn on the PATH, or MVN=/path/to/mvn).
#
#   deploy/ontop/fetch-ontop-cli.sh [target dir, default ~/ontop-cli]
#
# The jars end up in <target>/lib. Run the endpoint with:
#   java -cp '<target>/lib/*' it.unibz.inf.ontop.cli.Ontop endpoint --mapping deploy/ontop/mapping.ttl \
#        --properties ontop.properties --port 8080
# The tests (backend/tests/test_sparql_ontop.py) run when AMCAT4_ONTOP_CLI_DIR=<target>.
set -euo pipefail

ONTOP_VERSION="${ONTOP_VERSION:-5.5.0}"
PGJDBC_VERSION="${PGJDBC_VERSION:-42.7.4}"
TARGET="${1:-$HOME/ontop-cli}"
MVN="${MVN:-$(command -v mvn || echo /opt/maven/bin/mvn)}"

mkdir -p "$TARGET"
cat > "$TARGET/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>local.amcat4</groupId>
  <artifactId>ontop-cli-download</artifactId>
  <version>1</version>
  <packaging>pom</packaging>
  <dependencies>
    <dependency>
      <groupId>it.unibz.inf.ontop</groupId>
      <artifactId>ontop-cli</artifactId>
      <version>${ONTOP_VERSION}</version>
    </dependency>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <version>${PGJDBC_VERSION}</version>
    </dependency>
  </dependencies>
</project>
POM

"$MVN" -q -f "$TARGET/pom.xml" dependency:copy-dependencies -DoutputDirectory="$TARGET/lib" -DincludeScope=runtime
echo "Ontop ${ONTOP_VERSION} CLI in $TARGET/lib ($(ls "$TARGET/lib" | wc -l) jars)"
