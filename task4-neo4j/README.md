# Task 4: Graph Database Engineering using Neo4j

**Objective:** Run Neo4j Community Edition in Docker (ports 7474/7687, custom credentials), bulk
import the first 5,000 citation edges from the SNAP cit-Patents dataset via `LOAD CSV`, and write
Cypher queries for neighbor lookup, in-degree centrality (top cited patents), and shortest path.

**Dataset:** https://snap.stanford.edu/data/cit-Patents.html

**Status:** 4.1 done — Neo4j Community container up via `docker-compose.yml` (ports 7474/7687,
custom auth via `NEO4J_AUTH`), browser login verified at localhost:7474. Next: 4.2 bulk-load first
5,000 citation edges.
