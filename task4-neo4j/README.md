# Task 4: Graph Database Engineering using Neo4j

**Objective:** Run Neo4j Community Edition in Docker (ports 7474/7687, custom credentials), bulk
import the first 5,000 citation edges from the SNAP cit-Patents dataset via `LOAD CSV`, and write
Cypher queries for neighbor lookup, in-degree centrality (top cited patents), and shortest path.

**Dataset:** https://snap.stanford.edu/data/cit-Patents.html

**Status:** 4.1 done — Neo4j Community container up via `docker-compose.yml` (ports 7474/7687,
custom auth via `NEO4J_AUTH`), browser login verified at localhost:7474.
4.2 done — first 5,000 edges streamed from `cit-Patents.txt.gz` (stopped early, no full 85MB
download needed), converted to `import/cit-patents-5000.csv` (gitignored), bulk-loaded via
`scripts/load_patents.cypher` (`LOAD CSV` + `MERGE`) into `(:Patent)-[:CITES]->(:Patent)`.
Verified: 5,000 `CITES` relationships, 5,912 distinct `Patent` nodes.
4.3 done — three queries in `scripts/queries.cypher`, output captured in
`scripts/queries_output.txt`:
- (a) direct neighbors of patent `3308054` (cites: none; cited by: 4 patents)
- (b) in-degree centrality — top patents by incoming `CITES` count (max in-degree 4, since the
  5,000-edge sample is sparse)
- (c) shortest path between patents `1884442` and `3858801` — not directly connected; true
  shortest path is 3 hops via two intermediate patents (found with `shortestPath`, undirected
  `CITES*` since the sample has no multi-hop *directed* chains at this size)

Task 4 complete.
