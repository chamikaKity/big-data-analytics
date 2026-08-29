# Task 4: Graph Database Engineering using Neo4j

## Objective
Run Neo4j Community Edition in Docker, bulk-load the first 5,000 citation edges from the SNAP
cit-Patents dataset via `LOAD CSV`, and write Cypher queries for neighbor lookup, in-degree
centrality, and shortest path.

## 4.1 — Environment
Neo4j 5 Community runs via `docker-compose.yml`: ports 7474 (browser) and 7687 (Bolt) exposed,
custom credentials (`neo4j`/`patents123`) set via `NEO4J_AUTH`, data persisted to a local
bind-mounted volume. Browser login at `localhost:7474` verified directly.

## Dataset
[SNAP cit-Patents](https://snap.stanford.edu/data/cit-Patents.html) — a directed US patent
citation graph. The first 5,000 edges are streamed from the source gzip (stopping early, so the
full ~85MB file is never downloaded) via `scripts/prepare_data.sh`.

## 4.2 — Bulk Load
`scripts/load_patents.cypher` creates a uniqueness constraint on `Patent.id`, then uses
`LOAD CSV WITH HEADERS` + `MERGE` to load the 5,000 edges into `(:Patent)-[:CITES]->(:Patent)`.
Verified: 5,000 `CITES` relationships across 5,912 distinct `Patent` nodes (fewer nodes than
2×5,000 since several patents appear as both citing and cited across rows).

**Fig 1** — the loaded graph in Neo4j Browser: database info panel confirming the node/relationship
counts, and a sample `MATCH p=()-[:CITES]->() RETURN p LIMIT 25` visualization showing the
citation structure:

![Loaded graph in Neo4j Browser](figures/fig-1-neo4j.png)

## 4.3 — Cypher Queries
All three queries are in `scripts/queries.cypher`, output captured in `scripts/queries_output.log`.

**a) Direct neighbors** — both citing and cited directions for a given patent. Patent `3308054`
has no outgoing citations in this sample but is cited by 4 patents.

**Fig 2**:

![Direct neighbors of patent 3308054](figures/task4_3a_combined.png)

**b) In-degree centrality** — top patents by incoming `CITES` count. With only a 5,000-edge
sample, the graph is sparse: the top in-degree is 4, shared by four patents (`3308054`,
`3284344`, `3338819`, `3717571`).

**Fig 3**:

![Top 10 patents by in-degree centrality](figures/task4_3b_combined.png)

**c) Shortest path** — between two patents not directly connected. `shortestPath` on
`1884442` → `3858801` (undirected `CITES*`, since the sample has no multi-hop *directed* chains
at this size) finds a true shortest path of 3 hops, via intermediate patents `3858800` and
`2881616`.

**Fig 4**:

![Shortest path between patents 1884442 and 3858801](figures/task4_3c_combined.png)

## 4.4 — Index Seeks vs. SQL Multi-Join Cost
The stated point of using a graph database is running "performance-driven node lookups without
incurring standard SQL multi-join computational penalties." `PROFILE`-ing the neighbor and
shortest-path queries (full plans in `scripts/profile_output.log`) confirms this directly:

- **Neighbor lookup (4.3a)**: the plan opens with `NodeUniqueIndexSeek` on the `Patent.id`
  constraint (2 DB hits to locate the node), then two `OptionalExpand` operators walk the `CITES`
  relationships directly off that node — 35 total database accesses for the whole query.
- **Shortest path (4.3c)**: both endpoint patents are located via `NodeUniqueIndexSeek` (2 hits
  each), then the native `ShortestPath` operator expands outward from both ends — just 20 total
  database accesses to resolve a 3-hop path across the graph.

Neither plan touches an index or a table for anything beyond the two starting node lookups; every
hop after that is pointer-chasing along a relationship physically stored on the node record
("index-free adjacency"), so cost scales with the number of hops actually taken, not with the size
of the graph. An equivalent relational schema (`patents(id)`, `citations(source_id, target_id)`)
would resolve the same shortest-path query as three chained self-joins of the `citations` table (or
a recursive CTE unrolling to the same joins) — each hop re-scanning or re-hash-joining the full
edge table against the previous hop's result set, with cost that grows both with hop count and with
table size. That is the multi-join penalty a graph traversal avoids.

This advantage is specific to targeted point-lookups and multi-hop traversals, not whole-graph
aggregates: the in-degree query (4.3b) necessarily opens with a `NodeByLabelScan` over all 5,912
`Patent` nodes, because computing in-degree for every patent means visiting every `CITES`
relationship at least once — exactly the same full-table cost a SQL `GROUP BY` over the entire
`citations` table would incur. The graph model wins specifically where the query starts from a
known node and traverses a bounded number of hops, which is the case 4.3a and 4.3c demonstrate
and 4.3b, by its aggregate nature, does not.

## Limitations / what I'd do differently at scale
Only the first 5,000 edges of cit-Patents were loaded (the full dataset has ~16.5M) — at this
size the top in-degree found is just 4, so 4.3b's "centrality" result reflects sampling noise, not
the real citation network's actual hub patents. `LOAD CSV` + `MERGE` also doesn't scale much past
low millions of rows; at the dataset's real size I'd switch to `neo4j-admin database import` (bulk
offline load) and run on Neo4j Enterprise for clustering, since this is a single, unclustered
Community Edition instance with no replication.

## References
- Neo4j, *Cypher Manual* (`LOAD CSV`, `MERGE`, `PROFILE`) — https://neo4j.com/docs/cypher-manual/current/
- Neo4j, *neo4j-admin database import* — https://neo4j.com/docs/operations-manual/current/import/
- Neo4j, *Docker documentation* — https://neo4j.com/docs/operations-manual/current/docker/
- SNAP, *cit-Patents dataset* — https://snap.stanford.edu/data/cit-Patents.html
