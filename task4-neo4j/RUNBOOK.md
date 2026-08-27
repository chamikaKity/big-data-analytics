# Task 4: Manual Run Path

All commands run from `task4-neo4j/`.

## 1. Start Neo4j

```bash
docker compose up -d
```

Wait ~10s, then confirm it's healthy:

```bash
docker logs neo4j-patents --tail 20
```

## 2. Fetch the first 5,000 citation edges

Streams from the SNAP `cit-Patents.txt.gz` source (stops early, no full 85MB download) into
`import/cit-patents-5000.csv`:

```bash
./scripts/prepare_data.sh
```

## 3. Bulk-load into `(:Patent)-[:CITES]->(:Patent)`

```bash
docker exec -i neo4j-patents cypher-shell -u neo4j -p patents123 < scripts/load_patents.cypher
```

## 4. Run the analysis queries

Direct neighbors, in-degree centrality, shortest path:

```bash
docker exec -i neo4j-patents cypher-shell -u neo4j -p patents123 --format plain -f /dev/stdin < scripts/queries.cypher
```

## 5. Browser UI (optional)

```
http://localhost:7474
```

Login: `neo4j` / `patents123`

## Teardown

```bash
docker compose down
```

This only removes the container — the graph persists in the bind-mounted `./data/` directory and
will still be there on the next `docker compose up -d`. `load_patents.cypher` is idempotent
(`MERGE` + `CREATE CONSTRAINT IF NOT EXISTS`), so re-running steps 2-3 on top of existing data is
safe and won't duplicate anything.

Only wipe `data/*` if you want to prove the pipeline reproduces cleanly from an empty database:

```bash
docker compose down
rm -rf data/*
docker compose up -d
# then repeat steps 2-4
```
