CREATE CONSTRAINT patent_id IF NOT EXISTS FOR (p:Patent) REQUIRE p.id IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///cit-patents-5000.csv' AS row
MERGE (a:Patent {id: row.source})
MERGE (b:Patent {id: row.target})
MERGE (a)-[:CITES]->(b);
