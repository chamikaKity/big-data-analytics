// 4.3(a) Direct neighbors of a given patent node (both citing and cited directions)
MATCH (p:Patent {id: "3308054"})
OPTIONAL MATCH (p)-[:CITES]->(cites)
OPTIONAL MATCH (p)<-[:CITES]-(citedBy)
RETURN p.id AS patent,
       collect(DISTINCT cites.id) AS cites,
       collect(DISTINCT citedBy.id) AS citedBy;

// 4.3(b) In-degree centrality: top patents by incoming CITES count
MATCH (p:Patent)<-[:CITES]-()
RETURN p.id AS patent, count(*) AS inDegree
ORDER BY inDegree DESC
LIMIT 10;

// 4.3(c) Shortest path between two distant patent nodes (not directly connected;
// true shortest path is 3 hops via two intermediate patents)
MATCH path = shortestPath((a:Patent {id: "1884442"})-[:CITES*]-(b:Patent {id: "3858801"}))
RETURN [n IN nodes(path) | n.id] AS pathNodes, length(path) AS hops;
