import requests
from urllib.parse import quote_plus

class LineageService:
    def __init__(self, lineage_url, namespace):
        self.api_url = lineage_url
        self.namespace = namespace

    def get_upstream_and_downstream_tables(self, schema, table):
        urn = quote_plus(f"dataset:{self.namespace}:{schema}.{table}")
        try:
            resp = requests.get(f"{self.api_url}?nodeId={urn}&depth=10")
            if resp.status_code != 200: 
                return [], []
            
            graph_data = resp.json().get('graph', [])
            return self._parse_directions(graph_data, urn)
        except Exception:
            return [], []

    def _parse_directions(self, graph, center_urn):
        nodes = {n['id']: n for n in graph}
        
        def traverse(edge_type):
            visited, queue, results = set(), [center_urn], set()
            while queue:
                curr = queue.pop(0)
                if curr in visited or curr not in nodes: continue
                visited.add(curr)
                
                node = nodes[curr]
                if node.get('type') == 'DATASET' and curr != center_urn:
                    results.add(self._extract_name(node))
                
                for edge in node.get(edge_type, []):
                    queue.append(edge.get('origin') or edge.get('destination'))
            return list(results)

        return traverse('inEdges'), traverse('outEdges')

    def _extract_name(self, node):
        name_parts = node['data']['name'].split('.')
        schema = name_parts[-2] if len(name_parts) >= 2 else 'public'
        table = name_parts[-1]
        return (schema, table)