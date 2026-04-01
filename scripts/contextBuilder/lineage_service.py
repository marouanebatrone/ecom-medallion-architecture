import requests
from urllib.parse import quote_plus

class LineageService:
    def __init__(self, lineage_url, namespace):
        self.api_url = lineage_url
        self.namespace = namespace

    def get_rich_lineage_context(self, schema, table):
        dataset_name = f"{schema}.{table}"
        urn = f"dataset:{self.namespace}:{dataset_name}"
        
        up, down = self.get_upstream_and_downstream_tables(schema, table)
        
        column_lineage = self.get_column_lineage(urn)
        
        facets = self.get_dataset_facets(schema, table)
        
        return {
            "upstreams": up,
            "downstreams": down,
            "column_level": column_lineage,
            "facets": facets
        }

    def get_upstream_and_downstream_tables(self, schema, table):
        urn = quote_plus(f"dataset:{self.namespace}:{schema}.{table}")
        try:
            resp = requests.get(f"{self.api_url}?nodeId={urn}&depth=5")
            if resp.status_code != 200: return [], []
            
            graph_data = resp.json().get('graph', [])
            return self._parse_directions(graph_data, f"dataset:{self.namespace}:{schema}.{table}")
        except Exception:
            return [], []

    def get_column_lineage(self, urn):
        """Fetches how columns in this table relate to source tables."""
        encoded_urn = quote_plus(urn)
        url = self.api_url.replace('/lineage', '/column-lineage')
        try:
            resp = requests.get(f"{url}?nodeId={encoded_urn}&depth=3")
            return resp.json().get('graph', []) if resp.status_code == 200 else []
        except: return []

    def get_dataset_facets(self, schema, table):
        """Fetches technical metadata (facets) for the specific dataset."""
        url = self.api_url.replace('/lineage', f'/facets/datasets/{quote_plus(self.namespace)}/{schema}.{table}')
        try:
            resp = requests.get(url)
            return resp.json().get('facets', {}) if resp.status_code == 200 else {}
        except: return {}

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
                    next_node = edge.get('origin') or edge.get('destination')
                    if next_node:
                        queue.append(next_node)
            return list(results)

        return traverse('inEdges'), traverse('outEdges')

    def _extract_name(self, node):
        full_name = node['data'].get('name', 'unknown.unknown')
        parts = full_name.split('.')
        return (parts[-2], parts[-1]) if len(parts) >= 2 else ('public', parts[-1])