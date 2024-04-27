from collections import defaultdict
from heapq import heappushpop, heappush

class Graph:
    """
    Graph for maintaining relationships between nodes (bike rack locations) and finding optimal transfer to new cluster
    """
    def __init__(self, top_k: int) -> None:
        self._station_name_map = defaultdict(lambda: None)

        # nodes[station_id] = (station_lat, station_lon, name) 
        self.nodes = defaultdict(lambda: (0, 0, ""))

        self.k = top_k
        self.top_k_neighbors = defaultdict(list)

    def _fill_nodes(self, payload: dict) -> None:
        """
        fills nodes (stations) with fetched information from the payload

        Args:
            payload: dict, a json format representing the fetched data
        """
        try: 
            for station in payload["data"]["stations"]:
                self.nodes[station["station_id"]] = (station["lat"], station["lon"], station["name"])
        except:
            raise Exception("Invalid Payload Format") 

    def get_top_k_neighbors(self, target_node: tuple(float, float, str)) -> list:
        """
        Args:
            target_node: tuple(float, float), target node to fetch top-k neighbors for
        Returns:
            top-k neighbors for a given input node
        """
        return self.top_k_neighbors[target_node]
    
    def get_distances(self, target_node_id: str) -> list:
        """
        Calculate the Manhattan distances from the target node to all other nodes and return the top k closest nodes.

        Args:
            target_node_id: str, the identifier of the target node

        Returns:
            list of tuples: Each tuple contains the distance and the node identifier of the top k closest nodes.
        """
        target_lat, target_lon = self.nodes[target_node_id][0:2]
        distances_heap = []
        for new_id in self.nodes:
            if new_id == target_node_id:
                continue
            new_lat, new_lon = self.nodes[new_id][0:2]
            distance = abs(target_lat - new_lat) + abs(target_lon - new_lon)
            if len(distances_heap) < self.k:
                heappush(distances_heap, (-distance, new_id))
            else:
                heappushpop(distances_heap, (-distance, new_id))
        return sorted((-dist, id) for dist, id in distances_heap)
            
    def transfer_objects(self, start_node: tuple(float, float, str), target_node: tuple(float, float, str)) -> None:
        """
        Transfer an object from the start node to the target node, decrementing the count at the start node and incrementing at the target node.

        Args:
            start_node: tuple(float, float, str), the starting node as a tuple containing latitude, longitude, and identifier
            target_node: tuple(float, float, str), the target node as a tuple containing latitude, longitude, and identifier
        """
        self.nodes[start_node] = max(self.nodes[start_node] - 1, 0)
        self.nodes[target_node] += 1

