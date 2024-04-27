from collections import defaultdict
from heapq import heappushpop, heappush

import polars as pl

class Graph:
    """
    Graph for maintaining relationships between nodes (bike rack locations) and finding optimal transfer to new cluster
    """
    def __init__(self, top_k: int) -> None:
        self._station_name_map = defaultdict(lambda: None)

        # nodes[station_id] = (num_bikes, station_lat, station_lon, name) 
        self.nodes = defaultdict(lambda: (0, 0, 0, ""))

        self.k = top_k
        self.top_k_neighbors = defaultdict(list)

    def _fill_nodes(self, dataframe: pl.DataFrame) -> None:
        """
        fills nodes (stations) with fetched information from the payload

        Args:
            payload: dict, a json format representing the fetched data
        """
        try: 
            for row in dataframe.to_dicts():
                station_id = row['station_id']
                num_bikes = row['capacity']
                lat = row['lat']
                lon = row['lon']
                name = row['name']
                self.nodes[station_id] = (num_bikes, lat, lon, name)
        except Exception as e:
            raise ValueError(f"Error processing dataframe: {e}") 
        
    def get_nodes(self) -> dict[tuple[float, float, str]]:
        """
        Retrieve all nodes with their details.

        Returns:
            dict[tuple[float, float, str]]: A dictionary where the key is the station_id and the value is a tuple containing latitude, longitude, and name of the station.
        """
        return self.nodes

    def get_top_k_neighbors(self, target_node_id: int) -> list:
        """
        Args:
            target_node: tuple[float, float, str], target node to fetch top-k neighbors for
        Returns:
            top-k neighbors for a given input node
        """
        return self.top_k_neighbors[target_node_id]
    
    def set_top_k_neighbors(self, target_node_id: int) -> None:
        """
        Calculate and set the top k closest neighbors for a given node.

        Args:
            target_node_id: The identifier of the target node for which to find the top k neighbors.
        """
        distances_heap = [] 
        target_lat, target_lon = map(float, self.nodes[target_node_id][1:3])
        for node_id in self.nodes:
            if node_id == target_node_id:
                continue
            node_lat, node_lon = map(float, self.nodes[node_id][1:3])
            distance = abs(target_lat - node_lat) + abs(target_lon - node_lon)
            if len(distances_heap) < self.k:
                heappush(distances_heap, (-distance, node_id))
            else:
                heappushpop(distances_heap, (-distance, node_id))

        self.top_k_neighbors[target_node_id] = sorted((-dist, id) for dist, id in distances_heap)
    
    def get_top_k_distances(self) -> list:
        """
        Return the Manhattan distances between all nodes and return the top k closest nodes.

        Args:
            target_node_id: str, the identifier of the target node

        Returns:
            list of tuples: Each tuple contains the distance and the node identifier of the top k closest nodes.
        """

        return [self.get_top_k_neighbors(target_node_id) for target_node_id in self.top_k_neighbors]
    
    def set_top_k_distances(self) -> None:
        """
        Compute and set the top k distances for all nodes in the graph.
        """
        for target_node_id in self.nodes:
            self.set_top_k_neighbors(target_node_id)
            
    def _transfer_objects(self, start_node_id: int, target_node_id: int, num_bikes: int) -> None:
        """
        Moves a specified number of objects from the start node to the target node. This operation decreases the object count at the start node and increases it at the target node by the same amount.

        Args:
            start_node_id: int, the identifier of the node from which objects are moved
            target_node_id: int, the identifier of the node to which objects are moved
            num_bikes: int, number of bikes to transfer
        """
        start_bikes, start_lat, start_lon, start_name = self.nodes[start_node_id]
        target_bikes, target_lat, target_lon, target_name = self.nodes[target_node_id]
        
        updated_start_bikes = start_bikes - num_bikes
        updated_target_bikes = target_bikes + num_bikes

        self.nodes[start_node_id] = (updated_start_bikes, start_lat, start_lon, start_name)
        self.nodes[target_node_id] = (updated_target_bikes, target_lat, target_lon, target_name)

    def rebalance_stations(self, min_bikes: int, max_bikes: int):
        """
        Rebalance the bike stations to ensure all stations have bikes within the specified range.

        This method identifies stations that are understocked (below min_bikes) and overstocked (above max_bikes).
        It then attempts to rebalance the bikes by transferring bikes from overstocked to understocked stations,
        prioritizing transfers from the closest overstocked neighbors.

        Args:
            min_bikes: int, the minimum number of bikes that each station should have.
            max_bikes: int, the maximum number of bikes that each station should have.
        """
        understocked = {id: node for id, node in self.nodes.items() if node[0] < min_bikes}
        overstocked = {id: node for id, node in self.nodes.items() if node[0] > max_bikes}

        for under_id, under_node in understocked.items():
            self.set_top_k_neighbors(under_id)
            neighbors = self.top_k_neighbors[under_id]
            for _, over_id in neighbors:
                if over_id in overstocked:
                    needed = min_bikes - under_node[0]
                    available = overstocked[over_id][0] - max_bikes
                    transfer_amount = min(needed, available)

                    if transfer_amount > 0:
                        self._transfer_objects(over_id, under_id, transfer_amount)
                        under_node = (under_node[0] + transfer_amount, under_node[1], under_node[2], under_node[3])
                        overstocked[over_id] = (overstocked[over_id][0] - transfer_amount, overstocked[over_id][1], overstocked[over_id][2], overstocked[over_id][3])
                    if under_node[0] >= min_bikes:
                        break
