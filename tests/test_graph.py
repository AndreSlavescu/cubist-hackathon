import unittest
from unittest.mock import patch
from rebalancing_algo.graph import Graph

import polars as pl

from collections import defaultdict

class TestGraph(unittest.TestCase):
    def setUp(self):
        data = [
            {"station_id": "1", "num_bikes_available": 15, "lat": 40.7486, "lon": -73.9864, "name": "Station1"},
            {"station_id": "2", "num_bikes_available": 25, "lat": 40.7496, "lon": -73.9874, "name": "Station2"},
            {"station_id": "3", "num_bikes_available": 35, "lat": 40.7506, "lon": -73.9884, "name": "Station3"},
            {"station_id": "4", "num_bikes_available": 45, "lat": 40.7516, "lon": -73.9894, "name": "Station4"}
        ]
        df = pl.DataFrame(data)
        self.graph = Graph(top_k=3)
        self.graph._fill_nodes(df)

    @patch("rebalancing_algo.graph.Graph._fill_nodes")
    def test_fill_nodes_success(self, mock_fill_nodes):
        mock_fill_nodes.return_value = None 
        self.graph._fill_nodes({})
        mock_fill_nodes.assert_called_with({})

    @patch("rebalancing_algo.graph.Graph._fill_nodes")
    def test_fill_nodes_with_empty_data(self, mock_fill_nodes):
        mock_fill_nodes.return_value = None
        self.graph._fill_nodes({"data": {"stations": []}})
        mock_fill_nodes.assert_called_with({"data": {"stations": []}})

    @patch("rebalancing_algo.graph.Graph._fill_nodes")
    def test_fill_nodes_with_incorrect_format(self, mock_fill_nodes):
        mock_fill_nodes.side_effect = Exception("Invalid Payload Format")
        with self.assertRaises(Exception):
            self.graph._fill_nodes({"incorrect": "data"})

    def test_initialization(self):
        self.assertIsInstance(self.graph.nodes, defaultdict)
        self.assertIsInstance(self.graph.top_k_neighbors, defaultdict)
        self.assertEqual(self.graph.k, 3)

    def test_set_top_k_distances(self):
        self.graph.set_top_k_distances()
        self.assertEqual(len(self.graph.top_k_neighbors['1']), 3)

    def test_get_top_k_neighbors(self):
        self.graph.top_k_neighbors[(40.7486, -73.9864, "Station1")] = ["Station2", "Station3", "Station4"]
        neighbors = self.graph.get_top_k_neighbors((40.7486, -73.9864, "Station1"))
        self.assertIn("Station2", neighbors)
        self.assertIn("Station3", neighbors)
        self.assertIn("Station4", neighbors)

    def test_set_top_k_neighbors(self):
        target_node_id = '1'
        self.graph.nodes = {
            '1': (15, 40.7486, -73.9864, "Station1"),
            '2': (25, 40.7496, -73.9874, "Station2"),
            '3': (35, 40.7506, -73.9884, "Station3")
        }
        self.graph.set_top_k_neighbors(target_node_id)
        self.graph.set_top_k_neighbors(target_node_id)
        expected_neighbors = [(0.0019999999999882334, '2'), (0.003999999999990678, '3')]

        self.assertEqual(self.graph.top_k_neighbors[target_node_id], expected_neighbors)

    def test_get_top_k_distances(self):
        self.graph.nodes = {
            '1': (15, 40.7486, -73.9864, "Station1"),
            '2': (25, 40.7496, -73.9874, "Station2"),
            '3': (35, 40.7506, -73.9884, "Station3")
        }
        self.graph.set_top_k_distances()  

        distances = self.graph.get_top_k_distances()

        expected_distances = [
            [(0.0019999999999882334, '2'), (0.003999999999990678, '3')],
            [(0.0019999999999882334, '1'), (0.0020000000000024443, '3')],
            [(0.0020000000000024443, '2'), (0.003999999999990678, '1')]
        ]

        self.assertEqual(distances, expected_distances)

if __name__ == '__main__':
    unittest.main()
