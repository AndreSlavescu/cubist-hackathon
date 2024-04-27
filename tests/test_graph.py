import unittest
from unittest.mock import patch, MagicMock
from rebalancing_algo.graph import Graph
from pipeline.pipeline import DatasetLoader

from collections import defaultdict

class TestGraph(unittest.TestCase):
    def setUp(self):
        self.loader = DatasetLoader("test_data/citibike_data_march_2024.csv")
        mock_data = {
            "data": {
                "stations": [
                    {"station_id": "1", "lat": 40.7486, "lon": -73.9864, "name": "Station1"},
                    {"station_id": "2", "lat": 40.7496, "lon": -73.9874, "name": "Station2"},
                    {"station_id": "3", "lat": 40.7506, "lon": -73.9884, "name": "Station3"},
                    {"station_id": "4", "lat": 40.7516, "lon": -73.9894, "name": "Station4"}
                ]
            }
        }
        self.graph = Graph(top_k=3)
        self.graph._fill_nodes(mock_data)

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
        self.assertEqual(self.graph.k, 3)

    def test_get_top_k_neighbors(self):
        self.graph.top_k_neighbors[(40.7486, -73.9864, "Station1")] = ["Station2", "Station3", "Station4"]
        neighbors = self.graph.get_top_k_neighbors((40.7486, -73.9864, "Station1"))
        self.assertIn("Station2", neighbors)
        self.assertIn("Station3", neighbors)
        self.assertIn("Station4", neighbors)

if __name__ == '__main__':
    unittest.main()
