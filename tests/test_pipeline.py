import unittest
import polars as pl
from unittest.mock import patch, MagicMock

from pipeline.pipeline import DatasetLoader

class TestDatasetLoader(unittest.TestCase):
    def setUp(self):
        self.loader = DatasetLoader("test_data/Iris.csv")

    @patch("polars.scan_csv")
    def test_load_data_success(self, mock_scan_csv):
        mock_stream = MagicMock()
        mock_scan_csv.return_value = mock_stream
        self.loader.load_data(batch_size=500)
        mock_scan_csv.assert_called_with("test_data/Iris.csv", n_rows=500)
        self.assertIsNotNone(self.loader.stream)
        self.assertIs(self.loader.stream, mock_stream)

    @patch("polars.scan_csv")
    def test_finalize_data_success(self, mock_scan_csv):
        mock_stream = MagicMock()
        mock_stream.collect.return_value = pl.DataFrame({"column1": [1, 2], "column2": [3, 4]})
        mock_scan_csv.return_value = mock_stream

        self.loader.load_data(batch_size=500)
        self.loader.finalize_data()
        self.assertIsNotNone(self.loader.dataframe)
        self.assertIsInstance(self.loader.dataframe, pl.DataFrame)

    @patch("polars.scan_csv")
    def test_load_data_failure(self, mock_scan_csv):
        mock_scan_csv.side_effect = Exception("Failed to initialize data stream")
        with self.assertLogs(level='ERROR') as log:
            self.loader.load_data()
            self.assertIn("Failed to load data: Failed to initialize data stream", log.output[0])

    def test_check_fields(self):
        self.loader.dataframe = pl.DataFrame({"column1": [1, 2], "column2": [3, 4]})
        result = self.loader.check_fields(["column1", "column3"])
        self.assertFalse(result)

    def test_get_summary(self):
        self.loader.dataframe = pl.DataFrame({"column1": [1, 2, 3], "column2": [4, 5, 6]})
        summary = self.loader.get_summary()
        self.assertIsNotNone(summary)
        self.assertTrue("column1" in summary.columns)
        self.assertTrue("column2" in summary.columns)

if __name__ == "__main__":
    unittest.main()
