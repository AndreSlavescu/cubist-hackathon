import polars as pl
import httpx

from typing import List, Optional
from functools import lru_cache
import logging

CITIBIKE_STATION_INFORMATION = (
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
)
CITIBIKE_STATION_STATUS = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"
CITIBIKE_VEHICLE_TYPE = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/vehicle_types.json"

class DatasetLoader:
    def __init__(self, file_path: str = ""):
        """
        Initialize the DatasetLoader with a path to the dataset.

        Args:
            file_path: Path to the dataset file (can be a local path or a URL)
        """
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.file_path = file_path
        self.dataframe = None
        self.stream = None

    def load_data(self, batch_size: int = 1024):
        """
        Load data from the file_path using Polars.
        """
        try:
            self.stream = pl.scan_csv(self.file_path, batch_size=batch_size)
            self.logger.info("Data loaded successfully.")
        except Exception as e:
            self.logger.error(f"Failed to load data: {e}")

    def load_data_json(self, data: str):
        """
        Load data from the file_path using Polars.
        """
        try:
            self.stream = pl.read_json(data)
            self.logger.info("Data loaded successfully.")
        except Exception as e:
            self.logger.error(f"Failed to load data: {e}")

    def finalize_data(self):
        """
        Finalize the data streaming and collect the DataFrame.
        """
        if self.stream is not None:
            try:
                self.dataframe = self.stream.collect()
                self.logger.info("Data loaded successfully.")
            except Exception as e:
                self.logger.error(f"Failed to load data: {e}")

    def check_fields(self, required_fields: List[str]) -> bool:
        """
        Check if the required fields exist in the dataframe.

        Args:
            required_fields: A list of fields that are expected in the dataframe

        Returns: 
            True if all fields exist, False otherwise
        """
        if self.dataframe is None:
            print("Data not loaded. Call finalize_data() first.")
            return False
        
        existing_fields = self.dataframe.columns
        missing_fields = [field for field in required_fields if field not in existing_fields]

        if missing_fields:
            print(f"Missing fields: {missing_fields}")
            return False
        else:
            print("All required fields are present.")
            return True

    def get_summary(self) -> Optional[pl.DataFrame]:
        """
        Generate a summary of the dataframe, including basic statistics for numeric columns.

        Returns: 
            A DataFrame with the summary statistics, or None if the data is not loaded.
        """
        if self.dataframe is None:
            print("Data not loaded. Call finalize_data() first.")
            return None
        
        return self.dataframe.describe()
    
    @lru_cache
    def get_stations(self):
        dat = httpx.get(CITIBIKE_STATION_INFORMATION).json()
        return pl.DataFrame(dat['data']['stations'], schema=["station_id", "capacity", "name", "short_name", "region_id", "lon", "lat"])


    @lru_cache
    def get_vehicle_types(self):
        dat = httpx.get(CITIBIKE_VEHICLE_TYPE).json()
        return pl.DataFrame(dat['data']['vehicle_types'], schema=["vehicle_type_id", "form_factor", "propulsion_type", "max_range_meters"])


    def get_station_status(self):
        records = httpx.get(CITIBIKE_STATION_STATUS).json()["data"]["stations"]
        status = pl.DataFrame(records, schema=["station_id", "num_bikes_available", "num_bikes_disabled", "num_docks_available", "num_docks_disabled", "num_ebikes_available", "is_installed", "is_renting", "is_returning", "last_reported"])

        stations = self.get_stations()
        return stations.join(status, on="station_id", how="inner")
