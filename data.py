import requests
from google.transit import gtfs_realtime_pb2

def fetch_mta_data():
    # Endpoint URL
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr"

  
    # Make a GET request to the MTA endpoint
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Initialize a FeedMessage
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        
        # Print each TripUpdate, VehiclePosition, or Alert in the feed
        for ent in feed.entity:
            print(ent)
        # for entity in feed.entity:
        #     if entity.HasField('trip_update'):
        #         print('Trip Update:', entity.trip_update)
        #     elif entity.HasField('vehicle'):
        #         print('Vehicle:', entity.vehicle)
        #     elif entity.HasField('alert'):
        #         print('Alert:', entity.alert)
    else:
        print(f"Failed to retrieve data: {response.status_code}")


# Call the function to fetch data
fetch_mta_data()
