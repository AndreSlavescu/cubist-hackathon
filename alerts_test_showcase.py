import polars as pl
from geopy.geocoders import Nominatim
import csp
from time import sleep
import os
import openai
from dotenv import load_dotenv

from pipeline.pipeline import DatasetLoader

dl = DatasetLoader()
json = dl.get_mta_alarms()

all_text = ""
counter = 0
for alert in json:
    all_text += alert.alert.header_text.translation[0].text
    # break; 

load_dotenv()
print("Starting")
client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
# q =  "Southbound BxM1 stops on Lexington Ave at E 96th St and E 86th St will be closed"
response = client.chat.completions.create(
messages=[{
        "role": "system",
        "content": "Give just all addresses as a comma separated list based off of the user message.  Give just the location if additional information is missing do not add. Make sure to return a comma  separated list of addresses. Make sure to return the address in the same language as the user message."},
    {"role": "user", "content": all_text}],
model="gpt-3.5-turbo",
)

# print(q)
expl = response.choices[0].message.content
addresses = expl.split(",")
aspiringdf=[]
# print(expl)
# print(addresses)
geolocator = Nominatim(user_agent="DEF")
for address in addresses:
    location = None
    try:
        location = geolocator.geocode(address+", NY")
    except Exception as e:
        location = None
        sleep(.1)

    if location is not None:
        print(location.address, location.latitude, location.longitude)
        aspiringdf.append((location.latitude, location.longitude, address))
    else: 
        print("No location found")

df = pl.DataFrame(aspiringdf, schema=["lat", "lon", "address"])
print(df.head())



# location = None
# geolocator = Nominatim(user_agent="abc")
# for i in range(3):
#     try:
#         location = geolocator.geocode(expl)
#     except Exception as e:
#         location = None
#         continue
#     break

# print(location.address)
# print(location.latitude, location.longitude)

# def get_mta_alarms(self): 
#     url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fall-alerts"
#     # Make a GET request to the MTA endpoint
#     response = requests.get(url)

#     # Check if the request was successful
#     if response.status_code == 200:
#         # Initialize a FeedMessage
#         feed = gtfs_realtime_pb2.FeedMessage()
#         feed.ParseFromString(response.content)
        
#         return feed.entity
#     else:
#         print(f"Failed to retrieve data: {response.status_code}")