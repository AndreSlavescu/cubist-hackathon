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

load_dotenv()
print("Starting")
client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
response = client.chat.completions.create(
messages=[{
        "role": "system",
        "content": "Give just all addresses as a comma separated list based off of the user message.  Give just the location if additional information is missing do not add. Make sure to return a comma  separated list of addresses. Make sure to return the address in the same language as the user message."},
    {"role": "user", "content": all_text}],
model="gpt-3.5-turbo",
)

expl = response.choices[0].message.content
addresses = expl.split(",")
aspiringdf=[]
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

df = pl.DataFrame(aspiringdf, schema=["lat", "lon", "address"])
print(df.head())
