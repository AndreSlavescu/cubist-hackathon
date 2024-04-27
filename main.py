import asyncio
import logging
import os.path
import threading
import pyarrow as pa
from dotenv import load_dotenv

import csp
from csp import ts
from datetime import timedelta, datetime
from pipeline.pipeline import DatasetLoader
from rebalancing_algo.graph import Graph
import polars as pl


import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from perspective import PerspectiveManager, PerspectiveStarletteHandler
from perspective import Table as PerspectiveTable
from starlette.staticfiles import StaticFiles



def make_perspective_app(manager: PerspectiveManager):
    """Code to create a Perspective webserver. This code is adapted from
    https://github.com/finos/perspective/blob/master/examples/python-starlette/server.py

    Args:
        manager (PerspectiveManager): PerspectiveManager instance (hosts the tables)

    Returns:
        app: returns the FastAPI back
    """

    def perspective_thread(manager):
        # This thread runs the perspective processing callback
        psp_loop = asyncio.new_event_loop()
        manager.set_loop_callback(psp_loop.call_soon_threadsafe)
        psp_loop.run_forever()

    thread = threading.Thread(target=perspective_thread, args=(manager,), daemon=True)
    thread.start()

    async def websocket_handler(websocket: WebSocket):
        handler = PerspectiveStarletteHandler(manager=manager, websocket=websocket)
        try:
            await handler.run()
        except Exception:
            ...

    app = FastAPI()
    app.add_api_websocket_route("/websocket", websocket_handler)
    app.mount(
        "/",
        StaticFiles(
            directory=os.path.join(
                os.path.abspath(os.path.dirname(__file__)), "static"
            ),
            html=True,
        ),
        name="static",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app

    
@csp.node
def poll_data(interval: timedelta) -> ts[pl.DataFrame]:
    with csp.alarms():
        a_poll = csp.alarm(bool)

    with csp.start():
        csp.schedule_alarm(a_poll, timedelta(), True)
        ds_loader = DatasetLoader()

    if csp.ticked(a_poll):
        status = ds_loader.get_station_status()

        csp.schedule_alarm(a_poll, interval, True)
        return status

@csp.node
def push_data_to_perspective_table(data: ts[pl.DataFrame], table: PerspectiveTable):
    if csp.ticked(data):
        # table_data = data.to_arrow()
        # stream = pa.BufferOutputStream()
        # writer = pa.RecordBatchStreamWriter(stream, table_data.schema)
        # writer.write_table(table_data)
        # writer.close()
        # table.update(stream.getvalue().to_pybytes())
        table.update(data.to_pandas().to_dict(orient="records"))


@csp.node()
def mta_alerts(table: PerspectiveTable, timedelta: timedelta = timedelta(seconds=500)):
    with csp.alarms():
        a_poll = csp.alarm(bool)

    with csp.start():
        csp.schedule_alarm(a_poll, timedelta, True)
        ds_loader = DatasetLoader()
    
    if csp.ticked(a_poll):
        # csp.print("HEY I TICKED")
        json = ds_loader.get_mta_alarms()

        all_text = ""
        for alert in json:
            all_text += alert.alert.header_text.translation[0].text

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
                print(location.latitude, location.longitude)
                aspiringdf.append((location.latitude, location.longitude, address))
            else: 
                print("No location found")

        

        df = pl.DataFrame(aspiringdf, schema=["lat", "lon", "address"])
        print(df.head())
        table.update(df.to_pandas().to_dict(orient="records"))
    

        
    # if csp.ticked(table):
    #     alerts = table.get_mta_alarms()
    #     print(alerts)
        
@csp.node
def process_data(df: ts[pl.DataFrame]) -> ts[pl.DataFrame]:
    if csp.ticked(df):
        if isinstance(df, pl.DataFrame):
            graph = Graph(top_k=8)  
            graph._fill_nodes(df)  
            graph.set_top_k_distances()
            graph.rebalance_stations(25, 40)

            modified_df = df.clone()


            # This loop iterates over each station in the graph. For each station, it updates the 'capacity' column in the DataFrame 'modified_df'.
            # It sets the 'capacity' to 'num_bikes' where the 'station_id' matches, otherwise it retains the original 'capacity'.
            for station_id, (num_bikes, _, _, _) in graph.nodes.items():
                modified_df = modified_df.with_columns(pl.when(pl.col("station_id") == station_id).then(num_bikes).otherwise(pl.col("num_bikes_available")).alias("num_bikes_available"))
            return modified_df

@csp.graph        
def main_graph(table: PerspectiveTable, mod_table: PerspectiveTable, alert_table: PerspectiveTable, interval: timedelta = timedelta(seconds=10)):
    data = poll_data(interval)
    modified_df = process_data(data)
    # mta_alerts = mta_alerts(alert_table)

    # data.vstack(modified_df)
    push_data_to_perspective_table(data, table)
    push_data_to_perspective_table(modified_df, mod_table)
    # mta_alerts = mta_alerts(alert_table)

    # csp.print("data", data)
    # csp.print("graph", modified_df)

def run_app(manager: PerspectiveManager):
    """Connect to csp to perspective and load data

    Args:
        manager (PerspectiveManager): PerspectiveManager instance (hosts the tables)
    """
    table = PerspectiveTable(
        {
            "station_id": str,
            "capacity": int,
            "name": str,
            'short_name': str,
            "region_id": str, 
            "lon": float,
            "lat": float,
            "num_bikes_available": int,
            "num_bikes_disabled": int,
            "num_docks_disabled": int,
            "num_ebikes_available": int,
            "is_installed": bool,
            "is_renting": bool,
            "is_returning": bool,
            "last_reported": datetime,
        },
        index="station_id",
    )

    table2 = PerspectiveTable(
        {
            "station_id": str,
            "capacity": int,
            "name": str,
            'short_name': str,
            "region_id": str, 
            "lon": float,
            "lat": float,
            "num_bikes_available": int,
            "num_bikes_disabled": int,
            "num_docks_disabled": int,
            "num_ebikes_available": int,
            "is_installed": bool,
            "is_renting": bool,
            "is_returning": bool,
            "last_reported": datetime,
        },
        index="station_id",
    )

    table3 = PerspectiveTable(
        {
            "lat": float,
            "lon": float,
            "address": str,
        },
        index="address",
    )

    # host these tables
    manager.host_table("data", table)
    manager.host_table("mod_data", table2)
    manager.host_table("alerts_table", table3)

    
    return csp.run_on_thread(main_graph, table, table2, table3,timedelta(seconds=60), realtime=True)


def main():
    # csp.run(main_graph, None, timedelta(seconds=60), realtime=True)
    load_dotenv()
    perspective_manager = PerspectiveManager()

    app = make_perspective_app(perspective_manager)
    run_app(perspective_manager)
    # logging.critical("Listening on http://localhost:8080")
    uvicorn.run(app, host="0.0.0.0", port=8080)


if __name__ == '__main__':
    main()
