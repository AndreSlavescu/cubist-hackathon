import asyncio
import logging
import os.path
import threading

import csp
from csp import ts
from datetime import timedelta, datetime
from pipeline.pipeline import DatasetLoader
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
        test = data.to_pandas()
        table.update(test)

@csp.graph
def main_graph(table: PerspectiveTable, interval: timedelta = timedelta(seconds=60)):
    data = poll_data(interval)
    push_data_to_perspective_table(data, table)

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

    # host these tables
    manager.host_table("data", table)

    
    return csp.run_on_thread(main_graph, table, timedelta(seconds=60), realtime=True)


def main():
    # csp.run(main_graph, None, timedelta(seconds=60), realtime=True)

    perspective_manager = PerspectiveManager()

    app = make_perspective_app(perspective_manager)
    run_app(perspective_manager)
    # logging.critical("Listening on http://localhost:8080")
    uvicorn.run(app, host="0.0.0.0", port=8080)

if __name__ == '__main__':
    main()
