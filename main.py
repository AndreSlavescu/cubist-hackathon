import csp

from csp import ts
from datetime import timedelta
from pipeline.pipeline import DatasetLoader
import polars as pl

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

@csp.graph
def main_graph(interval: timedelta = timedelta(seconds=10)):
    data = poll_data(interval)
    csp.print("data", data)


def main():
    csp.run(main_graph, timedelta(seconds=10), realtime=True)

if __name__ == '__main__':
    main()
