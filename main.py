import csp

from csp import ts
from datetime import timedelta
from pipeline.pipeline import DatasetLoader
from rebalancing_algo.graph import Graph
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
    
@csp.node
def process_data(df: ts[pl.DataFrame]) -> ts[pl.DataFrame]:
    if csp.ticked(df):
        if isinstance(df, pl.DataFrame):
            graph = Graph(top_k=3)  
            graph._fill_nodes(df)  
            graph.set_top_k_distances()
            graph.rebalance_stations(25, 45)

            modified_df = df.clone()
            count = 0
            for station_id, (num_bikes, _, _, _) in graph.nodes.items():
                if count == 5:
                    break
                modified_df = modified_df.with_columns(pl.when(pl.col("station_id") == station_id).then(num_bikes).otherwise(pl.col("capacity")).alias("capacity"))
                count += 1
            return modified_df

@csp.graph
def main_graph(interval: timedelta = timedelta(seconds=10)):
    data = poll_data(interval)
    graph = process_data(data)
    csp.print("data", data)
    csp.print("graph", graph)

def main():
    csp.run(main_graph, timedelta(seconds=10), realtime=True)

if __name__ == '__main__':
    main()
