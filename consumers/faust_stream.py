"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record, validation=True, serializer="json"):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record, validation=True, serializer="json"):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App(
    "stations-stream",
    broker="kafka://localhost:9092",
    store="memory://",
)
topic = app.topic("org.chicago.cta.raw.stations", value_type=Station)
out_topic = app.topic(
    "org.chicago.cta.stations.table.v1",
    partitions=1,
    key_type=str,
    value_type=TransformedStation,
)
table = app.Table(
    name="station_transformer",
    default=None,
    partitions=1,
    changelog_topic=out_topic,
    key_type=str,
    value_type=TransformedStation,
)


@app.agent(topic)
async def station_data(stations):
    async for station in stations.group_by(Station.station_name, partitions=1):
        station_line = (
            station.red * "red" + station.blue * "blue" + station.green * "green"
        )
        if not station_line:
            continue
        table[station.station_name] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=station_line,
        )
        print(table[station.station_name])


if __name__ == "__main__":
    app.main()
