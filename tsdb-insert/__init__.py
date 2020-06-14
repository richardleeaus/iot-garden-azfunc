import logging
import psycopg2
import os
import json
import azure.functions as func
from psycopg2.extras import execute_values


class TimescaleDB(object):
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.environ.get("tsdb_dbname", "postgres"),
            user=os.environ.get("tsdb_user"),
            password=os.environ.get("tsdb_password"),
            host=os.environ.get("tsdb_host")
        )

    def insert_sensor_records(self, records):
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO sensor_data (timestamp, device, category, value, anomaly_score, is_anomaly)
                VALUES %s
                """,
                records,
            )
            self.conn.commit()
            logging.info('Inserted into TimescaleDB')


def main(event: func.EventHubEvent):
    historian = TimescaleDB()
    logging.info('Python EventHub trigger processed')
    logging.info("object type:\t{}".format(type(event)))

    events = event.get_body().decode('utf-8').splitlines()
    to_send = []
    for eventstring in events:
        event = json.loads(eventstring)
        record = []
        for key, value in event.items():
            record.append(value)
        to_send.add(tuple(record))
    historian.insert_sensor_records(to_send)

