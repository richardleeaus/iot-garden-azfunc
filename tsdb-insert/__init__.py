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
                INSERT INTO sensor_data (timestamp, category, value)
                VALUES %s
                """,
                records,
            )
            self.conn.commit()


def main(event: func.EventHubEvent):
    historian = TimescaleDB()
    logging.info('Python EventHub trigger processed')
    logging.info("object type:\t{}".format(type(event)))
    logging.info("Data:\t{}".format(event.get_body().decode('utf-8')))

    records = []
    event = json.loads(event.get_body().decode('utf-8'))
    iot_datetime = event.pop('timestamp')
    record = [(iot_datetime, key, value) for key, value in event.items()]
    records.extend(record)

    # Python doesn't seem to be working with cardinality 'many'
    # records = []
    # events = [eventrecord.get_body().decode('utf-8') for eventrecord in event]
    # for event in events:
    #     event = json.loads(event)
    #     iot_datetime = event.pop('timestamp')
    #     record = [(iot_datetime, key, value) for key, value in dict(event).items()]
    #     records.extend(record)

    historian.insert_sensor_records(records)
