import logging
import json

import azure.functions as func


def main(event: func.EventHubEvent):
    logging.info('Python EventHub trigger processed an event: %s',
                 event.get_body().decode('utf-8'))

    records = []
    device = event.iothub_metadata['connection-device-id']
    event = json.loads(event.get_body().decode('utf-8'))
    iot_datetime = event.pop('timestamp')
    record = [{"iot_datetime": iot_datetime, "device": device, "category": key, "value": value} for key, value in event.items()]
    records.extend(record)

    return json.dumps(records)