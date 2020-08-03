import os
import sys
import json
import subprocess
from datetime import datetime, timedelta

DATE_FORMAT_STRING = '%Y-%m-%dT%H:%M'

def fetch_prediction_file(timestamp):
    file_path = f'output/concentrate-{timestamp}.json'
    if os.path.isfile(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        cmd = f"S3_BUCKET_NAME=mbta-gtfs-s3 pipenv run getArchive --datetime {timestamp} --feed concentrate"
        subprocess.run(cmd, shell=True)
        assert os.path.isfile(file_path)
        with open(file_path, "r") as f:
            return json.load(f)

def check_timestamp(trip_id, stop_id, timestamp):
    prediction_data = fetch_prediction_file(timestamp)
    matching_entities = [e for e in prediction_data['entity'] if e['trip_update']['trip']['trip_id'] == trip_id]
    assert len(matching_entities) == 1
    stop_predictions = matching_entities[0]['trip_update']['stop_time_update']
    matching_predictions = [p for p in stop_predictions if p['stop_id'] == stop_id]
    assert len(matching_predictions) == 1
    return matching_predictions[0]

def score_timestamp(scheduled_trip_id, actual_trip_id, actual_vehicle_id, stop_id, timestamp):
    prediction_data = fetch_prediction_file(timestamp)
    matching_predictions = []
    for e in prediction_data['entity']:
        if e['trip_update']['trip']['trip_id'] == scheduled_trip_id:
            stus = e['trip_update']['stop_time_update']
            stop_ids = set(stu['stop_id'] for stu in stus)
            if stop_id in stop_ids:
                matching_predictions.append(e)
    if len(matching_predictions) == 0:
        return "FALSE_NEGATIVE"

    assert len(matching_predictions) == 1

    predicted_vehicle_id = matching_predictions[0]['trip_update']['vehicle']['id']

    if scheduled_trip_id == actual_trip_id and predicted_vehicle_id == actual_vehicle_id:
        return "ACCURATE"
    else:
        return "FALSE_POSITIVE"


if __name__ == '__main__':
    args = sys.argv[1:]
    main(*args)
