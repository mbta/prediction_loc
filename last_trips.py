import sys

from datetime import datetime, timedelta

from actual_last_trips import find_last_trip
from scheduled_last_trips import last_scheduled_trip
from predicted_last_trips import score_timestamp

MINUTES_BEFORE = 30
DATE_FORMAT_STRING = '%Y-%m-%dT%H:%M'
RESULT_KEYS = ["ACCURATE", "FALSE_POSITIVE", "FALSE_NEGATIVE"]

def check_date(route_id, direction_id, stop_id, date):
    (scheduled_trip_id, schedule_timestamp) = last_scheduled_trip(route_id, direction_id, stop_id, date)
    # print(f"Last scheduled trip_id is {scheduled_trip_id} at {schedule_timestamp}")

    (actual_trip_id, actual_vehicle_id, actual_timestamp) = find_last_trip(route_id, direction_id, stop_id, date)
    # print(f"Actual last trip_id is {actual_trip_id}, from vehicle {actual_vehicle_id} at {actual_timestamp}")

    results = {key: 0 for key in RESULT_KEYS}
    actual_dt = datetime.strptime(actual_timestamp, DATE_FORMAT_STRING)
    for n in range(1, 1 + MINUTES_BEFORE):
        test_dt = actual_dt + timedelta(minutes=-n)
        test_timestamp = datetime.strftime(test_dt, DATE_FORMAT_STRING)
        result = score_timestamp(scheduled_trip_id, actual_trip_id, actual_vehicle_id, stop_id, test_timestamp)
        results[result] += 1

    return [
        str(results['ACCURATE']),
        str(results['FALSE_POSITIVE']),
        str(results['FALSE_NEGATIVE']),
        scheduled_trip_id,
        schedule_timestamp,
        actual_trip_id,
        actual_vehicle_id,
        actual_timestamp
    ]


def main(route_id, direction_id, stop_id):
    results = []
    for d in range(20, 27):
        date = f"2020-07-{d}"
        # print(date)
        results.append(check_date(route_id, direction_id, stop_id, date))
        # print(results)
    
    print('\n'.join(','.join(result) for result in results))

if __name__ == '__main__':
    [_, stop_id, route_id, direction_id] = sys.argv
    direction_id = int(direction_id)
    main(route_id, direction_id, stop_id)
