import json
import gtfstk
import pandas as pd
from collections import defaultdict
from pathlib import Path
import datetime
import os
import sys
import subprocess
FILE_PATH = 'output/concentrate_vehicle-2020-07-23T10:00.json'
GTFS_DIRECTORY = '/Users/jzimbel/Downloads/MBTA_GTFS/'

def entity_data(entity):
    stop_id = entity['vehicle'].get('stop_id')
    trip_id = entity['vehicle']['trip']['trip_id']
    route_id = entity['vehicle']['trip'].get('route_id')
    vehicle_id = entity['vehicle']['vehicle']['id']
    return {"stop_id": stop_id, "trip_id": trip_id, "route_id": route_id, "vehicle_id": vehicle_id}

def fetch_entity_data():
    f = open(FILE_PATH, 'r')
    s = f.read()
    data = json.loads(s)
    return [entity_data(entity) for entity in data['entity']]

def group_by(data_list, key):
    groups = defaultdict(list)
    for data in data_list:
        groups[data[key]].append(data)
    return groups

def group_entities(entities):
    d = defaultdict(lambda: defaultdict(list))
    for route_id, route_entities in group_by(entities, 'route_id').items():
        for trip_id, trip_entities in group_by(route_entities, 'trip_id').items():
            # print(trip_entities)
            # meh, there can be multiple vehicles with the same trip id
            # assert len(trip_entities) == 1
            stop_id = trip_entities[0]['stop_id']
            if stop_id is not None:
                d[route_id][trip_id] = stop_id
    return d

################################################################################
# Nothing to see here
extra_tables = [
    "multi_route_trips",
    "pathways",
    "facilities",
    "facilities_properties",
    "directions",
    "route_patterns",
    "levels",
    "lines",
    "calendar_attributes",
    "ignored_alerts",
    "checkpoints",
]
DTYPE = {
    "shape_id": str,
    "elevator_id": str,
    "sub_code": str,
    "added_route_id": str,
    "modified_route_id": str,
    "first_affected_stop_id": str,
    "last_affected_stop_id": str,
    "combo_route_id": str,
    "platform_code": str,
    "facility_code": str,
    "facility_id": str,
    "walk_escalators_traversed": str,
    "checkpoint_id": str,
    "route_pattern_id": str,
    "pathway_code": str,
    "rating_start_date": str,
    "rating_end_date": str,
    "rating_description": str,
    **gtfstk.constants.DTYPE,
}
INT_COLS = gtfstk.constants.INT_COLS[:] + [
    "trip_route_type",
    "facility_class",
    "wheelchair_facility",
    "value",
    "min_walk_time",
    "min_wheelchair_time",
    "suggested_buffer_time",
    "wheelchair_transfer",
    "new_sort_order",
    "traversal_time",
    "wheelchair_traversal_time",
    "route_pattern_sort_order",
    "route_pattern_typicality",
    "listed_route",
    "service_schedule_typicality",
    "stair_count",
    "continuous_pickup",
    "continuous_drop_off",
    "vehicle_type",
]
gtfstk.constants.DTYPE = DTYPE
gtfstk.constants.INT_COLS = INT_COLS

def read_gtfs(directory, dist_units="m", **kwargs):
    """
    Read a gtfstk.Feed from a directory.
    Raises NotADirectoryError if if the passed in path is not a directory.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise NotADirectoryError(directory)
    feed = gtfstk.feed.read_gtfs(directory, dist_units=dist_units, **kwargs)
    for table in extra_tables:
        df = None
        path = directory / f"{table}.txt"
        if path.is_file():
            df = read_csv(path)
        setattr(feed, table, df)
    return feed

def read_csv(path):
    return pd.read_csv(path, dtype=DTYPE, low_memory=False, memory_map=True)

################################################################################

def get_target_stop_ids(route_id, direction_id, stop_id, date):
    # TODO look up the date in gtfs archive and fetch the right file
    # NOTE: not worrying about services for now. (i.e. we'll include route
    # patterns that don't actually happen on the given date).
    gtfs = read_gtfs(GTFS_DIRECTORY)
    representative_trips = gtfs.route_patterns.loc[gtfs.route_patterns.route_id.eq(route_id) & gtfs.route_patterns.direction_id.eq(direction_id), "representative_trip_id"]
    pre_target_stop_ids = set()
    post_target_stop_ids = set()
    for trip_id in representative_trips:
        stop_ids = list(gtfs.stop_times.loc[gtfs.stop_times.trip_id.eq(trip_id), "stop_id"])
        if stop_id in stop_ids:
            target_index = stop_ids.index(stop_id)
            pre_target_stop_ids = pre_target_stop_ids | set(stop_ids[:target_index + 1])
            post_target_stop_ids = post_target_stop_ids | set(stop_ids[target_index + 1:])
    return (pre_target_stop_ids, post_target_stop_ids)

def fetch_vehicle_file(timestamp):
    file_path = f'output/concentrate_vehicle-{timestamp}.json'
    if os.path.isfile(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        cmd = f"S3_BUCKET_NAME=mbta-gtfs-s3 pipenv run getArchive --datetime {timestamp} --feed concentrate_vehicle"
        subprocess.run(cmd, shell=True)
        assert os.path.isfile(file_path)
        with open(file_path, "r") as f:
            return json.load(f)

def match_entities(vehicle_json, route_id, target_stop_ids, vehicle_ids_after_target_stop, is_last_stop):
    for entity in vehicle_json['entity']:
        vehicle = entity['vehicle']
        entity_route_id = vehicle['trip']['route_id']
        entity_stop_id = vehicle.get('stop_id', None)
        vehicle_id = vehicle['vehicle']['id']
        if entity_route_id == route_id and entity_stop_id in target_stop_ids and (is_last_stop or vehicle_id in vehicle_ids_after_target_stop):
            trip_id = vehicle['trip']['trip_id']
            return (trip_id, vehicle_id)

    return False

def vehicle_ids_at_stops(vehicle_json, stop_ids):
    ids = set()
    for entity in vehicle_json['entity']:
        vehicle = entity['vehicle']
        stop_id = vehicle.get('stop_id', None)
        vehicle_id = vehicle['vehicle']['id']
        if stop_id in stop_ids:
            ids.add(vehicle_id)
    return ids

def str_to_datetime(s):
  return datetime.datetime(*(int(part) for part in s.split('-')))

def find_last_trip(route_id, direction_id, stop_id, date):
    date = str_to_datetime(date)
    pre_target_stop_ids, post_target_stop_ids = get_target_stop_ids(route_id, direction_id, stop_id, date)
    is_last_stop = len(post_target_stop_ids) == 0
    vehicle_ids_after_target_stop_id = set()
    current_time_minutes = 1619
    while current_time_minutes > 0:
        td = datetime.timedelta(minutes=current_time_minutes)
        target_datetime = date + td
        timestamp = target_datetime.strftime('%Y-%m-%dT%H:%M')
        vehicle_json = fetch_vehicle_file(timestamp)
        result = match_entities(vehicle_json, route_id, pre_target_stop_ids, vehicle_ids_after_target_stop_id, is_last_stop)
        if result:
          return (*result, timestamp)

        vehicle_ids_after_target_stop_id |= vehicle_ids_at_stops(vehicle_json, post_target_stop_ids)
        current_time_minutes -= 1
    print('No last trip found 🤔')
    print(f'Are you sure route {route_id} services stop {stop_id}?')

if __name__ == '__main__':
  args = sys.argv[1:]
  args[1] = int(args[1])
  find_last_trip(*args)
