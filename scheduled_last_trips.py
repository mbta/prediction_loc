import sys
import gtfstk
import pandas as pd
import os

from pathlib import Path

# borrowed from gtfs creator
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



def is_service_active(feed, service, date, use_feed_dates=True):
    """Is the given service ID active on the given date?

    Borrowed from gtfstk.trips.is_trip_active.

    If use_feed_dates is True (default), then use the feed_start_date
    and feed_end_date as shortcuts for whether service is active. If
    False, only rely on the dates in the calendar.
    """
    if use_feed_dates and feed.feed_info is not None:
        if date < feed.feed_info.feed_start_date.min():
            return False
        if date > feed.feed_info.feed_end_date.max():
            return False

    # Check feed._calendar_dates_g.
    caldg = feed._calendar_dates_g
    if caldg is not None:
        if (service, date) in caldg.groups:
            et = caldg.get_group((service, date))["exception_type"].iat[0]
            if et == 1:
                return True
            else:
                # Exception type is 2
                return False
    # Check feed._calendar_i
    cali = feed._calendar_i
    if cali is not None:
        if service in cali.index:
            weekday_str = gtfstk.helpers.weekday_to_str(
                gtfstk.helpers.datestr_to_date(date).weekday()
            )
            return (
                cali.loc[[service], "start_date"].le(date)
                & cali.loc[[service], "end_date"].ge(date)
                & cali.loc[[service], weekday_str].eq(1)
            ).any()
    # If you made it here, then something went wrong
    return False


################################################################################

GTFS_DIRECTORY = os.path.expanduser('~/Downloads/MBTA_GTFS')

def services_on_date(feed, date_str):
    return set(s for s in feed.calendar.service_id if is_service_active(feed, s, date_str))

def trips_on_date(feed, date_str):
    service_ids = services_on_date(feed, date_str)
    return feed.trips.loc[feed.trips.service_id.isin(service_ids)]

def matching_trip_ids(feed, route_id, direction_id, date_str):
    all_trips = trips_on_date(feed, date_str)
    return set(all_trips.loc[all_trips.route_id.eq(route_id) & all_trips.direction_id.eq(direction_id), "trip_id"])

def find_last(feed, trip_ids, stop_id):
    matching_stop_times = feed.stop_times.loc[feed.stop_times.trip_id.isin(trip_ids) & feed.stop_times.stop_id.eq(stop_id)]
    # print("\n".join(map(str, sorted(zip(list(matching_stop_times.departure_time), list(matching_stop_times.trip_id))))))
    latest_departure_time = max(matching_stop_times.departure_time)
    latest_departure = matching_stop_times.loc[matching_stop_times.departure_time.eq(latest_departure_time)].iloc[0]
    return (latest_departure.trip_id, latest_departure.departure_time)

def last_scheduled_trip(route_id, direction_id, stop_id, date):
    feed = read_gtfs(GTFS_DIRECTORY)
    date_str = "".join(date.split("-"))
    trip_ids = matching_trip_ids(feed, route_id, direction_id, date_str)
    return find_last(feed, trip_ids, stop_id)

if __name__ == '__main__':
  args = sys.argv[1:]
  args[1] = int(args[1])
  last_scheduled_trip(*args)
