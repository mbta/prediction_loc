import argparse
import boto3
import gzip
import json
import os
import pytz
import requests
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict

DATETIME_FORMAT = "%Y-%m-%dT%H:%M"
LOCAL_TIMEZONE = pytz.timezone("US/Eastern")
TIMESTAMP_FORMAT = "%Y-%m-%d %-I:%M:%S %p"
URL_FORMAT = "https://s3.amazonaws.com/{0}/{1}"
FEED_TO_KEY_MAPPING = {
    "bus": [["mbta_bus_", "trip_updates"]],
    "subway": [["rtr", "TripUpdates"]],
    "cr": [["mbta_cr_", "trip_updates"]],
    "cr_vehicle": [["mbta_cr_", "vehicle_positions"]],
    "cr_boarding": [["com_TripUpdates_enhanced"]],
    "winthrop": [["mbta_winthrop_", "trip_updates"]],
    "concentrate": [["concentrate_TripUpdates_enhanced"],
                    ["realtime_TripUpdates_enhanced"]],
    "concentrate_vehicle": [["concentrate_VehiclePositions_enhanced"],
                            ["realtime_VehiclePositions_enhanced"]],
    "alerts": [["Alerts_enhanced"]],
    "busloc": [["busloc", "TripUpdates"]],
    "busloc_vehicle": [["busloc", "VehiclePositions"]],
    "swiftly_bus_vehicle": [["goswift.ly", "mbta_bus", "vehicle_positions"]]
}

def bucket_object_prefix_format_string(args):
    OBJECT_PREFIX_FORMAT = (
        "{0}/{1:02d}/{2:02d}/{0:02d}-{1:02d}-{2:02d}T{3:02d}:{4:02d}"
    )

    if args["object_prefix"]:
        return f'{args["object_prefix"]}/{OBJECT_PREFIX_FORMAT}'
    elif not args["feed"].startswith("concentrate"):
        return f"concentrate/{OBJECT_PREFIX_FORMAT}"

def matches_filters(ent, args):
    trip = entity_trip(ent)
    if args["trip"]:
        return trip and args["trip"] == trip["trip_id"]

    if args["route"]:
        return trip and matches_route(trip["route_id"], args)

    # If we get here, there was either no route filter, OR there was a filter & it matched
    if args["stops"]:
        for stu in ent["trip_update"].get("stop_time_update", {}):
            if stu["stop_id"] in args["stops"]:
                return True
        else:
            return False
    return True


def entity_trip(ent):
    if "trip_update" in ent:
        return ent["trip_update"]["trip"]
    if "vehicle" in ent:
        return ent["vehicle"].get("trip")


def matches_route(route, args):
    if not route:
        return False
    # do exact route matching on bus so route 1 filter won't include route 111, etc.
    if args["feed"] == "bus" or args["feed"] == "concentrate":
        return args["route"] == route
    # do fuzzy matching on all other feeds
    else:
        return args["route"] in route


def unix_to_local_string(unix):
    if unix is None:
        return None
    else:
        time = pytz.utc.localize(datetime.utcfromtimestamp(unix)).astimezone(
            LOCAL_TIMEZONE
        )
        return datetime.strftime(time, TIMESTAMP_FORMAT)


def convert_timestamps(ent):
    if not ent:
        return ent
    if ent.get("trip_update"):
        if "timestamp" in ent["trip_update"].keys():
            trip_update_timestamp = unix_to_local_string(
                ent["trip_update"]["timestamp"]
            )
            ent["trip_update"]["timestamp"] = trip_update_timestamp
        for stu in ent["trip_update"].get("stop_time_update", {}):
            if "arrival" in stu.keys() and stu["arrival"] is not None:
                arr_time = unix_to_local_string(stu["arrival"]["time"])
                stu["arrival"]["time"] = arr_time
            if "departure" in stu.keys() and stu["departure"] is not None:
                dep_time = unix_to_local_string(stu["departure"]["time"])
                stu["departure"]["time"] = dep_time
    if ent.get("vehicle"):
        vehicle_timestamp = unix_to_local_string(ent["vehicle"]["timestamp"])
        ent["vehicle"]["timestamp"] = vehicle_timestamp
    if ent.get("alert"):
        alert = ent["alert"]
        alert["created_timestamp"] = unix_to_local_string(alert["created_timestamp"])
        alert["last_modified_timestamp"] = unix_to_local_string(
            alert["last_modified_timestamp"]
        )
        active_periods = [
            {
                "start": unix_to_local_string(p.get("start")),
                "end": unix_to_local_string(p.get("end")),
            }
            for p in alert["active_period"]
        ]
        alert["active_period"] = active_periods
        ent["alert"] = alert
    return ent

def parse_args():
    parser = argparse.ArgumentParser(
        description="Retrieve an archived GTFS-rt file from S3"
    )
    parser.add_argument(
        "-D",
        "--datetime",
        dest="datetime",
        required=True,
        help="Datetime of desired archive file, in ISO 8601 & 3339 formats ({YYYY}-{MM}-{DD}T{HH}:{mm}-{utc_tz_offset}?)",
    )
    parser.add_argument(
        "-o", "--output", dest="output", help="Location for where to place the output file"
    )
    parser.add_argument(
        "-s",
        "--stop",
        dest="stops",
        help="Use to only include trip_updates affecting the given stop_id(s). Multiple ids should be comma-separated",
    )
    parser.add_argument(
        "-r",
        "--route",
        dest="route",
        help="Use to only include trip_updates affecting the given route",
    )
    parser.add_argument(
        "-t", "--trip", dest="trip", help="Use to only include a specific trip_id"
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Flag that the archive file should be downloaded directly without processing.",
    )
    parser.add_argument(
        "-f",
        "--feed",
        dest="feed",
        choices=FEED_TO_KEY_MAPPING.keys(),
        default="bus",
        help='Feed to retrieve. Defaults to "bus"',
    )
    parser.add_argument(
        "--object-prefix",
        dest="object_prefix",
        help="Specify a custom prefix for the key of the object to load from S3",
    )
    return vars(parser.parse_args())

def main(args):
    dateTime = datetime.fromisoformat(args["datetime"]).astimezone(pytz.utc)

    feed_type_choices = FEED_TO_KEY_MAPPING[args["feed"]]
    if args["stops"]:
        args["stops"] = args["stops"].split(",")
    else:
        args["stops"] = []

    if not args["output"]:
        if os.path.exists("scripts/"):
            # If the script is being called from the PredictionLoc root directory:
            if not os.path.exists("output/"):
                os.mkdir("output/")
            args["output"] = "output/{0}-{1}.json".format(args["feed"], args["datetime"])
        else:
            # Assume the script is being called from the prediction-loc/scripts directory:
            if not os.path.exists("../output/"):
                os.mkdir("../output/")
            args["output"] = "../output/{0}-{1}.json".format(args["feed"], args["datetime"])

    outputfile = os.path.expanduser(args["output"])
    with open(outputfile, "w") as file:
        bucketName = os.getenv("S3_BUCKET_NAME")
        print('Using bucket "{0}"'.format(bucketName))
        s3 = boto3.resource("s3")
        feed = None
        bucket = s3.Bucket(bucketName)
        prefix = bucket_object_prefix_format_string(args).format(
            dateTime.year, dateTime.month, dateTime.day, dateTime.hour, dateTime.minute
        )
        objectsWithPrefix = bucket.objects.filter(Prefix=prefix)
        for obj in objectsWithPrefix:
            if any(
                    all(feed_type in obj.key for feed_type in feed_types)
                    for feed_types in feed_type_choices):
                if args["raw"]:
                    print("Downloading {0}...".format(obj.key))
                    bucket.download_file(obj.key, outputfile)
                else:
                    url = URL_FORMAT.format(bucketName, obj.key)
                    print("Processing {0}...".format(url))
                    response = requests.get(url)
                    if "json" in obj.key:
                        feed = response.json()
                    else:
                        feed_obj = gtfs_realtime_pb2.FeedMessage()
                        feed_obj.ParseFromString(response.content)
                        feed = protobuf_to_dict(feed_obj)
                    feed["header"]["timestamp"] = unix_to_local_string(
                        int(feed["header"]["timestamp"])
                    )
                    feed["entity"] = [
                        convert_timestamps(e)
                        for e in feed["entity"]
                        if matches_filters(e, args)
                    ]
                    file.write(json.dumps(feed, indent=2))
                break
        else:
            print('No matching file found with prefix "{0}".'.format(prefix))
        print("Done.")

if __name__ == "__main__":
    main(parse_args())
