import argparse
import boto3
import json
import os
import pytz
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from urllib import request

def apply_filters(feed, args):
    entities = None
    new_feed = None
    if isinstance(feed, gtfs_realtime_pb2.FeedMessage):
        new_feed = gtfs_realtime_pb2.FeedMessage()
        new_feed.header.CopyFrom(feed.header)
        entities = feed.entity
    elif isinstance(feed, dict):
        new_feed = {"header": None, "entity": []}
        new_feed["header"] = feed["header"]
        entities = feed["entity"]
    for e in entities:
        if matches_filters(e, args):
            new_feed = add_entity(new_feed, e)
    return new_feed

def matches_filters(ent, args):
    if isinstance(ent, dict):
        if args["route"] and not matches_route(ent["trip_update"]["trip"]["route_id"], args):
            return False

        # If we get here, there was either no route filter, OR there was a filter & it matched
        if args["stop"]:
            found_stop = False
            for stu in ent["trip_update"]["stop_time_update"]:
                if stu["stop_id"] == args["stop"]:
                    found_stop = True
            if not found_stop:
                return False
        return True
    else:
        if args["route"] and not matches_route(ent.trip_update.trip.route_id, args):
            return False

        # If we get here, there was either no route filter, OR there was a filter & it matched
        if args["stop"]:
            found_stop = False
            for stu in ent.trip_update.stop_time_update:
                if stu.stop_id == args["stop"]:
                    found_stop = True
            if not found_stop:
                return False
        return True

def matches_route(route, args):
    # do exact route matching on bus so route 1 filter won't include route 111, etc.
    if args["feed"] == "bus":
        return args["route"] == route
    # do fuzzy matching on all other feeds
    else:
        return args["route"] in route

def add_entity(feed, ent):
    if isinstance(feed, gtfs_realtime_pb2.FeedMessage):
        feed.entity.extend([ent])
    elif isinstance(feed, dict):
        feed["entity"].append(ent)
    return feed

def string(feed):
    if isinstance(feed, gtfs_realtime_pb2.FeedMessage):
        return str(feed)
    elif isinstance(feed, dict):
        return json.dumps(feed)

OBJECT_PREFIX_FORMAT = "concentrate/{0}/{1:02d}/{2:02d}/{0:02d}-{1:02d}-{2:02d}T{3:02d}:{4:02d}"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M"
LOCAL_TIMEZONE = pytz.timezone("US/Eastern")
URL_FORMAT = "https://s3.amazonaws.com/{0}/{1}"
FEED_TO_KEY_MAPPING = {"bus": ("mbta_bus_", "trip_updates"), "subway": ("rtr", "TripUpdates"), "cr": ("mbta_cr_", "trip_updates")}

parser = argparse.ArgumentParser(description="Retrieve an archived GTFS-rt file from S3")
parser.add_argument("-D", "--datetime", dest="datetime", required=True, help="Datetime of desired archive file, in format {YYYY}-{MM}-{DD}T{HH}:{mm}")
parser.add_argument("-o", "--output", dest="output", required=True, help="Location for where to place the output file")
parser.add_argument("-s", "--stop", dest="stop", help="Use to only include trip_updates affecting the given stop_id")
parser.add_argument("-r", "--route", dest="route", help="Use to only include trip_updates affecting the given route")
parser.add_argument("--raw", action="store_true", help="Flag that the archive file should be downloaded as raw protobuf")
parser.add_argument("-f", "--feed", dest="feed", choices=FEED_TO_KEY_MAPPING.keys(), default="bus" help="Feed to retrieve.")
args = vars(parser.parse_args())

(feed_name, feed_type) = FEED_TO_KEY_MAPPING[args["feed"]]

outputfile = os.path.expanduser(args["output"])
dateTime = LOCAL_TIMEZONE.localize(datetime.strptime(args["datetime"], DATETIME_FORMAT)).astimezone(pytz.utc)
with open(outputfile, "w") as file:
    bucketName = os.getenv("S3_BUCKET_NAME")
    print("Using bucket \"{0}\"".format(bucketName))
    s3 = boto3.resource("s3")
    feed = None
    bucket = s3.Bucket(bucketName)
    prefix = OBJECT_PREFIX_FORMAT.format(dateTime.year, dateTime.month, dateTime.day, dateTime.hour, dateTime.minute)
    objectsWithPrefix = bucket.objects.filter(Prefix=prefix)
    for obj in objectsWithPrefix:
        if feed_name in obj.key and feed_type in obj.key:
            if args["raw"]:
                print("Downloading {0}...".format(obj.key))
                bucket.download_file(obj.key, outputfile)
            else:
                url = URL_FORMAT.format(bucketName, obj.key)
                print("Processing {0}...".format(url))
                response = request.urlopen(url)
                if "json" in obj.key:
                    feed = json.loads(response.read())
                else:
                    feed = gtfs_realtime_pb2.FeedMessage()
                    feed.ParseFromString(response.read())
                feed = apply_filters(feed, args)
                file.write(string(feed))
            break
    print("Done.")
