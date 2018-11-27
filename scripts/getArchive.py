import argparse
import boto3
import os
import pytz
import urllib2
from datetime import datetime
from sys import path

path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import gtfs_realtime_pb2

OBJECT_PREFIX_FORMAT = "concentrate/{0}/{1}/{2}/{0}-{1}-{2}T{3}:{4}"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M"
LOCAL_TIMEZONE = pytz.timezone("US/Eastern")

parser = argparse.ArgumentParser(description="Retrieve an archived GTFS-rt file from S3")
parser.add_argument("-D", "--datetime", dest="datetime", required=True, help="Datetime of desired archive file, in format {YYYY}-{MM}-{DD}T{HH}:{mm}")
parser.add_argument("-o", "--output", dest="output", required=True, help="Location for where to place the output file")
parser.add_argument("--raw", action="store_true", help="Flag that the archive file should be downloaded as raw protobuf")
args = vars(parser.parse_args())

outputfile = os.path.expanduser(args["output"])
dateTime = LOCAL_TIMEZONE.localize(datetime.strptime(args["datetime"], DATETIME_FORMAT)).astimezone(pytz.utc)
with open(outputfile, "w") as file:
    bucketName = os.getenv("S3_BUCKET_NAME")
    s3 = boto3.resource("s3")
    feed_message = gtfs_realtime_pb2.FeedMessage()
    bucket = s3.Bucket(bucketName)
    prefix = OBJECT_PREFIX_FORMAT.format(dateTime.year, dateTime.month, dateTime.day, dateTime.hour, dateTime.minute)
    objectsWithPrefix = bucket.objects.filter(Prefix=prefix)
    for obj in objectsWithPrefix:
        if "mbta_bus_" in obj.key and "trip_updates" in obj.key:
            if args["raw"]:
                print("Downloading {0}...".format(obj.key))
                bucket.download_file(obj.key, outputfile)
            else:
                pb_url = "https://s3.amazonaws.com/{0}/{1}".format(bucketName, obj.key)
                print("Processing {0}...".format(pb_url))
                response = urllib2.urlopen(pb_url)
                feed_message.ParseFromString(response.read())
                file.write(str(feed_message))
            break
    print("Done.")
