import argparse
import boto3
import os
import pytz
from datetime import datetime

OBJECT_PREFIX_FORMAT = "concentrate/{0}/{1}/{2}/{0}-{1}-{2}T{3}:{4}"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M"
LOCAL_TIMEZONE = pytz.timezone("US/Eastern")

parser = argparse.ArgumentParser(description="Retrieve an archived GTFS-rt file from S3")
parser.add_argument("-D", "--datetime", dest="datetime", required=True, help="Datetime of desired archive file, in format {YYYY}-{MM}-{DD}T{HH}:{mm}")
parser.add_argument("-o", "--output", dest="output", required=True, help="Location for where to place the output file")
parser.add_argument("-C", "--convert", action="store_const", const="convert", help="NOT IMPLEMENTED YET - Flag that the resulting file should be converted from protobuf to plaintext")
args = vars(parser.parse_args())

outputfile = os.path.expanduser(args["output"])
dateTime = LOCAL_TIMEZONE.localize(datetime.strptime(args["datetime"], DATETIME_FORMAT)).astimezone(pytz.utc)

bucketName = os.getenv("S3_BUCKET_NAME")
s3 = boto3.resource("s3")
bucket = s3.Bucket(bucketName)
prefix = OBJECT_PREFIX_FORMAT.format(dateTime.year, dateTime.month, dateTime.day, dateTime.hour, dateTime.minute)
objectsWithPrefix = bucket.objects.filter(Prefix=prefix)
for obj in objectsWithPrefix:
    if "mbta_bus_" in obj.key and "trip_updates" in obj.key:
        print("Downloading {0}".format(obj.key))
        bucket.download_file(obj.key, outputfile)
        break
print("Done.")
