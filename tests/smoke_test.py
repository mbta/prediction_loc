import os
import hashlib
from scripts.getArchive import parse_args, main
from tempfile import TemporaryDirectory


def test_smoke():
    """
    Smoke test for getArchive.py script. This test will run the script with a set of arguments
    and verify that the output file is created and has the expected checksum.
    """
    os.environ["S3_BUCKET_NAME"] = "mbta-gtfs-s3"

    with TemporaryDirectory() as tempdir:
        output_file = os.path.join(tempdir, "getArchive-smoke-test.json")
        smoke_test_args = parse_args(
            [
                "--datetime",
                "2026-06-25T04:04-04:00",
                "--feed",
                "bus",
                "--stop",
                "place-north,place-sstat",
                "--route",
                "1",
                "--output",
                output_file,
            ]
        )
        main(smoke_test_args)

        with open(output_file, "rb") as f:
            checksum = hashlib.sha256(f.read()).hexdigest()
        # assert output_file
        assert (
            checksum
            == "047970d16351c93134adf2fde47f80e5b8de4e451eb1189c0d29661f20c43aaa"
        )
