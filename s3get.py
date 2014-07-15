import argparse
import boto
import dateutil
from datetime import datetime
from boto.s3.connection import OrdinaryCallingFormat
from dateutil import parser

def main(downloadSince = 8, args = None):
	if(args.days):
		print "Setting downloadSince to %s" % args.days
		downloadSince = args.days
	today = datetime.now()
	s3 = boto.connect_s3(calling_format=OrdinaryCallingFormat())
	bucket = s3.get_bucket(args.bucket)
	files = bucket.list()

	for file in files:
		if file.name.endswith(".WAV"):
			td = parser.parse(file.name[11:28])
			delta = (today - td).days
			if delta < downloadSince:
				print "Downloading %s." % file.name[11:]
				file.get_contents_to_filename(file.name[11:])

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="""Pulls call data from
        SQL Server call log database, and matches to recorded calls.""")

    argparser.add_argument('-d', '--days', type=int, required=False,
        help="""Download all calls less than this many days old (default: 8).""")
    argparser.add_argument('-b', '--bucket', type=str, require=True,
    	help="""The bucket you wish to download WAV files from.""")

    args = argparser.parse_args()

    main(args = args)