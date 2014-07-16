import argparse
import boto
import dateutil
from os import chdir,getcwd
from datetime import datetime
from boto.s3.connection import OrdinaryCallingFormat
from dateutil import parser

def main(downloadSince = 8, args = None):
	chdir(args.path)
	if(args.days):
		print "Setting downloadSince to %s days" % args.days
		downloadSince = args.days
	elif (args.hours):
		print "Setting downloadSince to %s hours" % args.hours
		downloadSince = args.hours*3600
	today = datetime.now()
	s3 = boto.connect_s3(calling_format=OrdinaryCallingFormat())
	bucket = s3.get_bucket(args.bucket)
	files = bucket.list()

	for file in files:
		if file.name.endswith(".WAV"):
			td = parser.parse(file.name[11:28])
			if(args.hours):
		            delta = (today - td).total_seconds()
			else:
			    delta = (today - td).days
			print "Delta since call is %s." % delta
			if delta < downloadSince:
				print "Downloading %s to %s." % (file.name[11:], getcwd())
				file.get_contents_to_filename(file.name[11:])

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="""Pulls call data from
        SQL Server call log database, and matches to recorded calls.""")

    argparser.add_argument('-d', '--days', type=int, required=False,
        help="""Download all calls less than this many days old (default: 8), cannot be used with --hours.""")
    argparser.add_argument('--hours', type=float, required=False,
        help="""Download all calls less than this many hours old (default: days), cannot be used with -d, supports fractions.""")
    argparser.add_argument('-b', '--bucket', type=str, required=True,
    	help="""The bucket you wish to download WAV files from.""")
    argparser.add_argument('-p', '--path', type=str, required=True,
    	help="""The path files will be downloaded to.""")

    args = argparser.parse_args()

    main(args = args)
