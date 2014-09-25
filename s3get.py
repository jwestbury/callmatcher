import argparse
import boto
import dateutil
import redis
from os import chdir,getcwd,path
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
    if(args.redisserver):
        if(args.redispassword):
            r = redis.Redis(host="%s" % args.redisserver, password="%s" % args.redispassword)
        else:
            r = redis.Redis(host="%s" % (args.redisserver))
    else:
        r = None

    for file in files:
        if file.name.endswith(".WAV"): # call recordings are all .WAV files
            # timedate is in the below characters for a given bucket, but this might change
            # should probably update this to detect the timedate more universally
            td = parser.parse(file.name[11:28])
            if(args.hours):
                delta = (today - td).total_seconds()
            else:
                delta = (today - td).days
            print "Delta since call is %s." % delta
            if delta < downloadSince:
                # like td, the actual filename starts at 11 for a given bucket, but may start
                # at a different spot in other buckets - update to parse better!
                filename = file.name[11:] # for reference!
                # if not os.path.isfile(file.name[11:]): # does the file exist?
                #     print "Downloading %s to %s." % (file.name[11:], getcwd())
                #     file.get_contents_to_filename(file.name[11:])
                if r:
                    if not r.exists(filename): # does the key exist in redis?
                        print "Downloading %s to %s." % (filename, getcwd())
                        try:
                            file.get_contents_to_filename(filename)
                            r.set(filename,"%s" % datetime.now())
                        except:
                            print "Failed."
                    else:
                        print "Skipping %s, already downloaded at %s." % (filename, r.get(filename))
                else:
                    if not os.path.isfile(file.name[11:]): # does the file exist?
                        print "Downloading %s to %s." % (file.name[11:], getcwd())
                        file.get_contents_to_filename(file.name[11:])

    r.save()

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
    argparser.add_argument('--redisserver', type=str, required=False,
        help="""Optional Redis server location, stores data on call downloads to prevent duplicate downloads.""")
    argparser.add_argument('--redispassword', type=str, required=False,
        help="""Optional Redis server password, use in combination with --redis-server if your server requires authentication.""")

    args = argparser.parse_args()

    main(args = args)
