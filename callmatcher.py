import pyodbc
import re
import os
import argparse
import dateutil
from shutil import copyfile
from os import remove
from dateutil import parser
from operator import attrgetter

class callData:
    """This class stores call data"""
    def __init__(self, **kwds):
        self.__dict__.update(kwds)
    sourcenumber = None
    destnumber = None
    time = None
    filename = None
    callid = None

def parseRecordingData(recordingTempDirectory = None):
    """Parse call data (phone numbers, time, date) from files in specified directory.

    Returns an object with call time, phone numbers, and filename."""
    timePattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{6}") # e.g. 2014-07-14T073257
    recording_zone = dateutil.tz.gettz('America/Denver') # files on server are MT
    utc_zone = dateutil.tz.gettz('UTC')
    recordings = []

    try:
        for rootdir, dirnames, filenames in os.walk(recordingTempDirectory):
            print "Parsing data on recordings in %s." % rootdir
            for file in filenames:
                data = file.split('~')
                timedate = timePattern.findall(data[0]) # time should be the first part of our filename
                if timedate:
                 # if we found something in this format, it's pretty safe to guess it's a recording
                    td = parser.parse(timedate[0])
                    td = td.replace(tzinfo=recording_zone)
                    td = td.astimezone(tz=utc_zone) # convert MT time to UTC for easier comparisons
                    try:
                        s, d = (data[2], data[3])
                    except:
                        print "Couldn't split %s into the correct number of chunks (at least 4, split on ~), skipping." % file
                        continue
                    recordings.append(callData(sourcenumber=s, destnumber=d, filename=file, time=td))
                    recordings.sort(key = lambda x: x.time) # sort the objects in recordings[] by time
                else:
                    continue # if it didn't match our earlier format, it's not a recording
        return recordings
    except IOError:
        print "Cannot access recording directory '%s'. Directory does not exist or is inaccessible." % recordingTempDirectory
        exit(3)
    else:
        print "Unknown error accessing recording directory '%s'. Quitting." % recordingTempDirectory
        exit(4)

def matchCalls(authType = "windows", server = None, database = None, username = None, password = None,
    recordingTempDirectory = None, destinationDirectory = None, recordings = None):
    """Gets data from call log stored in SQL Server, matches to recordings, and copies files.

    Can use Windows auth or SQL Server auth."""

    if authType == 'windows':
        print "Connecting to %s with Windows auth." % server
        conn = pyodbc.connect(driver="{SQL Server}", server=server, database=database,
            trusted_connection="yes") # create our PyODBC connection with Windows auth
    elif authType == 'sql':
        if username and password:
            print "Connecting to %s with username %s and password %s." % (server, username, password)
            conn = pyodbc.connect(driver="{SQL Server}", server=server, database=database,
                uid=username, password=password) # create our PyODBC connection with user/pass
        else:
            print "When using SQL Server authentication, you must specify a username and password."
            exit(1057)
    else:
        print "Unknown error occurred when setting database authentication mode."
        exit(13816)

    for rec in recordings:
        callLog = {}
        cursor = conn.cursor()
        print "Selecting records that match '%s' and '%s'" % (rec.sourcenumber, rec.destnumber)
        # now we need to get all of the calls from our given source number to our given destination
        cursor.execute("SELECT * FROM PhoneLog WHERE CallerNumber=\'%s\' and DialedNumber=\'%s\'" % (rec.sourcenumber, rec.destnumber))

        while 1:
            row = cursor.fetchone()
            if not row:
                break
            else:
                td = parser.parse(row.Time)
                td = td.replace(tzinfo=dateutil.tz.gettz('UTC')) # tag the call log time as UTC (it's already UTC, no need to convert)
                callLog[row.CallID] = td

        try:
            # call recordings aren't matched to call data by any UID
            # we'll find the matching recording (same source/destination numbers) with the nearest timestamp to what we've got in our logs
            matched = min(callLog, key=lambda y:abs((callLog[y]-rec.time).total_seconds())) # get the least difference between logs and recording
            print "Matched call is call ID %s at %s." % (matched, rec.time)
            copyfile(recordingTempDirectory+"/"+rec.filename, destinationDirectory+"/"+matched+".WAV") # copy to the destination using 
            try:
                remove("%s/%s" % (recordingTempDirectory, rec.filename)) # once it's copied, remove it from our source dir
            except:
                print "Tried to remove %s/%s but failed." % (recordingTempDirectory, rec.filename)
        except:
            print "Couldn't find a match, moving on."
            continue

def main(args):
    recordingData = parseRecordingData(args.temp)
    if(args.authsql):
        matchCalls(authType = "sql", server = args.server, database = args.database, username = args.username,
            password = args.password, recordings=recordingData, recordingTempDirectory = args.temp,
            destinationDirectory = args.dest)
    else:
        matchCalls(server = args.server, database = args.database, username = args.username, password = args.password,
            recordings=recordingData, recordingTempDirectory = args.temp, destinationDirectory = args.dest)

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="""Pulls call data from
        SQL Server call log database, and matches to recorded calls.""")

    argparser.add_argument('-t', '--temp', type=str, required=True,
        help="""Temporary recording directory.""")
    argparser.add_argument('-d', '--dest', type=str, required=True,
        help="""Destination recording directory (after the call is matched).""")
    argparser.add_argument('-s', '--server', type=str, required=True,
        help="""SQL Server instance which stores call log data.""")
    argparser.add_argument('-db', '--database', type=str, required=True,
        help="""Name of the database which stores the call log data.""")
    argparser.add_argument('-a', '--authsql', action='store_true', required=False,
        help="""Use SQL Server authentication instead of default Windows auth. Requires -u and -p.""")
    argparser.add_argument('-u', '--username', type=str, required=False,
        help="""Username for SQL Server.""")
    argparser.add_argument('-p', '--password', type=str, required=False,
        help="""Password for SQL Server.""")

    args = argparser.parse_args()

    main(args)