"""Misc tools"""
import csv
import datetime

def write_csv_file(fname, data, *args, **kwargs):
    """writes data to a csv file"""
    outfile = open(fname, 'wb')
    mycsv = csv.writer(outfile, *args, **kwargs)
    for row in data:
        mycsv.writerow(row)
    outfile.close()
    
def timestamp_to_timedelta(timestamp):
    """converts timestamp to timedelta"""
    timedelta = (timestamp-datetime.datetime(1970, 1, 1))
    total_seconds = (timedelta.seconds + timedelta.days * 24 * 3600)
    timedelta = ((timedelta.microseconds + total_seconds * 10**6) / 10**6)*1000
    return timedelta

