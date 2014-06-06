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

def levenshtein(seq1, seq2):
    oneago = None
    thisrow = range(1, len(seq2) + 1) + [0]
    for x in xrange(len(seq1)):
        twoago, oneago, thisrow = oneago, thisrow, [0] * len(seq2) + [x + 1]
        for y in xrange(len(seq2)):
            delcost = oneago[y] + 1
            addcost = thisrow[y - 1] + 1
            subcost = oneago[y - 1] + (seq1[x] != seq2[y])
            thisrow[y] = min(delcost, addcost, subcost)
    return thisrow[len(seq2) - 1]