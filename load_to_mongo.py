#!/usr/bin/python
import os
import sqlite3
import sys
import utils
import pymongo
import tarfile
import gzip
import zlib
import uuid
import md5
import datetime
import re
from geoip import geolite2

ipv4_re = '^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'

def geolocateIPs(d):
  for key in d:
    val = d[key]
    if (type(val) is str or type(val) is unicode) and key != 'source' and re.match(ipv4_re, val):
      geo = geolite2.lookup(val)
      if geo != None:
        match = geo.to_dict()
        match['lat'] = match['location'][0]
        match['long'] = match['location'][1]
        del d[key]
        del match['timezone']
        del match['subdivisions']
        del match['location']
        d[key] = match


def load(fpath):
  source,dbname = utils.parseFileName(fpath)
  client = pymongo.MongoClient('localhost')
  tmpDir = utils.uncompress(fpath)
  for f in os.listdir(tmpDir):
    with open(os.path.join(tmpDir, f), 'r') as the_file:
      count = 0
      d,c = utils.parseFileName(f)
      #if c == 'auth'
      #  c = 'authz'
      for line in the_file.readlines():
        line = line.strip()
        if len(line) > 0:
          try:
            toinsert = utils.json_loads(line) 
            if len(toinsert) > 0:
              geolocateIPs(toinsert)
              client[d][c].insert(toinsert)
              count = count+1
          except ValueError:
            print 'could not decode JSON object from ' + f
          except pymongo.errors.DuplicateKeyError:
   	    #already exists
   	    pass
      if count > 0:
        print str(datetime.datetime.now()) +  ' : created ' + str(count) + ' records in ' + d + '.' + c
  utils.removeTmpDir(tmpDir) 
 

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print 'Usage: ' + sys.argv[0] + ' <path_to_file>'
    sys.exit(-1)

  load(sys.argv[1])
 
   
