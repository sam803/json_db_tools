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

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print 'Usage: ' + sys.argv[0] + ' <path_to_file>'
    sys.exit(-1)

  fpath = sys.argv[1]
  source,dbname = utils.parseFileName(fpath)
  
  client = pymongo.MongoClient('localhost')
  tmpDir = utils.uncompress(fpath)
  for f in os.listdir(tmpDir):
    with open(os.path.join(tmpDir, f), 'r') as the_file:
      count = 0
      d,c = utils.parseFileName(f)
      for line in the_file.readlines():
        line = line.strip()
        if len(line) > 0:
          try:
            toinsert = utils.json_loads(line) 
            if len(toinsert) > 0:
              client[d][c].insert(toinsert)
              count = count+1
          except ValueError:
            print 'could not decode JSON object from ' + f
          except pymongo.errors.DuplicateKeyError:
   	    #already exists
   	    pass
      if count > 0:
        print 'created ' + str(count) + ' records in ' + d + '.' + c
  utils.removeTmpDir(tmpDir) 
  
   
