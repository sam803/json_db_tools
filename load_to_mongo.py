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
      try:
        toinsert = utils.json_loads(the_file.read()) 
        #todo create hash of record and use as _id so no duplicates can be created
        if len(toinsert) > 0:
          d,c = utils.parseFileName(f)
          client[d][c].insert(toinsert)
          print 'created ' + str(len(toinsert)) + ' records in ' + d + '.' + c
      except ValueError:
        print 'could not decode JSON object from ' + f
      except pymongo.errors.DuplicateKeyError:
   	#already exists
	pass
  utils.removeTmpDir(tmpDir) 
  
   
