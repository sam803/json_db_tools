#!/usr/bin/python
import os
import sqlite3
import sys
import json 
import utils
import pymongo
import tarfile
import gzip
import zlib
def parse_name(path):
  fname = os.path.basename(path)
  arr = fname.split('.')
  return arr[0],arr[1]


if __name__ == "__main__":
  if len(sys.argv) < 2:
    print 'Usage: ' + sys.argv[0] + ' <path_to_file>'
    sys.exit(-1)

  fpath = sys.argv[1]
  source,dbname = parse_name(fpath)
  
  #tfile = tarfile.open(mode="r:gz", fileobj = file(fpath))
  with tarfile.open(mode="r:gz", fileobj = file(fpath)) as tf:
    for entry in tf:  # list each entry one by one
       fileobj = tf.extractfile(entry)
        print fileobj.read()
  #for member in tfile:
  #  print member
    # Print contents of every file
   # print tfile.extractfile(member).read()

  #for member in tfile:
   # name = member.name
   # print name
    #print member.tobuf()
  #tfile.close()
  #client = pymongo.MongoClient('localhost')
  #db = client[dbname]
    
