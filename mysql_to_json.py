#!/usr/bin/python
import os
import sys
import json 
import mysql.connector
import datetime
import utils

def print_table_to_file(cursor, fname):
  res = []
  desc = cursor.description
  for row in cursor.fetchall():
    d = dict(zip([col[0] for col in desc], row))
    if len(d) > 0:
      utils.format_result(d)
      res.append(d)
  if len(res) > 0:
    with open(fname, 'w') as the_file:
      the_file.write(json.dumps(res))

def get_table_names(cursor):
  res = []
  cursor.execute("show tables");
  for r in cursor.fetchall():
    res.append(r[0])
  return res 

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print 'Usage: ' + sys.argv[0] + ' <database> <user>'
    sys.exit(-1)
  
  dbname = sys.argv[1]
  dbuser = sys.argv[2]

  tmpDir = utils.createTmpDir(dbname)

  connection = mysql.connector.connect(user = dbuser, database = dbname)
  cursor = connection.cursor()
  files = []
  for name in get_table_names(cursor):
    cursor.execute("select * from " + name)
    fname = os.path.join(tmpDir, sys.argv[1] + '.' +  name + '.json')
    print_table_to_file(cursor, fname) 
  connection.close()
  print 'created ' + utils.compress(dbname, tmpDir) 
  utils.removeTmpDir(tmpDir)
