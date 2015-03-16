#!/usr/bin/python
import os
import sys
import json 
import mysql.connector
import datetime
import utils

def print_table_to_file(cursor, file):
  desc = cursor.description
  with open(fname, 'w') as the_file:
    the_file.write('[\n')
    for row in cursor.fetchall():
      d = dict(zip([col[0] for col in desc], row))
      nettitude.format_result(d)
      the_file.write(json.dumps(d) + ',\n')
    the_file.write(']')

def get_table_names(cursor):
  res = []
  cursor.execute("show tables");
  for r in cursor.fetchall():
    res.append(r[0])
  return res 

if __name__ == "__main__":
  if len(sys.argv) < 4:
    print 'Usage: ' + sys.argv[0] + ' <database> <user> <outdir>'
    sys.exit(-1)
  
  outdir = sys.argv[3]
  dbname = sys.argv[1]
  dbuser = sys.argv[2]

  connection = mysql.connector.connect(user = dbuser, database = dbname)
  cursor = connection.cursor()
  files = []
  for name in get_table_names(cursor):
    cursor.execute("select * from " + name)
    fname = outdir + '/' + sys.argv[1] + '.' +  name + '.json'
    print_table_to_file(cursor, fname) 
  connection.close()

  print 'created ' + nettitude.compress(dbname, files) 
