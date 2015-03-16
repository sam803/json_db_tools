#!/usr/bin/python
import os
import sqlite3
import sys
import json 
import utils

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_table_names(cursor):
  res = []
  cursor.execute("select name from sqlite_master where type = 'table'");
  for r in cursor.fetchall():
    name = r['name']
    res.append(name)
  return res 

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print 'Usage: ' + sys.argv[0] + ' <path_to_sqlite_db_file> <output_directory>'
    sys.exit(-1)

  connection = sqlite3.connect(sys.argv[1])
  connection.row_factory = dict_factory
  cursor = connection.cursor()
  dbname = os.path.basename(sys.argv[1]);
  files = []
  if dbname.endswith('.sqlite'):
    dbname = dbname[:-7]
  for name in get_table_names(cursor):
    cursor.execute("select * from " + name)
    fname = sys.argv[2] + '/' + dbname + '.' +  name + '.json'
    with open(fname, 'w') as the_file:
      the_file.write('[\n')
      for r in cursor.fetchall():
        nettitude.format_result(r)
        the_file.write(json.dumps(r) + ',\n')
      the_file.write(']')
  connection.close()

  print 'created ' + nettitude.compress(dbname, files) 
