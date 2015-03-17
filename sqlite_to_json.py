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
  if len(sys.argv) < 2:
    print 'Usage: ' + sys.argv[0] + ' <path_to_sqlite_db_file>'
    sys.exit(-1)

  connection = sqlite3.connect(sys.argv[1])
  connection.row_factory = dict_factory
  cursor = connection.cursor()
  dbname = os.path.basename(sys.argv[1]);

  tmpDir = utils.createTmpDir(dbname)

  if dbname.endswith('.sqlite'):
    dbname = dbname[:-7]
  for name in get_table_names(cursor):
    cursor.execute("select * from " + name)
    fname = dbname + '.' +  name + '.json'
    res = []
    for r in cursor.fetchall():
      if len(r) > 0:
        utils.format_result(r)
        res.append(r)
    if len(res) > 0:
      with open(tmpDir + '/' + fname, 'w') as the_file:
        the_file.write(json.dumps(res))

  connection.close()

  print 'created ' + utils.compress(dbname, tmpDir) 
  utils.removeTmpDir(tmpDir)
