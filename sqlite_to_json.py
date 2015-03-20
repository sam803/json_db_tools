#!/usr/bin/python
import os
import sqlite3
import sys
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
    print 'Usage: ' + sys.argv[0] + ' <path_to_sqlite_db_file> <output_dir>'
    sys.exit(-1)

  connection = sqlite3.connect(sys.argv[1])
  connection.row_factory = dict_factory
  cursor = connection.cursor()
  dbname = os.path.basename(sys.argv[1]);
  
  outDir = sys.argv[2]
  tmpDir = utils.createTmpDir(dbname)

  if dbname.endswith('.sqlite'):
    dbname = dbname[:-7]
  for name in get_table_names(cursor):
    cursor.execute("select * from " + name)
    fname = dbname + '.' +  name + '.json'
    with open(tmpDir + '/' + fname, 'w') as the_file:
      for r in cursor.fetchall():
        if len(r) > 0:
          utils.format_result(r)
          the_file.write(utils.json_dumps(r) + '\r\n')

  connection.close()

  print 'created ' + utils.compress(dbname, tmpDir, outDir) 
  utils.removeTmpDir(tmpDir)
