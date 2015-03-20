#!/usr/bin/python
import os
import sys
import mysql.connector
import datetime
import utils
import ConfigParser

def print_table_to_file(cursor, fname):
  desc = cursor.description
  data = False
  with open(fname, 'a') as the_file:
    for row in cursor.fetchall():
      data = True
      d = dict(zip([col[0] for col in desc], row))
      if len(d) > 0:
        utils.format_result(d)
        the_file.write(utils.json_dumps(d) + '\r\n')
  return data

def get_table_names(cursor):
  res = []
  cursor.execute("show tables");
  for r in cursor.fetchall():
    res.append(r[0])
  return res 

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print 'Usage: ' + sys.argv[0] + ' <db_config_file> <output_dir>'
    sys.exit(-1)

  outDir = sys.argv[2]
  config = ConfigParser.ConfigParser()
  config.read(sys.argv[1])
  section_name = 'database_mysql'
  sections = config.sections()
  if section_name not in sections:
    print 'ERROR: ' + sys.argv[1] + ' must contain section called \'' + section_name + '\' with connection details'
    sys.exit(-1)

  dbconf = {} 
  opts = config.options(section_name)
  for o in opts:
    if o == 'username':
      dbconf['user'] = config.get(section_name, o).strip()
    elif o == 'port':
      v = config.get(section_name, o).strip()
      if len(v) > 0:
        dbconf[o] = int(v)
    else:
      dbconf[o] = config.get(section_name, o).strip()
 
  if 'database' not in dbconf:
    print 'ERROR: \'database\' must be defined in config file'
    sys.exit(-2)

  dbname = dbconf['database']
  connection = mysql.connector.connect(**dbconf)
  cursor = connection.cursor()
  files = []
  tmpDir = utils.createTmpDir(dbname)
  for name in get_table_names(cursor):
    offset = 0
    limit = 50000
    hasData = True
    fname = os.path.join(tmpDir, dbname + '.' +  name + '.json')
    while hasData:
      cursor.execute("select * from " + name + " limit " + str(limit) + " offset " + str(offset) )
      hasData = print_table_to_file(cursor, fname)
      offset = offset + limit
  connection.close()
  print 'created ' + utils.compress(dbname, tmpDir, outDir)
  utils.removeTmpDir(tmpDir)
