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

def createExportTable(cursor):
  cursor.execute('create table if not exists ' + utils.export_table + '(_id varchar(128) not null primary key)')


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

  #create a tracking db - could do it in the local database but sometimes it is locked
  track_connection = sqlite3.connect('trk.sqlite')
  track_cursor = track_connection.cursor()
  createExportTable(track_cursor)

  if dbname.endswith('.sqlite'):
    dbname = dbname[:-7]
  for name in get_table_names(cursor):
    if name == utils.export_table:
      continue
    cursor.execute("select * from " + name)
    fname = dbname + '.' +  name + '.json'
    with open(tmpDir + '/' + fname, 'w') as the_file:
      for r in cursor.fetchall():
        if len(r) > 0:
          utils.format_result(r)
          track_cursor.execute('''select count(*) from ''' + utils.export_table + ''' where _id = ?''', (r['_id'],))
          count = track_cursor.fetchone()[0]
          if count == 0:
            the_file.write(utils.json_dumps(r) + '\r\n')
            track_cursor.execute('''insert into ''' + utils.export_table + ' values (?)''', (r['_id'],))

  connection.close() 
  track_connection.commit()
  track_connection.close()

  print 'created ' + utils.compress(dbname, tmpDir, outDir) 
  utils.removeTmpDir(tmpDir)
