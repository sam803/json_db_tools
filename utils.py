import datetime
import tarfile
import os
import socket
import uuid
import shutil
import md5
import json
from bson import json_util

def createTmpDir(prefix=''):
  dirname = '/tmp/' + prefix + str(uuid.uuid4())
  os.mkdir(dirname)
  return dirname

def removeTmpDir(tmp):
  shutil.rmtree(tmp)

def parseFileName(path):
  fname = os.path.basename(path)
  arr = fname.split('.')
  return arr[0],arr[1]


def compress(name, dirpath, outdir, mode='bz2'):
  tarname = os.path.join(outdir, socket.gethostname() + "." + name + ".tar." + mode)
  tar = tarfile.open(tarname, "w:" + mode)
  for d,dn,fn in os.walk(dirpath):
    for f in fn:
      path = os.path.join(d, f)
      tar.add(name=path, arcname=f)
  tar.close()
  return tarname

def uncompress(path, mode='bz2'):
  tmpDir = createTmpDir()
  tf = tarfile.open(mode="r:" + mode, fileobj = file(path)) 
  tf.extractall(tmpDir)
  tf.close()
  return tmpDir

def json_dumps(d):
  return json.dumps(d, default=json_util.default)

def json_loads(s):
  return json.loads(s,  object_hook=json_util.object_hook)

def format_result(d):
  m = md5.new()
  keys = d.keys()
  keys.sort()
  for key in keys:
    val = d[key]
    m.update(str(val)) 

  d['_id'] = m.hexdigest()
  d['source'] = socket.gethostname() 

 
