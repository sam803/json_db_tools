import datetime
import tarfile
import os
import socket
import uuid
import shutil

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


def compress(name, dirpath, mode='bz2'):
  tarname = socket.gethostname() + "." + name + ".tar." + mode
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

def format_result(d):
  for key in d:
      val = d[key]
      if type(val) == datetime.datetime:
        d[key] = str(val)
  d['source'] = socket.gethostname() 

 
