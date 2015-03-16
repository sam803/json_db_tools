import datetime
import tarfile
import os
import socket
def compress(name, files):
  tarname = socket.gethostname() + "." + name + ".tar.gz"
  tar = tarfile.open(tarname, "w:gz")
  for f in files: #os.listdir(dirpath):
    f = os.path.expanduser(f)
    tar.addfile(tarfile.TarInfo(os.path.basename(f)), file(f))
  tar.close()
  return tarname

def format_result(d):
  for key in d:
      val = d[key]
      if type(val) == datetime.datetime:
        d[key] = str(val)
