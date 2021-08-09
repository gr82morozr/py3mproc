import os,sys
import re


module_name = "py3mproc"

def _get_credentials():
  pypirc_file = os.path.join(os.path.expanduser("~"), '.pypirc')
  with open (pypirc_file, 'r' ) as f:
    lines = f.readlines()

  lookup_cred = False
  username = None
  password = None
  for l in lines:
    if '[pypi]'     in l:  lookup_cred = True
    if '[pypitest]' in l:  lookup_cred = False
    if lookup_cred == True :
      if 'username' in l : 
        m = re.search(r"\s*username\s*=\s*(\S+)\s*", l, flags=0)
        username = m.group(1)
      if 'password' in l : 
        m = re.search(r"\s*password\s*=\s*(\S+)\s*", l, flags=0)
        password = m.group(1)

  assert username is not None, "failed to get username from pypirc file :" + pypirc_file
  assert password is not None, "failed to get password from pypirc file :" + pypirc_file

  return username, password



def _increase_version(given_version=None):
  setup_file = 'setup.py'
  init_file  = 'src/' + module_name + '/__init__.py'

  # read setup.py
  with open (setup_file, 'r' ) as f:
    content = f.read()

  m = re.search(r"\s*version\s*=\s*\'(\d+)\.(\d+)\.(\d+)\'", content, flags=0)    
  version     = m.group(1) + '.' + m.group(2) + '.' + m.group(3)
  new_version = m.group(1) + '.' + m.group(2) + '.' + str(int(m.group(3)) + 1)
  if given_version is not None:  new_version = given_version
  new_content = re.sub(version, new_version,content)

  # write setup.py
  with open (setup_file, 'w' ) as f:
      f.write(new_content)

  # read init_file
  with open (init_file, 'r' ) as f:
    content = f.read()


  m = re.search(r"\s*__version__\s*=\s*\"(\d+)\.(\d+)\.(\d+)\"", content, flags=0) 
  version     = m.group(1) + '.' + m.group(2) + '.' + m.group(3)
  new_content = re.sub(version, new_version,content)

  # write setup.py
  with open(init_file, 'w' ) as f:
      f.write(new_content)

  return new_version


def _build_package():
  sys_cmd = "python setup.py sdist"
  os.system(sys_cmd) 



def _upload_package(version, u,p) :
  dist_path = os.path.join( os.path.dirname(os.path.realpath(__file__)), 'dist' )
  dist_package = None

  for pkg in os.listdir(dist_path):
    if version in pkg:
      dist_package = pkg
      break

  
 
  sys_cmd = "twine upload --repository-url https://upload.pypi.org/legacy/   dist/" +  dist_package + " -u " + u + " -p " + p + " --verbose"
  os.system(sys_cmd) 


if __name__ == "__main__":

  print ("Make sure install setuptools, twine")
  print ("pip install setuptools twine")

  given_version = None
  if len(sys.argv)> 1:
    given_version = sys.argv[1]
    print ("builing " + given_version)
  


  u,p = _get_credentials()
  build_version = _increase_version(given_version)
  _build_package()
  _upload_package(build_version, u, p)


  print ("\n")
  print ('=================================')
  print ('pip install ' + module_name + ' --upgrade --no-cache-dir')
  print ('Don\'t forget - git add and commit.')
  print ('=================================')