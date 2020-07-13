import re
import os
setup_file = 'setup.py'
init_file  = 'py3mproc/__init__.py'

# read setup.py
with open (setup_file, 'r' ) as f:
  content = f.read()

m = re.search(r"\s*version\s*=\s*\'(\d+)\.(\d+)\.(\d+)\'", content, flags=0)    
version     = m.group(1) + '.' + m.group(2) + '.' + m.group(3)
new_version = m.group(1) + '.' + m.group(2) + '.' + str(int(m.group(3)) + 1)
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

os.system("python setup.py sdist upload -r pypi")
#os.system('pip install py3toolbox --upgrade  --no-cache-dir')
print ('pip install py3mproc --upgrade --no-cache-dir')