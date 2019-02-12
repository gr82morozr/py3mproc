import re
import os
setup_file = 'setup.py'

with open (setup_file, 'r' ) as f:
    content = f.read()
m = re.search(r"\s*version\s*=\s*\'(\d+)\.(\d+)\.(\d+)\'", content, flags=0)    
version     = m.group(1) + '.' + m.group(2) + '.' + m.group(3)
new_version = m.group(1) + '.' + m.group(2) + '.' + str(int(m.group(3)) + 1)
new_content = re.sub(version, new_version,content)
with open (setup_file, 'w' ) as f:
    f.write(new_content)

os.system("python setup.py sdist upload -r pypi")
print ('pip install py3mproc --upgrade -vvv  --no-cache-dir')
print ('pip3 install py3mproc --upgrade -vvv  --no-cache-dir')
os.system("pip show py3mproc")