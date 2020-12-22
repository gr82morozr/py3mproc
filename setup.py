from distutils.core import setup
setup(
  name          = 'py3mproc',
  packages      = ['py3mproc'],
  version       = '0.1.9',
  description   = 'A Python3 multiprocessing workflow',
  author        = 'Great Tomorrow',
  author_email  = 'gr82morozr@gmail.com',
  url           = 'https://github.com/gr82morozr/py3mproc.git',  
  download_url  = 'https://github.com/gr82morozr/py3mproc.git', 
  keywords      = ['Python3', 'Multiprocessing', 'Workflow', 'Framework'], 
  classifiers   = [],
  install_requires=[ 'psutil', 'py3toolbox']
)
