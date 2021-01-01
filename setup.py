from setuptools import setup

setup(
   name = 'autoscalingsim',
   version = '0.1',
   description = 'Autoscaling simulator Multiverse',
   author = 'Vladimir Podolskiy',
   author_email = 'v.e.podoslkiy@gmail.com',
   packages = ['autoscalingsim'],
   install_requires = [
                        'tqdm',
                        'pandas',
                        'uuid',
                        'matplotlib',
                        'networkx',
                        'python-igraph',
                        'urllib',
                        'tarfile',
                        'progressbar',
                        'statsmodels>=0.12.1',
                        'scipy',
                        'sklearn',
                        'numpy==1.19.3 ; sys_platform == windows',
                        'tensorflow==2.4.0',
                        'https://github.com/kieferk/pymssa/tarball/master ; sys_platform == linux',
                        'https://github.com/Remit/pymssa/tarball/master ; sys_platform == windows',
                        'dcor==0.5.2'
                      ],
   scripts = [
               'autoscalingsim-cl.py',
               'analysis.ipynb'
             ]
)
