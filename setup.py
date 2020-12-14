from setuptools import setup

setup(
   name = 'autoscalingsim',
   version = '0.1',
   description = 'An autoscaling simulator',
   author = 'Vladimir Podolskiy',
   author_email = 'v.e.podoslkiy@gmail.com',
   packages = ['autoscalingsim'],
   install_requires = [
                        'tqdm',
                        'pandas',
                        'uuid',
                        'matplotlib',
                        'networkx',
                        'python-igraph'
                      ],
   scripts = [
               'autoscalingsim-cl.py',
               'analysis.ipynb'
             ]
)
