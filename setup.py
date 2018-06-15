from __future__ import absolute_import
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# README as the long description
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines()]

setup(name='lfc',
      version='0.1.0',
      description='Client for Large File caching',
      long_description=long_description,
      maintainer='Maria Karanasou',
      maintainer_email='karanasou@gmail.com',
      license='MIT',
      author='Maria Karanasou',
      author_email='karanasou@gmail.com',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Programming Language :: Python :: 2.7',
          'Intended Audience :: Developers',
          'Topic :: Software Development :: Build Tools',
          'License :: OSI Approved :: MIT License',
      ],
      keywords=['memcached',
                'pymemcache'],
      package_dir={'': 'src'},
      packages=['lfc'] + find_packages(exclude=['contrib', 'docs', 'tests']),
      tests_require=[
          'nose',
          'mock'
      ],
      test_suite='nose.collector',
      install_requires=REQUIREMENTS,

      )
