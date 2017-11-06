# Large File Caching

## Installation
To install:

`python setup.py install`

or if you have cloned the repo and want to work on it, you can:

`pip install -e . --process-dependency-links`

## Usage
- Memcached:

Make sure Memcached is running.

Depending on how you've installed Memcached, 
you can either run:
```commandline
memcached
```
or start the Docker container for Memcached.

set

```python
from lfc.client import LargeFileCacheClientFactory

client = LargeFileCacheClientFactory()('memcached', ('MEMCACHED_HOST', 'MEMCACHED_PORT'))

file_name = 'somebigfile'

# to save in Memcached:
with open(file_name, 'rb') as bigfile:
    client.set(file_name, bigfile)

# to retrieve
cached_file = client.get(file_name)

```
