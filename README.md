# Large File Caching

A simple library for caching large files, currently supporting Memcached only (through the [`pymemcache`](https://github.com/pinterest/pymemcache) package).
The file is split into chunks in order to be stored and the integrity is checked upon retrieval.

## Installation
To install:

`python setup.py install`

or if you have cloned the repo and want to work on it, you can:

`pip install -e setup.py`

## Usage
### Memcached

Make sure Memcached is running.

Depending on how you've installed Memcached, 
you can either run:
```commandline
memcached
```
or start the Docker container for Memcached.

The only parameters that need to be set, is the `MEMCACHED_HOST`, e.g. `localhost` and the `MEMCACHED_PORT`, e.g.
`11211`.

Other parameters that can be set is the `MAX_FILE_SIZE` and the `MAX_CHUNK`, which defines the maximum size of the value chunk in the `{key:value}` pair stored in Memcached.

The defaults are in `lfc.config`. 

```python
from lfc.client import LargeFileCacheClientFactory

client = LargeFileCacheClientFactory()('memcached', (
    'MEMCACHED_HOST', 
    'MEMCACHED_PORT'
    )
)

file_name = 'somebigfile'

# to save in Memcached:
with open(file_name, 'rb') as bigfile:
    client.set(file_name, bigfile)

# to retrieve
cached_file = client.get(file_name)

```
