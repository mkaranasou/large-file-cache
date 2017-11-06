# Large File Caching

`pip install -e . --process-dependency-links`

- With Memcached:

```python
from lfc.client import LargeFileCacheClientFactory
from lfc.config import MEMCACHED_HOST, MEMCACHED_PORT

client = LargeFileCacheClientFactory()('memcached', (MEMCACHED_HOST, MEMCACHED_PORT))

file_name = 'somebigfile'
with open(file_name, 'rb') as bigfile:
    client.set(file_name, bigfile)

cached_file = client.get(file_name)

```
