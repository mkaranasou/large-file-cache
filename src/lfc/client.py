import json
import logging
import sys
from pymemcache.client import Client
from hashlib import md5

from pymemcache.exceptions import (
    MemcacheClientError,
    MemcacheUnknownCommandError,
    MemcacheIllegalInputError,
    MemcacheServerError,
    MemcacheUnknownError,
    MemcacheUnexpectedCloseError
)


class LargeFileCacheClientFactory(object):
    def __call__(self, backend='memcache', *args, **kwargs):
        if backend == 'memcache':
            return LargeFileMemcacheClient(*args, **kwargs)
        raise NotImplementedError("Large file caching client for backend {} "
                                  "is not yet implemented".format(backend))


class LargeFileMemcacheClient(Client):
    """
        A client to store and retrieve large files, up to 50MB (MAX_FILE_SIZE)
        as MAX_CHUNK sized chunks in Memcached.
        It inherits from pymemcache's Client and overrides all the relative and
        necessary methods
    """
    def __init__(self, *args, **kwargs):
        from config import MAX_FILE_SIZE, MAX_CHUNK

        self.raise_on_error = False
        if 'raise_on_error' in kwargs:
            self.raise_on_error = kwargs.get('raise_on_error', False)
            del kwargs['raise_on_error']

        super(LargeFileMemcacheClient, self).__init__(*args, **kwargs)

        self.__use_base = False
        self._max_file_size = MAX_FILE_SIZE
        self._max_chunk = MAX_CHUNK
        self._max_no_parts = self._max_chunk / self._max_file_size
        self._cache = super(LargeFileMemcacheClient, self)
        self._max_post_fix = "_100"

        # requires serializer - deserializer # todo: can yield errors
        if self.serializer is None:
            self.serializer = lambda k, v: (v, 1) if type(v) == str \
                else (json.dumps(v), 2)
        if self.deserializer is None:
            self.deserializer = lambda k, v, f: v if f == 1 else json.loads(v)

    def _raise_or_return(self, msg, exc=Exception):
        """
        Depending on the LargeFileClient's configuration, either raise an
        exception or log it and return failure (False)
        :param msg: str, the Exception msg
        :param exc: type, the exception's type, default=Exception
        :return: None if raise_on_error is set, False otherwise
        """
        if self.raise_on_error:
            raise exc(msg)
        else:
            logging.error(msg)
            return False

    @staticmethod
    def get_file_part_key(fname, part):
        """Returns filename_partno"""
        return "{}_{}".format(fname, part)

    @staticmethod
    def get_size(f):
        """
        Returns the size of the file else None
        :param f: file, the input file
        :return: int, the size of the file
        """
        if hasattr(f, 'content_length'):
            return f.content_length
        try:
            position = f.tell()
            f.seek(0, 2)      # seek to end
            size = f.tell()
            f.seek(position)  # back
            return size
        except (AttributeError, IOError):
            pass

        return 0  # assume small

    def is_of_appropriate_size(self, f):
        """
        Checks if the file abides with the max file size we can handle
        :param f: file
        :return: boolean, True if file is within the limit, False otherwise
        """
        if hasattr(f, 'content_length'):
            return f.content_length <= self._max_file_size
        try:
            position = f.tell()
            f.seek(0, 2)      # seek to end
            size = f.tell()
            f.seek(position)  # back
            return size <= self._max_file_size
        except (AttributeError, IOError):
            pass

        return True

    def get_chunk_size(self, key):
        return self._max_chunk - (sys.getsizeof(key) + sys.getsizeof(self._max_post_fix))

    def get(self, key, default=None):
        """
        Overrides default get functionality to provide chunk retrieval and verify everything went ok.
        :param key: str, The key to search in memcached, usually the filename
        :param default: boolean
        :return: [], a single stream of bytes
        """

        # Get the file info first
        file_info = self._cache.get(key, default=default)

        if not file_info:  # file not found
            return self._raise_or_return("File for key {} not found".format(key))

        data = []
        hash_md5 = md5()

        # todo: get_many should be better but not memory wise
        # todo: do not load all at once
        for i in range(int(file_info["parts_num"])):
            logging.info("{} vs {}".format(self.get_file_part_key(key, i), key))
            part = self._cache.get(self.get_file_part_key(key, i), default=None)
            hash_md5.update(part)
            data.append(part)
        digest = hash_md5.hexdigest()
        if not file_info["checksum"] == hash_md5.hexdigest():
            logging.error("{} vs {}".format(file_info, digest))
            raise IOError("Could not retrieve the file correctly")

        return data

    def get_partial(self, key, default=None):
        """
        Overrides default get functionality to provide chunk retrieval and verify everything went ok.
        :param key: str, The key to search in memcached, usually the filename
        :param default: boolean
        :return: [], a single stream of bytes
        """

        # Get the file info first
        file_info = self._cache.get(key, default=default)

        if not file_info:  # file not found
            yield self._raise_or_return("File for key {} not found".format(key))

        data = []
        hash_md5 = md5()

        # todo: get_many should be better but not memory wise
        # todo: do not load all at once
        for i in range(int(file_info["parts_num"])):
            logging.info("{} vs {}".format(self.get_file_part_key(key, i), key))
            part = self._cache.get(self.get_file_part_key(key, i), default=None)
            hash_md5.update(part)
            data.append(part)
            yield part
        digest = hash_md5.hexdigest()
        if not file_info["checksum"] == hash_md5.hexdigest():
            logging.error("{} vs {}".format(file_info, digest))
            raise IOError("Could not retrieve the file correctly")

        # return data

    def set(self, key, f, expire=0, noreply=False):
        """
        Stores a file in memcached.
        The file will be chunked appropriately and stored in a way that can be
        retrieved and restored properly.
        :param key: str, the name to store the file, usually the filename
        :param f: file, the file object to store
        :param expire:
        :param noreply:
        :param store_chunk: boolean, flag to indicate whether to store a chunk
        or proceed in chunking, defaults to False.
        :return: boolean, True if everything went well, False otherwise
        - raises exception if `raise_on_error`
        """

        success = False

        # check if size within limits
        if not self.is_of_appropriate_size(f):
            return self._raise_or_return("Size greater than allowed.")

        # check if not duplicate key
        if self._cache.get(key):
            return self._raise_or_return("Key {} already exists.".format(key))

        if self.__use_base:
            try:
                return self._cache.set(key, f)
            except MemcacheIllegalInputError as e:
                return self._raise_or_return(e.message, MemcacheIllegalInputError)

        # if not self.__use_base means we are not storing a chunk
        # so let's check if file or has read
        if not isinstance(f, file) and not hasattr(f, 'read'):
            return self._raise_or_return("{} is not a file.".format(key))

        # check if file exists
        if not self._cache.get(key):
            i = 0
            parts_to_store = {}
            hash_md5 = md5()

            # the proper chunk will be found by removing the size of the key + max prefix size from the max chunk
            chunk = self.get_chunk_size(key)

            for piece in iter(lambda: f.read(chunk), b""):
                hash_md5.update(piece)
                parts_to_store.update({self.get_file_part_key(key, i): piece})
                i += 1

            # also store the hash for the reconstruction
            parts_to_store[key] = {"checksum": hash_md5.hexdigest(), "parts_num": i}

            self.__use_base = True
            try:
                # save the hash to compare when retrieving
                success = self._cache.set_many(parts_to_store)
            except MemcacheIllegalInputError as e:
                return self._raise_or_return(e, MemcacheIllegalInputError)

            self.__use_base = False

            if not success:
                logging.error("Could not save part {} to memcached. Performing roll-back".format(i))
                if not self._cache.delete_many(parts_to_store):
                    return self._raise_or_return("Could not rollback for {}".format(key))
        else:
            return self._raise_or_return("Key {} already exists.".format(key))

        return success

    def set_many(self, values, expire=0, noreply=None):
        """
        Save many files to memcached.
        Uses set underneath to insert many values
        :param values: list[dict[str, file]], a list with the string keys - file pairs to set
        :param expire: int, the expiration of the files, defaults to 0 - no expiration
        :param noreply: boolean, if memcached is required to reply - defaults to None to have a consistent behavior
        :return: boolean, indicates whether everything went ok or not. True if all is good, else False
        """
        success = []
        # todo: optimize
        for each in values:
            success.append(self.set(expire=expire, noreply=noreply, **each))
        success = all(success)
        if not success:
            self._raise_or_return("Could not delete")
        return success

    def delete(self, key, noreply=None):
        """
        Deletes the given file (key) from memcached
        :param key:str, the key to delete, e.g. the name of the file
        :param noreply: boolean, if memcached is required to reply - defaults to None to have a consistent behavior
        :return: boolean, indicates whether everything went ok or not. True if all is good, else False
        """
        success = False

        if self.__use_base:
            return self._cache.delete(key)

        file_info = self._cache.get(key)
        if file_info:
            to_remove = []
            for i in range(int(file_info["parts_num"])):
                to_remove.append(self.get_file_part_key(key, i))

            to_remove.append(key)
            self.__use_base = True
            success = self._cache.delete_many(to_remove)
            self.__use_base = False
            if not success:
                return self._raise_or_return("Could not delete")
        else:
            return self._raise_or_return("Could not delete {}. File not found in cache".format(key))

        return success

    def delete_many(self, keys, noreply=None):
        """
        Uses simple delete underneath to delete files and their data from memcached
        :param keys: [str], a list of the string keys we want to delete. E.g. a list of filenames
        :param noreply: boolean, if memcached is required to reply - defaults to None to have a consistent behavior
        :return: boolean, indicates whether everything went ok or not. True if all is good, else False
        """
        success = []
        for key in keys:
            success.append(self.delete(key, noreply))
        success = all(success)
        if not success:
            return self._raise_or_return("Could not delete")
        return success

    def cas(self, key, value, cas, expire=0, noreply=False):
        raise NotImplementedError("TODO")

    def gets(self, key, default=None, cas_default=None):
        raise NotImplementedError("TODO")

    def gets_many(self, key, default=None, cas_default=None):
        raise NotImplementedError("TODO")

    def replace(self, key, f, expire=0, noreply=None):
        """
        Implementation for the replace command.
        :param f: File instance, the file to replace
        :param key:str
        :param value: file
        :param expire: int
        :param noreply: boolean
        :return: boolean, True if everything went ok, False otherwise
        """
        self.delete(key)
        return self.set(key, f)

    def __setitem__(self, key, value):
        self.set(key, value, noreply=True)

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError
        return value

    def __delitem__(self, key):
        self.delete(key, noreply=True)


