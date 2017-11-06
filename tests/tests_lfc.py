import os
import unittest

import sys
from lfc.client import LargeFileCacheClientFactory, LargeFileMemcacheClient
from lfc.config import MEMCACHED_HOST, MEMCACHED_PORT, MAX_FILE_SIZE


class TestLargeFileMemcachedClient(unittest.TestCase):
    """
    Basic tests for the LargeFileClient lib
    """
    def setUp(self):

        self.large_file_path = 'bigoldfile.dat'
        self.larger_file_path = 'bigoldfile_illegal.dat'
        self.temp_path = None

        with open(self.large_file_path, "wb") as out:
            out.truncate(MAX_FILE_SIZE)

        with open(self.larger_file_path, "wb") as out:
            out.truncate(MAX_FILE_SIZE + 1024)

        self.large_file = open(self.large_file_path, 'rb')
        self.larger_file = open(self.larger_file_path, 'rb')
        self.lfc = None

    def tearDown(self):
        """
        Clear Memcached
        Close Files
        Delete files
        :return:
        """
        self.lfc.flush_all()
        self.lfc.close()
        self.large_file.close()
        self.larger_file.close()
        if self.temp_path:
            os.remove(self.temp_path)
        os.remove(self.large_file_path)
        os.remove(self.larger_file_path)

    def test_smoke_can_instantiate(self):
        """
        Basic check that it can connect and does not throw an exception
        :return: None
        """
        self.lfc = LargeFileCacheClientFactory()((MEMCACHED_HOST, MEMCACHED_PORT))
        self.assertTrue(isinstance(self.lfc, LargeFileMemcacheClient))

    def test_successfull_set(self):
        """
        Tests successfully saving the large file to memcached
        :return: None
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT))
        success = self.lfc.set(self.large_file_path, self.large_file)
        self.assertTrue(success)

    def test_successfull_get(self):
        """
        Test the successful retrieval of a file
        :return: None
        """
        import filecmp

        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT))
        # first set
        success = self.lfc.set(self.large_file_path, self.large_file)
        self.assertTrue(success)

        # then get
        data = list(self.lfc.get(self.large_file_path))
        self.assertIsNotNone(data)
        self.assertTrue(len(data) > 0)

        self.temp_path = "out.dat"
        with open(self.temp_path,  'wb') as out:
            for each in data:
                out.write(each)

        self.assertTrue(filecmp.cmp('out.dat', self.large_file_path))

    def test_successful_delete(self):
        """
        Correctly save and delete a file and its parts
        :return: None
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT))
        # firt set
        success = self.lfc.set(self.large_file_path, self.large_file)
        self.assertTrue(success)

        # then get
        success = self.lfc.delete(self.large_file_path)
        self.assertIsNotNone(success)
        self.assertTrue(success)

    def test_successful_delete_many(self):
        """
        Correctly delete many files
        :return: None
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT))
        # first set
        success = self.lfc.set(self.large_file_path, self.large_file)
        self.assertTrue(success)

        # second set
        success = self.lfc.set(self.larger_file_path, self.large_file)
        self.assertTrue(success)

        # then delete many
        success = self.lfc.delete_many([self.large_file_path,
                                        self.larger_file_path])

        self.assertIsNotNone(success)
        self.assertTrue(success)

    def test_unsuccessful_delete_file_not_found(self):
        """
        Test delete with wrong key
        :return: None
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT),
                                        raise_on_error=True)

        with self.assertRaises(Exception) as context:
            self.lfc.delete(self.large_file_path + "test")

        self.assertTrue('Could not delete bigoldfile.dattest. '
                        'File not found in cache' in context.exception)

    def test_unsuccessful_set_larger_than_max_size_no_error(self):
        """
        Do not raise error when trying to set larger file than allowed
        :return:
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT))

        self.assertFalse(self.lfc.raise_on_error)

        success = self.lfc.set(self.larger_file_path, self.larger_file)
        self.assertFalse(success)

    def test_unsuccessful_set_larger_than_max_size_raise_error(self):
        """
        Raise error when trying to set larger file than allowed
        :return: None
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT),
                                        raise_on_error=True)
        self.assertTrue(self.lfc.raise_on_error)

        with self.assertRaises(Exception) as context:
            self.lfc.set(self.larger_file_path, self.larger_file)

        self.assertTrue('Size greater than allowed.' in context.exception)

    def test_unsuccessful_get_key_not_found_no_exc(self):
        """
        Test invalid key get - raise_on_error=False
        :return:
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT))

        success = self.lfc.get(self.large_file_path + "_not_valid")
        self.assertFalse(success)

    def test_unsuccessful_get_key_not_found_raise_exc(self):
        """
        Test invalid key get - raise_on_error=True
        :return:
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT),
                                   raise_on_error=True)
        self.assertTrue(self.lfc.raise_on_error)
        with self.assertRaises(Exception) as context:
            self.lfc.get(self.large_file_path + "_not_valid")

        self.assertTrue('File for key bigoldfile.dat_not_valid not found'
                        in context.exception)

    def test_get_file_part_key(self):
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT),
                                        raise_on_error=True)

        self.assertTrue(self.lfc.get_file_part_key(self.larger_file_path, 1)
                        == "{}_{}".format(self.larger_file_path, 1))

    def test_successful_is_of_appropriate_size(self):
        """
        Checks the size of a file with size within the limit of MAX_FILE_SIZE
        and returns True
        :return: None
        """
        self.lfc = LargeFileCacheClient((MEMCACHED_HOST, MEMCACHED_PORT))

        self.assertTrue(self.lfc.is_of_appropriate_size(self.large_file))

    def test_unsuccessful_is_of_appropriate_size(self):
        """
        Checks the size of a file **larger** than MAX_FILE_SIZE and
        returns False
        :return: None
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT))

        self.assertFalse(self.lfc.is_of_appropriate_size(self.larger_file))

    def test_get_chunk_size(self):
        """
        Asserts that get_chunk_size returns a size in bytes less than the
        max chunk size of Memcached
        :return: None
        """
        self.lfc = LargeFileCacheClientFactory((MEMCACHED_HOST, MEMCACHED_PORT))
        self.assertLess(self.lfc.get_chunk_size(self.large_file_path),
                        self.lfc._max_chunk)


if __name__ == '__main__':
    unittest.main()
