import os
import unittest

import mock as mock
from mocks import MockCache
from lfc.client import LargeFileCacheClientFactory, LargeFileMemcacheClient
from lfc.config import MEMCACHED_HOST, MEMCACHED_PORT, MAX_FILE_SIZE, MAX_CHUNK


class TestLargeFileMemcachedClient(unittest.TestCase):
    """
    Basic tests for the LargeFileClient lib
    """

    def setUp(self):
        self.patcher = mock.patch('pymemcache.client.Client')
        self.mock_client = self.patcher.start()
        self.large_file_path = 'bigoldfile.dat'
        self.larger_file_path = 'bigoldfile_illegal.dat'
        self.temp_path = None

        with open(self.large_file_path, "wb") as out:
            out.truncate(MAX_FILE_SIZE)

        with open(self.larger_file_path, "wb") as out:
            out.truncate(MAX_FILE_SIZE + 1024)

        self.large_file = open(self.large_file_path, 'rb')
        self.larger_file = open(self.larger_file_path, 'rb')
        self.lfc = LargeFileCacheClientFactory()('memcached',
                                                 (MEMCACHED_HOST,
                                                  MEMCACHED_PORT))
        self.lfc._cache = MockCache()

    def tearDown(self):
        """
        Stop pymemcache patcher, close and remove test files
        :return:
        """
        self.patcher.stop()
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
        self.assertTrue(isinstance(self.lfc, LargeFileMemcacheClient))

    def test_successfull_set(self):
        """
        Tests successfully saving the large file to memcached
        :return: None
        """
        # the file does not exist in cache
        self.lfc._cache.get = mock.MagicMock(return_value=False)
        success = self.lfc.set(self.large_file_path, self.large_file)
        self.assertTrue(success)
        calls = [mock.call(self.large_file_path),
                 mock.call(self.large_file_path)]
        self.lfc._cache.get.assert_has_calls(calls)

    def test_successfull_get(self):
        """
        Test the successful retrieval of a file
        :return: None
        """
        import filecmp
        # first set
        success = self.lfc.set(self.large_file_path, self.large_file)
        self.assertTrue(success)

        # then get
        data = list(self.lfc.get(self.large_file_path))
        self.assertIsNotNone(data)
        self.assertTrue(len(data) > 0)

        self.temp_path = "out.dat"
        with open(self.temp_path, 'wb') as out:
            for each in data:
                out.write(each)

        self.assertTrue(filecmp.cmp('out.dat', self.large_file_path))

        # import filecmp
        # # todo
        # # return
        # mock_hasher.return_value.update = mock.MagicMock()
        # mock_hasher.return_value.hexdigest = mock.MagicMock()
        # mock_hasher.return_value.hexdigest.side_effect = [
        # 1 for i in range(50)]
        #
        # self.lfc._cache = self.mock_client
        # # the file does not exist in cache
        # self.lfc._cache.get = mock.MagicMock(return_value=False)
        #
        # # first set
        # success = self.lfc.set(self.large_file_path, self.large_file)
        # self.assertTrue(success)
        #
        # self.lfc._cache.get = mock.MagicMock(return_value={'parts_num': 2,
        #                                                    'checksum': 1})
        # # then get
        # data = list(self.lfc.get(self.large_file_path))
        # self.assertIsNotNone(data)
        # self.assertTrue(len(data) > 0)
        #
        # self.temp_path = "out.dat"
        # with open(self.temp_path, 'wb') as out:
        #     for each in data:
        #         out.write(each)
        #
        # self.assertTrue(filecmp.cmp('out.dat', self.large_file_path))

    def test_successful_delete(self):
        """
        Correctly save and delete a file and its parts
        :return: None
        """
        self.lfc._cache = self.mock_client
        # the file does not exist in cache
        self.lfc._cache.get = mock.MagicMock(return_value=False)

        # firt set
        success = self.lfc.set(self.large_file_path, self.large_file)
        self.assertTrue(success)

        # file is present now - mock responses
        self.lfc._cache.get = mock.MagicMock(return_value={'parts_num': 2})
        self.lfc._cache.delete = mock.MagicMock(return_value=True)
        self.lfc._cache.delete_many = mock.MagicMock(return_value=True)
        # then delete
        success = self.lfc.delete(self.large_file_path)
        self.assertIsNotNone(success)
        self.assertTrue(success)

    def test_successful_delete_many(self):
        """
        Correctly delete many files
        :return: None
        """
        self.lfc._cache = self.mock_client
        # the file does not exist in cache
        self.lfc._cache.get = mock.MagicMock(return_value=False)

        # first set
        success = self.lfc.set(self.large_file_path, self.large_file)
        self.assertTrue(success)

        # second set
        success = self.lfc.set(self.larger_file_path, self.large_file)
        self.assertTrue(success)

        # file is present now - mock responses
        self.lfc._cache.get = mock.MagicMock(return_value={'parts_num': 2})
        self.lfc._cache.delete_many = mock.MagicMock(return_value=True)
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
        self.lfc.raise_on_error = True

        with self.assertRaises(Exception) as context:
            self.lfc.delete(self.large_file_path + "test")

        self.assertTrue('Could not delete bigoldfile.dattest. '
                        'File not found in cache' in context.exception)

    def test_unsuccessful_set_larger_than_max_size_no_error(self):
        """
        Do not raise error when trying to set larger file than allowed
        :return:
        """

        self.assertFalse(self.lfc.raise_on_error)

        success = self.lfc.set(self.larger_file_path, self.larger_file)
        self.assertFalse(success)

    def test_unsuccessful_set_larger_than_max_size_raise_error(self):
        """
        Raise error when trying to set larger file than allowed
        :return: None
        """
        self.lfc.raise_on_error = True
        with self.assertRaises(Exception) as context:
            self.lfc.set(self.larger_file_path, self.larger_file)

        self.assertTrue('Size greater than allowed.' in context.exception)

    def test_unsuccessful_get_key_not_found_no_exc(self):
        """
        Test invalid key get - raise_on_error=False
        :return:
        """
        success = self.lfc.get(self.large_file_path + "_not_valid")
        self.assertFalse(success)

    def test_unsuccessful_get_key_not_found_raise_exc(self):
        """
        Test invalid key get - raise_on_error=True
        :return:
        """
        self.lfc.raise_on_error = True
        with self.assertRaises(Exception) as context:
            self.lfc.get(self.large_file_path + "_not_valid")

        self.assertTrue('File for key bigoldfile.dat_not_valid not found'
                        in context.exception)

    def test_get_file_part_key(self):
        self.lfc.raise_on_error = True
        self.assertTrue(
            self.lfc.get_file_part_key(
                self.larger_file_path, 1
            ) == "{}_{}".format(self.larger_file_path, 1)
        )

    def test_successful_is_of_appropriate_size(self):
        """
        Checks the size of a file with size within the limit of MAX_FILE_SIZE
        and returns True
        :return: None
        """
        self.assertTrue(self.lfc.is_of_appropriate_size(self.large_file))

    def test_unsuccessful_is_of_appropriate_size(self):
        """
        Checks the size of a file **larger** than MAX_FILE_SIZE and
        returns False
        :return: None
        """
        self.assertFalse(self.lfc.is_of_appropriate_size(self.larger_file))

    def test_get_chunk_size(self):
        """
        Asserts that get_chunk_size returns a size in bytes less than the
        max chunk size of Memcached
        :return: None
        """
        self.assertLess(self.lfc.get_chunk_size(self.large_file_path),
                        self.lfc._max_chunk)

    def test_change_max_file_size(self):
        """
        Asserts that get_chunk_size returns a size in bytes less than the
        max chunk size of Memcached
        :return: None
        """
        self.lfc._max_file_size = 49 * 1024 * 1024
        # first set
        success = self.lfc.set(self.large_file_path, self.large_file)
        self.assertFalse(success)

        self.assertLess(self.lfc.get_chunk_size(self.large_file_path),
                        self.lfc.max_chunk)

    def test_change_max_chunk(self):
        """
        Asserts that get_chunk_size returns a size in bytes less than the
        max chunk size of Memcached
        :return: None
        """
        with self.assertRaises(AssertionError) as context:
            self.lfc.max_chunk = 2 * 1024 * 1024

        print(context.exception)
        self.assertTrue('Chunk is bigger than MAX_CHUNK {}.'.format(MAX_CHUNK)
                        in context.exception.message)


if __name__ == '__main__':
    unittest.main()
