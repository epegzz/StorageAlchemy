import shutil

from . import BaseTestCase

from storagealchemy import Storage
from storagealchemy.handler import FilesystemHandler
from storagealchemy.test import TestFile

import sqlahelper
import transaction

Session = sqlahelper.get_session()
Base = sqlahelper.get_base()

import os
import logging

log = logging.getLogger(__name__)

storage = None




class StorageTest(BaseTestCase):

    test_storage_path = '/tmp/storage_test'
    test_uid = 1000
    test_gid = 100

    def setUp(self):
        # clear out old files
        super(StorageTest, self).setUp()
        shutil.rmtree(self.test_storage_path)
        os.mkdir(self.test_storage_path)
        TestFile.__table__.create(checkfirst=True)
        Session.query(TestFile).delete()
        transaction.commit()
        global storage
        storage = Storage()
        storage.add_handler('test', FilesystemHandler(self.test_storage_path, uid=self.test_uid, gid=self.test_gid))
        #}}}

    def _get_from_filesystem(self, uri):#{{{
        path = os.path.join(self.test_storage_path, uri.replace('test://', ''))
        if not os.path.isfile(path):
            return None
        fh = open(path, 'r')
        return fh.read()#}}}



    def test_committing_session_stores_file(self):

        uri = 'test://test_committing_session_stores_file.txt'
        data = 'test data'

        storage.write(uri, data)
        transaction.commit()

        self.assertEqual(storage.read(uri), data)
        self.assertEqual(self._get_from_filesystem(uri), data)


    def test_rolling_back_session_does_not_store_file(self):

        uri = 'test://test_rolling_back_session_does_not_store_file.txt'
        data = 'test data'

        storage.write(uri, data)
        Session.rollback()

        self.assertIsNone(storage.read(uri))
        self.assertIsNone(self._get_from_filesystem(uri))



    def test_setting_data_to_none_deletes_file(self):

        uri = 'test://test_setting_data_to_none_deletes_file.txt'
        data = 'test data'

        storage.write(uri, data)
        transaction.commit()

        self.assertEqual(storage.read(uri), data)
        self.assertEqual(self._get_from_filesystem(uri), data)

        storage.write(uri, None)
        transaction.commit()

        self.assertIsNone(storage.read(uri))
        self.assertIsNone(self._get_from_filesystem(uri))


