from sqlalchemy.orm.exc import NoResultFound

import shutil

from . import BaseTestCase

from storagealchemy import StorableFile, StorableClass
from storagealchemy.test import TestStorage, TestFile

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
        global storage
        if storage is None:
            storage = TestStorage()
        TestFile.__table__.create(checkfirst=True)
        Session.query(StorableClass).delete()
        Session.query(StorableFile).delete()
        Session.query(TestFile).delete()
        transaction.commit()
        #}}}

    def _get_from_filesystem(self, storage_uri):#{{{
        if not storage_uri.startswith('test://'):
            raise Exception('unknown storage_uri %s' % storage_uri)
        path = os.path.join(self.test_storage_path, storage_uri.replace('test://', ''))
        if not os.path.isfile(path):
            return None
        fh = open(path, 'r')
        return fh.read()#}}}

    def _get_from_database(self, storage_uri):#{{{
        try:
            return Session\
                .query(StorableFile)\
                .filter_by(storage_uri=storage_uri)\
                .one()
        except NoResultFound:
            return None#}}}




    def test_adding_file_to_session_makes_it_persistant(self):
        #{{{

        file_storage_uri = 'test://test_adding_file_to_session_makes_it_persistant.txt'
        data = 'some data'

        file = TestFile()
        file.storage_uri = file_storage_uri
        file.data = data

        self.assertIsNone(self._get_from_database(storage_uri=file_storage_uri))
        self.assertIsNone(self._get_from_filesystem(storage_uri=file_storage_uri))

        Session.add(file)

        transaction.commit()

        Session.add(file._storable_file)

        self.assertEqual(self._get_from_database(storage_uri=file_storage_uri), file._storable_file)
        self.assertEqual(self._get_from_filesystem(storage_uri=file_storage_uri), data)
        self.assertTrue(False)
        #}}}


    def test_file_is_not_persistent_if_not_added_to_session(self):
        #{{{

        file_storage_uri = 'test://test_file_is_not_persistent_if_not_added_to_session.txt'
        data = 'some data'

        file = TestFile()
        file.storage_uri = file_storage_uri
        file.data = data

        transaction.commit()

        self.assertIsNone(self._get_from_database(storage_uri=file_storage_uri))
        self.assertIsNone(self._get_from_filesystem(storage_uri=file_storage_uri))
        #}}}


    def test_setting_data_to_none_deletes_it_from_storage(self):
        #{{{

        file_storage_uri = 'test://test_setting_data_to_none_deletes_it_from_storage.txt'
        data = 'some data'

        file = TestFile()
        file.storage_uri = file_storage_uri
        file.data = data

        Session.add(file)
        transaction.commit()

        Session.add(file)
        file.data = None

        transaction.commit()

        Session.add(file)
        self.assertIsNone(file.data)
        self.assertIsNone(self._get_from_filesystem(storage_uri=file_storage_uri))
        #}}}



    def test_setting_data_before_uri_works(self):
        #{{{

        file_storage_uri = 'test://test_setting_data_before_uri_works.txt'
        data = 'some data'

        file = TestFile()
        file.data = data
        file.storage_uri = file_storage_uri
        file.foo = file_storage_uri

        Session.add(file)
        transaction.commit()

        Session.add(file._storable_file)

        self.assertEqual(self._get_from_database(storage_uri=file_storage_uri), file._storable_file)
        self.assertEqual(self._get_from_filesystem(storage_uri=file_storage_uri), data)
        #}}}



