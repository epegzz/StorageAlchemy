from sqlalchemy.orm.exc import NoResultFound

import shutil

from . import BaseTestCase, session, Base

from storagealchemy import Storage, Storable
from storagealchemy.handler import FilesystemHandler

import os
import logging

import sqlalchemy as sa

log = logging.getLogger(__name__)




class TestFile(Storable, Base):
    __tablename__ = 'TestFile'
    __add_table_args__ = {'mysql_engine':'InnoDB', 'mysql_charset':'utf8'}

    id = sa.Column(sa.BigInteger(20, unsigned=True), primary_key=True, autoincrement=True)
    type = sa.Column('type', sa.Enum(u'image', u'video'))

    __mapper_args__ = {'polymorphic_on': type}



class StorageTest(BaseTestCase):

    test_storage_path = '/tmp/vcn_storage_test'
    test_uid = 1000
    test_gid = 100

    def setUp(self):
        # clear out old files
        super(StorageTest, self).setUp()
        shutil.rmtree(self.test_storage_path)
        os.mkdir(self.test_storage_path)


    def create_test_storage\
        ( self
        , path=None
        , read=True
        , write=True
        , delete=True
        , uid = None
        , gid = None
        , max_lock_time = 2
        ):#{{{

        path = path or self.test_storage_path
        uid = uid or self.test_uid
        gid = gid or self.test_gid

        filesystem_handler = FilesystemHandler\
            ( path
            , uid = uid
            , gid = gid
            , max_lock_time = max_lock_time
            )

        storage = Storage(session)
        storage.add_handler('test://', filesystem_handler)
        return storage
        #}}}

    def _get_from_filesystem(self, storage_uri):#{{{
        if not storage_uri.startswith('test://'):
            raise Exception('unknown storage_uri %s' % storage_uri)
        path = os.path.join(self.test_storage_path, storage_uri.replace('test://', ''))
        if not os.path.isfile(path):
            return None
        fh = open(path, 'r')
        return fh.read()#}}}

    def _get_from_database(self, id=None, storage_uri=None):#{{{
        if id == None and storage_uri == None:
            raise Exception('Neither id nor storage_uri given.')
        try:
            if id:
                return session\
                    .query(TestFile)\
                    .filter_by(id=id)\
                    .one()
            if storage_uri:
                return session\
                    .query(TestFile)\
                    .filter_by(storage_uri=storage_uri)\
                    .one()
            return None
        except NoResultFound:
            return None#}}}




    def test_adding_file_to_session_makes_it_persistant(self):
        #{{{
        session.commit()
        self.create_test_storage()

        file_storage_uri = 'test://test_adding_file_to_session_makes_it_persistant.txt'
        data = 'some data'

        file = TestFile()
        file.storage_uri = file_storage_uri
        file.data = data

        self.assertIsNone(self._get_from_database(storage_uri=file_storage_uri))
        self.assertIsNone(self._get_from_filesystem(storage_uri=file_storage_uri))

        session.add(file)

        self.assertEqual(self._get_from_database(storage_uri=file_storage_uri), file)
        self.assertIsNone(self._get_from_filesystem(storage_uri=file_storage_uri))

        session.commit()

        self.assertEqual(self._get_from_database(storage_uri=file_storage_uri), file)
        self.assertEqual(self._get_from_filesystem(storage_uri=file_storage_uri), data)
        #}}}


    def test_file_is_not_persistent_if_not_added_to_session(self):
        #{{{
        session.commit()
        self.create_test_storage()

        file_storage_uri = 'test://test_file_is_not_persistent_if_not_added_to_session.txt'
        data = 'some data'

        file = TestFile()
        file.storage_uri = file_storage_uri
        file.data = data

        session.commit()

        self.assertIsNone(self._get_from_database(storage_uri=file_storage_uri))
        self.assertIsNone(self._get_from_filesystem(storage_uri=file_storage_uri))
        #}}}


    def test_setting_data_to_none_deletes_it_from_storage(self):
        #{{{
        session.commit()
        self.create_test_storage()

        file_storage_uri = 'test://test_setting_data_to_none_deletes_it_from_storage.txt'
        data = 'some data'

        file = TestFile()
        file.storage_uri = file_storage_uri
        file.data = data
        session.commit()

        file.data = None

        session.commit()

        self.assertIsNone(file.data)
        self.assertIsNone(self._get_from_filesystem(storage_uri=file_storage_uri))
        #}}}


