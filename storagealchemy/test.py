from . import Storage, Storable
from .handler import FilesystemHandler

import sqlalchemy as sa
import sqlahelper

Base = sqlahelper.get_base()


class TestFile(Storable, Base):
    __tablename__ = 'TestFile'

    id     = sa.Column(sa.BigInteger(20, unsigned=True), primary_key=True, autoincrement=True)
    foo    = sa.Column(sa.Text(length=32))


class TestStorage():

    test_storage_path = '/tmp/storage_test'
    test_uid = 1000
    test_gid = 100

    storage = None

    def __init__\
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

        self.storage = Storage()
        self.storage.add_handler('test://', filesystem_handler)
        #}}}
