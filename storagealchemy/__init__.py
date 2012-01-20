# -*- coding:utf8 -*-

import logging
import sqlalchemy as sa
import hashlib

from sqlalchemy.orm import mapper
from sqlalchemy.dialects import mysql as mysql
from sqlalchemy.ext.declarative import declared_attr

from sqlalchemy.schema import Index

from .exception import StorageError, NoSuchFile

from . import handler

log = logging.getLogger(__name__)

__all__ = ['Storage', 'Storable']


class Storage():

    def __init__\
        ( self
        , db_session
        ):
        self._storage_dict = dict()
        self._stash_dict = dict()
        self._locks = dict()

        sa.event.listen(db_session, "after_commit", self._after_commit_callback())
        sa.event.listen(db_session, "after_soft_rollback", self._after_rollback_callback())

        # Whenever an object is created that inherits from Storaable,
        # set object._storage to self, so that read/write methods from
        # StorageFile will work.
        def on_storage_file_init(target, *args, **kwargs):
            target._storage = self
        @sa.event.listens_for(mapper, "mapper_configured")
        def _listen_init(m, cls_):
            if issubclass(cls_, Storable):
                sa.event.listen(cls_, 'init', on_storage_file_init)


    def add_handler(self, key, storage_handler, read=True, write=True, delete=True):
        self._storage_dict[key] = dict\
            ( handler = storage_handler
            , read = read
            , write = write
            , delete = delete
            )

    def read(self, file):
        #{{{
        """Returns file contents.

        """
        if file.storage_uri in self._stash_dict:
            return self._stash_dict[file.storage_uri]['data']
        storage, path = self._get_storage(file.storage_uri, 'r')
        try:
            return storage.read(path)
        except NoSuchFile:
            return None
        #}}}


    def write(self, file, data):
        #{{{
        """Writes content data to file.

        """
        # Make sure that no other process tries to write to this file.
        # If file is currently locked by another process, this process
        # will wait until the fill becomes unlocked again and than lock it.
        self.lock(file)

        if data is None:
            if file.content_checksum == None:
                return  # nothing changed
            file.content_length = None
            file.content_checksum = None
            file.modification_time = sa.func.now()
            self.delete_on_commit(file) # no data to store
        else:
            new_checksum = str(hashlib.md5(data).hexdigest())
            if file.content_checksum == new_checksum:
                return # nothing changed
            file.content_length = len(data)
            file.content_checksum = new_checksum
            file.modification_time = sa.func.now()
            self.write_on_commit(file, data)
        #}}}



    def lock(self, file):
        """Lock a file to protect it from being overwritten by another process.

        """
        storage, path = self._get_storage(file.storage_uri, 'w')
        lock = storage.get_lock(path)
        if not lock \
        or not file.storage_uri in self._locks \
        or not self._locks[file.storage_uri] == str(lock):
            # wait until we can gain lock again
            lock = storage.lock(path)
            self._locks[file.storage_uri] = str(lock)
        return lock


    def unlock(self, file):
        """Unlock a file.

        """
        if file.storage_uri in self._locks:
            storage, path = self._get_storage(file.storage_uri, 'w')
            storage.unlock(path, lock=self._locks[file.storage_uri])
            del(self._locks[file.storage_uri])


    def is_locked(self, file):
        """Returns whether a file is locked (by any process including the current one) or not.

        """
        storage, path = self._get_storage(file.storage_uri, 'w')
        return storage.is_locked(path)

    def owns_lock(self, file):
        """Returns whether the current process owns a lock on the file.

        """
        storage, path = self._get_storage(file.storage_uri, 'w')
        return file.storage_uri in self._locks and str(storage.get_lock(path)) == self._locks[file.uri]


    def _unlock_all(self):
        for storage_uri in self._locks:
            storage, path = self._get_storage(storage_uri, 'w')
            storage.unlock(path, self._locks[storage_uri])
        self._locks = dict()


    def write_on_commit(self, file, data):#{{{
        storage, path = self._get_storage(file.storage_uri, 'w')
        def write_later(session):
            if file in session \
            and not file in session.deleted:
                storage.write(path, data)
        self._stash_dict[file.storage_uri] = dict\
            ( callback = write_later
            , data = data
            , file = file
            , action = 'write'
            )
        #}}}


    def delete_on_commit(self, file):#{{{
        storage, path = self._get_storage(file.storage_uri, 'w')
        def delete_later(session):
            try:
                storage.delete(path)
            except NoSuchFile:
                pass
        self._stash_dict[file.storage_uri] = dict\
            ( callback = delete_later
            , data = None
            , file = file
            , action = 'delete'
            )
        #}}}


    def _before_commit_callback(self):
        def _before_commit(session):#{{{
            for file in session.deleted:
                # Delete all that got deleted from database
                # after next commit.
                self.delete_on_commit(file)
        return _before_commit
        #}}}


    def _after_commit_callback(self):
        def _after_commit(session):#{{{
            for key in self._stash_dict:
                # perform write/delete action
                self._stash_dict[key]['callback'](session)
            self._stash_dict = dict()
            self._unlock_all()
        return _after_commit
        #}}}


    def _after_rollback_callback(self):
        def _after_rollback(session, previous_transaction):#{{{
            self._stash_dict = dict()
            self._unlock_all()
        return _after_rollback
        #}}}


    def _get_storage(self, storage_uri, mode):#{{{
        storage_key = None
        for key in self._storage_dict:
            if storage_uri.startswith(key):
                if   mode == 'rw' \
                and  self._storage_dict[key]['read'] \
                and  self._storage_dict[key]['write'] :
                     storage_key = key

                elif mode == 'r' \
                and  self._storage_dict[key]['read']:
                     storage_key = key

                elif mode == 'w' \
                and  self._storage_dict[key]['write']:
                     storage_key = key

        if storage_key and storage_key in self._storage_dict:
            return self._storage_dict[storage_key]['handler'], storage_uri.replace(storage_key,'')

        raise StorageError('could not find storage for storage_uri %s' % storage_uri)
        #}}}





class Storable(object):

    storage_uri         = sa.Column(sa.String(length=255))
    content_length      = sa.Column(sa.BigInteger(20, unsigned=True))
    content_checksum    = sa.Column(sa.Text(length=32))
    creation_time       = sa.Column(sa.DateTime(), nullable=False, default=sa.func.now())
    modification_time   = sa.Column(sa.DateTime(), nullable=False, default=sa.func.now())

    #_parent_file_id = sa.Column('parent_file_id', mysql.BIGINT(20, unsigned=True))
    #on_parent_modify = sa.Column(mysql.ENUM(u'delete'))
    #on_parent_delete = sa.Column(mysql.ENUM(u'delete'))
    #@declared_attr
    #def parent_file(cls):
        #return relationship\
            #( cls
            #, remote_side = '[%s.id]' % cls.__tablename__
            #, uselist = False
            #, backref = backref('child_files', single_parent=True)
            #)

    @declared_attr
    def __table_args__(cls):
        args = [Index('ix_%s_storage_uri' % cls.__tablename__, 'storage_uri', unique=True)]
        if hasattr(cls, '__add_table_args__'):
            args.append(cls.__add_table_args__)
        return tuple(args)


    def _get_file_contents(self):
        self._storage.read(self)
    def _set_file_contents(self, data):
        self._storage.write(self, data)
    data = property(_get_file_contents, _set_file_contents)



