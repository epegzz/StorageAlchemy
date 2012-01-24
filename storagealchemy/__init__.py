# -*- coding:utf8 -*-

import logging
import sqlalchemy as sa
import hashlib

from sqlalchemy.schema import Index
from sqlalchemy.orm import mapper, object_session
from sqlalchemy.orm.util import has_identity

import sqlahelper

from .exception import StorageError, NoSuchFile
from . import handler

Base = sqlahelper.get_base()
Session = sqlahelper.get_session()


log = logging.getLogger(__name__)

__all__ = ['Storage', 'Storable', 'StorableClass', 'StorableFile', 'handler']


class Storage():

    def __init__(self):
        self._storage_dict = dict()
        self._tasks = dict()
        self._unbound = list()
        self._committed = list()
        self._uriless_stash_dict = dict()
        Storable._storage = self

        # create database tables if necessary
        StorableClass.__table__.create(checkfirst=True)
        StorableFile.__table__.create(checkfirst=True)

        def after_bulk_delete(session, query, query_context, result):
            # @TODO: implement later
            # >>> query_context.statement.execute().fetchall()
            # [(3L, None), (5L, None)]
            # >>> Session.x_query_context.statement.froms
            # [Table('TestFile', MetaData(bind=Engine(mysql://...)), Column('id', BigInteger(), table=<TestFile>, primary_key=True, nullable=False), Column('foo', Text(length=32), table=<TestFile>), schema=None)]
            pass

        sa.event.listen(Session(), "after_bulk_delete", after_bulk_delete)
        sa.event.listen(Session(), "before_flush", self._before_flush_callback())
        sa.event.listen(Session(), "after_flush_postexec", self._after_flush_callback())
        sa.event.listen(Session(), "before_commit", self._before_commit_callback())
        sa.event.listen(Session(), "after_commit", self._after_commit_callback())
        sa.event.listen(Session(), "after_soft_rollback", self._after_rollback_callback())

        def on_storage_file_init(bind, *args, **kwargs):
            bind.on_create_new_instance()
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

    def read(self, storable_file):
        #{{{
        """Returns storable_file contents.

        """
        if storable_file not in Session:
            Session.add(storable_file)
        try:
            storage_uri = storable_file.storage_uri or ''
            return self._tasks[storable_file][storage_uri]['write']['data']
        except KeyError:
            if storable_file.storage_uri is None:
                return None
            try:
                storage, path = self._get_storage(storable_file.storage_uri, 'r')
                return storage.read(path)
            except NoSuchFile:
                return None
        #}}}


    def write(self, storable_file, data):
        #{{{
        """Writes content data to storable_file.

        """
        if storable_file not in Session:
            Session.add(storable_file)
        if data is None:
            if storable_file.content_checksum == None:
                return  # nothing changed
            storable_file.content_length = None
            storable_file.content_checksum = None
            storable_file.modification_time = sa.func.now()
            self.delete_on_commit(storable_file) # no data to store
        else:
            new_checksum = str(hashlib.md5(data).hexdigest())
            if storable_file.content_checksum == new_checksum:
                return # nothing changed
            storable_file.content_length = len(data)
            storable_file.content_checksum = new_checksum
            storable_file.modification_time = sa.func.now()
            self.write_on_commit(storable_file, data)
        #}}}



    def write_on_commit(self, storable_file, data):#{{{
        def write_later(session, _uri):
            storage, path = self._get_storage(_uri, 'w')
            storage.write(path, data)
        task = dict\
            ( callback = write_later
            , data = data
            )
        storage_uri = storable_file.storage_uri or ''
        self._add_task(storable_file, storage_uri, 'write', task)
        #}}}


    def delete_on_commit(self, storable_file):#{{{
        def delete_later(session, _uri):
            storage, path = self._get_storage(_uri, 'w')
            try:
                storage.delete(path)
            except NoSuchFile:
                pass
        task = dict\
            ( callback = delete_later
            )
        storage_uri = storable_file.storage_uri or ''
        self._add_task(storable_file, storage_uri, 'delete', task)
        self._drop_tasks(storable_file, storage_uri, 'write')
        #}}}



    def _before_flush_callback(self):
        def _before_flush(session, flush_context, instances):#{{{
            log.debug("*** before flush callback")
            log.debug("session.new: %s" % session.new)
            log.debug("session.dirty: %s" % session.dirty)
            log.debug("session.deleted: %s" % session.deleted)

            # Make sure that _storable_file of all storables in session
            # is in session as well.
            for obj in session:

                if isinstance(obj, Storable) and obj not in session.deleted:
                    if obj.id is not None:
                        log.debug("> add storable_file: %s" % obj._storable_file.storage_uri)
                        session.add(obj._storable_file)
                    else:
                        # obj has no id yet. we have to let this flush
                        # go my first so that obj gets inserted into database
                        # and receives an id. After flush we can set the id on
                        # obj._storable_file and add it to session as well.
                        log.debug("> bind after flush: %s" % obj._storable_file)
                        self._unbound.append(obj._storable_file)
            # Make sure that if _storable_file.bind is not in session
            # _storable_file is not in session either.
            for storable_file in self._tasks:
                if storable_file.bind not in session\
                and storable_file in session:
                    log.debug("> expunging storage_file: %s (bind not in session)" % storable_file)
                    session.expunge(storable_file)
            # Make sure that if _storable_file.bind gets deleted,
            # _storable_file gets deleted as well.
            for obj in session.deleted:
                if isinstance(obj, Storable):
                    # Delete all that got deleted from database
                    # after next commit.
                    self.delete_on_commit(obj._storable_file)
                    log.debug("> delete obj._storable_file: %s" % obj)
                    Session.delete(obj._storable_file)
        return _before_flush
        #}}}


    def _after_flush_callback(self):
        def _after_flush(session, flush_context):#{{{
            log.debug("*** after flush callback")
            for storable_file in self._unbound:
                log.debug("> binding obj: %s to %s" % (storable_file, storable_file.bind))
                Session.add(storable_file.bind)
                storable_file.bind_id = storable_file.bind.id
                Session.add(storable_file)
            self._unbound = list()
        return _after_flush
        #}}}


    def _before_commit_callback(self):
        def _before_commit(session):#{{{
            log.debug("*** before commit callback")
            # perform write/delete action
            for storable_file in self._tasks:
                if storable_file in session:
                    self._committed.append(storable_file)
        return _before_commit
        #}}}

    def _after_commit_callback(self):
        def _after_commit(session):#{{{
            log.debug("*** after commit callback")
            # perform write/delete action
            drop_tasks = list()
            for storable_file in self._tasks:
                if storable_file in self._committed:
                    for storage_uri in self._tasks[storable_file]:
                        tasks = self._tasks[storable_file][storage_uri]
                        if 'delete' in tasks:
                            tasks['delete']['callback'](session, storage_uri)
                        elif 'write' in tasks:
                            tasks['write']['callback'](session, storage_uri)
                    self._committed.remove(storable_file)
                    drop_tasks.append(storable_file)
            for storable_file in drop_tasks:
                self._drop_tasks(storable_file)
        return _after_commit
        #}}}


    def _after_rollback_callback(self):
        def _after_rollback(session, previous_transaction):#{{{
            self._tasks = dict()
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


    def _on_storage_uri_change(self, storable_file, new_uri):

        if new_uri is None:
            raise StorageError('You cannot set the uri to None.')

        if storable_file not in Session:
            Session.add(storable_file)

        old_uri = storable_file.storage_uri

        if old_uri is None:
            # If we had no url before, now we do.
            self._change_task_storage_uri(storable_file, '', new_uri)
            storable_file.storage_uri = new_uri
            return


        if not has_identity(storable_file):
            # forget about the prior uri and just replace it
            # with the new one.
            self._change_task_storage_uri(storable_file, old_uri, new_uri)
            storable_file.storage_uri = new_uri
            return


        if has_identity(storable_file):
            data = storable_file.data
            storable_file.data = None # delete data from old uri
            storable_file.storage_uri = new_uri
            storable_file.data = data # write data to new uri
            return



    def _add_task(self, storable_file, storage_uri, action, task):
        if storable_file not in self._tasks:
            self._tasks[storable_file] = dict()
        if storage_uri not in self._tasks[storable_file]:
            self._tasks[storable_file][storage_uri] = dict()
        self._tasks[storable_file][storage_uri][action] = task


    def _drop_tasks(self, storable_file=None, storage_uri=None, action=None):
        try:
            if action is not None and storage_uri is not None and storable_file is not None:
                del(self._tasks[storable_file][storage_uri][action])
            elif storage_uri is not None and storable_file is not None:
                del(self._tasks[storable_file][storage_uri])
            elif storable_file is not None:
                del(self._tasks[storable_file])
        except KeyError:
            pass


    def _change_task_storage_uri(self, storable_file, old_uri, new_uri):
        if storable_file not in self._tasks:
            return
        if old_uri not in self._tasks[storable_file]:
            return
        for action in self._tasks[storable_file][old_uri]:
            self._add_task(storable_file, new_uri, action, self._tasks[storable_file][old_uri][action])
        self._drop_tasks(storable_file, old_uri)



    def _get_db_state(self, obj):
        # Transient - an instance that’s not in a session, and is not saved
        # to the database; i.e. it has no database identity. The only relationship
        # such an object has to the ORM is that its class has a mapper() associated with it.
        if object_session(obj) is None and not has_identity(obj):
            return 'transient'

        # Pending - when you add() a transient instance, it becomes pending. It still
        # wasn’t actually flushed to the database yet, but it will be when the next flush occurs.
        if object_session(obj) is not None and not has_identity(obj):
            return 'pending'

        # Persistent - An instance which is present in the session and has a record in
        # the database. You get persistent instances by either flushing so that the pending
        # instances become persistent, or by querying the database for existing instances
        # (or moving persistent instances from other sessions into your local session).
        if object_session(obj) is not None and has_identity(obj):
            return 'persistent'

        # Detached - an instance which has a record in the database, but is not in any
        # session. There’s nothing wrong with this, and you can use objects normally
        # when they’re detached, except they will not be able to issue any SQL in order
        # to load collections or attributes which are not yet loaded, or were marked as “expired”.
        if object_session(obj) is None and has_identity(obj):
            return 'detached'




class StorableClass(Base):
    __tablename__ = 'StorableClass'
    __table_args__ = {'mysql_engine':'InnoDB', 'mysql_charset':'utf8'}

    id                = sa.Column(sa.BigInteger(20, unsigned=True), primary_key=True, autoincrement=True)
    tablename         = sa.Column(sa.String(length=255), index=True, unique=True )


class StorableFile(Base):
    __tablename__ = 'StorableFile'
    __table_args__ = (Index('ix_StorableFile_bind', "bind_class_id", "bind_id"), )

    id                  = sa.Column(sa.BigInteger(20, unsigned=True), primary_key=True, autoincrement=True)

    bind_class_id       = sa.Column(sa.BigInteger(20, unsigned=True), sa.ForeignKey('StorableClass.id', onupdate='CASCADE', ondelete='SET NULL'))
    bind_id             = sa.Column(sa.BigInteger(20, unsigned=True))

    storage_uri         = sa.Column(sa.String(length=255), index=True, unique=True )
    content_length      = sa.Column(sa.BigInteger(20, unsigned=True))
    content_checksum    = sa.Column(sa.Text(length=32))
    creation_time       = sa.Column(sa.DateTime(), nullable=False, default=sa.func.now())
    modification_time   = sa.Column(sa.DateTime(), nullable=False, default=sa.func.now())



class Storable(object):

    def on_create_new_instance(self):
        self._set_storable_class()
        self._storable_file = StorableFile()
        self._storable_file.bind_class_id = self._storable_class_id
        self._storable_file.bind = self
        log.debug("> created new Storable instance: %s" % self)
        log.debug("> created new StorableFile instance: %s" % self._storable_file)

    @sa.orm.reconstructor
    def on_load_from_session(self):
        self._set_storable_class()
        try:
            self._storable_file = Session\
                .query(StorableFile)\
                .filter_by(bind_class_id = self._storable_class_id)\
                .filter_by(bind_id = self.id)\
                .one()
            self._storable_file.bind = self
            log.debug("> loaded existing Storable instance: %s" % self)
            log.debug("> loaded existing StorableFile instance: %s" % self._storable_file)
        except sa.orm.exc.NoResultFound:
            self._storable_file = StorableFile()
            self._storable_file.bind_class_id = self._storable_class_id
            self._storable_file.bind_id = self.id
            self._storable_file.bind = self
            Session.add(self._storable_file)
            log.debug("> loaded existing Storable instance: %s" % self)
            log.debug("> created new StorableFile instance: %s" % self._storable_file)


    def _set_storable_class(self):
        if not hasattr(self.__class__, '__storable_class_id__'):
            try:
                self.__class__.__storable_class_id__ = Session\
                    .query(StorableClass.id)\
                    .filter_by(tablename = self.__tablename__)\
                    .scalar()
                log.debug("> loaded existing StorableClass: %s" % self.__tablename__)
            except sa.orm.exc.NoResultFound:
                log.debug("> created new StorableClass: %s" % self.__tablename__)
                storable_class = StorableClass()
                storable_class.tablename = self.__tablename__
                Session.add(storable_class)
                Session.flush()
                self.__class__.__storable_class_id__ = storable_class.id
        self._storable_class_id = self.__class__.__storable_class_id__


    def _get_file_contents(self):
        return self._storage.read(self._storable_file)
    def _set_file_contents(self, data):
        self._storage.write(self._storable_file, data)
    data = property(_get_file_contents, _set_file_contents)


    def _get_storage_uri(self):
        return self._storable_file.storage_uri
    def _set_storage_uri(self, uri):
        self._storage._on_storage_uri_change(self._storable_file, uri)
    storage_uri = property(_get_storage_uri, _set_storage_uri)




