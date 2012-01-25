# -*- coding:utf8 -*-

import logging
import sqlalchemy as sa

import sqlahelper
import transaction

from .exception import StorageError, NoSuchFile
from . import handler

Base = sqlahelper.get_base()
Session = sqlahelper.get_session()


log = logging.getLogger(__name__)

__all__ = ['Storage', 'handler']


class Storage():

    def __init__(self):#{{{
        self._handlers = dict()
        self._tasks = dict()

        def add_hooks(*args, **kwargs):
            sa.event.listen(Session(), "after_soft_rollback", self._after_rollback_callback())
            sa.event.listen(Session(), "after_soft_rollback", add_hooks)
            transaction.get().addAfterCommitHook( self._after_commit_callback() )
            transaction.get().addAfterCommitHook( add_hooks )

        add_hooks()#}}}


    def add_handler(self, scheme, handler, read=True, write=True):#{{{
        self._handlers[scheme] = dict\
            ( handler = handler
            , read = read
            , write = write
            )#}}}


    def read(self, uri):
        #{{{
        """Returns storage contents for uri.

        """
        try:
            return self._tasks[uri]['write']['data']
        except KeyError:
            try:
                storage, path = self._get_storage(uri, 'r')
                return storage.read(path)
            except NoSuchFile:
                return None
        #}}}


    def write(self, uri, data):
        #{{{
        """Writes storage contents for uri.

        """
        if data is None:
            self._delete_on_commit(uri)
        else:
            self._write_on_commit(uri, data)
        #}}}

    def delete(self, uri):
        #{{{
        """Deletes an uri.

        """
        self._delete_on_commit(uri)
        #}}}


    def list(self, uri_path):
        """Returns a list of all uris within a given path.

        """
        storage, path = self._get_storage(uri_path, 'r')
        result = set()
        for file in storage.list(path):
            result.add(file)

        for uri in self._tasks:
            if uri.startswith(uri_path):
                _uri = uri.replace(uri_path,'')
                if _uri.startswith('/'):
                    _uri = uri[1:]
                if len(_uri.split('/')) == 1:
                    if 'delete' not in self._tasks[uri]:
                        result.add(_uri)
                    elif _uri in result:
                        result.remove(_uri)
        return sorted(result)


    def _write_on_commit(self, uri, data):#{{{
        def write_later():
            storage, path = self._get_storage(uri, 'w')
            storage.write(path, data)
        task = dict\
            ( callback = write_later
            , data = data
            )
        self._add_task(uri, 'write', task)
        self._drop_tasks(uri, 'delete')
        #}}}


    def _delete_on_commit(self, uri):#{{{
        def delete_later():
            storage, path = self._get_storage(uri, 'w')
            try:
                storage.delete(path)
            except NoSuchFile:
                pass
        task = dict(callback=delete_later)
        self._add_task(uri, 'delete', task)
        self._drop_tasks(uri, 'write')
        #}}}



    def _after_commit_callback(self):
        def _after_commit(status):#{{{
            # perform write/delete action
            drop_tasks = list()
            for uri in self._tasks:
                tasks = self._tasks[uri]
                if 'delete' in tasks:
                    tasks['delete']['callback']()
                elif 'write' in tasks:
                    tasks['write']['callback']()
                drop_tasks.append(uri)
            for uri in drop_tasks:
                self._drop_tasks(uri)
        return _after_commit
        #}}}


    def _after_rollback_callback(self):
        def _after_rollback(session, previous_transaction):#{{{
            self._tasks = dict()
        return _after_rollback
        #}}}


    def _get_storage(self, uri, mode):#{{{
        uri_scheme = None
        for scheme in self._handlers:
            if uri.startswith("%s://" % scheme):
                if   mode == 'rw' \
                and  self._handlers[scheme]['read'] \
                and  self._handlers[scheme]['write'] :
                     uri_scheme = scheme

                elif mode == 'r' \
                and  self._handlers[scheme]['read']:
                     uri_scheme = scheme

                elif mode == 'w' \
                and  self._handlers[scheme]['write']:
                     uri_scheme = scheme

        if uri_scheme is not None \
        and uri_scheme in self._handlers:
            return self._handlers[uri_scheme]['handler'], uri.replace("%s://" % uri_scheme ,'')

        raise StorageError('could not find storage for uri %s' % uri)
        #}}}


    def _add_task(self, uri, action, task):
        if uri not in self._tasks:
            self._tasks[uri] = dict()
        self._tasks[uri][action] = task


    def _drop_tasks(self, uri=None, action=None):
        try:
            if action is not None and uri is not None:
                del(self._tasks[uri][action])
            elif uri is not None:
                del(self._tasks[uri])
        except KeyError:
            pass


