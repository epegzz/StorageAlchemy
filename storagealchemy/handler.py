# -*- coding:utf8 -*-

import logging
import os
import time
import datetime

from .exception import StorageError, NoSuchFile

log = logging.getLogger(__name__)



class FilesystemHandler(object):
    """
    Stores files in local directory.

    """

    storage_path = None

    def __init__(self, storage_path, uid, gid, max_lock_time = 10):
        self.storage_path = os.path.realpath(storage_path)
        self.uid = uid
        self.gid = gid
        self.max_lock_time = max_lock_time  # max lock time in seconds


    def has(self, path):
        return os.path.isfile(os.path.join(self.storage_path, path))


    def read(self, path, **kwargs):#{{{
        if path.startswith('/'):
            path = path[1:]
        path = os.path.join(self.storage_path, path)

        if not os.path.isfile(path):
            raise NoSuchFile(path)

        try:
            fp = open(path, 'r')
            data = fp.read()
            fp.close()
        except IOError:
            raise NoSuchFile(path)

        if not path.startswith('.') and not path.endswith('.lock'):
            log.debug('storage.read: path=%(path)s, len(data) = %(len_data)s' % dict(path=path, len_data=len(data)))
        return data#}}}


    def write(self, path, data, **kwargs):#{{{
        if not path.startswith('.') and not path.endswith('.lock'):
            log.debug('storage.write: path=%(path)s, len(data) = %(len_data)s' % dict(path=path, len_data=len(data)))

        if path.startswith('/'):
            path = path[1:]
        path = os.path.join(self.storage_path, path)
        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        fp = open(path, 'w')
        fp.write(data)
        fp.close()
        os.chown(path, int(self.uid), int(self.gid))
        return True#}}}


    def delete(self, path, **kwargs):#{{{
        if not path.startswith('.') and not path.endswith('.lock'):
            log.debug('filesystem.storage.delete: path=%(path)s' % dict(path=path))
        if path.startswith('/'):
            path = path[1:]
        path = os.path.realpath(os.path.join(self.storage_path, path))

        if not os.path.isfile(path):
            raise NoSuchFile(path)

        if not path.startswith(self.storage_path):
            raise StorageError

        os.remove(path)

        path = os.path.realpath(os.path.dirname(path))
        while not len(os.listdir(path)):
            if os.path.realpath(self.storage_path) == os.path.realpath(path):
                break
            os.rmdir(path)
            path = os.path.realpath(os.path.join(path, os.path.pardir))
        return True#}}}



    def lock(self, path):
        while self.is_locked(path):
            time.sleep(0.1)
        timestamp = time.time()
        self.write(".%s.lock" % path, str(timestamp))
        log.debug('filesystem.storage.lock: path=%(path)s' % dict(path=path))
        return timestamp


    def unlock(self, path, lock=None, force=False):
        lock_path = ".%s.lock" % path
        try:
            if not force:
                lock_timestamp = self.read(lock_path)
                if lock_timestamp != str(lock):
                    return
        except NoSuchFile:
            return
        try:
            self.delete(lock_path)
            log.debug('filesystem.storage.unlock: path=%(path)s' % dict(path=path))
        except NoSuchFile:
            pass


    def get_lock(self, path):
        lock_path = ".%s.lock" % path
        if not self.has(lock_path):
            return None
        try:
            lock_timestamp = float(self.read(lock_path))
            lock_time = datetime.datetime.fromtimestamp(lock_timestamp)
            if self.max_lock_time < (datetime.datetime.now() - lock_time).total_seconds():
                self.unlock(path, force=True)
                return None
            else:
                return lock_timestamp
        except NoSuchFile:
            return None


    def is_locked(self, path):
        return self.get_lock(path) is not None






class DevNullStorage(object):
    """
    Dummy storage which does not store files at all.
    """

    def has(self, path):
        return False

    def read(self, path, **kwargs):
        return None

    def write(self, path, data, **kwargs):
        return True

    def delete(self, path, **kwargs):
        return True

    def lock(self, path):
        pass

    def unlock(self, path):
        pass

    def is_locked(self, path):
        return False





