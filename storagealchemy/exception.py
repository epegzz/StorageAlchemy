# -*- coding:utf8 -*-


class StorageError(Exception):
    pass

class NoSuchFile(StorageError):
    pass

class FileExists(StorageError):
    pass

class WaitForLockTimout(StorageError):
    pass

class WaitForUnlockTimout(StorageError):
    pass
