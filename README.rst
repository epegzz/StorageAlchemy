
StorageAlchemy
**************

Use sqlalchemy to commit/rollback changes to files on the filesystem or any other external storage.


Main Features
=============

* **sqlalchemy transactions**: All write/delete actions will be rolled back whenever the sqlalchemy session gets rolled back.
  It behaves the same way, as if you would store the data in your sql database, but actually uses
  any kind of storage you like.


Integration & Configuration
===========================

Right now StorageAlchemy ships with only one StorageHandler, the `FilesystemHandler` which stores
files on the local filesystem. Creating your own handler (that uses for example FTP, MogileFS, …) 
is pretty easy and will be documented pretty soon though.
For now, just go with the following bootstrap code::

    from storagealchemy import Storage
    from storagealchemy.handler import FilesystemHandler

    filesystem_handler = FilesystemHandler\
    ( '/srv/http/myProject/data'        # path to your files
    , uid = 104                         # system uid of file owner
    , gid = 1004                        # system gid of file owner
    )
    storage = Storage()
    # Tell StorageAlchemy to use filesystem_handler for all uris starting with 'file://'.
    # You can add as many handlers for different prefixes as you like.
    storage.add_handler('file://', filesystem_handler)


Using StorageAlchemy
====================

In order to allow StorageAlchemy to handle a python object, the object
must inherit from :class:`Storable`. Here is some little real life example::

    from storagealchemy import Storable

    class Picture(Storable, Base):
        __tablename__ = 'Picture'

        # some fields you might have
        id = sa.Column(BigInteger(20, unsigned=True), primary_key=True, autoincrement=True)
        width = Column(Integer(10))
        height = Column(Integer(10))


    picture = Picture()
    picture.width=100
    picture.height=80
    # Storable adds 'storage_uri' and 'data' properties.
    # We use them to tell StorageAlchemy what and where to save the image data.
    picture.storage_uri = 'file://path/to/picture.png'
    picture.data = some_image_data

    # Add the picture object to a sqlachemy session.
    Session.add(picture)
    Session.commit()

    # That`s it. Now the picture is stored in the database and its raw data on the 
    # storage (in this case on the filesystem).
    # Wasn`t that easy? :)

    # Want to delete both again?
    Session.delete(picture)

    # done ;)




