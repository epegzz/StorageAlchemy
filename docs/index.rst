
.. automodule:: storagealchemy

Storage
*******

The storage lib provides the possibility to access different kinds of storages
through one unified api.

It uses sqalchemy to store meta information about the files it manages.


Main Features
=============

* **sqlalchemy transactions**: All write/delete actions will be rolled back whenever the sqlalchemy session gets rolled back.
  It behaves the same way, as if you would store the data in your sql database, but actually uses
  any kind of storage you like.

* **easy integration**: The storage can use any class that extends sqlalchemy Base/DeclarativeBase
  for storing file information. That way it nicely integrates into your sqlalchemy project.



Creating a StorageFile class
============================

In order to allow storagealchemy to handle a python object, the object
must inherit from :class:`Storable`. To create your own
storable files, Simply subclass from :class:`Storable`::

    from storagealchemy import Storable

    class Picture(Storable, Base):
        __tablename__ = 'Picture'

        # some columns you might want
        id = sa.Column(BigInteger(20, unsigned=True), primary_key=True, autoincrement=True)
        width = Column(Integer(10))
        height = Column(Integer(10))

    picture = Picture()
    picture.uri = 'storage://path/to/file'
    picture.data = some_image_data

    # Add the picture to a sqlachemy session.
    Session.add(picture)
    Session.commit()

    # That`s it. Now the picture is stored in the database and on the storage.
    # Wasn`t that easy? :)

    # Now let's delete it
    Session.delete(picture)

    # done ;)


.. :warning:

    Note that you you have to use **__add_table_args__** instead of
    **__table_args__**.


API
===

.. automodule:: storagealchemy
.. autoclass:: Storage
    :members: add_handler

.. autoclass:: Storable
    :members: read, write
