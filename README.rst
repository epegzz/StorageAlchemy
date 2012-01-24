
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

Documentation coming soon …
