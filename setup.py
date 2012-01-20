# -*- coding:utf-8 -*-

from distutils.core import setup

setup(
    name='StorageAlchemy',
    version='0.1.0',
    description='Feels like storing file contents in the database, but actually stores them wherever you like to. Combines the power of transactional database queries with the freedom of storing data using whatever external API you like.',
    url='http://github.com/epegzz/StorageAlchemy',

    author='Daniel Schäfer',
    author_email='epegzz@gmail.com',
    license='BSD-3',

    packages=['storagealchemy'],

    install_requires=['sqlalchemy'],

    classifiers=[
       'Development Status :: 5 - Production/Stable',
       'Intended Audience :: Developers',
       'License :: OSI Approved :: BSD License',
       'Natural Language :: English',
       'Operating System :: OS Independent',
       'Programming Language :: Python :: 2.5',
       'Programming Language :: Python :: 2.6',
       'Programming Language :: Python :: 2.7',
       'Topic :: Software Development :: Libraries :: Python Modules',
   ],
)
