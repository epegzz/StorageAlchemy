from unittest import TestCase

from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
import sqlahelper

meta = MetaData()
engine = create_engine('mysql://test:test@localhost:3306/test?charset=utf8&use_unicode=0')
Base = declarative_base(metadata = meta)
Base.metadata.bind = engine
sqlahelper.add_engine(engine)

Session = scoped_session(sessionmaker(bind=engine))

database_created = False


class BaseTestCase(TestCase):

    def init_testing_db(self):#{{{
        global database_created
        if database_created:
            return
        # create database
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        database_created = True
        #}}}

    def setUp(self):#{{{
        self.init_testing_db()

    def tearDown(self):#{{{
        #clear_models(Session)
        #Session.rollback()#}}}
        pass


