import logging

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager

import traceback


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session_object = scoped_session(sessionmaker())
    # The Session object created here will be used by the function f1 directly.
    session = session_object()
    try:
        yield session
        session.commit()
    except Exception as err:
        session.rollback()
        raise
    finally:
        session.close()


class Database:
    def __init__(self, url: str, name: str = None):
        self.__engine_name = name
        # self.__pool = QueuePool(creator=None, pool_size=30, max_overflow=20, timeout=600) # timeout=600, retry=(30, 3), retry_on_timeout=True,
        self.__engine = create_engine(url, pool_size=30, max_overflow=20, pool_pre_ping=True, pool_use_lifo=True,
                                      pool_recycle=3600)

    def connect(self):
        try:
            return self.__engine.connect()
        except SQLAlchemyError as err:
            logging.error(f'Error creating connection: {err}')
            logging.exception(traceback.print_exc())
            return None

    def session_object(self):
        try:
            return sessionmaker(bind=self.connect())
        # The Session object created here will be used by the function f1 directly.
        except SQLAlchemyError as err:
            logging.error(f'Error creating session maker factory: {err}')
            logging.exception(traceback.print_exc())
            return None

    def session(self):
        try:
            return scoped_session(self.session_object)
        # The Session object created here will be used by the function f1 directly.
        except SQLAlchemyError as err:
            logging.error(f'Error creating session maker factory: {err}')
            logging.exception(traceback.print_exc())
            return None

    def one_close(self, connection):
        try:
            connection.dispose(close=False)
        except SQLAlchemyError as err:
            logging.error(f'Error closing connection: {err}')
            logging.exception(traceback.print_exc())

    def close(self, connection):
        try:
            connection.close()
        except SQLAlchemyError as err:
            logging.error(f'Error closing connection: {err}')
            logging.exception(traceback.print_exc())

    def __del__(self):
        try:
            if self.__engine:
                self.__pool.dispose()
                self.__engine.dispose()
        except AttributeError as err:
            logging.error(f'Engine is already disposed.')

