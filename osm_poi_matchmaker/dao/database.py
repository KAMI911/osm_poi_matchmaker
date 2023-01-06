import logging

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import traceback


class Database:
    def __init__(self, url: str, name: str = None):
        self.__engine_name = name
        self.__pool = QueuePool(creator=None, pool_size=30, max_overflow=20, pool_timeout=600, pool_pre_ping=True,
                                pool_use_lifo=True)
        self.__engine = create_engine(url, pool=self.__pool, retry=(30, 3), retry_on_timeout=True)

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
        except SQLAlchemyError as err:
            logging.error(f'Error creating session maker factory: {err}')
            logging.exception(traceback.print_exc())
            return None

    def close(self, connection):
        try:
            connection.close()
        except SQLAlchemyError as err:
            logging.error(f'Error closing connection: {err}')
            logging.exception(traceback.print_exc())

    def __del__(self):
        if self.__engine:
            self.__pool.dispose()
            self.__engine.dispose()
