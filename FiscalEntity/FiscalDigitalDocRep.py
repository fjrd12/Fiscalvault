import logging
from abc import ABC, abstractmethod
from pymongo import MongoClient
URI = 'mongodb://root:rjon2457@localhost:27017/?retryWrites=true&w=majority'
DB_NAME = 'fiscalvault'

import mysql.connector
class IRepository(ABC):
    @abstractmethod
    def get_all(self):
        raise NotImplementedError

    @abstractmethod
    def get_by_id(self, id):
        raise NotImplementedError

    @abstractmethod
    def create(self, item):
        raise NotImplementedError

    @abstractmethod
    def update(self, item):
        raise NotImplementedError

    @abstractmethod
    def delete(self, id):
        raise NotImplementedError


class MongoRepositoryfiscalDigitalDoc(IRepository):
    def __init__(self, host, db, user, password):
        # initialize database connection
        try:
            connection_string = 'mongodb://{}:{}@{}:27017/?retryWrites=true&w=majority'.format(user, password, host)
            connection = MongoClient(connection_string)
            db = connection.fiscalvault
        except Exception as e:
            return None
        self._connection = connection
        self._db = db

    def get_all(self):
        # # query all records from database
        # cursor = self._connection.cursor()
        # cursor.execute("SELECT * FROM fiscalvault.FiscalEntity")
        # results = cursor.fetchall()
        # return results
        pass

    def get_by_id(self, rfc, log):
        # # query record by id from database
        # cursor = self._connection.cursor()
        # try:
        #     sentence = "SELECT * FROM fiscalvault.FiscalEntity WHERE RFC='{}'".format(rfc)
        #     cursor.execute(sentence)
        #     result = cursor.fetchone()
        # except Exception as e:
        #     logging.error('The record with the RFC {} does not exist'.format(rfc))
        # return result
        pass

    def create(self, rfc, content, uuid, log):
        # insert record into database
        record = 0
        localcollection = self._db[rfc]
        record = localcollection.find({"uuid": uuid})
        if record.retrieved == 0:
            try:
                result = localcollection.insert_one(content)
            except Exception as e:
                log.error(e)
        else:
            log.error('The record {} {}exists'.format(rfc, content.uuid))
            return 0
        return result

    def update(self, FiscalEntity, log):
        # # update record in database
        # cursor = self._connection.cursor()
        # sentence = "UPDATE fiscalvault.FiscalEntity SET nombre='{}', Nombres='{}', Apellido='{}', NombreComercial='{}', ChangeT='{}', UsrChange='{}' where RFC = '{}'".format(FiscalEntity['nombre'], FiscalEntity['Nombres'], FiscalEntity['Apellido'], FiscalEntity['NombreComercial'], 'now()',  FiscalEntity['UsrChange'], FiscalEntity['RFC'],)
        # try:
        #     cursor.execute(sentence)
        #     self._connection.commit()
        # except Exception as e:
        #     log.error(e)
        # if cursor.rowcount > 0:
        #     log.debug('Fiscal entity RFC {} updated'. format(FiscalEntity['RFC']))
        # return cursor.rowcount > 0
        pass

    def delete(self, rfc,log):
        # delete record from database
        try:
            # cursor = self._connection.cursor()
            # sentence = "DELETE FROM fiscalvault.FiscalEntity WHERE RFC='{}'".format(rfc)
            # cursor.execute(sentence)
            # self._connection.commit()
            log.debug('Record with RFC {} has been deleted'.format(rfc))
        except Exception as e:
            log.error(e)
        pass
        # if cursor.rowcount < 0:
        #     log.error('No records match with the criteria {}'.format(rfc))
        # return cursor.rowcount > 0


