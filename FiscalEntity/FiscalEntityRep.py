
import logging
from abc import ABC, abstractmethod

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
    def create_key(self, item):
        raise NotImplementedError
    @abstractmethod
    def create_req(self, item):
        raise NotImplementedError
    @abstractmethod
    def create_req_extr(self, item):
        raise NotImplementedError


    @abstractmethod
    def update(self, item):
        raise NotImplementedError

    @abstractmethod
    def delete(self, id):
        raise NotImplementedError


class MySQLRepositoryfiscalentity(IRepository):
    def __init__(self, host, db, user, password):
        # initialize database connection
        try:
            connection = mysql.connector.connect(host=host,
                                                 database=db,
                                                 user=user,
                                                 password=password)
        except Exception as e:
            print(e)
            return None
        self._connection = connection

    def get_all(self):
        # query all records from database
        cursor = self._connection.cursor()
        cursor.execute("SELECT * FROM fiscalvault.FiscalEntity")
        results = cursor.fetchall()
        return results

    def get_all_fiscal_keys(self,rfc):
        # query all records from database
        cursor = self._connection.cursor()
        cursor.execute("SELECT * FROM fiscalvault.FiscalEntityKeys where fkrfc = '{}'".format(rfc))
        results = cursor.fetchall()
        return results

    def get_all_fiscal_extractions(self,rfc):
        # query all records from database
        cursor = self._connection.cursor()
        cursor.execute(
            """SELECT `meta`.`idFiscalDigitalExtraction` as id,
                       `meta`.`RFC`,
                       `meta`.`Type`,
                       `meta`.`ExtrMode`,
                       `meta`.`Month`,
                       `meta`.`FiscalYear`,
                       `extdet`.IdSolicitud,
                       `extdet`.FechaIni,
                       `extdet`.Fechafin,
                       `extdet`.Status
                FROM `fiscalvault`.`FiscalDigitalExtMeta` as meta
                inner join fiscalvault.FiscalDigitalExtraction as extdet on
                      meta.idFiscalDigitalExtraction = extdet.idFiscalDigitalExtraction and
                      meta.RFC = extdet.RFC
                where meta.rfc = '{}'""".format(rfc))
        results = cursor.fetchall()
        return results
    def get_by_id(self, rfc, log):
        # query record by id from database
        cursor = self._connection.cursor()
        try:
            sentence = "SELECT * FROM fiscalvault.FiscalEntity WHERE RFC='{}'".format(rfc)
            cursor.execute(sentence)
            result = cursor.fetchone()
        except Exception as e:
            logging.error('The record with the RFC {} does not exist'.format(rfc))
        return result

    def create(self, FiscalEntity,log):
        # insert record into database
        cursor = self._connection.cursor()
        record = self.get_by_id(FiscalEntity['rfc'], log)
        if not record:
            sentence = "INSERT INTO fiscalvault.FiscalEntity(rfc,nombre,CURP,Nombres,Apellido,NombreComercial,CreationT,UsrCreation) VALUES ('{}','{}','{}','{}','{}','{}',{},'{}')".format(FiscalEntity['rfc'], FiscalEntity['nombre'], FiscalEntity['CURP'], FiscalEntity['Nombres'], FiscalEntity['Apellido'], FiscalEntity['NombreComercial'], 'now()', FiscalEntity['UsrCreation'])
            try:
                cursor.execute(sentence)
                self._connection.commit()
            except Exception as e:
                log.error(e)
        else:
            log.error('The record {} exists'.format(FiscalEntity['rfc']))
            return 0
        return FiscalEntity

    def create_key(self, rfc, keytype, passw, raw, scndkey, log):
        # insert record into database
        cursor = self._connection.cursor()
        record = self.get_by_id(rfc, log)
        if record:
            insert_blob_tuple = (rfc, keytype, raw, passw, scndkey)
            sentence = "INSERT INTO fiscalvault.FiscalEntityKeys(fkrfc, keyType, Raw, Keysecret, 2ndkey) VALUES (%s,%s,%s,%s,%s)"
            try:
                cursor.execute(sentence, insert_blob_tuple)
                self._connection.commit()
            except Exception as e:
                log.error(e)
        else:
            log.error('The record {} does not exists'.rfc)
            return 0
        return insert_blob_tuple

    def create_req(self, rfc, type, extrmode, month, fiscalyear, log):
        # insert record into database
        cursor = self._connection.cursor()
        record = self.get_by_id(rfc, log)
        insert_tuple = (rfc, type, extrmode, month, fiscalyear)
        if record:
            sentence = "INSERT INTO fiscalvault.FiscalDigitalExtMeta(RFC, Type, ExtrMode, Month, FiscalYear) VALUES (%s,%s,%s,%s,%s)"
            try:
                record = cursor.execute(sentence, insert_tuple)
                self._connection.commit()
                id = cursor.lastrowid
                insert_tuple = (id, rfc, type, extrmode, month, fiscalyear)
            except Exception as e:
                log.error(e)
        else:
            log.error('The record {} does not exists'.rfc)
            return 0
        return insert_tuple

    def create_req_extr(self, id, rfc, idsol, fechaIni, fechaFin, status, creation, log):
        # insert record into database
        cursor = self._connection.cursor()
        record = self.get_by_id(rfc, log)
        insert_tuple = (id, rfc, idsol, fechaIni, fechaFin, status, creation)
        if record:
            sentence = "INSERT INTO fiscalvault.FiscalDigitalExtraction(idFiscalDigitalExtraction, RFC, IdSolicitud, FechaIni, Fechafin, Status, Creation) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            try:
                record = cursor.execute(sentence, insert_tuple)
                self._connection.commit()
            except Exception as e:
                log.error(e)
        else:
            log.error('The record {} does not exists'.rfc)
            return 0
        return insert_tuple

    def update_fiscal_key(self, rfc, keytype, passw, raw, scndkey, log):
        # update record in database
        update_blob_tuple = (raw, passw, scndkey, rfc, keytype)
        cursor = self._connection.cursor()
        sentence = "UPDATE fiscalvault.FiscalEntityKeys SET Raw=%s, keysecret=%s, 2ndkey=%s where fkRFC = %s and keyType=%s"
        try:
            cursor.execute(sentence, update_blob_tuple)
            self._connection.commit()
        except Exception as e:
            log.error(e)
        if cursor.rowcount > 0:
            log.debug('Fiscal entity RFC {} updated'. format(rfc))
        return cursor.rowcount > 0

    def update(self, FiscalEntity, log):
        # update record in database
        cursor = self._connection.cursor()
        sentence = "UPDATE fiscalvault.FiscalEntity SET nombre='{}', Nombres='{}', Apellido='{}', NombreComercial='{}', ChangeT='{}', UsrChange='{}' where RFC = '{}'".format(FiscalEntity['nombre'], FiscalEntity['Nombres'], FiscalEntity['Apellido'], FiscalEntity['NombreComercial'], 'now()',  FiscalEntity['UsrChange'], FiscalEntity['RFC'],)
        try:
            cursor.execute(sentence)
            self._connection.commit()
        except Exception as e:
            log.error(e)
        if cursor.rowcount > 0:
            log.debug('Fiscal entity RFC {} updated'. format(FiscalEntity['RFC']))
        return cursor.rowcount > 0

    def delete(self, rfc,log):
        # delete record from database
        try:
            cursor = self._connection.cursor()
            sentence = "DELETE FROM fiscalvault.FiscalEntity WHERE RFC='{}'".format(rfc)
            cursor.execute(sentence)
            self._connection.commit()
            log.debug('Record with RFC {} has been deleted'.format(rfc))
        except Exception as e:
            log.error(e)
        if cursor.rowcount < 0:
            log.error('No records match with the criteria {}'.format(rfc))
        return cursor.rowcount > 0


