import datetime
import logging

class FiscalEntity:
    def __init__(self, repository):
        """This method will be used to create the entity FiscalEntity, and it's abstraction
        :repositoryIrepository: Will have the repository abstraction
        :returns: The object created
        :rtype: object
        """
        self._repository = repository
        self.attributes = {
                           'rfc': None,
                           'nombre': None,
                           'CURP': None,
                           'Nombres': None,
                           'Apellido': None,
                           'NombreComercial': None,
                           'CreationT': None,
                           'ChangeT': None,
                           'UsrCreation': None,
                           'UsrChange': None
                           }
        self.keys = []
        self.cleared = self.attributes
        logging.basicConfig(level=logging.DEBUG)
        self.log = logging.getLogger(__name__)

    def get_all_fiscal_entities(self):
        return self._repository.get_all()

    def get_all_fiscal_entities_keys(self, rfc):
        return self._repository.get_all_fiscal_keys(rfc)
    def get_fiscal_entity_by_id(self, rfc):
        record = self._repository.get_by_id(rfc, self.log)

        if type(record) is tuple:
            self.attributes['rfc'] = record[0]
            self.attributes['nombre'] = record[1]
            self.attributes['CURP'] = record[2]
            self.attributes['Nombre'] = record[3]
            self.attributes['Apellido'] = record[4]
            self.attributes['NombreComercial'] = record[5]
            self.attributes['CreationT'] = record[6]
            self.attributes['ChangeT'] = record[7]
            self.attributes['UsrCreation'] = record[8]
            self.attributes['UsrChange'] = record[9]
        self.attributes['keys'] = self.get_all_fiscal_entities_keys(self.attributes['rfc'])
        return record

    def create_fiscal_entity(self, rfc, nombre, curp, nombres, apellido, nombrecomercial, systemuser):
        now = datetime.datetime.now()
        fiscal_entity = {'rfc': rfc,
                         'nombre': nombre,
                         'CURP': curp,
                         'Nombres': nombres,
                         'Apellido': apellido,
                         'NombreComercial': nombrecomercial,
                         'CreationT': now.strftime("%m/%d/%Y, %H:%M:%S"),
                         'UsrCreation': systemuser}
        result = self._repository.create(fiscal_entity, self.log)
        if type(result) is dict:
            self.log.debug('The record {} can be created'.format(fiscal_entity['rfc']))
        else:
            self.attributes = fiscal_entity
            self.log.error('The record {} cannot be created'.format(fiscal_entity['rfc']))
        return result

    def update_item(self, rfc, nombre, curp, nombres, apellido, nombrecomercial, systemuser):
        fiscal_entity = self._repository.get_by_id(rfc, self.log)

        if fiscal_entity is None:
            self.log('Register with RFC {} not exist'.format(rfc))
            return False
        fiscal_dict = {'RFC': rfc, 'nombre': nombre, 'CURP': curp, 'Nombres': nombres, 'Apellido': apellido,
                       'NombreComercial': nombrecomercial, 'UsrChange': systemuser, 'ChangeT': datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}

        result = self._repository.update(fiscal_dict, self.log)
        if result is not None:
            self.attributes = fiscal_entity
        return result

    def delete_item(self, rfc):
        result = self._repository.delete(rfc, self.log)
        if result is not None:
            self.attributes = self.cleared
        return result

    def create_fiscal_key(self, keytype, psw, raw):
        now = datetime.datetime.now()

        fiscal_key = {
                         'rfc': self.attributes['rfc'],
                         'keyType': keytype,
                         'Raw': raw,
                         'keysecret': psw
                          }
        if len(fiscal_key['Raw']) == 0:
            fiscal_key['Raw'] = "''"
        result = self._repository.create_key(fiscal_key['rfc'], fiscal_key['keyType'], fiscal_key['keysecret'], fiscal_key['Raw'], self.log)
        if type(result) is tuple:
            self.log.debug('The key {},{} can be created'.format(fiscal_key['rfc'], fiscal_key['keyType']))
        else:
            self.keys.append(fiscal_key)
            self.log.error('The key {},{} cannot be created'.format(fiscal_key['rfc'], fiscal_key['keyType']))
        return result

    def update_fiscal_key(self, keytype, psw, raw):
        now = datetime.datetime.now()

        fiscal_key = {
                         'rfc': self.attributes['rfc'],
                         'keyType': keytype,
                         'Raw': raw,
                         'keysecret': psw
                          }
        if len(fiscal_key['Raw']) == 0:
            fiscal_key['Raw'] = "''"
        result = self._repository.update_fiscal_key(fiscal_key['rfc'], fiscal_key['keyType'], fiscal_key['keysecret'], fiscal_key['Raw'], self.log)
        if type(result) is tuple:
            self.log.debug('The key {},{} can be updated'.format(fiscal_key['rfc'], fiscal_key['keyType']))
        else:
            self.keys.append(fiscal_key)
            self.log.error('The key {},{} cannot be updated'.format(fiscal_key['rfc'], fiscal_key['keyType']))
        return result

    def read_all_fiscal_keys(self):
        return self._repository.get_all_fiscal_keys(self.attributes['rfc'])
