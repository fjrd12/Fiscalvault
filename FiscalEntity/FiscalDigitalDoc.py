import datetime
import logging


class FiscalDigitalDocs:
    def __init__(self, repository, rfc):
        """This method will be used to create the entity FiscalEntity, and it's abstraction
        :repositoryIrepository: Will have the repository abstraction
        :returns: The object created
        :rtype: object
        """
        self._repository = repository
        self.attributes = {
            'rfc': None
        }
        logging.basicConfig(level=logging.DEBUG)
        self.log = logging.getLogger(__name__)

    def get_all_fiscal_entities(self):
        return self._repository.get_all()

    def create_digital_document(self, rfc, content, uuid):
        result = self._repository.create(rfc, content, uuid, self.log)
        if hasattr(result, 'inserted_id'):
            self.log.debug('The record {} can be created'.format(content))
        else:
            self.log.error('The record {} cannot be created'.format(content))
        return result

    def update_item(self, uuid, content):
        fiscal_document = self._repository.get_by_id(uuid, self.log)
        if fiscal_document is None:
            self.log('Fiscal document with the UUID {} not exist'.format(uuid))
            return False
        result = self._repository.update(content, self.log)
        if result is not None:
            self.attributes = content
        return result

    def delete_item(self, uuid):
        result = self._repository.delete(uuid, self.log)
        if result is not None:
            self.attributes = self.cleared
        return result

