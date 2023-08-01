from FiscalEntity.FiscalEntity import FiscalEntity
from FiscalEntity.FiscalDigitalDoc import FiscalDigitalDocs
from FiscalEntity.FiscalEntityRep import MySQLRepositoryfiscalentity
from FiscalEntity.FiscalDigitalDocRep import MongoRepositoryfiscalDigitalDoc

host = '127.0.0.1'
db = 'fiscalvault'
user = 'root'
password = 'helloworld'
password_mongo = 'rjon2457'
path = '/home/francisco/PycharmProjects/Fiscalvault/workspace'

if __name__ == '__main__':
    MySQL_repository = MySQLRepositoryfiscalentity(host, db, user, password)
    Mongo_repository = MongoRepositoryfiscalDigitalDoc(host, db, user, password_mongo)
    digital_documents = FiscalDigitalDocs(Mongo_repository,'RODF8012119N3')
    fiscal_entity = FiscalEntity(MySQL_repository, digital_documents)
    fiscal_entityr = fiscal_entity.get_fiscal_entity_by_id('RODF8012119N3')
    # fiscal_entity.delete_item('RODF8012119N3')
    # fiscal_entity.create_fiscal_entity('RODF8012119N3','Francisco Jhonathan Rodriguez Diaz', 'RODF801211HNEDZR09', 'Francisco Jhonathan','Rodriguez Diaz', 'NA', 'NA')
    # fiscal_entities = fiscal_entity.get_all_fiscal_entities()
    # print(fiscal_entities)
    # fiscal_entity.update_item('RODF8012119N3', 'Francisco Jhonathan Rodriguez Diaz', 'RODF801211HNEDZR09', 'Francisco Jhonathan','Rodriguez D', 'NA', 'NA')
    # fiscal_entity.create_fiscal_key('PSW', 'Xjon2459','')
        # # Convert digital data to binary format
    # with open('00001000000701080543.cer', 'rb') as file:
    #     binaryData = file.read()
    # fiscal_entity.create_fiscal_key('CER', '', binaryData)
    #
    # # Convert digital data to binary format
    # with open('Claveprivada_FIEL_RODF8012119N3_20230714_144817.key', 'rb') as file:
    #     binaryData = file.read()
    # fiscal_entity.create_fiscal_key('KEY', '', binaryData)



    # fiscal_entity.create_fiscal_key('PSW', 'jasdason2457', '')
    # fiscal_entity.req_month_digital_docs('M', 2023, 1)
    # fiscal_entity.req_month_digital_docs('M', 2023, 2)
    # fiscal_entity.req_month_digital_docs('M', 2023, 3)
    # fiscal_entity.req_month_digital_docs('M', 2023, 4)
    # # # fiscal_entity.req_month_digital_docs('M', 2023, 5)
    # fiscal_entity.req_month_digital_docs('M', 2023, 6)
    # fiscal_entity.download_pending_requests_v2(path)
    # fiscal_entity.unpack_requests(path)
    fiscal_entity.parse_xml_to_main(path)
    # fiscal_entity.parse_xml_to_rep(path)
    # fiscal_entity.parse_uuids_archive(path)




