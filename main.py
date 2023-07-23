from FiscalEntity.FiscalEntity import FiscalEntity
from FiscalEntity.FiscalEntityRep import MySQLRepositoryfiscalentity
import datetime

host = '127.0.0.1'
db = 'fiscalvault'
user = 'root'
password = 'helloworld'
path = ''

if __name__ == '__main__':
    MySQL_repository = MySQLRepositoryfiscalentity(host, db, user, password)
    fiscal_entity = FiscalEntity(MySQL_repository)
    # fiscal_entity.delete_item('RODF8012119N3')
    # fiscal_entity.create_fiscal_entity('RODF8012119N3','Francisco Jhonathan Rodriguez Diaz', 'RODF801211HNEDZR09', 'Francisco Jhonathan','Rodriguez Diaz', 'NA', 'NA')
    # fiscal_entities = fiscal_entity.get_all_fiscal_entities()
    # print(fiscal_entities)
    # fiscal_entity.update_item('RODF8012119N3', 'Francisco Jhonathan Rodriguez Diaz', 'RODF801211HNEDZR09', 'Francisco Jhonathan','Rodriguez D', 'NA', 'NA')
    # fiscal_entityr = fiscal_entity.get_fiscal_entity_by_id('RODF8012119N3')
    fiscal_entityr = fiscal_entity.get_fiscal_entity_by_id('RODF8012119N3')
    # fiscal_entity.create_fiscal_key('PSW', 'lmnopq', '')
    #
    # # Convert digital data to binary format
    # with open('00001000000701080543.cer', 'rb') as file:
    #     binaryData = file.read()
    # fiscal_entity.create_fiscal_key('CER', '', binaryData)
    #
    # # Convert digital data to binary format
    # with open('Claveprivada_FIEL_RODF8012119N3_20230714_144817.key', 'rb') as file:
    #     binaryData = file.read()
    # fiscal_entity.create_fiscal_key('KEY', '', binaryData)




