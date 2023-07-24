import base64
import calendar
import logging
import os
from datetime import datetime
from cfdiclient import (Autenticacion, DescargaMasiva, Fiel, SolicitaDescarga,
                        VerificaSolicitudDescarga)
from cryptography.fernet import Fernet


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

    def get_all_fiscal_extractions(self, rfc):
        return self._repository.get_all_fiscal_extractions(rfc)
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
        self.attributes['extractions'] = self.get_all_fiscal_extractions(self.attributes['rfc'])
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
                       'NombreComercial': nombrecomercial, 'UsrChange': systemuser,
                       'ChangeT': datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")}

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
        now = datetime.now()
        key = Fernet.generate_key()
        f = Fernet(key)
        bstring = bytes(psw, 'utf-8')
        token = f.encrypt(bstring)

        fiscal_key = {
            'rfc': self.attributes['rfc'],
            'keyType': keytype,
            'Raw': raw,
            'keysecret': token,
            '2ndkey': key
        }
        if len(fiscal_key['Raw']) == 0:
            fiscal_key['Raw'] = "''"
        result = self._repository.create_key(fiscal_key['rfc'], fiscal_key['keyType'], fiscal_key['keysecret'],
                                             fiscal_key['Raw'], key, self.log)
        if type(result) is tuple:
            self.log.debug('The key {},{} can be created'.format(fiscal_key['rfc'], fiscal_key['keyType']))
        else:
            self.keys.append(fiscal_key)
            self.log.error('The key {},{} cannot be created'.format(fiscal_key['rfc'], fiscal_key['keyType']))
        return result

    def create_req(self, rfc, rtype, extrmode, month, fiscalyear, ):

        fiscal_key = {
            'rfc': self.attributes['rfc'],
            'Type': rtype,
            'ExtrMode': extrmode,
            'Month': month,
            'FiscalYear': fiscalyear
        }

        result = self._repository.create_req(fiscal_key['rfc'], fiscal_key['Type'], fiscal_key['ExtrMode'],
                                             fiscal_key['Month'], fiscal_key['FiscalYear'], self.log)
        if type(result) is tuple:
            self.log.debug(
                'The extraction metadata {},{},{},{} can be created'.format(fiscal_key['Type'], fiscal_key['ExtrMode'],
                                                                            fiscal_key['Month'],
                                                                            fiscal_key['FiscalYear']))
        else:
            self.keys.append(fiscal_key)
            self.log.debug(
                'The extraction metadata {},{},{},{} can be created'.format(fiscal_key['Type'], fiscal_key['ExtrMode'],
                                                                            fiscal_key['Month'],
                                                                            fiscal_key['FiscalYear']))
        return result

    def create_req_ext(self, id_extr, idsol, fecha_ini, fecha_fin, status, creation):

        fiscal_key = {
            'idFiscalDigitalExtraction': id_extr,
            'RFC': self.attributes['rfc'],
            'IdSolicitud': idsol,
            'FechaIni': fecha_ini,
            'FechaFin': fecha_fin,
            'Status': status,
            'Creation': creation
        }

        result = self._repository.create_req_extr(fiscal_key['idFiscalDigitalExtraction'], fiscal_key['RFC'], fiscal_key['IdSolicitud'],
                                              fiscal_key['FechaIni'], fiscal_key['FechaFin'], fiscal_key['Status'], fiscal_key['Creation'], self.log)
        if type(result) is tuple:
            self.log.debug(
                'The extraction request {},{},{},{} can be created'.format(fiscal_key['RFC'], fiscal_key['IdSolicitud'],
                                                                            fiscal_key['FechaIni'],
                                                                            fiscal_key['FechaFin']))
        else:
            self.log.debug(
                'The extraction request {},{},{},{} can be created'.format(fiscal_key['RFC'], fiscal_key['IdSolicitud'],
                                                                           fiscal_key['FechaIni'],
                                                                           fiscal_key['FechaFin']))
        return result

    def update_fiscal_key(self, keytype, psw, raw):

        now = datetime.datetime.now()
        key = Fernet.generate_key()
        f = Fernet(key)
        token = f.encrypt(bytes(psw))

        fiscal_key = {
            'rfc': self.attributes['rfc'],
            'keyType': keytype,
            'Raw': raw,
            'keysecret': token,
            '2ndkey': key
        }
        if len(fiscal_key['Raw']) == 0:
            fiscal_key['Raw'] = "''"
        result = self._repository.update_fiscal_key(fiscal_key['rfc'], fiscal_key['keyType'], fiscal_key['keysecret'],
                                                    fiscal_key['Raw'], fiscal_key['2ndkey'], self.log)
        if type(result) is tuple:
            self.log.debug('The key {},{} can be updated'.format(fiscal_key['rfc'], fiscal_key['keyType']))
        else:
            self.keys.append(fiscal_key)
            self.log.error('The key {},{} cannot be updated'.format(fiscal_key['rfc'], fiscal_key['keyType']))
        return result

    def read_all_fiscal_keys(self):
        return self._repository.get_all_fiscal_keys(self.attributes['rfc'])

    def req_month_digital_docs(self, extraction_basis, FiscalYear, Month):

        for item in self.attributes['keys']:
            match item[1]:
                case 'PSW':
                    f = Fernet(item[4])
                    FIEL_PAS = bytes.decode(f.decrypt(item[3]), 'utf-8')
                case 'CER':
                    cer_der = item[2]
                case 'KEY':
                    key_der = item[2]

        match extraction_basis:
            case 'D':
                a = 1
            case 'W':
                a = 1
            case 'H':
                a = 1
            case 'M':
                now = datetime.now()
                current_time = now.strftime("%H%M%S")
                fiel = Fiel(cer_der, key_der, FIEL_PAS)
                auth = Autenticacion(fiel)
                token = auth.obtener_token()
                descarga = SolicitaDescarga(fiel)
                first_dt = datetime(FiscalYear, Month, 1)
                res = calendar.monthrange(first_dt.year, first_dt.month)
                end_date = datetime(FiscalYear, Month, res[1], int(current_time[0:1]), int(current_time[2:3]),
                                    int(current_time[3:4]))
                fecha_inicial = first_dt.date()
                fecha_final = end_date
                current_time = now.strftime("%H:%M:%S")

                extr_metar = self.create_req(self.attributes['rfc'], 'R', extraction_basis, Month, FiscalYear)
                if type(extr_metar) is tuple:
                    sol_recibidos = descarga.solicitar_descarga(token, self.attributes['rfc'], fecha_inicial, fecha_final,
                                                                rfc_receptor=self.attributes['rfc'], tipo_solicitud='CFDI')
                    if sol_recibidos['cod_estatus'] == '5000':
                        self.create_req_ext(extr_metar[0], sol_recibidos['id_solicitud'], fecha_inicial, fecha_final, 'R', datetime.now())


                extr_metae = self.create_req(self.attributes['rfc'], 'E', extraction_basis, Month, FiscalYear)
                if type(extr_metae) is tuple:
                    sol_emitidos = descarga.solicitar_descarga(token, self.attributes['rfc'], fecha_inicial, fecha_final,
                                                               rfc_emisor=self.attributes['rfc'], tipo_solicitud='CFDI')
                    if sol_emitidos['cod_estatus'] == '5000':
                        self.create_req_ext(extr_metae[0], sol_emitidos['id_solicitud'], fecha_inicial, fecha_final, 'R', datetime.now())

    def download_pending_requests(self, path_base):
        #Validate base processing path exists or not
        path_complete = path_base + '/' + self.attributes['rfc']
        isExist = os.path.exists(path_complete)
        mode = 0o777
        if not isExist:
            os.mkdir(path_complete, mode)
        #Validate zip processing path exists or not
        path_complete = path_complete + '/downloaded'
        isExist = os.path.exists(path_complete)
        if not isExist:
            os.mkdir(path_complete, mode)
        #Validate pending extractions
        if 'extractions' in self.attributes:
            print(self.attributes['keys'])
            for item_key in self.attributes['keys']:
                match item_key[1]:
                    case 'PSW':
                        f = Fernet(item_key[4])
                        FIEL_PAS = bytes.decode(f.decrypt(item_key[3]), 'utf-8')
                    case 'CER':
                        cer_der = item_key[2]
                    case 'KEY':
                        key_der = item_key[2]
            path_complete = path_complete + '/'
            # Download the pending extractions
            for item in self.attributes['extractions']:
                print(item)
                if item[9] == 'R':
                    fiel = Fiel(cer_der, key_der, FIEL_PAS)
                    auth = Autenticacion(fiel)
                    token = auth.obtener_token()
                    verificacion = VerificaSolicitudDescarga(fiel)
                    verificacion = verificacion.verificar_descarga(token, self.attributes['rfc'], item[6])
                    estado_solicitud = int(verificacion['estado_solicitud'])
                    # 0, Token invalido.
                    # 1, Aceptada
                    # 2, En proceso
                    # 3, Terminada
                    # 4, Error
                    # 5, Rechazada
                    # 6, Vencida

                    if estado_solicitud <= 2:
                        print(item[6], 'status:', estado_solicitud)
                        continue
                    elif estado_solicitud >= 4:
                        print(item[6],',ERROR:', estado_solicitud)
                        continue
                    else:
                        # Si el estatus es 3 se trata de descargar los paquetes
                        print(verificacion['paquetes'])
                        for paquete in verificacion['paquetes']:
                            descarga = DescargaMasiva(fiel)
                            descarga = descarga.descargar_paquete(token, self.attributes['rfc'], paquete)
                            print(descarga)
                            print('PAQUETE: ', paquete, descarga['paquete_b64'])
                            if 'paquete_b64' in descarga and descarga['paquete_b64'] != None:
                                with open(path_complete + '{}.zip'.format(paquete), 'wb') as fp:
                                    fp.write(base64.b64decode(descarga['paquete_b64']))
                                    break
                    #TODO: set the status of the request
