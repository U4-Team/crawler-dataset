import re
from datetime import datetime
from typing import IO

import pandas
import phonenumbers

from core.mixins import LoggerMixin
from crawler_dataset.repositories import MongoRepository


class ParseError(Exception):
    pass


class XLSXDatasetService(LoggerMixin):
    mongo_repo: MongoRepository = MongoRepository()
    def parse_dataset_from_file(self, xlsx_file: IO[str]):
        self.logger.info('Parsing dataset...')
        dataframe = pandas.read_excel(xlsx_file)
        dataframe = dataframe.where(dataframe.notnull(), None)
        size, _ = dataframe.shape
        companies = []
        for index, row in dataframe.iterrows():
            progress = round((index/size) * 100, 2)
            if progress == int(progress):
                self.logger.info('Progress: %s', progress)

            try:
                normalized_row = self._normalize_row(row)
            except ParseError:
                continue
            else:
                companies.append(normalized_row.to_dict())

        self.logger.info('Writing datas')
        self.mongo_repo.store_companies(companies)
    
    def _normalize_row(self, row: pandas.Series) -> pandas.Series:
        try:
            spark_id, full_name, short_name, eng_name, inn, kpp, \
            ogrn, registration_date, ceo, legal_address, site, \
            email, phone, okved, okved_descr, msp_type, _, proven_manufacturer, _, _ = row

            spark_id = str(int(spark_id))
            if not full_name:
                raise ValueError('Full name is empty or none')
        
            if not inn:
                raise ValueError('INN is required')
            
            try:
                inn = str(int(inn))
            except Exception as e:
                raise ValueError(f'Unable to parse INN. Reason: {e}')
            try:
                kpp = str(int(kpp))
            except Exception as e:
                raise ValueError(f'Unable to parse KPP. Reason: {e}')
            
            try:
                ogrn = str(int(ogrn))
            except Exception as e:
                raise ValueError(f'Unable to parse OGRN. Reason: {e}')

            try:
                registration_date = datetime.strptime(registration_date, '%Y-%m-%d')
            except:
                registration_date = None
            ceo = ceo.upper() if ceo else None
            legal_address = legal_address.upper() if legal_address else None
            if legal_address:
                legal_address = legal_address.replace('Г. МОСКВА', 'ГОРОД МОСКВА')
            try:
                phones = re.split(';|,', phone) if phone else []
                phones = map(lambda phone: phone.strip(), phones)
                phones = filter(lambda phone: phone, phones)
                _phones = []
                for phone in phones:
                    if not phone.startswith('+') or not phone.startswith('8 '):
                        phone = f'+7{phone}'
                    p = phonenumbers.parse(phone)
                    _phones.append(phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.E164))
                phones = _phones                    
            except Exception as e:
                phones = []
            
            try:
                msp_type = int(msp_type)
                if msp_type == 1:
                    msp_type = 'LEGAL_ENTITY'
                elif msp_type == 2:
                    msp_type = 'INDIVIDUAL'
            except Exception as e:
                raise ValueError(f'Unable to parse MSP type. Reason {e}')

            if not proven_manufacturer:
                raise ValueError('proven manufacturer is required')
            
            if proven_manufacturer.lower() == 'да':
                proven_manufacturer = True
            elif proven_manufacturer.lower() == 'нет':
                proven_manufacturer = False

            new_row = pandas.Series(
                data=[
                    spark_id, full_name, short_name, eng_name, inn, kpp,
                    ogrn, registration_date, ceo, legal_address, site,
                    email, phones, okved, okved_descr, msp_type, proven_manufacturer
                ],
                index=[
                    'SPARK_ID', 'COMPANY_FULL_NAME', 'COMPANY_SHORT_NAME', 'COMPANY_ENG_NAME', 'INN', 'KPP',
                    'OGRN', 'REG_DATE', 'CEO', 'LEGAL_ADDRESS', 'SITE',
                    'EMAIL', 'PHONES', 'OKVED_CODE', 'OKVED_DESCR', 'MSP_TYPE', 'PROVEN_MANUFACTURER'
                ]
            )
            return new_row
        except (ValueError, TypeError) as e:
            self.logger.error('Unable to parse row with spark_id=%s. Reason: %s', spark_id, e)
            raise ParseError


            